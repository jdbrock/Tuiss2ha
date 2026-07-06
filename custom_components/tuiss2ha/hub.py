"""Tuiss Smartview and Blinds2go BLE Home."""

from __future__ import annotations

import asyncio
import logging
import datetime
import uuid

from bleak.backends.characteristic import BleakGATTCharacteristic
from bleak.exc import BleakError
from bleak_retry_connector import (
    BLEAK_RETRY_EXCEPTIONS,
    BleakClientWithServiceCache,
    establish_connection,
)

from homeassistant.components import bluetooth
from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.exceptions import HomeAssistantError

from .const import (
    DOMAIN,
    BLIND_NOTIFY_CHARACTERISTIC,
    TRAVERSAL_UPDATE_THRESHOLD,
    UUID,
    CONNECTION_MESSAGE,
    INITIALIZATION_MESSAGE,
    DEFAULT_RESTART_ATTEMPTS,
    DeviceNotFound,
    ConnectionTimeout,
    NoConnectableBluetoothAdapter,
    TIMEOUT_SECONDS,
    CONNECT_BUDGET_SECONDS,
    PER_ATTEMPT_CONNECT_TIMEOUT,
    DISCONNECT_TIMEOUT_SECONDS,
    KEEP_AWAKE_HOSTS,
    KEEP_AWAKE_RETRY_SECONDS,
    KEEP_AWAKE_HOLD_POLL_SECONDS,
)

_LOGGER = logging.getLogger(__name__)


class Hub:
    """Tuiss BLE hub."""

    manufacturer = "Tuiss Smartview"

    def __init__(self, hass: HomeAssistant, host: str, name: str) -> None:
        """Init dummy hub."""
        self._host = host
        self._hass = hass
        self._name = name
        self._id = host
        self.blinds = [TuissBlind(self._host, self._name, self)]

    @property
    def hub_id(self) -> str:
        """ID for dummy hub."""
        return self._id

    @property
    def host(self) -> str:
        """Return the host address."""
        return self._host

    @property
    def name(self) -> str:
        """Return the hub name."""
        return self._name


class TuissBlind:
    """Tuiss Blind object."""

    def __init__(self, host: str, name: str, hub: Hub) -> None:
        """Init tuiss blind."""
        self._id = host  # also the host address
        self.host = host
        self.name = name
        self.hub = hub
        self._ble_device = bluetooth.async_ble_device_from_address(
            self.hub._hass, self.host, connectable=True
        )
        if self._ble_device is None:
            self._ble_device = bluetooth.async_ble_device_from_address(
                self.hub._hass, self.host, connectable=False
            )
        self.model = self._ble_device.name if self._ble_device else None
        self._rssi: int | None = None
        self._client: BleakClientWithServiceCache | None = None
        self._callbacks = set()
        self._battery_status = False
        self._moving = 0
        self._is_stopping = False
        self._stopped_event = asyncio.Event()
        self._current_cover_position: float | None = None
        self._desired_position: int | None = None
        self._desired_orientation = False
        self._restart_attempts: int | None = None
        self._position_on_restart: bool | None = None
        self._blind_speed: str | None = None
        self._locked = False
        self._attr_traversal_speed: float | None = None
        self._last_connection_error: str | None = None  # For logging when connection fails
        # Battery check configuration
        self._battery_check_days: int = 0
        self._last_battery_check: datetime.datetime | None = None
        self.timers = {}
        self._store = Store(self.hub._hass, 1, f"tuiss2ha_{self.host.replace(':', '').lower()}_schedules")
        self._limits_heartbeat_task: asyncio.Task | None = None
        # Serializes connection establishment so keep-awake and move commands never try to
        # connect at the same time (double-connect clashes were causing ESP_GATT_CONN_CANCEL).
        self._conn_lock = asyncio.Lock()
        # Keep-awake mode: HOLD the connection open for chosen blinds (see KEEP_AWAKE_HOSTS).
        self._keep_awake = self.host in KEEP_AWAKE_HOSTS
        self._keep_awake_task: asyncio.Task | None = None
        # Notify subscription is created ONCE per connection and reused (the app does the same).
        # Re-subscribing on a held keep-awake connection hangs, so every operation just sets
        # _response_handler and calls _ensure_notify(); responses are dispatched to the handler.
        self._notify_active = False
        self._response_handler = None


    @property
    def blind_id(self) -> str:
        """Return ID for blind."""
        return self._id

    @property
    def rssi(self) -> int | None:
        """Return the rssi for the blind."""
        return self._rssi

    def set_rssi(self, rssi: int) -> None:
        """Update the RSSI for the blind."""
        if self._rssi == rssi:
            return
        self._rssi = rssi
        self.publish_updates()

    def publish_updates(self) -> None:
        """Schedule call all registered callbacks."""
        for callback in self._callbacks:
            self.hub._hass.loop.call_soon(callback)

    def register_callback(self, callback) -> None:
        """Register callback, called when blind changes state."""
        self._callbacks.add(callback)

    def remove_callback(self, callback) -> None:
        """Remove previously registered callback."""
        self._callbacks.discard(callback)
        

    ##################################################################################################
    ## CONNECTION METHODS ############################################################################
    ##################################################################################################

    # Attempt Connections
    async def attempt_connection(self):
        """Attempt to connect to the blind."""

        #Set restart attempts if not set in options
        rediscover_attempts = 0
        _LOGGER.debug("%s: Number of attempts: %s", self.name, self._restart_attempts)
        _LOGGER.debug("%s: Startup position check: %s",self.name, self._position_on_restart)
        if self._restart_attempts is None:
            self._restart_attempts = DEFAULT_RESTART_ATTEMPTS

        # check if the device not loaded at boot and retry a connection
        while self._ble_device is None and rediscover_attempts < self._restart_attempts:
            _LOGGER.debug("Unable to find device %s, attempting rediscovery", self.name)
            self._ble_device = bluetooth.async_ble_device_from_address(
                self.hub._hass, self.host, connectable=True
            )
            if self._ble_device is None:
                self._ble_device = bluetooth.async_ble_device_from_address(
                    self.hub._hass, self.host, connectable=False
                )
            rediscover_attempts += 1
            if self._ble_device is None and rediscover_attempts < self._restart_attempts:
                await asyncio.sleep(2)
        if self._ble_device is None:
            _LOGGER.error(
                "Cannot find the device %s. Check your bluetooth adapters and proxies",
                self.name,
            )
            raise DeviceNotFound(
                f"{self.name}: Cannot find the device. Check your bluetooth adapters and proxies"
            )

        # Retry within a bounded wall-clock budget. Each attempt re-fetches the freshest
        # connectable BLEDevice (inside connect()) so retries re-home to whichever proxy
        # hears the blind best right now - important for these sleepy peripherals whose
        # best proxy changes between adverts. Bounded so HA never hangs for minutes.
        loop = self.hub._hass.loop
        deadline = loop.time() + CONNECT_BUDGET_SECONDS
        attempt = 0
        backoff = 1.0
        while True:
            attempt += 1
            _LOGGER.debug(
                "%s %s: Attempting connection (attempt %d, %.0fs left of %ds budget)",
                self.name,
                self._ble_device,
                attempt,
                max(0.0, deadline - loop.time()),
                CONNECT_BUDGET_SECONDS,
            )
            # Hard-cap each attempt so one blocked establish_connection can't blow the
            # whole budget. On timeout we drop any half-open client and re-home next loop.
            per_attempt = min(PER_ATTEMPT_CONNECT_TIMEOUT, max(1.0, deadline - loop.time()))
            try:
                await asyncio.wait_for(self.connect(), timeout=per_attempt)
            except asyncio.TimeoutError:
                _LOGGER.debug(
                    "%s: Connection attempt %d exceeded %.0fs; re-homing", self.name, attempt, per_attempt
                )
                self._last_connection_error = f"connect attempt timed out after {per_attempt:.0f}s"
                self._client = None

            # If the client is connected, return early
            if self._client and self._client.is_connected:
                _LOGGER.debug("%s: Connected after %d attempt(s)", self.name, attempt)
                return

            if loop.time() >= deadline:
                break
            await asyncio.sleep(min(backoff, max(0.1, deadline - loop.time())))
            backoff = min(backoff * 1.5, 5.0)

        # Budget exhausted without connecting - log the actual error at ERROR so it's visible
        last_err = self._last_connection_error or "unknown (no error captured)"
        _LOGGER.error(
            "%s: Connection failed after %d attempt(s) within %ds budget. Last error: %s",
            self.name,
            attempt,
            CONNECT_BUDGET_SECONDS,
            last_err,
        )
        # Give a clear error when user has only passive Bluetooth (e.g. Shelly)
        if last_err and (
            "passive-only" in last_err.lower()
            or "no connectable bluetooth" in last_err.lower()
        ):
            raise NoConnectableBluetoothAdapter(
                "No connectable Bluetooth adapter. Shelly and similar devices are passive-only. "
                "You need an ESPHome Bluetooth proxy or a USB Bluetooth adapter to control Tuiss blinds."
            )
        raise ConnectionTimeout(f"{self.name}: Connection failed within {CONNECT_BUDGET_SECONDS}s budget")

    # Connect
    async def connect(self):
        """Connect to the blind.

        Re-fetches the freshest connectable BLEDevice so establish_connection (and each of
        its internal retries) routes through whichever proxy currently hears the blind
        best, instead of pinning a possibly-stale device/proxy.
        """
        def _fresh_device():
            return (
                bluetooth.async_ble_device_from_address(
                    self.hub._hass, self.host, connectable=True
                )
                or self._ble_device
            )

        device = _fresh_device()
        if device is None:
            self._last_connection_error = "no connectable BLEDevice available"
            _LOGGER.debug("%s: No connectable BLEDevice to connect to", self.name)
            return
        self._ble_device = device
        try:
            client: BleakClientWithServiceCache = await establish_connection(
                client_class=BleakClientWithServiceCache,
                device=device,
                name=self.host,
                use_services_cache=True,
                max_attempts=1,
                ble_device_callback=_fresh_device,
            )
            self._client = client
            # Fresh connection has no notify subscription yet — force _ensure_notify() to
            # re-subscribe on this new client (a drop+reconnect that bypassed disconnect()
            # would otherwise leave _notify_active stale-True and silently skip subscribing).
            self._notify_active = False
            # send the maintain connection message
            await self._client.write_gatt_char(UUID, bytes.fromhex(CONNECTION_MESSAGE))

            # send the connection timestamp message
            await self.send_timestamp()
    
            _LOGGER.debug(
                "%s: Connected. Current Position: %s. Current Moving: %s",
                self.name,
                self._current_cover_position,
                self._moving,
            )
        except (BleakError, asyncio.TimeoutError) as e:
            self._last_connection_error = str(e)
            _LOGGER.debug("Failed to connect to blind: %s", e)
        except Exception as e:
            self._last_connection_error = f"{type(e).__name__}: {e}"
            _LOGGER.debug("%s: Unexpected error during connect: %s", self.name, e)

    # Disconnect
    async def disconnect(self):
        """Disconnect from the blind."""

        if self._limits_heartbeat_task:
            self._limits_heartbeat_task.cancel()
            self._limits_heartbeat_task = None

        client = self._client
        if not client:
            _LOGGER.debug("%s: Already disconnected", self.name)
            self._stopped_event.set()
            return
        _LOGGER.debug("%s: Disconnecting", self.name)
        try:
            await asyncio.wait_for(
                self._teardown_client(client), timeout=DISCONNECT_TIMEOUT_SECONDS
            )
            _LOGGER.debug("%s: Disconnect completed successfully", self.name)
        except asyncio.TimeoutError:
            _LOGGER.warning(
                "%s: Disconnect timed out after %ss; dropping client reference",
                self.name,
                DISCONNECT_TIMEOUT_SECONDS,
            )
        except BLEAK_RETRY_EXCEPTIONS as ex:
            _LOGGER.warning("%s: Error disconnecting: %s", self.name, ex)
        finally:
            # Always clear the client + notify state so ensure_connected()/_ensure_notify()
            # start fresh next time rather than reusing a half-dead client/subscription.
            self._client = None
            self._notify_active = False
            self._response_handler = None
            self._stopped_event.set()

    async def _teardown_client(self, client) -> None:
        """Stop notifications and disconnect a BLE client (helper for disconnect())."""
        try:
            await client.stop_notify(BLIND_NOTIFY_CHARACTERISTIC)
        except Exception as notify_ex:
            # Characteristic might not exist or notifications not started
            _LOGGER.debug("%s: Could not stop notifications: %s", self.name, notify_ex)
        await client.disconnect()

    async def wait_for_stop(self):
        """Wait for the blind to stop moving."""
        self._stopped_event.clear()
        await self._stopped_event.wait()
        
    async def ensure_connected(self) -> None:
        """Ensure the blind is connected before sending a command.

        Serialized via _conn_lock so a keep-awake re-grab and a user move can't both launch a
        connection at once (concurrent connects to these one-connection motors cause
        ESP_GATT_CONN_CANCEL). If another task connected while we waited, return early.
        """
        if self._client and self._client.is_connected:
            return
        async with self._conn_lock:
            if not self._client or not self._client.is_connected:
                await self.attempt_connection()

    async def _hold_or_disconnect(self) -> None:
        """End-of-operation teardown. For keep-awake blinds, HOLD the connection open (keeps
        the motor awake and instantly controllable, like the phone app) instead of dropping it.
        For all other blinds, disconnect exactly as before."""
        if self._keep_awake and self._client and self._client.is_connected:
            _LOGGER.debug("%s: keep-awake — holding connection open", self.name)
            self._stopped_event.set()
            return
        await self.disconnect()

    async def _dispatch_notify(self, sender, data) -> None:
        """Single persistent notify handler; routes each response to the current operation."""
        handler = self._response_handler
        if handler is not None:
            await handler(sender, data)

    async def _ensure_notify(self) -> None:
        """Subscribe to the blind's notify characteristic ONCE per connection and reuse it.

        Re-subscribing on a held (keep-awake) connection was hanging set_position() for 30s;
        instead we subscribe a single dispatcher and each operation swaps _response_handler.
        """
        if self._notify_active and self._client and self._client.is_connected:
            return
        assert self._client is not None
        await asyncio.wait_for(
            self._client.start_notify(BLIND_NOTIFY_CHARACTERISTIC, self._dispatch_notify),
            timeout=10.0,
        )
        self._notify_active = True

    ##################################################################################################
    ## KEEP-AWAKE (PERSISTENT CONNECTION) ###########################################################
    ##################################################################################################

    def start_keep_awake(self) -> None:
        """Start the background loop that grabs and holds the connection for this blind."""
        if not self._keep_awake or self._keep_awake_task:
            return
        _LOGGER.info("%s: starting keep-awake (persistent connection) loop", self.name)
        self._keep_awake_task = self.hub._hass.async_create_task(self.keep_awake_loop())

    def stop_keep_awake(self) -> None:
        """Stop the keep-awake loop (on entity removal / unload)."""
        if self._keep_awake_task:
            self._keep_awake_task.cancel()
            self._keep_awake_task = None

    async def keep_awake_loop(self) -> None:
        """Hold a persistent connection to keep this sleepy motor awake.

        When disconnected, gently try to grab the connection (bounded, serialized). Once held,
        just monitor it — holding the GATT link keeps the motor awake so moves are instant and
        reliable. On drop, re-grab. Gentle retry cadence so an unreachable blind never storms
        the Bluetooth stack.
        """
        # Small initial delay so entity setup / restore finishes first.
        await asyncio.sleep(10)
        while self._keep_awake:
            try:
                if not (self._client and self._client.is_connected):
                    if self._locked:
                        # A move is mid-flight and owns the connection lifecycle; wait.
                        await asyncio.sleep(5)
                        continue
                    try:
                        await self.ensure_connected()
                    except Exception as e:  # noqa: BLE001  (ConnectionTimeout/DeviceNotFound/Bleak)
                        _LOGGER.debug(
                            "%s: keep-awake could not grab connection (%s); retrying in %ss",
                            self.name, e, KEEP_AWAKE_RETRY_SECONDS,
                        )
                        await asyncio.sleep(KEEP_AWAKE_RETRY_SECONDS)
                        continue
                    if self._client and self._client.is_connected:
                        _LOGGER.info("%s: keep-awake grabbed & now HOLDING the connection", self.name)
                # Connected: hold and monitor.
                await asyncio.sleep(KEEP_AWAKE_HOLD_POLL_SECONDS)
            except asyncio.CancelledError:
                break
            except Exception as e:  # noqa: BLE001
                _LOGGER.debug("%s: keep-awake loop error: %s", self.name, e)
                await asyncio.sleep(KEEP_AWAKE_RETRY_SECONDS)

    ##################################################################################################
    ## SET METHODS ###################################################################################
    ##################################################################################################
    async def set_position(self, userPercent) -> None:
        """Set the position of the blind converting from HA to Tuiss first."""

        await self.ensure_connected()

        assert self._client is not None
        self._desired_position = 100 - userPercent
        _LOGGER.debug(
            "%s: Attempting to set position to: %s", self.name, self._desired_position
        )
        command = bytes.fromhex(self.hex_convert(userPercent))
        # Reuse the single persistent notify subscription (no re-subscribe hang on held conn).
        self._response_handler = self.set_position_callback
        await self._ensure_notify()
        await self.send_command(UUID, command)  # send the command

    async def stop(self) -> None:
        """Stop the blind at current position."""
        _LOGGER.debug("%s: Attempting to stop the blind.", self.name)
        command = bytes.fromhex("ff78ea415f0301")

        # skip if the blind is not moving
        if self._moving == 0:
            return

        # try to connect to blind if not connected, shouldnt really be necessary if the blind is already moving
        await self.ensure_connected()

        # send the stop command
        if self._client and self._client.is_connected:
            await self.send_command(UUID, command)
        if self._client and self._client.is_connected:
            await self.get_blind_position()



    async def set_speed(self) -> None:
        """Set the speed for supported blind types"""
        _LOGGER.debug("%s: Attempting to set the blind speed", self.name)
        match self._blind_speed:
            case "Standard":
                command = bytes.fromhex("ff78ea41f202")
            case "Comfort":
                command = bytes.fromhex("ff78ea41f201")
            case "Slow":
                command = bytes.fromhex("ff78ea41f200")


        await self.ensure_connected()
        
        # send the command
        try:
            if self._client and self._client.is_connected:
                await self.send_command(UUID, command)
        except (BleakError, RuntimeError) as e:
            _LOGGER.debug("%s: Unable to set the speed: %s", self.name, e)
            await self.disconnect()
            raise RuntimeError(
                "Unable to set the speed. Check has enough battery and within bluetooth range or that blind supports speed changes"
            ) from e
        finally:
            # Always disconnect after set_speed operation
            await self.disconnect()
        

    ##################################################################################################
    ## GET METHODS ###################################################################################
    ##################################################################################################

    async def get_from_blind(self, command, callback) -> None:
        """Get the battery state from the blind as good or bad."""

        # connect to the blind first
        await self.ensure_connected()

        assert self._client is not None
        # Reuse the single persistent notify subscription and route responses to `callback`.
        self._response_handler = callback
        try:
            await self._ensure_notify()
        except Exception as e:  # noqa: BLE001
            _LOGGER.warning("%s: Could not establish notifications: %s", self.name, e)
            await self.disconnect()
            return

        if self._client and self._client.is_connected:
            try:
                await self.send_command(UUID, command)
            except Exception as e:
                _LOGGER.error("%s: Error sending command during get_from_blind: %s", self.name, e)
                await self.disconnect()
                return

            # Wait for the response/callback to complete with timeout to prevent hanging
            try:
                await asyncio.wait_for(self.wait_for_stop(), timeout=10.0)
            except asyncio.TimeoutError:
                _LOGGER.warning("%s: Timeout waiting for response in get_from_blind", self.name)
            finally:
                await self._hold_or_disconnect()
        else:
            await self.disconnect()
                    

    async def get_battery_status(self) -> None:
        """Get the battery state from the blind as good or bad."""
        command = bytes.fromhex("ff78ea41f00301")
        await self.get_from_blind(command, self.battery_callback)


    async def get_blind_position(self) -> None:
        """Get the current position of the blind."""
        command = bytes.fromhex(INITIALIZATION_MESSAGE)
        await self.get_from_blind(command, self.position_callback)

    ##################################################################################################
    ## LIMIT CONFIGURATION METHODS ##################################################################
    ##################################################################################################

    def limits_heartbeat_start(self, move_command: str) -> None:
        """Start the heartbeat task for limits."""
        self.limits_heartbeat_stop()
        self._limits_heartbeat_task = self.hub._hass.async_create_task(
            self.limits_heartbeat_loop(move_command)
        )


    def limits_heartbeat_stop(self) -> None:
        """Stop the heartbeat task for limits."""
        if self._limits_heartbeat_task:
            self._limits_heartbeat_task.cancel()
            self._limits_heartbeat_task = None


    async def limits_heartbeat_loop(self, move_command_str: str) -> None:
        """Send heartbeat every 4 seconds while moving."""
        heartbeat_command = bytes.fromhex("ff010101010101")
        move_command = bytes.fromhex(move_command_str)
        while True:
            try:
                await asyncio.sleep(2)
                if self._client and self._client.is_connected:
                    await self.send_command(UUID, heartbeat_command)
                    await self.send_command(UUID, move_command)
                else:
                    break
            except asyncio.CancelledError:
                break
            except Exception as e:
                _LOGGER.debug("%s: Moving heartbeat failed: %s", self.name, e)
                break


    async def limits_initialise(self) -> None:
        """Initialise the limit configuration by connecting to the blind."""
        self.limits_heartbeat_stop()
        # Connect to the blind first
        _LOGGER.debug("Starting Limits Config. Attempting Connection")
        await self.ensure_connected()
            
        # Set the initialisation commands
        _LOGGER.debug("Sending initialisation commands")
        await self.send_command(UUID, bytes.fromhex(INITIALIZATION_MESSAGE))
        await self.send_command(UUID, bytes.fromhex("ff78ea41210301"))
    

    async def limits_step_up(self) -> None:
        """Move the blind up incrementally for manual positioning."""
        self.limits_heartbeat_stop()
        # Connect to the blind first
        if not self._client or not self._client.is_connected:
            _LOGGER.debug("Connection lost, limits set up failed")
        
        _LOGGER.debug("Stepping up")
        await self.send_command(UUID, bytes.fromhex("ff78ea41220301"))    
        

    async def limits_step_down(self) -> None:
        """Move the blind down incrementally for manual positioning."""
        self.limits_heartbeat_stop()
        # Connect to the blind first
        if not self._client or not self._client.is_connected:
            _LOGGER.debug("Connection lost, limits set up failed")
        
        _LOGGER.debug("Stepping down")
        await self.send_command(UUID, bytes.fromhex("ff78ea41230301"))


    async def limits_move_up(self) -> None:
        """Move the blind up continuously for manual positioning (stubbed for now)."""
        # Connect to the blind first
        if not self._client or not self._client.is_connected:
            _LOGGER.debug("Connection lost, limits set up failed")
        
        _LOGGER.debug("Moving up")
        move_command = "ff78ea41cf0301"
        await self.send_command(UUID, bytes.fromhex(move_command))
        self.limits_heartbeat_start(move_command)


    async def limits_move_down(self) -> None:
        """Move the blind down continuously for manual positioning (stubbed for now)."""
        # Connect to the blind first
        if not self._client or not self._client.is_connected:
            _LOGGER.debug("Connection lost, limits set up failed")  
        
        _LOGGER.debug("Moving down")
        move_command = "ff78ea411f0301"
        await self.send_command(UUID, bytes.fromhex(move_command))
        self.limits_heartbeat_start(move_command)
        
        
    async def limits_stop(self) -> None:
        """Stop the blind movement."""
        self.limits_heartbeat_stop()
        # Connect to the blind first
        if not self._client or not self._client.is_connected:
            _LOGGER.debug("Connection lost, limits set up failed")  
        
        _LOGGER.debug("Stopping movement")
        await self.send_command(UUID, bytes.fromhex("ff78ea415f0301"))
        
        
    async def limits_set(self) -> None:
        """Sets the limit."""
        self.limits_heartbeat_stop()
        # Connect to the blind first
        if not self._client or not self._client.is_connected:
            _LOGGER.debug("Connection lost, limits set up failed")
        
        _LOGGER.debug("Setting the limit")
        await self.send_command(UUID, bytes.fromhex("ff78ea415f0301"))
        await self.send_command(UUID, bytes.fromhex("ff78ea41410301"))

    ##################################################################################################
    ## TIMER METHODS #################################################################################
    ##################################################################################################

    async def async_load_timers(self) -> None:
        """Load stored schedules."""
        stored = await self._store.async_load()
        if stored:
            self.timers = stored
        else:
            self.timers = {}

    async def async_save_timer(self) -> None:
        """Save schedules to storage."""
        await self._store.async_save(self.timers)


    async def async_add_timer(self, days: list[str], time_str: str, position: float) -> str:
        """Add a new schedule."""
        await self.ensure_connected()   

        new_timer_id = None
        timer_id_event = asyncio.Event()

        async def timer_id_callback(sender, data):
            nonlocal new_timer_id
            decimals = self.split_data(data)
            # Filter for the correct response: 7 bytes long, where the 5th byte is 0xd6 (214)
            if len(decimals) >= 7 and decimals[4] == 214:
                new_timer_id = str(decimals[6])
                timer_id_event.set()

        self._response_handler = timer_id_callback
        await self._ensure_notify()

        await self.send_command(UUID, bytes.fromhex(CONNECTION_MESSAGE))
        await self.send_timestamp()
        await self.send_command(UUID, bytes.fromhex("ff78ea4104"))

        try:
            await asyncio.wait_for(timer_id_event.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            await self.disconnect()
            raise HomeAssistantError("Timeout waiting for timer ID from blind.")

        _LOGGER.debug("Received timer ID from blind: %s", new_timer_id)

        if not new_timer_id:
            await self.disconnect()
            _LOGGER.debug("Failed to obtain timer ID from the blind.")
            raise HomeAssistantError("Failed to obtain timer ID from the blind.")
            
        if int(new_timer_id) >= 17:
            await self.disconnect()
            _LOGGER.debug("Maximum number of timers reached.")
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key="max_timers_reached",
                translation_placeholders={"max_timers": "16"}
            )

        timer_id = new_timer_id
        timer_command = self.create_timer_command(timer_id, days, time_str, position)
        
        await self.send_command(UUID, bytes.fromhex(timer_command))   
        await self.send_command(UUID, bytes.fromhex("ff78ea41f00301"))
        await self.disconnect()       
        
        existing_ha_indices = {t.get("ha_index") for t in self.timers.values() if "ha_index" in t}
        available_indices = set(range(1, 17)) - existing_ha_indices
        ha_index = min(available_indices) if available_indices else len(self.timers) + 1

        self.timers[timer_id] = {
            "timer_id": timer_id,
            "ha_index": ha_index,
            "days": days,
            "time": time_str,
            "position": position
        }
        
        await self.async_save_timer()
        self.publish_updates()
        async_dispatcher_send(self.hub._hass, f"{DOMAIN}_add_timer_{self.blind_id}", timer_id)
        return timer_id
    

    async def async_delete_timer(self, timer_id: str) -> None:
        """Remove an existing schedule."""
        await self.ensure_connected()     
        
        await self.send_command(UUID, bytes.fromhex(CONNECTION_MESSAGE))
        await self.send_timestamp()      
        await self.send_command(UUID, bytes.fromhex(INITIALIZATION_MESSAGE))
        delete_hex = f"ff78ea410301{int(timer_id):02x}" #schedule index in hex, convert from string to int to hex
        await self.send_command(UUID, bytes.fromhex(delete_hex))
        await self.send_command(UUID, bytes.fromhex("ff78ea41f00301"))
        await self.disconnect()
        
        if timer_id in self.timers:
            del self.timers[timer_id]
            await self.async_save_timer()
            async_dispatcher_send(self.hub._hass, f"{DOMAIN}_delete_timer_{self.blind_id}_{timer_id}")
            self.publish_updates()



    async def delete_all_timers(self) -> None:
        """Delete all timers from the blind."""
        _LOGGER.debug("%s: Attempting to delete all timers.", self.name)
        # Connect to the blind first
        await self.ensure_connected()

        await self.send_command(UUID, bytes.fromhex(CONNECTION_MESSAGE))
        await self.send_timestamp()
        await self.send_command(UUID, bytes.fromhex(INITIALIZATION_MESSAGE))
        await self.send_command(UUID, bytes.fromhex("ff04040404")) # reset command
        
        await self.disconnect()
        
        # Reconnect to the blind to ensure it's back online after reset
        await self.attempt_connection()
        await self.send_command(UUID, bytes.fromhex("ff02020202787878787878")) # reactivate blind
        await self.disconnect()
         
        #remove any timer entities
        if self.timers:
            timer_ids = list(self.timers.keys())
            for timer_id in timer_ids:
                async_dispatcher_send(self.hub._hass, f"{DOMAIN}_delete_timer_{self.blind_id}_{timer_id}")
                
            self.timers.clear()
            await self.async_save_timer()
            self.publish_updates()




    def create_timer_command(self, index: str, days: list[str], time: str, position: float) -> str:
        # Convert days to bitmask
        day_map = {"sun": 1, "mon": 2, "tue": 4, "wed": 8, "thu": 16, "fri": 32, "sat": 64}
        day_bits = sum(day_map[day] for day in days if day in day_map)

        # Convert time to minutes since midnight
        time_parts = time.split(":")
        hours = int(time_parts[0])
        minutes = int(time_parts[1])

        # Convert position to fixed-point (e.g., multiply by 10)
        target_position_value = int(float(position) * 10)
        position_byte_1 = target_position_value % 256
        position_byte_2 = target_position_value // 256
        

        # Construct the command (example format)
        cmd_hex = "ff78ea410300"
        cmd_hex += f"{int(index):02x}"   # Timer index converted to hex
        cmd_hex += "b2"   # not sure
        cmd_hex += "3f"   # not sure
        cmd_hex += f"{day_bits:02x}" # Days bitmask
        cmd_hex += f"{hours:02x}" # Time hours
        cmd_hex += f"{minutes:02x}" # Time minutes
        cmd_hex += "00"  # Padding
        cmd_hex += f"{position_byte_1:02x}" # Position byte
        cmd_hex += f"{position_byte_2:02x}" # Position byte

        return cmd_hex




    ##################################################################################################
    ## CALLBACK METHODS ##############################################################################
    ##################################################################################################

    async def battery_callback(self, sender: BleakGATTCharacteristic, data: bytearray):
        """Wait for response from the blind and updates entity status."""
        _LOGGER.debug("%s: Attempting to get battery status from response %s", self.name, data.hex())

        decimals = self.split_data(data)

        if decimals[4] == 210:
            if len(decimals) == 7 or decimals[5] >= 10:
                _LOGGER.debug(
                    "%s: Please charge device", self.name
                )  # think its based on the length of the response? ff010203d2 (bad) vs ff010203d202e803 (good)
                self._battery_status = True
            elif decimals[5] < 10:
                _LOGGER.debug("%s: Battery is good", self.name)
                self._battery_status = False
            else:
                _LOGGER.debug("%s: Battery logic is wrong", self.name)
                self._battery_status = None
            # Record time of this battery check
            try:
                self._last_battery_check = datetime.datetime.now()
            except Exception:
                self._last_battery_check = None
            self._stopped_event.set()

    async def position_callback(self, sender: BleakGATTCharacteristic, data: bytearray):
        """Wait for response from the blind and updates entity status."""
        _LOGGER.debug("%s: Attempting to get position", self.name)

        decimals = self.split_data(data)

        blindPos = (decimals[7] + (256 * decimals[8])) / 10
        _LOGGER.debug("%s: Blind position is %s", self.name, blindPos)
        self._current_cover_position = blindPos
        self._moving = 0
        self._stopped_event.set()

    async def set_position_callback(
        self, sender: BleakGATTCharacteristic, data: bytearray
    ):
        """Handle response from the blind during movement. Keeps connection alive until target is reached."""
        decimals = self.split_data(data)
        _LOGGER.debug(
            "%s: Received response during movement: %s", self.name, decimals
        )
        if len(decimals) >= 9 and decimals[4] == 210:
            blindPos = decimals[6]
            self._current_cover_position = blindPos
            self.publish_updates()
            
            if self._desired_position is not None and abs(blindPos - self._desired_position) <= 2:
                _LOGGER.debug("%s: Reached desired position. Stopping wait.", self.name)
                self._stopped_event.set()

    ##################################################################################################
    ## DATA METHODS ############################################################################
    ##################################################################################################

    # Send the data
    async def send_command(self, UUID, command):
        """Send the command to the blind."""
        if self._client and self._client.is_connected:
            _LOGGER.debug(
                "%s (%s) connected state is %s",
                self.name,
                self._ble_device,
                self._client.is_connected,
            )
            try:
                _LOGGER.debug("%s: Sending the command %s", self.name, command.hex())
                # Bound the GATT write so a flapping proxy can't hang it for 30s; the move's
                # error path then unsticks quickly instead of stalling.
                await asyncio.wait_for(
                    self._client.write_gatt_char(UUID, command), timeout=12.0
                )
            except (BleakError, asyncio.TimeoutError) as e:
                _LOGGER.error("%s: Send Command error: %s", self.name, e)
                raise RuntimeError(e) from e

    async def send_timestamp(self) -> None:
        """Send the current timestamp command to the blind."""
        now = datetime.datetime.now()
        timestamp_command = f"ff78ea410200{now.year - 2000:02x}{now.month:02x}{now.day:02x}{now.hour:02x}{now.minute:02x}{now.second:02x}"
        await self.send_command(UUID, bytes.fromhex(timestamp_command))

    # Creates the % open/closed hex command
    def hex_convert(self, user_percent: float) -> str:
        """Convert the Home Assistant position percentage (0-100) to the Tuiss hex command."""
        # Tuiss uses an inverted percentage (0=open, 100=closed)
        tuiss_percent = 100 - user_percent

        # Calculate the absolute position value (0-1000)
        total_val = int(round(tuiss_percent * 10))

        # Extract lower byte (position) and upper byte (group)
        position_value = total_val % 256
        group_value = total_val // 256

        # Format the position value as a two-character hex (e.g., 0A, FF)
        hex_val = f"{position_value:02x}"
        group_str = f"{group_value:02x}"

        # Build the final command
        command_prefix = "ff78ea41bf03"
        return f"{command_prefix}{hex_val}{group_str}"

    def split_data(self, data: bytearray) -> list[int]:
        """Convert the byte response into a list of decimals."""
        decimals = list(data)
        _LOGGER.debug("%s: Received data decimals: %s", self.name, decimals)
        return decimals

    
    async def async_move_cover(
        self,
        movement_direction,
        target_position,
        skip_battery_check=False
    ):
        """Move the cover."""
        _LOGGER.debug("%s: Entering async_move_cover. Locked: %s", self.name, self._locked)
        if not self._locked:
            await self.ensure_connected()
            if self._client and self._client.is_connected:
                self._locked = True
                _LOGGER.debug("%s: Lock acquired.", self.name)
                self._is_stopping = False
                start_position = self._current_cover_position
                corrected_target_position = 100 - target_position
                self._moving = movement_direction

                # Update the state and trigger the moving
                self.publish_updates()
                
                _LOGGER.debug(
                            "%s: Battery check age (%s days). Last check: %s.",
                            self.name,
                            self._battery_check_days,
                            self._last_battery_check,
                        )
                
                # Perform a battery check before moving if configured
                try:
                    if not skip_battery_check and self._battery_check_days and (
                        self._last_battery_check is None
                        or (
                            (datetime.datetime.now() - self._last_battery_check).total_seconds()
                            / 86400
                        )
                        > float(self._battery_check_days)
                    ):
                        _LOGGER.debug(
                            "%s: Battery check age exceeded (%s days). Checking battery.",
                            self.name,
                            self._battery_check_days,
                        )
                        # It's OK if this fails — we still proceed with the movement
                        try:
                            await self.get_battery_status()
                        except Exception as e:
                            _LOGGER.debug("%s: Battery check failed: %s", self.name, e)
                except Exception:
                    # Defensive: don't let battery-check logic break movement
                    _LOGGER.debug("%s: Error while evaluating battery check timing", self.name)
                
                try:
                    # Timeout on set_position to prevent hanging indefinitely
                    await asyncio.wait_for(self.set_position(target_position), timeout=30.0)
                except asyncio.TimeoutError:
                    _LOGGER.error("%s: set_position() timed out after 30s. Unsticking blind.", self.name)
                    self._moving = 0
                    self._locked = False
                    self.publish_updates()
                    await self.disconnect()
                    return
                except Exception as e:
                    _LOGGER.error("%s: Failed to send move command: %s. Unsticking blind.", self.name, e)
                    # Command failed; unstick the blind immediately
                    self._moving = 0
                    self._locked = False
                    self.publish_updates()
                    await self.disconnect()
                    return
                
                start_time = datetime.datetime.now()

                # The state already shows opening/closing (self._moving set above). During the
                # physical move the ESP32 shares its one radio between WiFi and BLE, so live
                # position notifications often don't get through - and that's fine. We wait
                # roughly the travel time (a "reached" notification, if it makes it, just ends
                # the wait early), then read the TRUE final position once the radio is free.
                # Joe's model: a reliable command + one truthful update when the move is done.
                sp = start_position if start_position is not None else 0
                distance = abs(corrected_target_position - sp)
                if self._attr_traversal_speed is not None and 1 <= self._attr_traversal_speed < 6:
                    travel_time = (distance * 1.2) / self._attr_traversal_speed + 6
                else:
                    travel_time = (distance / 100.0) * 45 + 6  # assume ~45s for a full travel
                travel_time = min(max(travel_time, 6.0), float(TIMEOUT_SECONDS or 120))

                try:
                    _LOGGER.debug(
                        "%s: Move command sent; waiting up to %.0fs for travel.",
                        self.name, travel_time,
                    )
                    try:
                        await asyncio.wait_for(self.wait_for_stop(), timeout=travel_time)
                        _LOGGER.debug("%s: Move confirmed early by a live notification.", self.name)
                    except asyncio.TimeoutError:
                        _LOGGER.debug(
                            "%s: Travel time elapsed with no live notification (normal for "
                            "ESP32 WiFi/BLE coexistence); reading real position.", self.name,
                        )

                    self._moving = 0
                    self.publish_updates()

                    if not self._is_stopping:
                        # Calibrate travel speed for better future estimates.
                        self.update_traversal_speed(
                            corrected_target_position, sp, start_time, datetime.datetime.now()
                        )
                        # Read and report the TRUE final position (the "update once it's done").
                        try:
                            await self.get_blind_position()
                            _LOGGER.debug(
                                "%s: Move complete. Real position: %s",
                                self.name, self._current_cover_position,
                            )
                        except Exception as e:  # noqa: BLE001
                            _LOGGER.debug(
                                "%s: Post-move position read failed (%s); using target estimate.",
                                self.name, e,
                            )
                            self.set_final_state(corrected_target_position)
                finally:
                    # Hold the connection for keep-awake blinds, otherwise disconnect as usual.
                    await self._hold_or_disconnect()
                    self._locked = False
                    _LOGGER.debug("%s: Lock released in async_move_cover.", self.name)

        elif self._locked:
            _LOGGER.debug(
                "%s is locked, please wait for currrent command to complete and then try again.",
                self.name,
            )
            # Use translation placeholder so the frontend can localise the message
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key="device_locked",
                translation_placeholders={
                    "name": self.name,
                })

    def update_traversal_speed(self, target_position, start_position, start_time, end_time):
        """Update the traversal speed."""
        time_taken = (end_time - start_time).total_seconds()
        traversal_distance = abs(target_position - start_position)
        # Only update traversal speed if the blind has moved a significant distance to avoid skewing from small movements or noise
        if traversal_distance > TRAVERSAL_UPDATE_THRESHOLD:
            self._attr_traversal_speed = traversal_distance / time_taken
            _LOGGER.debug(
                "%s: Time Taken: %s. Start Pos: %s. End Pos: %s. Distance Travelled: %s. Traversal Speed: %s",
                self.name,
                time_taken,
                start_position,
                target_position,
                traversal_distance,
                self._attr_traversal_speed,
            )
        
    def set_final_state(self, position):
        """Set the final state of the blind after a move."""
        self._current_cover_position = position
        self._moving = 0
        self.publish_updates()
