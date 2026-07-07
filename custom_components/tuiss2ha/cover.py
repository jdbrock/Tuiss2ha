"""Platform for cover integration."""

from __future__ import annotations

import asyncio
import logging
import voluptuous as vol
import datetime


from typing import Any

from homeassistant.components.cover import (
    ATTR_CURRENT_POSITION,
    ATTR_POSITION,
    CoverDeviceClass,
    CoverEntity,
    CoverEntityFeature,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import STATE_CLOSED, STATE_OPEN, STATE_OPENING, STATE_CLOSING
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import entity_platform, config_validation as cv
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity
from homeassistant.helpers.device_registry import (
    CONNECTION_BLUETOOTH,
    DeviceInfo,
    format_mac,
)
from homeassistant.exceptions import HomeAssistantError


from .const import (
    DOMAIN,
    OPT_RESTART_ATTEMPTS,
    OPT_RESTART_POSITION,
    BLIND_SPEED_LIST,
    OPT_BLIND_SPEED,
    SPEED_CONTROL_SUPPORTED_MODELS,
    OPT_FAVORITE_POSITION,
    DEFAULT_FAVORITE_POSITION,
    OPT_BATTERY_CHECK_DAYS,
    DEFAULT_BATTERY_CHECK_DAYS,
    ConnectionTimeout,
    DeviceNotFound,
    MoveAborted,
    CONFIRM_REREAD_DELAYS,
)
from .hub import TuissBlind

_LOGGER = logging.getLogger(__name__)


ATTR_TRAVERSAL_SPEED = "traversal_speed"
ATTR_MAC_ADDRESS = "mac_address"

# Move blinds this many at a time in the simultaneous-positioning service, staggering each batch's
# connect burst. Firing all 13 connects at once — OR connecting all then moving — stalls at ~5 blinds:
# holding idle BLE connections open blocks the BT-proxy dongles from establishing more (each blind
# connects fine on its own). Small move-and-release batches land them all.
SIMULTANEOUS_CONNECT_BATCH = 4
# Seconds between batches — long enough for a batch to connect and start moving (connections active,
# not idle-blocking) before the next batch's connect burst. ~matches the manual wave that landed 13/13.
SIMULTANEOUS_BATCH_STAGGER = 20

GET_BLIND_POSITION_SCHEMA = cv.make_entity_service_schema({})
SET_BLIND_POSITION_SCHEMA = cv.make_entity_service_schema(
    {vol.Required("position"): vol.All(vol.Coerce(float), vol.Range(min=0, max=100))}
)
SET_BLIND_SPEED_SCHEMA = cv.make_entity_service_schema(
    {vol.Required("speed"): vol.In(BLIND_SPEED_LIST)}
)
SIMULTANEOUS_BLIND_POSITIONING_SCHEMA = vol.Schema(
    {
        vol.Required("entity_ids"): cv.entity_ids,
        vol.Optional("favourite"): bool,
        vol.Optional("position"): vol.All(vol.Coerce(float), vol.Range(min=0, max=100)),
    }
)
ADD_BLIND_TIMER_SCHEMA = cv.make_entity_service_schema(
    {
        vol.Required("position"): vol.All(vol.Coerce(float), vol.Range(min=0, max=100)),
        vol.Required("days"): vol.All(cv.ensure_list, [vol.In(["mon", "tue", "wed", "thu", "fri", "sat", "sun"])]),
        vol.Required("time"): cv.time,
    }
)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Add cover for passed config_entry in HA."""
    hub = hass.data[DOMAIN][config_entry.entry_id]
    blinds = [Tuiss(blind, config_entry) for blind in hub.blinds]
    async_add_entities(blinds)

    # Store entities in hass.data[DOMAIN] for easy retrieval by entity_id
    if DOMAIN not in hass.data:
        hass.data[DOMAIN] = {}
    if "entities" not in hass.data[DOMAIN]:
        hass.data[DOMAIN]["entities"] = {}
    for blind_entity in blinds:
        hass.data[DOMAIN]["entities"][blind_entity.entity_id] = blind_entity

    platform = entity_platform.async_get_current_platform()
    platform.async_register_entity_service(
        "get_blind_position", GET_BLIND_POSITION_SCHEMA, async_action_get_blind_position
    )

    platform.async_register_entity_service(
        "set_blind_position",
        SET_BLIND_POSITION_SCHEMA,
        async_action_set_blind_position,
    )
    
    platform.async_register_entity_service(
        "add_blind_timer",
        ADD_BLIND_TIMER_SCHEMA,
        async_action_add_timer,
    )
    
    # Register the set_speed service only for supported models
    for blind_entity in blinds:
        if blind_entity._blind.model in SPEED_CONTROL_SUPPORTED_MODELS:
            _LOGGER.debug("Adding blind speed service for %s, model %s",blind_entity._blind.name, blind_entity._blind.model)
            platform.async_register_entity_service(
                "set_blind_speed", SET_BLIND_SPEED_SCHEMA, async_action_set_blind_speed
            )

    
    async def handle_force_unlock(call):
        """Handle the force unlock service call."""
        entity_ids = call.data.get("entity_id")
        if not entity_ids:
            _LOGGER.error("No entity_id provided for force unlock")
            return

        # Handle both single string and list of entity_ids
        if isinstance(entity_ids, str):
            entity_ids = [entity_ids]

        for entity_id in entity_ids:
            entity = hass.data[DOMAIN]["entities"].get(entity_id)
            if not entity:
                _LOGGER.error("Entity %s not found for force unlock", entity_id)
                continue
            entity._blind._locked = False
            _LOGGER.info("Force unlocked blind %s", entity_id)

    # Register our service with Home Assistant.
    hass.services.async_register(DOMAIN, "force_unlock", handle_force_unlock)




    # Register the new parallel blind position service as a domain service
    async def async_action_simultaneous_blind_positioning(service_call: ServiceCall) -> None:
        """Set the position of multiple blinds simultaneously."""
        hass = service_call.hass
        entity_ids = service_call.data["entity_ids"]
        position = service_call.data.get("position")
        favourite = service_call.data.get("favourite", False)

        # Validate inputs
        if not favourite and position is None:
            _LOGGER.error("Position is required when 'favourite' is False.")
            return

        target_entities = []
        for entity_id in entity_ids:
            entity = hass.data[DOMAIN]["entities"].get(entity_id)
            if entity:
                target_entities.append(entity)
            else:
                _LOGGER.warning(
                    "Entity %s not found for parallel blind position setting.", entity_id
                )

        if not target_entities:
            _LOGGER.error("No valid entities found for parallel blind position setting.")
            return

        # Move in small STAGGERED batches. Holding idle BLE connections open blocks the BT-proxy
        # dongles from establishing more — an all-at-once fire, OR connecting all then moving, stalls
        # at ~5 of 13 even though each blind connects fine alone. Instead each move connects, travels,
        # then disconnects; we fire a batch, wait for it to get connecting/moving (so its connections
        # are active rather than idle-blocking), then fire the next. Moves run concurrently, so blinds
        # travel together within a batch and largely overlap across batches. Proven: lands all 13
        # where all-at-once lands 5. (Each move handles its own connection + retry/failure.)
        move_tasks: list[tuple[Tuiss, asyncio.Task]] = []
        total = len(target_entities)
        for i in range(0, total, SIMULTANEOUS_CONNECT_BATCH):
            batch = target_entities[i:i + SIMULTANEOUS_CONNECT_BATCH]
            for entity in batch:
                if favourite:
                    tgt = entity.config_entry.options.get(OPT_FAVORITE_POSITION, DEFAULT_FAVORITE_POSITION)
                else:
                    tgt = position
                move_tasks.append((
                    entity,
                    asyncio.create_task(
                        entity.async_set_cover_position(**{ATTR_POSITION: tgt, "skip_battery_check": True})
                    ),
                ))
            # Stagger the next batch's connect burst (not after the last batch).
            if i + SIMULTANEOUS_CONNECT_BATCH < total:
                await asyncio.sleep(SIMULTANEOUS_BATCH_STAGGER)

        for entity, task in move_tasks:
            try:
                await task
            except Exception as e:  # noqa: BLE001
                _LOGGER.warning("Failed to set position for %s: %s", entity.entity_id, e)

    hass.services.async_register(
        DOMAIN,
        "simultaneous_blind_positioning",
        async_action_simultaneous_blind_positioning,
        schema=SIMULTANEOUS_BLIND_POSITIONING_SCHEMA,
    )


async def async_action_get_blind_position(entity, service_call):
    """Get the blind position when called by service."""
    await entity._blind.get_blind_position()
    entity.schedule_update_ha_state()


async def async_action_set_blind_position(entity, service_call):
    """Set the blind position with decimal precision."""
    position = service_call.data["position"]
    await entity.async_set_cover_position(**{ATTR_POSITION: position})


async def async_action_add_timer(entity, service_call):
    """Add a timer to the blind."""
    position = service_call.data["position"]
    days = service_call.data["days"]
    time = str(service_call.data["time"])
    
    await entity._blind.async_add_timer(days, time, position)

async def async_action_set_blind_speed(entity, service_call):
    """Set the blind speed."""
    speed = service_call.data["speed"]
    entity._blind._blind_speed = speed
    await entity._blind.set_speed()

    # Update the config entry with the new speed
    new_data = entity.config_entry.data.copy()
    new_options = entity.config_entry.options.copy()
    new_options[OPT_BLIND_SPEED] = speed
    entity.hass.config_entries.async_update_entry(
        entity.config_entry, data=new_data, options=new_options
    )


class Tuiss(CoverEntity, RestoreEntity):
    """Create Cover Class."""

    def __init__(self, blind: TuissBlind, config: ConfigEntry) -> None:
        """Initialize the cover."""
        self._blind = blind
        self.config_entry = config
        self._attr_unique_id = f"{self._blind.blind_id}_cover"
        self._attr_name = self._blind.name
        self._state = None
        self._start_time: datetime.datetime | None = None
        self._end_time: datetime.datetime | None = None
        self._attr_mac_address = self._blind.host
        self._blind._restart_attempts = config.options.get(OPT_RESTART_ATTEMPTS)
        self._blind._position_on_restart = config.options.get(OPT_RESTART_POSITION)
        # Number of days between automatic battery checks when blinds move (0 = disabled)
        self._blind._battery_check_days = config.options.get(
            OPT_BATTERY_CHECK_DAYS, DEFAULT_BATTERY_CHECK_DAYS
        )

    @property
    def state(self):
        """Open/closed/opening/closing — derived from the SNAPPED position so the status agrees
        with the reported percentage, and None-safe so an unknown position can't crash setup."""
        if self._blind._moving > 0:
            self._state = STATE_OPENING
        elif self._blind._moving < 0:
            self._state = STATE_CLOSING
        else:
            pos = self.current_cover_position          # snapped (99 -> 100)
            if pos is None:
                self._state = None
            else:
                self._state = STATE_CLOSED if pos == 0 else STATE_OPEN
        return self._state
        
    @property
    def should_poll(self):
        """Set poll of object."""
        return False

    @property
    def device_class(self):
        """Set class of object."""
        return CoverDeviceClass.SHADE

    @property
    def available(self) -> bool:
        """Return True if blind and hub is available."""
        return True

    @property
    def device_info(self) -> DeviceInfo:
        """Information about this entity/device."""
        return DeviceInfo(
            identifiers={(DOMAIN, self._blind.blind_id)},
            name=self.name,
            model=self._blind.model,
            manufacturer=self._blind.hub.manufacturer,
            connections={
                (CONNECTION_BLUETOOTH, format_mac(self._blind.host)),
            },
        )

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Attributes for the traversal time of the blinds."""
        return {
            ATTR_TRAVERSAL_SPEED: self._blind._attr_traversal_speed,
            ATTR_MAC_ADDRESS: self._attr_mac_address,
            "timers": list(self._blind.timers.values()),
        }

    @property
    def current_cover_position(self) -> int | None:
        """Return the current position of the cover."""
        if self._blind._current_cover_position is None:
            return None
        pos = int(self._blind._current_cover_position)
        # These blinds calibrate the top of travel a hair off and report 99 when fully open;
        # snap it so HA shows a clean 100 fully-open state. (Closed end left alone: upstream
        # treats position 1 as a valid open state.)
        if pos >= 99:
            return 100
        return pos

    @property
    def is_closed(self) -> bool | None:
        """Closed only when fully closed — derived from the SNAPPED position, None-safe."""
        pos = self.current_cover_position
        if pos is None:
            return None
        return pos == 0

    @property
    def supported_features(self) -> CoverEntityFeature:
        """Set features of object."""
        return (
            CoverEntityFeature.OPEN
            | CoverEntityFeature.CLOSE
            | CoverEntityFeature.SET_POSITION
            | CoverEntityFeature.STOP
        )


    async def async_scheduled_update_request(self, *_):
        """Request a state update from the blind at a scheduled point in time."""
        self.async_write_ha_state()

    def update_state(self):
        """Update the state of the blind."""
        self.schedule_update_ha_state()


    async def async_added_to_hass(self) -> None:
        """Run when this Entity has been added to HA."""
        # Restore the last known state
        last_state = await self.async_get_last_state()
        if not last_state or last_state.attributes.get(ATTR_CURRENT_POSITION) is None:
            self._blind._current_cover_position = 0
        else:
            self._blind._current_cover_position = float(
                last_state.attributes.get(ATTR_CURRENT_POSITION)
            )
        if last_state and last_state.attributes.get(ATTR_TRAVERSAL_SPEED) is not None:
            self._blind._attr_traversal_speed = last_state.attributes.get(ATTR_TRAVERSAL_SPEED)

        self._blind.register_callback(self.update_state)

        # For keep-awake blinds, start the loop that grabs and HOLDS the BLE connection so the
        # sleepy motor stays awake and instantly controllable (scoped by MAC in const.py).
        self._blind.start_keep_awake()


    async def async_will_remove_from_hass(self) -> None:
        """Entity being removed from hass."""
        self._blind.stop_keep_awake()
        self._blind.remove_callback(self.update_state)


    async def _preempted(self, seconds: float = 0.0) -> bool:
        """True if a newer command has preempted this move (so the retry/re-read loop yields instead
        of chasing the old target). Optionally waits up to `seconds`, returning the instant the
        preemption arrives so a reverse/stop takes effect promptly rather than on a fixed boundary."""
        if self._blind._abort_event.is_set():
            return True
        if seconds <= 0:
            return False
        try:
            await asyncio.wait_for(self._blind._abort_event.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False

    async def _move_and_retry(self, ha_target: float, skip_battery_check: bool = False) -> None:
        """Move to ha_target (0-100), retrying through transient proxy flaps until reached.

        Serialized per blind by _move_lock so two moves never overlap, and preemptible via
        _abort_event: a newer command (opposite move, re-target, or stop) aborts the in-flight move
        cleanly and this loop YIELDS instead of chasing the superseded target. A single move over a
        flapping proxy can still drop mid-flight; reconnect re-grabs the connection, so retrying the
        whole move usually lands it. Up to 3 attempts, then error.
        """
        # Signal any in-flight move for this blind to bail, then take the lock exclusively so nothing
        # else moves this blind until we're done (or are ourselves preempted).
        self._blind._abort_event.set()
        async with self._blind._move_lock:
            self._blind._abort_event.clear()
            last_exc: Exception | None = None
            pos = self._blind._current_cover_position
            for attempt in range(3):
                current = self._blind._current_cover_position
                if current is None:
                    current = 0
                movement_direction = 1 if current <= ha_target else -1
                try:
                    await self._blind.async_move_cover(
                        movement_direction=movement_direction,
                        target_position=100 - ha_target,
                        skip_battery_check=skip_battery_check,
                    )
                except MoveAborted:
                    _LOGGER.debug("%s: superseded by a newer command; yielding", self._attr_name)
                    return
                except (ConnectionTimeout, DeviceNotFound) as e:
                    last_exc = e
                if await self._preempted():
                    _LOGGER.debug("%s: superseded between attempts; yielding", self._attr_name)
                    return
                pos = self._blind._current_cover_position
                if pos is not None and abs(pos - ha_target) <= 3:
                    return  # reached target
                if attempt < 2:
                    _LOGGER.debug(
                        "%s: move to %s didn't land (at %s); retrying (%d/3)",
                        self._attr_name, ha_target, pos, attempt + 2,
                    )
                    if await self._preempted(3):
                        _LOGGER.debug("%s: superseded during backoff; yielding", self._attr_name)
                        return
            # Immediate retries didn't confirm arrival. The blind may well have physically reached the
            # target but the position read-back was lost (common under multi-blind storm contention).
            # Re-read the true position on a progressive schedule (~+1s, +2s, +5s, +10s) before
            # declaring failure — so a successful-but-unconfirmed move self-heals fast instead of
            # sitting stale until the 4-hourly poll. position_callback doesn't push state, so write it.
            _LOGGER.debug(
                "%s: move to %s unconfirmed (at %s); re-reading position (waits %s)",
                self._attr_name, ha_target, pos, CONFIRM_REREAD_DELAYS,
            )
            for wait in CONFIRM_REREAD_DELAYS:
                if await self._preempted(wait):
                    return
                try:
                    await self._blind.get_blind_position()
                except (ConnectionTimeout, DeviceNotFound, RuntimeError) as e:
                    last_exc = e
                self.async_write_ha_state()
                pos = self._blind._current_cover_position
                if pos is not None and abs(pos - ha_target) <= 3:
                    _LOGGER.debug("%s: confirmed at %s on re-read", self._attr_name, pos)
                    return
            _LOGGER.warning("%s: failed to reach %s after retries + progressive re-reads", self._attr_name, ha_target)
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key="failed_set_position",
                translation_placeholders={
                    "name": self._attr_name,
                    "error": str(last_exc) if last_exc else "did not reach target",
                })

    async def async_open_cover(self, **kwargs: Any) -> None:
        """Open the cover."""
        await self._move_and_retry(100)

    async def async_close_cover(self, **kwargs: Any) -> None:
        """Close the cover."""
        await self._move_and_retry(0)

    async def async_set_cover_position(self, **kwargs: Any) -> None:
        """Set the cover position."""
        skip_battery_check = kwargs.get("skip_battery_check", False)
        await self._move_and_retry(kwargs[ATTR_POSITION], skip_battery_check=skip_battery_check)




    async def async_stop_cover(self, **kwargs: Any) -> None:
        """Stop the cover — abort any in-flight move and halt at the current position.

        Setting _abort_event breaks the in-flight move's travel wait so its retry YIELDS instead of
        chasing the old target (a stop used to be silently undone by that retry re-firing the move),
        and taking _move_lock ensures nothing else moves this blind while we halt it. The aborted
        move leaves the live connection open, so stop() reuses it and halts immediately."""
        _LOGGER.debug("%s: Entering async_stop_cover. moving: %s", self.name, self._blind._moving)
        self._blind._is_stopping = True
        self._blind._abort_event.set()
        async with self._blind._move_lock:
            self._blind._abort_event.clear()
            try:
                await self._blind.stop()
            except (ConnectionTimeout, DeviceNotFound, RuntimeError) as e:
                if self._blind._moving != 0:
                    _LOGGER.debug("Failed to stop %s. Error %s", self._attr_name, e)
                    # Use translation placeholder so the frontend can localise the message
                    raise HomeAssistantError(
                        translation_domain=DOMAIN,
                        translation_key="failed_to_stop",
                        translation_placeholders={
                            "name": self._attr_name,
                            "error": str(e),
                        })
            finally:
                self._blind._moving = 0
                self._blind._is_stopping = False
                if self._blind._client:
                    await self._blind.disconnect()
                self._blind._locked = False
                await self.async_scheduled_update_request()
                _LOGGER.debug("%s: stop complete; blind halted.", self._attr_name)
