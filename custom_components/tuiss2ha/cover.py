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
)
from .hub import TuissBlind

_LOGGER = logging.getLogger(__name__)


ATTR_TRAVERSAL_SPEED = "traversal_speed"
ATTR_MAC_ADDRESS = "mac_address"

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

        # Try to connect to all blinds in parallel, but continue on individual failures
        connect_tasks = [asyncio.create_task(entity._blind.attempt_connection()) for entity in target_entities]
        connect_results = await asyncio.gather(*connect_tasks, return_exceptions=True)

        connected_entities: list[Tuiss] = []
        for entity, res in zip(target_entities, connect_results):
            if isinstance(res, Exception):
                _LOGGER.warning("Failed to connect to %s: %s", entity.entity_id, res)
            else:
                connected_entities.append(entity)

        if not connected_entities:
            _LOGGER.error("No blinds connected for simultaneous positioning.")
            return

        # Build and dispatch set-position tasks
        set_position_tasks = []
        if favourite:
            for entity in connected_entities:
                fav_pos = entity.config_entry.options.get(
                    OPT_FAVORITE_POSITION, DEFAULT_FAVORITE_POSITION
                )
                set_position_tasks.append(
                    entity.async_set_cover_position(**{ATTR_POSITION: fav_pos, "skip_battery_check": True})
                )
        else:
            for entity in connected_entities:
                set_position_tasks.append(
                    entity.async_set_cover_position(**{ATTR_POSITION: position, "skip_battery_check": True})
                )

        results = await asyncio.gather(*set_position_tasks, return_exceptions=True)
        for entity, res in zip(connected_entities, results):
            if isinstance(res, Exception):
                _LOGGER.warning("Failed to set position for %s: %s", entity.entity_id, res)

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


    async def _move_and_retry(self, ha_target: float, skip_battery_check: bool = False) -> None:
        """Move to ha_target (0-100), retrying through transient proxy flaps until reached.

        A single move over a flapping proxy can drop mid-flight; keep-awake re-grabs the
        connection, so retrying the whole move usually lands it. Up to 3 attempts, then error.
        """
        last_exc: Exception | None = None
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
            except (ConnectionTimeout, DeviceNotFound) as e:
                last_exc = e
            pos = self._blind._current_cover_position
            if pos is not None and abs(pos - ha_target) <= 3:
                return  # reached target
            if attempt < 2:
                _LOGGER.debug(
                    "%s: move to %s didn't land (at %s); retrying (%d/3)",
                    self._attr_name, ha_target, pos, attempt + 2,
                )
                await asyncio.sleep(3)
        _LOGGER.warning("%s: failed to reach %s after retries", self._attr_name, ha_target)
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
        """Stop the cover."""
        _LOGGER.debug("%s: Entering async_stop_cover. is_stopping: %s", self.name, self._blind._is_stopping)
        self._blind._is_stopping = True
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
            if self._blind._client:
                # Re-check _client each loop: a disconnect (or the stop flow itself) can null it
                # out mid-wait, and the bare None.is_connected was raising during stop_cover.
                while self._blind._client and self._blind._client.is_connected:
                    await asyncio.sleep(1)
                self._blind._moving = 0
                await self.async_scheduled_update_request()
            _LOGGER.debug("%s: Lock released in async_stop_cover.", self._attr_name)
            self._blind._locked = False
