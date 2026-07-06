"""Constants for the ha2tuiss integration."""

# name for the integration.
DOMAIN = "tuiss2ha"

CONF_BLIND_HOST = "host"
CONF_BLIND_NAME = "name"

SPEED_CONTROL_SUPPORTED_MODELS = ["TS5200","TS5101","TS5001","TS2600"]

TIMEOUT_SECONDS = 120
TRAVERSAL_UPDATE_THRESHOLD = 5

# Wall-clock budget (seconds) for establishing a BLE connection before giving up.
# These blinds are "sleepy" peripherals: the ESP32 proxy can only complete a GATT
# connection during the blind's brief connectable window, so we retry within a bounded
# budget rather than hanging HA for minutes. See DIAGNOSIS notes.
CONNECT_BUDGET_SECONDS = 90
# Hard cap for a single connection attempt. establish_connection can otherwise block for
# 40s+ per attempt (≈20s connect timeout + ≈20s disconnect-cleanup) and its internal
# retries stack these into multi-minute hangs. We do ONE establish attempt per call and
# cap it here, then let attempt_connection re-home to a fresh proxy and try again within
# the overall CONNECT_BUDGET_SECONDS.
PER_ATTEMPT_CONNECT_TIMEOUT = 45
# Max time to wait for a clean BLE disconnect before force-dropping the client, so a
# stuck disconnect can't leave a zombie connection that sabotages the next attempt.
DISCONNECT_TIMEOUT_SECONDS = 8

# Keep-awake (persistent connection). These sleepy motors accept only ONE BLE connection and
# the ESP32 proxies can't reliably wake them on demand, so for chosen blinds HA grabs and HOLDS
# the connection (exactly like the phone app) to keep the motor awake and instantly controllable.
# Scoped by MAC so it can ONLY ever affect the listed blind(s) — never the others.
KEEP_AWAKE_HOSTS = ["C0:16:2C:5E:66:A0"]  # guinea pig: Living Room Blind 10 (Patio Right)
KEEP_AWAKE_RETRY_SECONDS = 240   # gentle re-grab interval while the blind is unreachable
KEEP_AWAKE_HOLD_POLL_SECONDS = 15  # how often the hold loop checks the connection is alive
BLIND_NOTIFY_CHARACTERISTIC = "00010304-0405-0607-0809-0a0b0c0d1910"
CONNECTION_MESSAGE = "ff03030303787878787878"
INITIALIZATION_MESSAGE = "ff78ea41d10301"
UUID = "00010405-0405-0607-0809-0a0b0c0d1910"

OPT_RESTART_POSITION = "blind_restart_position"
DEFAULT_RESTART_POSITION = False

OPT_RESTART_ATTEMPTS = "blind_restart_attempts"
DEFAULT_RESTART_ATTEMPTS = 4

OPT_BLIND_SPEED = "blind_speed"
DEFAULT_BLIND_SPEED = "Standard"
BLIND_SPEED_LIST = ["Standard", "Comfort", "Slow"]

OPT_FAVORITE_POSITION = "blind_favorite_position"
DEFAULT_FAVORITE_POSITION = 50

#Exceptions
OPT_BATTERY_CHECK_DAYS = "blind_battery_check_days"
DEFAULT_BATTERY_CHECK_DAYS = 0

class CannotConnect(Exception):
    """Error to indicate we cannot connect."""


class InvalidHost(Exception):
    """Error to indicate there is an invalid hostname."""


class InvalidName(Exception):
    """Error to indicate there is an invalid device name."""


class DeviceNotFound(Exception):
    """Error to indicate the device is not found."""


class ConnectionTimeout(Exception):
    """Error to indicate a connection timeout."""


class NoConnectableBluetoothAdapter(Exception):
    """Error to indicate no Bluetooth adapter can connect (e.g. Shelly is passive-only)."""
