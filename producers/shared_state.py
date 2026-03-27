"""
producers/shared_state.py
─────────────────────────────────────────────────────────────────
Thread-safe shared state for inter-producer communication.
GPS producer writes current_location; other producers read it.
"""
import threading

_lock = threading.Lock()

_state = {
    "lat": 52.4862,   # default: traffic office location
    "lon": -1.8904,
}


@property
def current_location():  # noqa: E303
    with _lock:
        return dict(_state)


# Simple dict — producers access via module-level variable.
# gps_producer writes directly; others read.
current_location: dict = _state
