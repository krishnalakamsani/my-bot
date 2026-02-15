import threading
import collections
import logging
from typing import Callable, Any

logger = logging.getLogger(__name__)

_subscribers = collections.defaultdict(list)
_lock = threading.RLock()

def subscribe(event: str, callback: Callable[[Any], None]):
    with _lock:
        _subscribers[event].append(callback)

def unsubscribe(event: str, callback: Callable[[Any], None]):
    with _lock:
        if callback in _subscribers.get(event, []):
            _subscribers[event].remove(callback)

def publish(event: str, payload: Any = None):
    """Publish an event to all subscribers. Calls subscribers in background threads.

    Lightweight, process-local event bus suitable for decoupling modules.
    """
    with _lock:
        subs = list(_subscribers.get(event, []))

    if not subs:
        logger.debug("EventBus.publish: no subscribers for %s", event)
        return

    for cb in subs:
        try:
            threading.Thread(target=_safe_call, args=(cb, payload), daemon=True).start()
        except Exception:
            logger.exception("Failed to dispatch event %s", event)

def _safe_call(cb, payload):
    try:
        cb(payload)
    except Exception:
        logger.exception("Event handler raised exception")
