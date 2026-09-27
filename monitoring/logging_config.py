"""
Centralized Structured Logging Configuration — Phase II
Provides JSON-formatted logging with correlation IDs,
component tags, and consistent formatting across all services.
"""
import logging
import json
import uuid
import sys
from datetime import datetime, timezone
from contextvars import ContextVar

# ── Context variable for correlation ID (thread-safe) ────────
_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")


def get_correlation_id() -> str:
    """Get the current correlation ID, or generate a new one."""
    cid = _correlation_id.get()
    if not cid:
        cid = str(uuid.uuid4())[:8]
        _correlation_id.set(cid)
    return cid


def set_correlation_id(cid: str):
    """Set the correlation ID for the current context."""
    _correlation_id.set(cid)


def new_correlation_id() -> str:
    """Generate and set a new correlation ID."""
    cid = str(uuid.uuid4())[:8]
    _correlation_id.set(cid)
    return cid


class JSONFormatter(logging.Formatter):
    """
    Emits each log record as a single-line JSON object.
    Fields: timestamp, level, component, correlation_id, message, extra
    """

    def __init__(self, component: str = "unknown"):
        super().__init__()
        self.component = component

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "component": self.component,
            "correlation_id": get_correlation_id(),
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include exception info if present
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Include any extra fields attached to the record
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in logging.LogRecord(
                "", 0, "", 0, "", (), None
            ).__dict__ and k not in ("message", "msg", "args")
        }
        if extras:
            log_entry["extra"] = extras

        return json.dumps(log_entry, default=str)


def setup_logging(
    component: str,
    level: int = logging.INFO,
    json_format: bool = True,
) -> logging.Logger:
    """
    Configure the root logger for a service component.

    Args:
        component: Name of the service (e.g., 'producer', 'spark-streaming', 'api')
        level: Logging level
        json_format: If True, emit JSON logs. If False, use human-readable format.

    Returns:
        Configured root logger.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Clear existing handlers to avoid duplicate output
    root_logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    if json_format:
        handler.setFormatter(JSONFormatter(component=component))
    else:
        handler.setFormatter(logging.Formatter(
            f"%(asctime)s [{component.upper()}] %(levelname)s "
            f"[%(name)s] %(message)s"
        ))

    root_logger.addHandler(handler)
    return root_logger


def get_logger(name: str) -> logging.Logger:
    """Get a named logger (child of root)."""
    return logging.getLogger(name)
