"""Structured logging utilities for mkg-lib-events.

Provides a structured logger that integrates with mkg-lib-core when available,
or falls back to a basic structlog configuration.
"""

import importlib.util
import logging
import sys
from typing import Any

try:
    from mkg_lib_core.logging import get_logger as core_get_logger

    _USE_CORE_LOGGING = True
except ImportError:
    _USE_CORE_LOGGING = False
    _HAS_STRUCTLOG = importlib.util.find_spec("structlog") is not None


def _configure_basic_structlog() -> None:
    """Configure basic structlog if mkg-lib-core is not available."""
    if not _HAS_STRUCTLOG:
        return

    import structlog

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str, **initial_context: Any) -> Any:
    """Get a structured logger instance.

    Uses mkg-lib-core's logger if available, otherwise falls back to
    structlog or standard logging.

    Args:
        name: Logger name, typically __name__.
        **initial_context: Initial context values to bind to the logger.

    Returns:
        A structured logger instance.

    Example:
        ```python
        logger = get_logger(__name__, service="mkg-lib-events")
        logger.info("event_published", event_id="123", tenant_id="t-1")
        ```
    """
    if _USE_CORE_LOGGING:
        return core_get_logger(name, **initial_context)

    if _HAS_STRUCTLOG:
        import structlog

        _configure_basic_structlog()
        logger = structlog.get_logger(name)
        if initial_context:
            logger = logger.bind(**initial_context)
        return logger

    # Fallback to standard logging
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
