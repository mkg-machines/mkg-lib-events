"""Base event handler for MKG Platform extensions.

Provides abstract base class for event processing with automatic
error handling, logging, and tenant context management.
"""

from abc import ABC, abstractmethod
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from mkg_lib_events.logging import get_logger
from mkg_lib_events.models.base import BaseEvent

logger = get_logger(__name__, component="event_handler")

# Context variable for current tenant
_current_tenant: ContextVar[str | None] = ContextVar("current_tenant", default=None)
_current_correlation_id: ContextVar[str | None] = ContextVar(
    "current_correlation_id", default=None
)

T = TypeVar("T", bound=BaseEvent)


def get_current_tenant() -> str | None:
    """Get the current tenant ID from context.

    Returns:
        Current tenant ID or None if not set.
    """
    return _current_tenant.get()


def get_current_correlation_id() -> str | None:
    """Get the current correlation ID from context.

    Returns:
        Current correlation ID or None if not set.
    """
    return _current_correlation_id.get()


@dataclass
class HandlerResult:
    """Result of event handler execution.

    Attributes:
        is_success: Whether the handler succeeded.
        event_id: ID of the processed event.
        tenant_id: Tenant ID of the event.
        error: Error message if failed.
        should_retry: Whether the event should be retried.
        metadata: Additional result metadata.
    """

    is_success: bool
    event_id: str
    tenant_id: str
    error: str | None = None
    should_retry: bool = False
    metadata: dict[str, Any] | None = None


class BaseEventHandler(ABC, Generic[T]):
    """Abstract base class for event handlers.

    Provides automatic error handling, logging, and tenant context
    setup for event processing in extensions.

    Subclasses must implement the `handle` method to process events.

    Attributes:
        handler_name: Name of the handler for logging.
        supported_event_types: List of event types this handler processes.

    Example:
        ```python
        class ArticleCreatedHandler(BaseEventHandler[EntityCreatedEvent]):
            handler_name = "article-created-handler"
            supported_event_types = ["entity.created"]

            def handle(self, event: EntityCreatedEvent) -> HandlerResult:
                if event.entity_type != "Article":
                    return self.skip("Not an Article entity")

                # Process the article...
                return self.success()
        ```
    """

    handler_name: str = "base-handler"
    supported_event_types: list[str] = []

    def __init__(
        self,
        on_error: Any | None = None,
        on_success: Any | None = None,
    ) -> None:
        """Initialize the handler.

        Args:
            on_error: Optional callback for error events.
            on_success: Optional callback for successful events.
        """
        self._on_error = on_error
        self._on_success = on_success

    @abstractmethod
    def handle(self, event: T) -> HandlerResult:
        """Process an event.

        Must be implemented by subclasses to define event processing logic.

        Args:
            event: The event to process.

        Returns:
            HandlerResult indicating success or failure.
        """
        pass

    def process(self, event: T) -> HandlerResult:
        """Process an event with automatic error handling and logging.

        This method wraps the `handle` method with:
        - Tenant context setup
        - Correlation ID tracking
        - Automatic error handling
        - Structured logging

        Args:
            event: The event to process.

        Returns:
            HandlerResult indicating success or failure.
        """
        event_id = str(event.event_id)
        tenant_id = event.tenant_id
        correlation_id = event.metadata.correlation_id

        # Set context variables
        tenant_token = _current_tenant.set(tenant_id)
        correlation_token = _current_correlation_id.set(correlation_id)

        try:
            # Check if handler supports this event type
            if (
                self.supported_event_types
                and event.event_type not in self.supported_event_types
            ):
                logger.debug(
                    "event_type_not_supported",
                    handler=self.handler_name,
                    event_type=event.event_type,
                    supported_types=self.supported_event_types,
                )
                return HandlerResult(
                    is_success=True,
                    event_id=event_id,
                    tenant_id=tenant_id,
                    metadata={"skipped": True, "reason": "event_type_not_supported"},
                )

            logger.info(
                "processing_event",
                handler=self.handler_name,
                event_id=event_id,
                event_type=event.event_type,
                tenant_id=tenant_id,
                correlation_id=correlation_id,
            )

            # Execute handler
            result = self.handle(event)

            if result.is_success:
                logger.info(
                    "event_processed",
                    handler=self.handler_name,
                    event_id=event_id,
                    tenant_id=tenant_id,
                )
                if self._on_success:
                    self._on_success(event, result)
            else:
                logger.warning(
                    "event_processing_failed",
                    handler=self.handler_name,
                    event_id=event_id,
                    tenant_id=tenant_id,
                    error=result.error,
                    should_retry=result.should_retry,
                )
                if self._on_error:
                    self._on_error(event, result)

            return result

        except Exception as e:
            logger.exception(
                "event_processing_exception",
                handler=self.handler_name,
                event_id=event_id,
                tenant_id=tenant_id,
                error=str(e),
            )

            result = HandlerResult(
                is_success=False,
                event_id=event_id,
                tenant_id=tenant_id,
                error=str(e),
                should_retry=self._is_retryable_error(e),
            )

            if self._on_error:
                self._on_error(event, result)

            return result

        finally:
            # Reset context variables
            _current_tenant.reset(tenant_token)
            _current_correlation_id.reset(correlation_token)

    def success(
        self,
        metadata: dict[str, Any] | None = None,
    ) -> HandlerResult:
        """Create a success result.

        Helper method for creating success results in handle() implementations.

        Args:
            metadata: Optional metadata to include in the result.

        Returns:
            HandlerResult with is_success=True.
        """
        tenant_id = get_current_tenant() or ""
        return HandlerResult(
            is_success=True,
            event_id="",  # Will be set by process()
            tenant_id=tenant_id,
            metadata=metadata,
        )

    def failure(
        self,
        error: str,
        should_retry: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> HandlerResult:
        """Create a failure result.

        Helper method for creating failure results in handle() implementations.

        Args:
            error: Error message.
            should_retry: Whether the event should be retried.
            metadata: Optional metadata to include in the result.

        Returns:
            HandlerResult with is_success=False.
        """
        tenant_id = get_current_tenant() or ""
        return HandlerResult(
            is_success=False,
            event_id="",  # Will be set by process()
            tenant_id=tenant_id,
            error=error,
            should_retry=should_retry,
            metadata=metadata,
        )

    def skip(
        self,
        reason: str,
        metadata: dict[str, Any] | None = None,
    ) -> HandlerResult:
        """Create a skip result.

        Helper method for skipping events that don't match criteria.

        Args:
            reason: Reason for skipping.
            metadata: Optional metadata to include in the result.

        Returns:
            HandlerResult with is_success=True and skip metadata.
        """
        tenant_id = get_current_tenant() or ""
        result_metadata = {"skipped": True, "reason": reason}
        if metadata:
            result_metadata.update(metadata)

        return HandlerResult(
            is_success=True,
            event_id="",
            tenant_id=tenant_id,
            metadata=result_metadata,
        )

    def _is_retryable_error(self, error: Exception) -> bool:
        """Determine if an error should trigger a retry.

        Override this method to customize retry behavior.

        Args:
            error: The exception that occurred.

        Returns:
            True if the event should be retried, False otherwise.
        """
        # By default, retry on transient errors
        retryable_types = (
            ConnectionError,
            TimeoutError,
            OSError,
        )
        return isinstance(error, retryable_types)
