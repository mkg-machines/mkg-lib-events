"""Dead Letter Queue handler for failed events.

Provides utilities for handling events that failed processing,
including logging, alerting, and storage for later analysis.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from mkg_lib_events.logging import get_logger
from mkg_lib_events.models.base import BaseEvent

logger = get_logger(__name__, component="dlq_handler")


@dataclass
class FailedEvent:
    """Represents a failed event in the dead letter queue.

    Attributes:
        id: Unique identifier for this DLQ entry.
        event: The original event that failed.
        error_message: Error message from the failure.
        error_type: Type of the exception.
        handler_name: Name of the handler that failed.
        attempt_count: Number of processing attempts.
        first_failure_at: Timestamp of first failure.
        last_failure_at: Timestamp of most recent failure.
        metadata: Additional metadata about the failure.
    """

    id: UUID
    event: BaseEvent
    error_message: str
    error_type: str
    handler_name: str
    attempt_count: int = 1
    first_failure_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_failure_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage/serialization.

        Returns:
            Dictionary representation of the failed event.
        """
        return {
            "id": str(self.id),
            "event_id": str(self.event.event_id),
            "event_type": self.event.event_type,
            "tenant_id": self.event.tenant_id,
            "error_message": self.error_message,
            "error_type": self.error_type,
            "handler_name": self.handler_name,
            "attempt_count": self.attempt_count,
            "first_failure_at": self.first_failure_at.isoformat(),
            "last_failure_at": self.last_failure_at.isoformat(),
            "metadata": self.metadata,
        }


# Type alias for alert callbacks
AlertCallback = Callable[[FailedEvent], None]
StorageCallback = Callable[[FailedEvent], None]


class DeadLetterHandler:
    """Handles failed events for later processing or analysis.

    Provides hooks for alerting, storage, and metrics collection
    when events fail processing.

    Example:
        ```python
        def send_alert(failed_event: FailedEvent) -> None:
            # Send alert to monitoring system
            print(f"Alert: Event {failed_event.event.event_id} failed")

        def store_failed_event(failed_event: FailedEvent) -> None:
            # Store to DynamoDB or S3
            dynamodb.put_item(TableName="dlq", Item=failed_event.to_dict())

        dlq = DeadLetterHandler(
            on_alert=send_alert,
            on_store=store_failed_event,
            alert_threshold=3,
        )

        # When an event fails
        dlq.handle_failure(
            event=event,
            error=exception,
            handler_name="my-handler",
        )
        ```
    """

    def __init__(
        self,
        on_alert: AlertCallback | None = None,
        on_store: StorageCallback | None = None,
        alert_threshold: int = 1,
        max_error_message_length: int = 1000,
    ) -> None:
        """Initialize DeadLetterHandler.

        Args:
            on_alert: Callback for alerting when events fail.
            on_store: Callback for storing failed events.
            alert_threshold: Number of failures before alerting.
            max_error_message_length: Maximum length of error messages to store.
        """
        self._on_alert = on_alert
        self._on_store = on_store
        self._alert_threshold = alert_threshold
        self._max_error_message_length = max_error_message_length
        self._failure_counts: dict[str, int] = {}

        logger.info(
            "dlq_handler_initialized",
            alert_threshold=alert_threshold,
            has_alert_callback=on_alert is not None,
            has_store_callback=on_store is not None,
        )

    def handle_failure(
        self,
        event: BaseEvent,
        error: Exception,
        handler_name: str,
        attempt_count: int = 1,
        metadata: dict[str, Any] | None = None,
    ) -> FailedEvent:
        """Handle a failed event.

        Logs the failure, optionally stores it, and triggers alerts
        if the threshold is reached.

        Args:
            event: The event that failed processing.
            error: The exception that caused the failure.
            handler_name: Name of the handler that failed.
            attempt_count: Number of processing attempts.
            metadata: Additional metadata about the failure.

        Returns:
            FailedEvent object representing the failure.
        """
        # Create failed event record
        error_message = str(error)
        if len(error_message) > self._max_error_message_length:
            error_message = error_message[: self._max_error_message_length] + "..."

        failed_event = FailedEvent(
            id=uuid4(),
            event=event,
            error_message=error_message,
            error_type=type(error).__name__,
            handler_name=handler_name,
            attempt_count=attempt_count,
            metadata=metadata or {},
        )

        # Log the failure
        logger.error(
            "event_failed",
            dlq_id=str(failed_event.id),
            event_id=str(event.event_id),
            event_type=event.event_type,
            tenant_id=event.tenant_id,
            handler_name=handler_name,
            error_type=failed_event.error_type,
            error_message=error_message,
            attempt_count=attempt_count,
        )

        # Store the failed event
        if self._on_store:
            try:
                self._on_store(failed_event)
                logger.debug(
                    "failed_event_stored",
                    dlq_id=str(failed_event.id),
                )
            except Exception as store_error:
                logger.exception(
                    "failed_to_store_dlq_event",
                    dlq_id=str(failed_event.id),
                    error=str(store_error),
                )

        # Track failure count for alerting
        failure_key = f"{handler_name}:{event.event_type}"
        self._failure_counts[failure_key] = self._failure_counts.get(failure_key, 0) + 1

        # Check if we should alert
        if self._should_alert(failure_key, failed_event):
            self._trigger_alert(failed_event)

        return failed_event

    def handle_batch_failure(
        self,
        events: list[BaseEvent],
        error: Exception,
        handler_name: str,
        metadata: dict[str, Any] | None = None,
    ) -> list[FailedEvent]:
        """Handle multiple failed events from a batch.

        Args:
            events: List of events that failed.
            error: The exception that caused the failure.
            handler_name: Name of the handler that failed.
            metadata: Additional metadata about the failure.

        Returns:
            List of FailedEvent objects.
        """
        failed_events = []
        for event in events:
            failed_event = self.handle_failure(
                event=event,
                error=error,
                handler_name=handler_name,
                metadata=metadata,
            )
            failed_events.append(failed_event)

        logger.warning(
            "batch_failure_processed",
            handler_name=handler_name,
            event_count=len(events),
            error_type=type(error).__name__,
        )

        return failed_events

    def get_failure_count(self, handler_name: str, event_type: str) -> int:
        """Get the failure count for a handler/event type combination.

        Args:
            handler_name: Name of the handler.
            event_type: Type of the event.

        Returns:
            Number of failures recorded.
        """
        key = f"{handler_name}:{event_type}"
        return self._failure_counts.get(key, 0)

    def reset_failure_count(
        self,
        handler_name: str | None = None,
        event_type: str | None = None,
    ) -> None:
        """Reset failure counts.

        Args:
            handler_name: Optional handler name to reset. If None, resets all.
            event_type: Optional event type to reset.
        """
        if handler_name is None and event_type is None:
            self._failure_counts.clear()
            logger.info("all_failure_counts_reset")
            return

        if handler_name and event_type:
            key = f"{handler_name}:{event_type}"
            self._failure_counts.pop(key, None)
        elif handler_name:
            keys_to_remove = [
                k for k in self._failure_counts if k.startswith(f"{handler_name}:")
            ]
            for key in keys_to_remove:
                del self._failure_counts[key]

        logger.info(
            "failure_counts_reset",
            handler_name=handler_name,
            event_type=event_type,
        )

    def _should_alert(self, failure_key: str, _failed_event: FailedEvent) -> bool:
        """Determine if an alert should be triggered.

        Args:
            failure_key: Key for tracking failures.
            _failed_event: The failed event (unused, for future extensibility).

        Returns:
            True if alert should be triggered.
        """
        if self._on_alert is None:
            return False

        count = self._failure_counts.get(failure_key, 0)

        # Alert on first failure if threshold is 1
        if self._alert_threshold == 1:
            return True

        # Alert when threshold is reached
        return count >= self._alert_threshold and count % self._alert_threshold == 0

    def _trigger_alert(self, failed_event: FailedEvent) -> None:
        """Trigger an alert for a failed event.

        Args:
            failed_event: The failed event to alert on.
        """
        if self._on_alert is None:
            return

        try:
            logger.info(
                "triggering_alert",
                dlq_id=str(failed_event.id),
                event_id=str(failed_event.event.event_id),
                handler_name=failed_event.handler_name,
            )
            self._on_alert(failed_event)
        except Exception as alert_error:
            logger.exception(
                "alert_callback_failed",
                dlq_id=str(failed_event.id),
                error=str(alert_error),
            )
