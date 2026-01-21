"""Event publisher for MKG Platform.

Publishes events to AWS EventBridge with automatic serialization,
tenant context validation, and batch support.
"""

from mkg_lib_events.client import EventBusClient
from mkg_lib_events.exceptions import EventPublishError
from mkg_lib_events.logging import get_logger
from mkg_lib_events.models.base import BaseEvent

logger = get_logger(__name__, component="event_publisher")


class EventPublisher:
    """Publishes events to AWS EventBridge.

    Handles event serialization, tenant context validation, and provides
    both single and batch publishing capabilities.

    Attributes:
        event_bus_name: Name of the EventBridge event bus.

    Example:
        ```python
        publisher = EventPublisher(event_bus_name="mkg-events")

        event = EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id=uuid4(),
            entity_type="Article",
        )

        event_id = publisher.publish(event)
        ```
    """

    def __init__(
        self,
        event_bus_name: str | None = None,
        client: EventBusClient | None = None,
    ) -> None:
        """Initialize EventPublisher.

        Args:
            event_bus_name: Name of the EventBridge event bus.
                Defaults to MKG_EVENT_BUS_NAME environment variable.
            client: Optional pre-configured EventBusClient for testing.
        """
        if client is not None:
            self._client = client
            self.event_bus_name = client.event_bus_name
        else:
            self._client = EventBusClient(event_bus_name=event_bus_name)
            self.event_bus_name = self._client.event_bus_name

        logger.info(
            "event_publisher_initialized",
            event_bus_name=self.event_bus_name,
        )

    def publish(
        self,
        event: BaseEvent,
        correlation_id: str | None = None,
    ) -> str:
        """Publish a single event to EventBridge.

        Args:
            event: The event to publish.
            correlation_id: Optional correlation ID to add to metadata.
                If provided and event has no correlation_id, it will be set.

        Returns:
            The event ID of the published event.

        Raises:
            EventPublishError: If the event cannot be published.

        Example:
            ```python
            event = EntityCreatedEvent(
                tenant_id="tenant-123",
                entity_id=uuid4(),
                entity_type="Article",
            )
            event_id = publisher.publish(event, correlation_id="req-456")
            ```
        """
        self._validate_event(event)

        # Create EventBridge entry
        entry = event.to_eventbridge_entry(self.event_bus_name)

        logger.info(
            "publishing_event",
            event_id=str(event.event_id),
            event_type=event.event_type,
            tenant_id=event.tenant_id,
            correlation_id=correlation_id or event.metadata.correlation_id,
        )

        try:
            response = self._client.put_events([entry])

            # Check for failures
            entries = response.get("Entries", [])
            if entries and entries[0].get("ErrorCode"):
                error_code = entries[0].get("ErrorCode")
                error_message = entries[0].get("ErrorMessage", "Unknown error")

                logger.error(
                    "event_publish_failed",
                    event_id=str(event.event_id),
                    event_type=event.event_type,
                    tenant_id=event.tenant_id,
                    error_code=error_code,
                    error_message=error_message,
                )

                raise EventPublishError(
                    f"Failed to publish event: {error_code} - {error_message}",
                    event_id=str(event.event_id),
                    event_type=event.event_type,
                    tenant_id=event.tenant_id,
                )

            logger.info(
                "event_published",
                event_id=str(event.event_id),
                event_type=event.event_type,
                tenant_id=event.tenant_id,
            )

            return str(event.event_id)

        except EventPublishError:
            raise
        except Exception as e:
            logger.error(
                "event_publish_unexpected_error",
                event_id=str(event.event_id),
                event_type=event.event_type,
                tenant_id=event.tenant_id,
                error=str(e),
            )
            raise EventPublishError(
                f"Unexpected error publishing event: {e}",
                event_id=str(event.event_id),
                event_type=event.event_type,
                tenant_id=event.tenant_id,
            ) from e

    def publish_batch(
        self,
        events: list[BaseEvent],
        _correlation_id: str | None = None,
    ) -> list[str]:
        """Publish multiple events to EventBridge in a batch.

        Events are published in batches of 10 (EventBridge limit).

        Args:
            events: List of events to publish.
            _correlation_id: Reserved for future use.

        Returns:
            List of event IDs for successfully published events.

        Raises:
            EventPublishError: If any event fails to publish.

        Example:
            ```python
            events = [event1, event2, event3]
            event_ids = publisher.publish_batch(events)
            ```
        """
        if not events:
            return []

        # Validate all events first
        for event in events:
            self._validate_event(event)

        published_ids: list[str] = []
        batch_size = 10  # EventBridge limit

        for i in range(0, len(events), batch_size):
            batch = events[i : i + batch_size]
            entries = [e.to_eventbridge_entry(self.event_bus_name) for e in batch]

            logger.info(
                "publishing_event_batch",
                batch_number=i // batch_size + 1,
                batch_size=len(batch),
                total_events=len(events),
            )

            try:
                response = self._client.put_events(entries)

                # Process results
                response_entries = response.get("Entries", [])
                for j, entry_response in enumerate(response_entries):
                    event = batch[j]

                    if entry_response.get("ErrorCode"):
                        error_code = entry_response.get("ErrorCode")
                        error_message = entry_response.get("ErrorMessage", "Unknown")

                        logger.error(
                            "batch_event_publish_failed",
                            event_id=str(event.event_id),
                            event_type=event.event_type,
                            tenant_id=event.tenant_id,
                            error_code=error_code,
                            error_message=error_message,
                        )

                        raise EventPublishError(
                            f"Failed to publish event in batch: {error_code}",
                            event_id=str(event.event_id),
                            event_type=event.event_type,
                            tenant_id=event.tenant_id,
                        )

                    published_ids.append(str(event.event_id))

            except EventPublishError:
                raise
            except Exception as e:
                logger.error(
                    "batch_publish_unexpected_error",
                    batch_number=i // batch_size + 1,
                    error=str(e),
                )
                raise EventPublishError(
                    f"Unexpected error in batch publish: {e}"
                ) from e

        logger.info(
            "batch_publish_complete",
            total_published=len(published_ids),
        )

        return published_ids

    def _validate_event(self, event: BaseEvent) -> None:
        """Validate event before publishing.

        Args:
            event: Event to validate.

        Raises:
            EventPublishError: If validation fails.
        """
        if not event.tenant_id:
            raise EventPublishError(
                "Event must have a tenant_id",
                event_id=str(event.event_id),
                event_type=event.event_type,
            )

        if not event.event_type:
            raise EventPublishError(
                "Event must have an event_type",
                event_id=str(event.event_id),
                tenant_id=event.tenant_id,
            )

        if not event.source:
            raise EventPublishError(
                "Event must have a source",
                event_id=str(event.event_id),
                event_type=event.event_type,
                tenant_id=event.tenant_id,
            )
