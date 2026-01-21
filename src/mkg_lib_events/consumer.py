"""Event consumer for MKG Platform.

Deserializes events from AWS EventBridge and SQS, with automatic
type detection and tenant context validation.
"""

import json
from typing import Any

from mkg_lib_events.exceptions import EventDeserializationError
from mkg_lib_events.logging import get_logger
from mkg_lib_events.models.base import BaseEvent
from mkg_lib_events.registry import EventRegistry

logger = get_logger(__name__, component="event_consumer")


class EventConsumer:
    """Deserializes events from EventBridge and SQS.

    Handles event deserialization from various AWS event sources,
    with automatic type detection using the EventRegistry.

    Example:
        ```python
        consumer = EventConsumer()

        # From EventBridge event
        event = consumer.from_eventbridge(eventbridge_event)

        # From SQS message
        event = consumer.from_sqs(sqs_message)

        # From Lambda event (auto-detect source)
        event = consumer.from_lambda_event(lambda_event)
        ```
    """

    def __init__(
        self,
        strict_mode: bool = True,
        allowed_tenant_ids: list[str] | None = None,
    ) -> None:
        """Initialize EventConsumer.

        Args:
            strict_mode: If True, raise errors for unknown event types.
                If False, return BaseEvent for unknown types.
            allowed_tenant_ids: Optional list of allowed tenant IDs.
                If provided, events from other tenants will be rejected.
        """
        self.strict_mode = strict_mode
        self.allowed_tenant_ids = (
            set(allowed_tenant_ids) if allowed_tenant_ids else None
        )

        logger.info(
            "event_consumer_initialized",
            strict_mode=strict_mode,
            tenant_filter_enabled=allowed_tenant_ids is not None,
        )

    def from_eventbridge(self, event: dict[str, Any]) -> BaseEvent:
        """Deserialize an event from EventBridge format.

        Args:
            event: EventBridge event containing 'detail' with event data.

        Returns:
            Deserialized event instance.

        Raises:
            EventDeserializationError: If deserialization fails.

        Example:
            ```python
            eventbridge_event = {
                "source": "mkg-kernel",
                "detail-type": "entity.created",
                "detail": {"tenant_id": "t-1", "entity_id": "e-1", ...}
            }
            event = consumer.from_eventbridge(eventbridge_event)
            ```
        """
        try:
            detail = event.get("detail")
            if detail is None:
                raise EventDeserializationError(
                    "EventBridge event missing 'detail' field"
                )

            # Handle string detail (shouldn't happen but be defensive)
            if isinstance(detail, str):
                detail = json.loads(detail)

            return self._deserialize(detail)

        except EventDeserializationError:
            raise
        except json.JSONDecodeError as e:
            raise EventDeserializationError(f"Invalid JSON in event detail: {e}") from e
        except Exception as e:
            raise EventDeserializationError(
                f"Failed to deserialize EventBridge event: {e}"
            ) from e

    def from_sqs(self, message: dict[str, Any]) -> BaseEvent:
        """Deserialize an event from SQS message format.

        Handles both direct SQS messages and EventBridge-to-SQS messages.

        Args:
            message: SQS message containing event data in 'body'.

        Returns:
            Deserialized event instance.

        Raises:
            EventDeserializationError: If deserialization fails.

        Example:
            ```python
            sqs_message = {
                "body": '{"tenant_id": "t-1", "event_type": "entity.created", ...}'
            }
            event = consumer.from_sqs(sqs_message)
            ```
        """
        try:
            body = message.get("body")
            if body is None:
                raise EventDeserializationError("SQS message missing 'body' field")

            # Parse body if it's a string
            if isinstance(body, str):
                body = json.loads(body)

            # Check if this is an EventBridge-forwarded message
            if "detail" in body and "detail-type" in body:
                return self.from_eventbridge(body)

            return self._deserialize(body)

        except EventDeserializationError:
            raise
        except json.JSONDecodeError as e:
            raise EventDeserializationError(
                f"Invalid JSON in SQS message body: {e}"
            ) from e
        except Exception as e:
            raise EventDeserializationError(
                f"Failed to deserialize SQS message: {e}"
            ) from e

    def from_lambda_event(self, event: dict[str, Any]) -> list[BaseEvent]:
        """Deserialize events from a Lambda event.

        Automatically detects the event source (EventBridge, SQS batch, or direct)
        and deserializes accordingly.

        Args:
            event: Lambda event payload.

        Returns:
            List of deserialized events.

        Raises:
            EventDeserializationError: If deserialization fails.

        Example:
            ```python
            def handler(event, context):
                consumer = EventConsumer()
                events = consumer.from_lambda_event(event)
                for e in events:
                    process_event(e)
            ```
        """
        try:
            # SQS batch event
            if "Records" in event:
                records = event["Records"]
                events: list[BaseEvent] = []

                for record in records:
                    event_source = record.get("eventSource", "")

                    if event_source == "aws:sqs":
                        events.append(self.from_sqs(record))
                    else:
                        # Try to deserialize as generic record
                        body = record.get("body")
                        if body:
                            if isinstance(body, str):
                                body = json.loads(body)
                            events.append(self._deserialize(body))

                return events

            # EventBridge event (has detail-type)
            if "detail-type" in event and "detail" in event:
                return [self.from_eventbridge(event)]

            # Direct event (already contains event data)
            if "event_type" in event:
                return [self._deserialize(event)]

            raise EventDeserializationError(
                "Unable to determine event source from Lambda event"
            )

        except EventDeserializationError:
            raise
        except Exception as e:
            raise EventDeserializationError(
                f"Failed to deserialize Lambda event: {e}"
            ) from e

    def from_dict(self, data: dict[str, Any]) -> BaseEvent:
        """Deserialize an event from a dictionary.

        Args:
            data: Dictionary containing event data.

        Returns:
            Deserialized event instance.

        Raises:
            EventDeserializationError: If deserialization fails.
        """
        return self._deserialize(data)

    def from_json(self, json_string: str) -> BaseEvent:
        """Deserialize an event from a JSON string.

        Args:
            json_string: JSON string containing event data.

        Returns:
            Deserialized event instance.

        Raises:
            EventDeserializationError: If deserialization fails.
        """
        try:
            data = json.loads(json_string)
            return self._deserialize(data)
        except json.JSONDecodeError as e:
            raise EventDeserializationError(
                f"Invalid JSON string: {e}",
                raw_data=json_string,
            ) from e

    def _deserialize(self, data: dict[str, Any]) -> BaseEvent:
        """Internal deserialization with validation.

        Args:
            data: Event data dictionary.

        Returns:
            Deserialized event instance.

        Raises:
            EventDeserializationError: If deserialization fails.
        """
        event_type = data.get("event_type")
        tenant_id = data.get("tenant_id")

        # Validate tenant if filter is configured
        if self.allowed_tenant_ids and tenant_id not in self.allowed_tenant_ids:
            logger.warning(
                "event_rejected_tenant_mismatch",
                event_type=event_type,
                tenant_id=tenant_id,
            )
            raise EventDeserializationError(
                f"Event tenant_id '{tenant_id}' not in allowed tenants",
                event_type=event_type,
            )

        try:
            event = EventRegistry.deserialize(data)

            logger.debug(
                "event_deserialized",
                event_id=str(event.event_id),
                event_type=event.event_type,
                tenant_id=event.tenant_id,
            )

            return event

        except ValueError as e:
            if self.strict_mode:
                raise EventDeserializationError(
                    str(e),
                    event_type=event_type,
                ) from e

            # Non-strict mode: return BaseEvent
            logger.warning(
                "unknown_event_type_fallback",
                event_type=event_type,
                tenant_id=tenant_id,
            )

            from mkg_lib_events.models.base import BaseEvent

            return BaseEvent.model_validate(data)

        except Exception as e:
            raise EventDeserializationError(
                f"Failed to deserialize event: {e}",
                event_type=event_type,
            ) from e
