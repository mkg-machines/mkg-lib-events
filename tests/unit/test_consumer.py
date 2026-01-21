"""Unit tests for EventConsumer."""

import json
from uuid import uuid4

import pytest

from mkg_lib_events import EntityCreatedEvent, EntityUpdatedEvent
from mkg_lib_events.consumer import EventConsumer
from mkg_lib_events.exceptions import EventDeserializationError


class TestEventConsumerFromEventBridge:
    """Tests for EventConsumer.from_eventbridge()."""

    def test_deserialize_eventbridge_event(self) -> None:
        """Test deserializing an EventBridge event."""
        consumer = EventConsumer()

        eventbridge_event = {
            "source": "mkg-kernel",
            "detail-type": "entity.created",
            "detail": {
                "event_type": "entity.created",
                "source": "mkg-kernel",
                "tenant_id": "tenant-123",
                "entity_id": "550e8400-e29b-41d4-a716-446655440000",
                "entity_type": "Article",
                "attributes": {"name": "Widget"},
            },
        }

        event = consumer.from_eventbridge(eventbridge_event)

        assert isinstance(event, EntityCreatedEvent)
        assert event.tenant_id == "tenant-123"
        assert event.entity_type == "Article"

    def test_eventbridge_missing_detail_raises(self) -> None:
        """Test that missing detail field raises error."""
        consumer = EventConsumer()

        eventbridge_event = {
            "source": "mkg-kernel",
            "detail-type": "entity.created",
        }

        with pytest.raises(EventDeserializationError, match="missing 'detail'"):
            consumer.from_eventbridge(eventbridge_event)

    def test_eventbridge_string_detail(self) -> None:
        """Test handling of string detail (JSON encoded)."""
        consumer = EventConsumer()

        detail = {
            "event_type": "entity.created",
            "source": "mkg-kernel",
            "tenant_id": "tenant-123",
            "entity_id": "550e8400-e29b-41d4-a716-446655440000",
            "entity_type": "Article",
        }

        eventbridge_event = {
            "source": "mkg-kernel",
            "detail-type": "entity.created",
            "detail": json.dumps(detail),
        }

        event = consumer.from_eventbridge(eventbridge_event)

        assert isinstance(event, EntityCreatedEvent)


class TestEventConsumerFromSQS:
    """Tests for EventConsumer.from_sqs()."""

    def test_deserialize_sqs_message(self) -> None:
        """Test deserializing an SQS message."""
        consumer = EventConsumer()

        sqs_message = {
            "body": json.dumps(
                {
                    "event_type": "entity.created",
                    "source": "mkg-kernel",
                    "tenant_id": "tenant-123",
                    "entity_id": "550e8400-e29b-41d4-a716-446655440000",
                    "entity_type": "Article",
                }
            ),
        }

        event = consumer.from_sqs(sqs_message)

        assert isinstance(event, EntityCreatedEvent)

    def test_sqs_eventbridge_forwarded(self) -> None:
        """Test SQS message that was forwarded from EventBridge."""
        consumer = EventConsumer()

        # EventBridge -> SQS format
        sqs_message = {
            "body": json.dumps(
                {
                    "source": "mkg-kernel",
                    "detail-type": "entity.created",
                    "detail": {
                        "event_type": "entity.created",
                        "source": "mkg-kernel",
                        "tenant_id": "tenant-123",
                        "entity_id": "550e8400-e29b-41d4-a716-446655440000",
                        "entity_type": "Article",
                    },
                }
            ),
        }

        event = consumer.from_sqs(sqs_message)

        assert isinstance(event, EntityCreatedEvent)

    def test_sqs_missing_body_raises(self) -> None:
        """Test that missing body raises error."""
        consumer = EventConsumer()

        sqs_message = {"messageId": "msg-123"}

        with pytest.raises(EventDeserializationError, match="missing 'body'"):
            consumer.from_sqs(sqs_message)

    def test_sqs_invalid_json_raises(self) -> None:
        """Test that invalid JSON raises error."""
        consumer = EventConsumer()

        sqs_message = {"body": "not valid json"}

        with pytest.raises(EventDeserializationError, match="Invalid JSON"):
            consumer.from_sqs(sqs_message)


class TestEventConsumerFromLambdaEvent:
    """Tests for EventConsumer.from_lambda_event()."""

    def test_lambda_sqs_batch(self) -> None:
        """Test Lambda event with SQS batch."""
        consumer = EventConsumer()

        lambda_event = {
            "Records": [
                {
                    "eventSource": "aws:sqs",
                    "body": json.dumps(
                        {
                            "event_type": "entity.created",
                            "source": "mkg-kernel",
                            "tenant_id": "tenant-123",
                            "entity_id": str(uuid4()),
                            "entity_type": "Article",
                        }
                    ),
                },
                {
                    "eventSource": "aws:sqs",
                    "body": json.dumps(
                        {
                            "event_type": "entity.updated",
                            "source": "mkg-kernel",
                            "tenant_id": "tenant-123",
                            "entity_id": str(uuid4()),
                            "entity_type": "Article",
                            "changed_attributes": ["name"],
                        }
                    ),
                },
            ]
        }

        events = consumer.from_lambda_event(lambda_event)

        assert len(events) == 2
        assert isinstance(events[0], EntityCreatedEvent)
        assert isinstance(events[1], EntityUpdatedEvent)

    def test_lambda_eventbridge_direct(self) -> None:
        """Test Lambda event directly from EventBridge."""
        consumer = EventConsumer()

        lambda_event = {
            "source": "mkg-kernel",
            "detail-type": "entity.created",
            "detail": {
                "event_type": "entity.created",
                "source": "mkg-kernel",
                "tenant_id": "tenant-123",
                "entity_id": str(uuid4()),
                "entity_type": "Article",
            },
        }

        events = consumer.from_lambda_event(lambda_event)

        assert len(events) == 1
        assert isinstance(events[0], EntityCreatedEvent)

    def test_lambda_direct_event(self) -> None:
        """Test Lambda event with direct event data."""
        consumer = EventConsumer()

        lambda_event = {
            "event_type": "entity.created",
            "source": "mkg-kernel",
            "tenant_id": "tenant-123",
            "entity_id": str(uuid4()),
            "entity_type": "Article",
        }

        events = consumer.from_lambda_event(lambda_event)

        assert len(events) == 1
        assert isinstance(events[0], EntityCreatedEvent)

    def test_lambda_unknown_format_raises(self) -> None:
        """Test that unknown Lambda event format raises error."""
        consumer = EventConsumer()

        lambda_event = {"unknown": "format"}

        with pytest.raises(EventDeserializationError, match="Unable to determine"):
            consumer.from_lambda_event(lambda_event)


class TestEventConsumerTenantFilter:
    """Tests for tenant filtering in EventConsumer."""

    def test_tenant_filter_allows_matching_tenant(self) -> None:
        """Test that matching tenant is allowed."""
        consumer = EventConsumer(allowed_tenant_ids=["tenant-123", "tenant-456"])

        event_data = {
            "event_type": "entity.created",
            "source": "mkg-kernel",
            "tenant_id": "tenant-123",
            "entity_id": str(uuid4()),
            "entity_type": "Article",
        }

        event = consumer.from_dict(event_data)

        assert event.tenant_id == "tenant-123"

    def test_tenant_filter_rejects_non_matching_tenant(self) -> None:
        """Test that non-matching tenant is rejected."""
        consumer = EventConsumer(allowed_tenant_ids=["tenant-123"])

        event_data = {
            "event_type": "entity.created",
            "source": "mkg-kernel",
            "tenant_id": "tenant-other",
            "entity_id": str(uuid4()),
            "entity_type": "Article",
        }

        with pytest.raises(EventDeserializationError, match="not in allowed tenants"):
            consumer.from_dict(event_data)

    def test_no_tenant_filter_allows_all(self) -> None:
        """Test that no filter allows all tenants."""
        consumer = EventConsumer()

        event_data = {
            "event_type": "entity.created",
            "source": "mkg-kernel",
            "tenant_id": "any-tenant",
            "entity_id": str(uuid4()),
            "entity_type": "Article",
        }

        event = consumer.from_dict(event_data)

        assert event.tenant_id == "any-tenant"


class TestEventConsumerStrictMode:
    """Tests for strict mode in EventConsumer."""

    def test_strict_mode_raises_for_unknown_event(self) -> None:
        """Test that strict mode raises for unknown event types."""
        consumer = EventConsumer(strict_mode=True)

        event_data = {
            "event_type": "unknown.event.type",
            "source": "mkg-kernel",
            "tenant_id": "tenant-123",
        }

        with pytest.raises(EventDeserializationError, match="Unknown event type"):
            consumer.from_dict(event_data)

    def test_non_strict_mode_returns_base_event(self) -> None:
        """Test that non-strict mode returns BaseEvent for unknown types."""
        consumer = EventConsumer(strict_mode=False)

        event_data = {
            "event_type": "unknown.event.type",
            "source": "mkg-kernel",
            "tenant_id": "tenant-123",
        }

        from mkg_lib_events import BaseEvent

        event = consumer.from_dict(event_data)

        assert isinstance(event, BaseEvent)
        assert event.event_type == "unknown.event.type"


class TestEventConsumerFromJson:
    """Tests for EventConsumer.from_json()."""

    def test_from_json_valid(self) -> None:
        """Test deserializing from valid JSON string."""
        consumer = EventConsumer()

        json_string = json.dumps(
            {
                "event_type": "entity.created",
                "source": "mkg-kernel",
                "tenant_id": "tenant-123",
                "entity_id": str(uuid4()),
                "entity_type": "Article",
            }
        )

        event = consumer.from_json(json_string)

        assert isinstance(event, EntityCreatedEvent)

    def test_from_json_invalid(self) -> None:
        """Test that invalid JSON raises error."""
        consumer = EventConsumer()

        with pytest.raises(EventDeserializationError, match="Invalid JSON"):
            consumer.from_json("not valid json")
