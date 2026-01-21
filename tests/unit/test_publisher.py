"""Unit tests for EventPublisher."""

from uuid import UUID, uuid4

import pytest

from mkg_lib_events import EntityCreatedEvent
from mkg_lib_events.exceptions import EventConfigurationError, EventPublishError
from mkg_lib_events.publisher import EventPublisher


class TestEventPublisher:
    """Tests for EventPublisher."""

    def test_publisher_requires_event_bus_name(self, mocker) -> None:
        """Test that publisher requires event bus name."""
        mocker.patch.dict("os.environ", {}, clear=True)

        with pytest.raises(EventConfigurationError):
            EventPublisher()

    def test_publisher_uses_env_variable(self, mocker) -> None:
        """Test that publisher uses MKG_EVENT_BUS_NAME env variable."""
        mocker.patch.dict("os.environ", {"MKG_EVENT_BUS_NAME": "test-bus"})
        mock_boto = mocker.patch("boto3.client")

        publisher = EventPublisher()

        assert publisher.event_bus_name == "test-bus"
        mock_boto.assert_called_once()

    def test_publish_single_event(self, mocker) -> None:
        """Test publishing a single event."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"
        mock_client.put_events.return_value = {
            "FailedEntryCount": 0,
            "Entries": [{"EventId": "evt-123"}],
        }

        publisher = EventPublisher(client=mock_client)

        event = EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id=uuid4(),
            entity_type="Article",
        )

        event_id = publisher.publish(event)

        assert event_id == str(event.event_id)
        mock_client.put_events.assert_called_once()

    def test_publish_validates_tenant_id(self, mocker) -> None:
        """Test that publish validates tenant_id."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"

        publisher = EventPublisher(client=mock_client)

        # Create event with empty tenant_id by manipulating the model
        from mkg_lib_events.models.base import BaseEvent

        event = mocker.MagicMock(spec=BaseEvent)
        event.tenant_id = ""
        event.event_id = uuid4()
        event.event_type = "test.event"
        event.source = "test"

        with pytest.raises(EventPublishError, match="tenant_id"):
            publisher.publish(event)

    def test_publish_handles_eventbridge_error(self, mocker) -> None:
        """Test that publish handles EventBridge entry errors."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"
        mock_client.put_events.return_value = {
            "FailedEntryCount": 1,
            "Entries": [
                {"ErrorCode": "InvalidEventBus", "ErrorMessage": "Bus not found"}
            ],
        }

        publisher = EventPublisher(client=mock_client)

        event = EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id=uuid4(),
            entity_type="Article",
        )

        with pytest.raises(EventPublishError, match="InvalidEventBus"):
            publisher.publish(event)

    def test_publish_batch_empty_list(self, mocker) -> None:
        """Test that publish_batch handles empty list."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"

        publisher = EventPublisher(client=mock_client)

        result = publisher.publish_batch([])

        assert result == []
        mock_client.put_events.assert_not_called()

    def test_publish_batch_multiple_events(self, mocker) -> None:
        """Test publishing multiple events in batch."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"
        mock_client.put_events.return_value = {
            "FailedEntryCount": 0,
            "Entries": [
                {"EventId": "evt-1"},
                {"EventId": "evt-2"},
                {"EventId": "evt-3"},
            ],
        }

        publisher = EventPublisher(client=mock_client)

        events = [
            EntityCreatedEvent(
                tenant_id="tenant-123",
                entity_id=uuid4(),
                entity_type="Article",
            )
            for _ in range(3)
        ]

        result = publisher.publish_batch(events)

        assert len(result) == 3
        mock_client.put_events.assert_called_once()

    def test_publish_batch_splits_large_batches(self, mocker) -> None:
        """Test that batch publishing splits into chunks of 10."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"

        # Return success for each batch
        mock_client.put_events.return_value = {
            "FailedEntryCount": 0,
            "Entries": [{"EventId": f"evt-{i}"} for i in range(10)],
        }

        publisher = EventPublisher(client=mock_client)

        # Create 25 events (should be split into 3 batches: 10, 10, 5)
        events = [
            EntityCreatedEvent(
                tenant_id="tenant-123",
                entity_id=uuid4(),
                entity_type="Article",
            )
            for _ in range(25)
        ]

        # Mock to return correct number of entries per batch
        def put_events_side_effect(Entries):
            return {
                "FailedEntryCount": 0,
                "Entries": [{"EventId": f"evt-{i}"} for i in range(len(Entries))],
            }

        mock_client.put_events.side_effect = put_events_side_effect

        result = publisher.publish_batch(events)

        assert len(result) == 25
        assert mock_client.put_events.call_count == 3

    def test_publish_batch_handles_partial_failure(self, mocker) -> None:
        """Test that batch publish handles partial failures."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"
        mock_client.put_events.return_value = {
            "FailedEntryCount": 1,
            "Entries": [
                {"EventId": "evt-1"},
                {"ErrorCode": "ThrottlingException", "ErrorMessage": "Rate exceeded"},
            ],
        }

        publisher = EventPublisher(client=mock_client)

        events = [
            EntityCreatedEvent(
                tenant_id="tenant-123",
                entity_id=uuid4(),
                entity_type="Article",
            )
            for _ in range(2)
        ]

        with pytest.raises(EventPublishError, match="ThrottlingException"):
            publisher.publish_batch(events)


class TestEventPublisherEventBridgeEntry:
    """Tests for EventBridge entry format."""

    def test_event_entry_format(self, mocker) -> None:
        """Test that events are converted to correct EventBridge format."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"
        mock_client.put_events.return_value = {
            "FailedEntryCount": 0,
            "Entries": [{"EventId": "evt-123"}],
        }

        publisher = EventPublisher(client=mock_client)

        event = EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id=UUID("550e8400-e29b-41d4-a716-446655440000"),
            entity_type="Article",
        )

        publisher.publish(event)

        # Verify the entry format
        call_args = mock_client.put_events.call_args
        entries = call_args[0][0] if call_args[0] else call_args[1].get("entries", [])

        assert len(entries) == 1
        entry = entries[0]

        assert entry["EventBusName"] == "test-bus"
        assert entry["Source"] == "mkg-kernel"
        assert entry["DetailType"] == "entity.created"
        assert "tenant-123" in entry["Detail"]
