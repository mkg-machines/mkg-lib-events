"""Unit tests for event models."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from mkg_lib_events import (
    BaseEvent,
    EntityCreatedEvent,
    EntityDeletedEvent,
    EntityUpdatedEvent,
    EventMetadata,
    EventRegistry,
    SchemaCreatedEvent,
    SchemaUpdatedEvent,
    get_event_class,
)


class TestEventMetadata:
    """Tests for EventMetadata model."""

    def test_create_empty_metadata(self) -> None:
        """Test creating metadata with all defaults."""
        metadata = EventMetadata()

        assert metadata.correlation_id is None
        assert metadata.causation_id is None
        assert metadata.user_id is None
        assert metadata.request_id is None

    def test_create_metadata_with_values(self) -> None:
        """Test creating metadata with all values."""
        metadata = EventMetadata(
            correlation_id="corr-123",
            causation_id="cause-456",
            user_id="user-789",
            request_id="req-abc",
        )

        assert metadata.correlation_id == "corr-123"
        assert metadata.causation_id == "cause-456"
        assert metadata.user_id == "user-789"
        assert metadata.request_id == "req-abc"

    def test_metadata_is_immutable(self) -> None:
        """Test that metadata cannot be modified after creation."""
        metadata = EventMetadata(correlation_id="corr-123")

        with pytest.raises(ValidationError):
            metadata.correlation_id = "new-value"  # type: ignore[misc]

    def test_metadata_forbids_extra_fields(self) -> None:
        """Test that extra fields are not allowed."""
        with pytest.raises(ValidationError):
            EventMetadata(unknown_field="value")  # type: ignore[call-arg]


class TestBaseEvent:
    """Tests for BaseEvent model."""

    def test_create_base_event(self) -> None:
        """Test creating a base event with required fields."""
        event = BaseEvent(
            event_type="test.event",
            source="test-service",
            tenant_id="tenant-123",
        )

        assert event.event_type == "test.event"
        assert event.source == "test-service"
        assert event.tenant_id == "tenant-123"
        assert isinstance(event.event_id, UUID)
        assert isinstance(event.timestamp, datetime)
        assert event.version == "1.0"
        assert event.data == {}
        assert isinstance(event.metadata, EventMetadata)

    def test_tenant_id_is_required(self) -> None:
        """Test that tenant_id is required."""
        with pytest.raises(ValidationError) as exc_info:
            BaseEvent(event_type="test.event", source="test-service")  # type: ignore[call-arg]

        errors = exc_info.value.errors()
        assert any(e["loc"] == ("tenant_id",) for e in errors)

    def test_tenant_id_cannot_be_empty(self) -> None:
        """Test that tenant_id cannot be empty."""
        with pytest.raises(ValidationError):
            BaseEvent(
                event_type="test.event",
                source="test-service",
                tenant_id="",
            )

    def test_event_is_immutable(self) -> None:
        """Test that events cannot be modified after creation."""
        event = BaseEvent(
            event_type="test.event",
            source="test-service",
            tenant_id="tenant-123",
        )

        with pytest.raises(ValidationError):
            event.tenant_id = "new-tenant"  # type: ignore[misc]

    def test_event_forbids_extra_fields(self) -> None:
        """Test that extra fields are not allowed."""
        with pytest.raises(ValidationError):
            BaseEvent(
                event_type="test.event",
                source="test-service",
                tenant_id="tenant-123",
                unknown_field="value",  # type: ignore[call-arg]
            )

    def test_to_eventbridge_entry(self) -> None:
        """Test conversion to EventBridge entry format."""
        event = BaseEvent(
            event_type="test.event",
            source="test-service",
            tenant_id="tenant-123",
        )

        entry = event.to_eventbridge_entry("mkg-events")

        assert entry["EventBusName"] == "mkg-events"
        assert entry["Source"] == "test-service"
        assert entry["DetailType"] == "test.event"
        assert isinstance(entry["Detail"], str)

    def test_timestamp_is_utc(self) -> None:
        """Test that timestamp is in UTC."""
        event = BaseEvent(
            event_type="test.event",
            source="test-service",
            tenant_id="tenant-123",
        )

        assert event.timestamp.tzinfo == UTC


class TestEntityCreatedEvent:
    """Tests for EntityCreatedEvent model."""

    def test_create_entity_created_event(self) -> None:
        """Test creating an EntityCreatedEvent."""
        entity_id = UUID("550e8400-e29b-41d4-a716-446655440000")
        event = EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id=entity_id,
            entity_type="Article",
            attributes={"name": "Widget", "sku": "WDG-001"},
        )

        assert event.event_type == "entity.created"
        assert event.source == "mkg-kernel"
        assert event.tenant_id == "tenant-123"
        assert event.entity_id == entity_id
        assert event.entity_type == "Article"
        assert event.attributes == {"name": "Widget", "sku": "WDG-001"}

    def test_data_field_is_populated(self) -> None:
        """Test that data field is auto-populated."""
        entity_id = UUID("550e8400-e29b-41d4-a716-446655440000")
        event = EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id=entity_id,
            entity_type="Article",
            attributes={"name": "Widget"},
        )

        assert event.data["entity_id"] == str(entity_id)
        assert event.data["entity_type"] == "Article"
        assert event.data["attributes"] == {"name": "Widget"}

    def test_entity_type_cannot_be_empty(self) -> None:
        """Test that entity_type cannot be empty."""
        with pytest.raises(ValidationError):
            EntityCreatedEvent(
                tenant_id="tenant-123",
                entity_id=UUID("550e8400-e29b-41d4-a716-446655440000"),
                entity_type="",
            )


class TestEntityUpdatedEvent:
    """Tests for EntityUpdatedEvent model."""

    def test_create_entity_updated_event(self) -> None:
        """Test creating an EntityUpdatedEvent."""
        entity_id = UUID("550e8400-e29b-41d4-a716-446655440000")
        event = EntityUpdatedEvent(
            tenant_id="tenant-123",
            entity_id=entity_id,
            entity_type="Article",
            changed_attributes=["name", "price"],
            previous_values={"name": "Old Name", "price": 10.0},
            new_values={"name": "New Name", "price": 15.0},
        )

        assert event.event_type == "entity.updated"
        assert event.entity_id == entity_id
        assert event.changed_attributes == ["name", "price"]
        assert event.previous_values == {"name": "Old Name", "price": 10.0}
        assert event.new_values == {"name": "New Name", "price": 15.0}

    def test_data_field_contains_changes(self) -> None:
        """Test that data field contains change information."""
        event = EntityUpdatedEvent(
            tenant_id="tenant-123",
            entity_id=UUID("550e8400-e29b-41d4-a716-446655440000"),
            entity_type="Article",
            changed_attributes=["name"],
            previous_values={"name": "Old"},
            new_values={"name": "New"},
        )

        assert event.data["changed_attributes"] == ["name"]
        assert event.data["previous_values"] == {"name": "Old"}
        assert event.data["new_values"] == {"name": "New"}


class TestEntityDeletedEvent:
    """Tests for EntityDeletedEvent model."""

    def test_create_entity_deleted_event(self) -> None:
        """Test creating an EntityDeletedEvent."""
        entity_id = UUID("550e8400-e29b-41d4-a716-446655440000")
        event = EntityDeletedEvent(
            tenant_id="tenant-123",
            entity_id=entity_id,
            entity_type="Article",
        )

        assert event.event_type == "entity.deleted"
        assert event.entity_id == entity_id
        assert event.is_soft_delete is True

    def test_hard_delete(self) -> None:
        """Test creating a hard delete event."""
        event = EntityDeletedEvent(
            tenant_id="tenant-123",
            entity_id=UUID("550e8400-e29b-41d4-a716-446655440000"),
            entity_type="Article",
            is_soft_delete=False,
        )

        assert event.is_soft_delete is False
        assert event.data["is_soft_delete"] is False


class TestSchemaCreatedEvent:
    """Tests for SchemaCreatedEvent model."""

    def test_create_schema_created_event(self) -> None:
        """Test creating a SchemaCreatedEvent."""
        schema_id = UUID("550e8400-e29b-41d4-a716-446655440000")
        event = SchemaCreatedEvent(
            tenant_id="tenant-123",
            schema_id=schema_id,
            entity_type="Article",
            attributes=[
                {"name": "sku", "type": "string", "required": True},
                {"name": "price", "type": "decimal", "required": False},
            ],
        )

        assert event.event_type == "schema.created"
        assert event.source == "mkg-kernel"
        assert event.schema_id == schema_id
        assert event.entity_type == "Article"
        assert len(event.attributes) == 2

    def test_data_field_contains_schema_info(self) -> None:
        """Test that data field contains schema information."""
        event = SchemaCreatedEvent(
            tenant_id="tenant-123",
            schema_id=UUID("550e8400-e29b-41d4-a716-446655440000"),
            entity_type="Article",
            attributes=[{"name": "sku", "type": "string"}],
        )

        assert event.data["entity_type"] == "Article"
        assert event.data["attributes"] == [{"name": "sku", "type": "string"}]


class TestSchemaUpdatedEvent:
    """Tests for SchemaUpdatedEvent model."""

    def test_create_schema_updated_event(self) -> None:
        """Test creating a SchemaUpdatedEvent."""
        schema_id = UUID("550e8400-e29b-41d4-a716-446655440000")
        event = SchemaUpdatedEvent(
            tenant_id="tenant-123",
            schema_id=schema_id,
            entity_type="Article",
            added_attributes=[{"name": "weight", "type": "decimal"}],
            removed_attributes=["old_field"],
            modified_attributes=[],
        )

        assert event.event_type == "schema.updated"
        assert event.added_attributes == [{"name": "weight", "type": "decimal"}]
        assert event.removed_attributes == ["old_field"]
        assert event.modified_attributes == []


class TestEventRegistry:
    """Tests for EventRegistry."""

    def test_core_events_are_registered(self) -> None:
        """Test that core event types are registered on import."""
        assert EventRegistry.get("entity.created") == EntityCreatedEvent
        assert EventRegistry.get("entity.updated") == EntityUpdatedEvent
        assert EventRegistry.get("entity.deleted") == EntityDeletedEvent
        assert EventRegistry.get("schema.created") == SchemaCreatedEvent
        assert EventRegistry.get("schema.updated") == SchemaUpdatedEvent

    def test_deserialize_entity_created_event(self) -> None:
        """Test deserializing an EntityCreatedEvent."""
        event_data = {
            "event_type": "entity.created",
            "tenant_id": "tenant-123",
            "entity_id": "550e8400-e29b-41d4-a716-446655440000",
            "entity_type": "Article",
            "attributes": {"name": "Widget"},
        }

        event = EventRegistry.deserialize(event_data)

        assert isinstance(event, EntityCreatedEvent)
        assert event.tenant_id == "tenant-123"
        assert event.entity_type == "Article"

    def test_deserialize_unknown_event_type_raises(self) -> None:
        """Test that deserializing unknown event type raises ValueError."""
        event_data = {
            "event_type": "unknown.event",
            "tenant_id": "tenant-123",
        }

        with pytest.raises(ValueError, match="Unknown event type"):
            EventRegistry.deserialize(event_data)

    def test_deserialize_missing_event_type_raises(self) -> None:
        """Test that deserializing without event_type raises ValueError."""
        event_data = {"tenant_id": "tenant-123"}

        with pytest.raises(ValueError, match="must contain 'event_type'"):
            EventRegistry.deserialize(event_data)

    def test_register_custom_event(self) -> None:
        """Test registering a custom event type."""

        class CustomEvent(BaseEvent):
            event_type: str = "custom.test"

        EventRegistry.register("custom.test", CustomEvent)

        try:
            assert EventRegistry.get("custom.test") == CustomEvent
        finally:
            EventRegistry.unregister("custom.test")

    def test_register_duplicate_raises(self) -> None:
        """Test that registering duplicate event type raises ValueError."""
        with pytest.raises(ValueError, match="already registered"):
            EventRegistry.register("entity.created", EntityCreatedEvent)

    def test_list_event_types(self) -> None:
        """Test listing all registered event types."""
        event_types = EventRegistry.list_event_types()

        assert "entity.created" in event_types
        assert "entity.updated" in event_types
        assert "entity.deleted" in event_types
        assert "schema.created" in event_types
        assert "schema.updated" in event_types

    def test_get_event_class_convenience_function(self) -> None:
        """Test get_event_class convenience function."""
        assert get_event_class("entity.created") == EntityCreatedEvent
        assert get_event_class("unknown.event") is None
