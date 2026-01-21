"""Base event model for the MKG Platform.

All events in the MKG Platform inherit from BaseEvent which provides
common fields for event identification, routing, and tracing.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class EventMetadata(BaseModel):
    """Metadata associated with an event.

    Contains optional contextual information that can be attached to any event
    for tracing, debugging, or audit purposes.
    """

    correlation_id: str | None = Field(
        default=None,
        description="Correlation ID for request tracing across services",
    )
    causation_id: str | None = Field(
        default=None,
        description="ID of the event that caused this event",
    )
    user_id: str | None = Field(
        default=None,
        description="ID of the user who triggered this event",
    )
    request_id: str | None = Field(
        default=None,
        description="Original API request ID",
    )

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )


class BaseEvent(BaseModel):
    """Base class for all MKG Platform events.

    Provides common fields required for event identification, routing,
    tenant isolation, and versioning.

    Attributes:
        event_id: Unique identifier for this event instance.
        event_type: Type identifier for routing (e.g., 'entity.created').
        source: Service that produced this event (e.g., 'mkg-kernel').
        tenant_id: Tenant identifier for multi-tenant isolation.
        timestamp: UTC timestamp when the event was created.
        version: Schema version for backward compatibility.
        data: Event-specific payload data.
        metadata: Optional metadata for tracing and debugging.

    Example:
        ```python
        event = BaseEvent(
            event_type="entity.created",
            source="mkg-kernel",
            tenant_id="tenant-123",
            data={"entity_id": "ent-456", "entity_type": "Article"},
        )
        ```
    """

    event_id: UUID = Field(
        default_factory=uuid4,
        description="Unique identifier for this event instance",
    )
    event_type: str = Field(
        description="Event type identifier for routing (e.g., 'entity.created')",
    )
    source: str = Field(
        description="Service that produced this event (e.g., 'mkg-kernel')",
    )
    tenant_id: str = Field(
        description="Tenant identifier for multi-tenant isolation",
        min_length=1,
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp when the event was created",
    )
    version: str = Field(
        default="1.0",
        description="Schema version for backward compatibility",
    )
    data: dict[str, Any] = Field(
        default_factory=dict,
        description="Event-specific payload data",
    )
    metadata: EventMetadata = Field(
        default_factory=EventMetadata,
        description="Optional metadata for tracing and debugging",
    )

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        json_schema_extra={
            "example": {
                "event_id": "550e8400-e29b-41d4-a716-446655440000",
                "event_type": "entity.created",
                "source": "mkg-kernel",
                "tenant_id": "tenant-123",
                "timestamp": "2024-01-15T10:30:00Z",
                "version": "1.0",
                "data": {"entity_id": "ent-456"},
                "metadata": {"correlation_id": "corr-789"},
            }
        },
    )

    def to_eventbridge_entry(self, event_bus_name: str) -> dict[str, Any]:
        """Convert event to AWS EventBridge PutEvents entry format.

        Args:
            event_bus_name: Name of the EventBridge event bus.

        Returns:
            Dictionary formatted for EventBridge PutEvents API.
        """
        return {
            "EventBusName": event_bus_name,
            "Source": self.source,
            "DetailType": self.event_type,
            "Detail": self.model_dump_json(),
        }
