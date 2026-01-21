"""Entity event models for the MKG Platform.

These events are published by the Kernel when entities are created,
updated, or deleted. Extensions can subscribe to these events to
react to entity changes.
"""

from typing import Any
from uuid import UUID

from pydantic import Field

from mkg_lib_events.models.base import BaseEvent


class EntityCreatedEvent(BaseEvent):
    """Event published when a new entity is created.

    Attributes:
        entity_id: Unique identifier of the created entity.
        entity_type: Type of the entity (e.g., 'Article', 'Asset').
        attributes: Initial attribute values of the entity.

    Example:
        ```python
        event = EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id="ent-456",
            entity_type="Article",
            attributes={"name": "Widget", "sku": "WDG-001"},
        )
        ```
    """

    event_type: str = Field(
        default="entity.created",
        description="Event type identifier",
    )
    source: str = Field(
        default="mkg-kernel",
        description="Source service",
    )
    entity_id: UUID = Field(
        description="Unique identifier of the created entity",
    )
    entity_type: str = Field(
        description="Type of the entity (e.g., 'Article', 'Asset')",
        min_length=1,
    )
    attributes: dict[str, Any] = Field(
        default_factory=dict,
        description="Initial attribute values of the entity",
    )

    def model_post_init(self, __context: Any) -> None:
        """Populate data field with entity information."""
        if not self.data:
            object.__setattr__(
                self,
                "data",
                {
                    "entity_id": str(self.entity_id),
                    "entity_type": self.entity_type,
                    "attributes": self.attributes,
                },
            )


class EntityUpdatedEvent(BaseEvent):
    """Event published when an entity is updated.

    Attributes:
        entity_id: Unique identifier of the updated entity.
        entity_type: Type of the entity.
        changed_attributes: Attributes that were modified.
        previous_values: Previous values of changed attributes.
        new_values: New values of changed attributes.

    Example:
        ```python
        event = EntityUpdatedEvent(
            tenant_id="tenant-123",
            entity_id="ent-456",
            entity_type="Article",
            changed_attributes=["name", "price"],
            previous_values={"name": "Old Name", "price": 10.0},
            new_values={"name": "New Name", "price": 15.0},
        )
        ```
    """

    event_type: str = Field(
        default="entity.updated",
        description="Event type identifier",
    )
    source: str = Field(
        default="mkg-kernel",
        description="Source service",
    )
    entity_id: UUID = Field(
        description="Unique identifier of the updated entity",
    )
    entity_type: str = Field(
        description="Type of the entity",
        min_length=1,
    )
    changed_attributes: list[str] = Field(
        default_factory=list,
        description="List of attribute names that were modified",
    )
    previous_values: dict[str, Any] = Field(
        default_factory=dict,
        description="Previous values of changed attributes",
    )
    new_values: dict[str, Any] = Field(
        default_factory=dict,
        description="New values of changed attributes",
    )

    def model_post_init(self, __context: Any) -> None:
        """Populate data field with entity information."""
        if not self.data:
            object.__setattr__(
                self,
                "data",
                {
                    "entity_id": str(self.entity_id),
                    "entity_type": self.entity_type,
                    "changed_attributes": self.changed_attributes,
                    "previous_values": self.previous_values,
                    "new_values": self.new_values,
                },
            )


class EntityDeletedEvent(BaseEvent):
    """Event published when an entity is deleted.

    Attributes:
        entity_id: Unique identifier of the deleted entity.
        entity_type: Type of the entity.
        is_soft_delete: Whether this is a soft delete (recoverable).

    Example:
        ```python
        event = EntityDeletedEvent(
            tenant_id="tenant-123",
            entity_id="ent-456",
            entity_type="Article",
            is_soft_delete=True,
        )
        ```
    """

    event_type: str = Field(
        default="entity.deleted",
        description="Event type identifier",
    )
    source: str = Field(
        default="mkg-kernel",
        description="Source service",
    )
    entity_id: UUID = Field(
        description="Unique identifier of the deleted entity",
    )
    entity_type: str = Field(
        description="Type of the entity",
        min_length=1,
    )
    is_soft_delete: bool = Field(
        default=True,
        description="Whether this is a soft delete (recoverable)",
    )

    def model_post_init(self, __context: Any) -> None:
        """Populate data field with entity information."""
        if not self.data:
            object.__setattr__(
                self,
                "data",
                {
                    "entity_id": str(self.entity_id),
                    "entity_type": self.entity_type,
                    "is_soft_delete": self.is_soft_delete,
                },
            )
