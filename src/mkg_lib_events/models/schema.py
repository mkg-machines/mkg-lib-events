"""Schema event models for the MKG Platform.

These events are published by the Kernel when entity type schemas
are created or updated. Extensions can subscribe to these events
to update their internal state or caches.
"""

from typing import Any
from uuid import UUID

from pydantic import Field

from mkg_lib_events.models.base import BaseEvent


class SchemaCreatedEvent(BaseEvent):
    """Event published when a new entity type schema is created.

    Attributes:
        schema_id: Unique identifier of the created schema.
        entity_type: Name of the entity type.
        attributes: List of attribute definitions.

    Example:
        ```python
        event = SchemaCreatedEvent(
            tenant_id="tenant-123",
            schema_id="schema-456",
            entity_type="Article",
            attributes=[
                {"name": "sku", "type": "string", "required": True},
                {"name": "price", "type": "decimal", "required": False},
            ],
        )
        ```
    """

    event_type: str = Field(
        default="schema.created",
        description="Event type identifier",
    )
    source: str = Field(
        default="mkg-kernel",
        description="Source service",
    )
    schema_id: UUID = Field(
        description="Unique identifier of the created schema",
    )
    entity_type: str = Field(
        description="Name of the entity type",
        min_length=1,
    )
    attributes: list[dict[str, Any]] = Field(
        default_factory=list,
        description="List of attribute definitions",
    )

    def model_post_init(self, __context: Any) -> None:
        """Populate data field with schema information."""
        if not self.data:
            object.__setattr__(
                self,
                "data",
                {
                    "schema_id": str(self.schema_id),
                    "entity_type": self.entity_type,
                    "attributes": self.attributes,
                },
            )


class SchemaUpdatedEvent(BaseEvent):
    """Event published when an entity type schema is updated.

    Attributes:
        schema_id: Unique identifier of the updated schema.
        entity_type: Name of the entity type.
        added_attributes: New attributes added to the schema.
        removed_attributes: Attributes removed from the schema.
        modified_attributes: Attributes that were modified.

    Example:
        ```python
        event = SchemaUpdatedEvent(
            tenant_id="tenant-123",
            schema_id="schema-456",
            entity_type="Article",
            added_attributes=[{"name": "weight", "type": "decimal"}],
            removed_attributes=[],
            modified_attributes=[],
        )
        ```
    """

    event_type: str = Field(
        default="schema.updated",
        description="Event type identifier",
    )
    source: str = Field(
        default="mkg-kernel",
        description="Source service",
    )
    schema_id: UUID = Field(
        description="Unique identifier of the updated schema",
    )
    entity_type: str = Field(
        description="Name of the entity type",
        min_length=1,
    )
    added_attributes: list[dict[str, Any]] = Field(
        default_factory=list,
        description="New attributes added to the schema",
    )
    removed_attributes: list[str] = Field(
        default_factory=list,
        description="Names of attributes removed from the schema",
    )
    modified_attributes: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Attributes that were modified",
    )

    def model_post_init(self, __context: Any) -> None:
        """Populate data field with schema information."""
        if not self.data:
            object.__setattr__(
                self,
                "data",
                {
                    "schema_id": str(self.schema_id),
                    "entity_type": self.entity_type,
                    "added_attributes": self.added_attributes,
                    "removed_attributes": self.removed_attributes,
                    "modified_attributes": self.modified_attributes,
                },
            )
