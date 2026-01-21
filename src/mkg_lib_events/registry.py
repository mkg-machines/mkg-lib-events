"""Event type registry for deserialization.

Provides a registry that maps event type strings to their corresponding
Pydantic model classes, enabling type-safe event deserialization.
"""

from typing import TypeVar

from mkg_lib_events.models.base import BaseEvent
from mkg_lib_events.models.entity import (
    EntityCreatedEvent,
    EntityDeletedEvent,
    EntityUpdatedEvent,
)
from mkg_lib_events.models.schema import SchemaCreatedEvent, SchemaUpdatedEvent

T = TypeVar("T", bound=BaseEvent)


class EventRegistry:
    """Registry for event type to model class mapping.

    Provides methods to register custom event types and deserialize
    events from their JSON representation.

    Example:
        ```python
        # Register a custom event type
        EventRegistry.register("custom.event", CustomEvent)

        # Deserialize an event
        event_data = {"event_type": "entity.created", "tenant_id": "t-1", ...}
        event = EventRegistry.deserialize(event_data)
        ```
    """

    _registry: dict[str, type[BaseEvent]] = {}

    @classmethod
    def register(cls, event_type: str, event_class: type[BaseEvent]) -> None:
        """Register an event type with its model class.

        Args:
            event_type: The event type string (e.g., 'entity.created').
            event_class: The Pydantic model class for this event type.

        Raises:
            ValueError: If event_type is already registered.
        """
        if event_type in cls._registry:
            raise ValueError(f"Event type '{event_type}' is already registered")
        cls._registry[event_type] = event_class

    @classmethod
    def unregister(cls, event_type: str) -> None:
        """Unregister an event type.

        Args:
            event_type: The event type string to unregister.
        """
        cls._registry.pop(event_type, None)

    @classmethod
    def get(cls, event_type: str) -> type[BaseEvent] | None:
        """Get the model class for an event type.

        Args:
            event_type: The event type string.

        Returns:
            The model class if registered, None otherwise.
        """
        return cls._registry.get(event_type)

    @classmethod
    def deserialize(cls, data: dict[str, object]) -> BaseEvent:
        """Deserialize event data to the appropriate model class.

        Args:
            data: Dictionary containing event data with 'event_type' field.

        Returns:
            Instance of the appropriate event model class.

        Raises:
            ValueError: If event_type is missing or not registered.
        """
        event_type = data.get("event_type")
        if not event_type or not isinstance(event_type, str):
            raise ValueError("Event data must contain 'event_type' field")

        event_class = cls._registry.get(event_type)
        if event_class is None:
            raise ValueError(f"Unknown event type: '{event_type}'")

        return event_class.model_validate(data)

    @classmethod
    def list_event_types(cls) -> list[str]:
        """List all registered event types.

        Returns:
            List of registered event type strings.
        """
        return list(cls._registry.keys())

    @classmethod
    def clear(cls) -> None:
        """Clear all registered event types.

        Primarily used for testing.
        """
        cls._registry.clear()


def register_event(event_type: str) -> type[T]:
    """Decorator to register an event class with the registry.

    Args:
        event_type: The event type string for this class.

    Returns:
        Decorator function that registers the class.

    Example:
        ```python
        @register_event("custom.event")
        class CustomEvent(BaseEvent):
            event_type: str = "custom.event"
            ...
        ```
    """

    def decorator(cls: type[T]) -> type[T]:
        EventRegistry.register(event_type, cls)  # type: ignore[arg-type]
        return cls

    return decorator  # type: ignore[return-value]


def get_event_class(event_type: str) -> type[BaseEvent] | None:
    """Get the model class for an event type.

    Convenience function that delegates to EventRegistry.get().

    Args:
        event_type: The event type string.

    Returns:
        The model class if registered, None otherwise.
    """
    return EventRegistry.get(event_type)


def _register_core_events() -> None:
    """Register all core event types with the registry."""
    core_events: list[tuple[str, type[BaseEvent]]] = [
        ("entity.created", EntityCreatedEvent),
        ("entity.updated", EntityUpdatedEvent),
        ("entity.deleted", EntityDeletedEvent),
        ("schema.created", SchemaCreatedEvent),
        ("schema.updated", SchemaUpdatedEvent),
    ]

    for event_type, event_class in core_events:
        if event_type not in EventRegistry._registry:
            EventRegistry.register(event_type, event_class)


# Register core events on module import
_register_core_events()
