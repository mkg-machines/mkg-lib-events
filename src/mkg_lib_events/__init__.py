"""MKG Platform Event Library.

This library provides event schemas, EventBridge integration, and utilities
for event-driven communication between Kernel and Extensions.
"""

from mkg_lib_events.client import EventBusClient
from mkg_lib_events.consumer import EventConsumer
from mkg_lib_events.dlq import DeadLetterHandler, FailedEvent
from mkg_lib_events.exceptions import (
    EventConfigurationError,
    EventDeserializationError,
    EventError,
    EventPublishError,
    SubscriptionError,
)
from mkg_lib_events.handler import (
    BaseEventHandler,
    HandlerResult,
    get_current_correlation_id,
    get_current_tenant,
)
from mkg_lib_events.models.base import BaseEvent, EventMetadata
from mkg_lib_events.models.entity import (
    EntityCreatedEvent,
    EntityDeletedEvent,
    EntityUpdatedEvent,
)
from mkg_lib_events.models.schema import SchemaCreatedEvent, SchemaUpdatedEvent
from mkg_lib_events.publisher import EventPublisher
from mkg_lib_events.registry import EventRegistry, get_event_class, register_event
from mkg_lib_events.retry import (
    BackoffStrategy,
    RetryConfig,
    RetryPolicy,
    RetryResult,
)
from mkg_lib_events.subscription import SubscriptionManager
from mkg_lib_events.validator import (
    EventValidator,
    ValidationResult,
    create_custom_validator,
)

__all__ = [
    # Base
    "BaseEvent",
    "EventMetadata",
    # Entity Events
    "EntityCreatedEvent",
    "EntityUpdatedEvent",
    "EntityDeletedEvent",
    # Schema Events
    "SchemaCreatedEvent",
    "SchemaUpdatedEvent",
    # Registry
    "EventRegistry",
    "register_event",
    "get_event_class",
    # EventBridge Integration
    "EventBusClient",
    "EventPublisher",
    "EventConsumer",
    "SubscriptionManager",
    # Handler
    "BaseEventHandler",
    "HandlerResult",
    "get_current_tenant",
    "get_current_correlation_id",
    # Retry
    "RetryPolicy",
    "RetryConfig",
    "RetryResult",
    "BackoffStrategy",
    # Dead Letter Queue
    "DeadLetterHandler",
    "FailedEvent",
    # Validator
    "EventValidator",
    "ValidationResult",
    "create_custom_validator",
    # Exceptions
    "EventError",
    "EventPublishError",
    "EventDeserializationError",
    "EventConfigurationError",
    "SubscriptionError",
]

__version__ = "0.1.0"
