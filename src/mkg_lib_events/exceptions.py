"""Custom exceptions for mkg-lib-events."""


class EventError(Exception):
    """Base exception for all event-related errors."""

    pass


class EventPublishError(EventError):
    """Raised when an event cannot be published to EventBridge."""

    def __init__(
        self,
        message: str,
        event_id: str | None = None,
        event_type: str | None = None,
        tenant_id: str | None = None,
    ) -> None:
        """Initialize EventPublishError.

        Args:
            message: Error message.
            event_id: ID of the event that failed to publish.
            event_type: Type of the event.
            tenant_id: Tenant ID associated with the event.
        """
        super().__init__(message)
        self.event_id = event_id
        self.event_type = event_type
        self.tenant_id = tenant_id


class EventDeserializationError(EventError):
    """Raised when an event cannot be deserialized."""

    def __init__(
        self,
        message: str,
        event_type: str | None = None,
        raw_data: str | None = None,
    ) -> None:
        """Initialize EventDeserializationError.

        Args:
            message: Error message.
            event_type: Type of the event that failed to deserialize.
            raw_data: Raw event data (truncated for safety).
        """
        super().__init__(message)
        self.event_type = event_type
        # Truncate raw data to avoid logging sensitive information
        self.raw_data = raw_data[:500] if raw_data and len(raw_data) > 500 else raw_data


class EventConfigurationError(EventError):
    """Raised when event configuration is invalid or missing."""

    pass


class SubscriptionError(EventError):
    """Raised when subscription management fails."""

    def __init__(
        self,
        message: str,
        rule_name: str | None = None,
        tenant_id: str | None = None,
    ) -> None:
        """Initialize SubscriptionError.

        Args:
            message: Error message.
            rule_name: Name of the EventBridge rule.
            tenant_id: Tenant ID associated with the subscription.
        """
        super().__init__(message)
        self.rule_name = rule_name
        self.tenant_id = tenant_id
