"""Event validator for MKG Platform.

Provides schema validation for events against the Event-Type Registry
and custom validation rules.
"""

from dataclasses import dataclass, field
from typing import Any

from pydantic import ValidationError

from mkg_lib_events.logging import get_logger
from mkg_lib_events.models.base import BaseEvent
from mkg_lib_events.registry import EventRegistry

logger = get_logger(__name__, component="event_validator")


@dataclass
class ValidationResult:
    """Result of event validation.

    Attributes:
        is_valid: Whether the event is valid.
        event_type: Type of the validated event.
        errors: List of validation error messages.
        warnings: List of validation warnings.
    """

    is_valid: bool
    event_type: str | None = None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class EventValidator:
    """Validates events against schemas and custom rules.

    Provides validation of event structure, required fields,
    and custom business rules.

    Example:
        ```python
        validator = EventValidator(
            require_correlation_id=True,
            allowed_sources=["mkg-kernel", "mkg-extension-search"],
        )

        result = validator.validate(event_data)

        if not result.is_valid:
            print(f"Validation errors: {result.errors}")
        ```
    """

    def __init__(
        self,
        require_correlation_id: bool = False,
        allowed_sources: list[str] | None = None,
        allowed_event_types: list[str] | None = None,
        allowed_tenant_ids: list[str] | None = None,
        custom_validators: list[Any] | None = None,
    ) -> None:
        """Initialize EventValidator.

        Args:
            require_correlation_id: Whether correlation_id is required.
            allowed_sources: List of allowed source values.
            allowed_event_types: List of allowed event types.
            allowed_tenant_ids: List of allowed tenant IDs.
            custom_validators: List of custom validator functions.
        """
        self.require_correlation_id = require_correlation_id
        self.allowed_sources = set(allowed_sources) if allowed_sources else None
        self.allowed_event_types = (
            set(allowed_event_types) if allowed_event_types else None
        )
        self.allowed_tenant_ids = (
            set(allowed_tenant_ids) if allowed_tenant_ids else None
        )
        self.custom_validators = custom_validators or []

        logger.info(
            "event_validator_initialized",
            require_correlation_id=require_correlation_id,
            source_filter_enabled=allowed_sources is not None,
            event_type_filter_enabled=allowed_event_types is not None,
            tenant_filter_enabled=allowed_tenant_ids is not None,
            custom_validator_count=len(self.custom_validators),
        )

    def validate(self, data: dict[str, Any]) -> ValidationResult:
        """Validate event data against schema and rules.

        Args:
            data: Dictionary containing event data.

        Returns:
            ValidationResult with validation status and any errors.
        """
        errors: list[str] = []
        warnings: list[str] = []
        event_type = data.get("event_type")

        # Check required fields
        if not event_type:
            errors.append("Missing required field: event_type")
            return ValidationResult(
                is_valid=False,
                event_type=None,
                errors=errors,
            )

        # Check if event type is registered
        event_class = EventRegistry.get(event_type)
        if event_class is None:
            warnings.append(f"Event type '{event_type}' is not registered in registry")

        # Validate against allowed event types
        if self.allowed_event_types and event_type not in self.allowed_event_types:
            errors.append(
                f"Event type '{event_type}' not in allowed types: "
                f"{sorted(self.allowed_event_types)}"
            )

        # Validate tenant_id
        tenant_id = data.get("tenant_id")
        if not tenant_id:
            errors.append("Missing required field: tenant_id")
        elif self.allowed_tenant_ids and tenant_id not in self.allowed_tenant_ids:
            errors.append(f"Tenant '{tenant_id}' not in allowed tenants")

        # Validate source
        source = data.get("source")
        if not source:
            errors.append("Missing required field: source")
        elif self.allowed_sources and source not in self.allowed_sources:
            errors.append(
                f"Source '{source}' not in allowed sources: "
                f"{sorted(self.allowed_sources)}"
            )

        # Validate correlation_id if required
        if self.require_correlation_id:
            metadata = data.get("metadata", {})
            correlation_id = metadata.get("correlation_id") if metadata else None
            if not correlation_id:
                errors.append("Missing required field: metadata.correlation_id")

        # Validate against Pydantic schema if registered
        if event_class is not None and not errors:
            try:
                event_class.model_validate(data)
            except ValidationError as e:
                for error in e.errors():
                    loc = ".".join(str(x) for x in error["loc"])
                    msg = error["msg"]
                    errors.append(f"Schema validation error at '{loc}': {msg}")

        # Run custom validators
        for validator in self.custom_validators:
            try:
                validator_errors = validator(data)
                if validator_errors:
                    errors.extend(validator_errors)
            except Exception as e:
                errors.append(f"Custom validator error: {e}")

        # Log validation result
        if errors:
            logger.warning(
                "event_validation_failed",
                event_type=event_type,
                error_count=len(errors),
                errors=errors[:5],  # Limit logged errors
            )
        elif warnings:
            logger.debug(
                "event_validation_warnings",
                event_type=event_type,
                warning_count=len(warnings),
            )

        return ValidationResult(
            is_valid=len(errors) == 0,
            event_type=event_type,
            errors=errors,
            warnings=warnings,
        )

    def validate_event(self, event: BaseEvent) -> ValidationResult:
        """Validate an already-parsed event object.

        Args:
            event: Event object to validate.

        Returns:
            ValidationResult with validation status.
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Validate against allowed event types
        if (
            self.allowed_event_types
            and event.event_type not in self.allowed_event_types
        ):
            errors.append(f"Event type '{event.event_type}' not in allowed types")

        # Validate tenant_id
        if self.allowed_tenant_ids and event.tenant_id not in self.allowed_tenant_ids:
            errors.append(f"Tenant '{event.tenant_id}' not in allowed tenants")

        # Validate source
        if self.allowed_sources and event.source not in self.allowed_sources:
            errors.append(f"Source '{event.source}' not in allowed sources")

        # Validate correlation_id if required
        if self.require_correlation_id and not event.metadata.correlation_id:
            errors.append("Missing required field: metadata.correlation_id")

        return ValidationResult(
            is_valid=len(errors) == 0,
            event_type=event.event_type,
            errors=errors,
            warnings=warnings,
        )

    def is_registered_type(self, event_type: str) -> bool:
        """Check if an event type is registered.

        Args:
            event_type: Event type to check.

        Returns:
            True if registered, False otherwise.
        """
        return EventRegistry.get(event_type) is not None

    def get_registered_types(self) -> list[str]:
        """Get all registered event types.

        Returns:
            List of registered event type strings.
        """
        return EventRegistry.list_event_types()


def create_custom_validator(
    field: str,
    validator_func: Any,
    error_message: str,
) -> Any:
    """Create a custom validator function.

    Helper for creating custom validators for use with EventValidator.

    Args:
        field: Field name to validate.
        validator_func: Function that returns True if valid.
        error_message: Error message if validation fails.

    Returns:
        Validator function compatible with EventValidator.

    Example:
        ```python
        # Create a validator that ensures entity_type is "Article"
        article_validator = create_custom_validator(
            field="entity_type",
            validator_func=lambda v: v == "Article",
            error_message="entity_type must be 'Article'",
        )

        validator = EventValidator(custom_validators=[article_validator])
        ```
    """

    def validator(data: dict[str, Any]) -> list[str]:
        value = data.get(field)
        if value is not None and not validator_func(value):
            return [error_message]
        return []

    return validator
