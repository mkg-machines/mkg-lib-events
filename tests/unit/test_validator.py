"""Unit tests for EventValidator."""

from uuid import uuid4

from mkg_lib_events import EntityCreatedEvent
from mkg_lib_events.validator import (
    EventValidator,
    ValidationResult,
    create_custom_validator,
)


class TestEventValidator:
    """Tests for EventValidator."""

    def _create_valid_event_data(self) -> dict:
        """Helper to create valid event data."""
        return {
            "event_type": "entity.created",
            "source": "mkg-kernel",
            "tenant_id": "tenant-123",
            "entity_id": str(uuid4()),
            "entity_type": "Article",
        }

    def test_validate_valid_event(self) -> None:
        """Test validating a valid event."""
        validator = EventValidator()
        data = self._create_valid_event_data()

        result = validator.validate(data)

        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_validate_missing_event_type(self) -> None:
        """Test validation fails without event_type."""
        validator = EventValidator()
        data = self._create_valid_event_data()
        del data["event_type"]

        result = validator.validate(data)

        assert result.is_valid is False
        assert any("event_type" in e for e in result.errors)

    def test_validate_missing_tenant_id(self) -> None:
        """Test validation fails without tenant_id."""
        validator = EventValidator()
        data = self._create_valid_event_data()
        del data["tenant_id"]

        result = validator.validate(data)

        assert result.is_valid is False
        assert any("tenant_id" in e for e in result.errors)

    def test_validate_missing_source(self) -> None:
        """Test validation fails without source."""
        validator = EventValidator()
        data = self._create_valid_event_data()
        del data["source"]

        result = validator.validate(data)

        assert result.is_valid is False
        assert any("source" in e for e in result.errors)

    def test_allowed_sources_filter(self) -> None:
        """Test source filtering."""
        validator = EventValidator(allowed_sources=["mkg-kernel"])
        data = self._create_valid_event_data()

        # Valid source
        result = validator.validate(data)
        assert result.is_valid is True

        # Invalid source
        data["source"] = "unknown-service"
        result = validator.validate(data)
        assert result.is_valid is False
        assert any("source" in e.lower() for e in result.errors)

    def test_allowed_event_types_filter(self) -> None:
        """Test event type filtering."""
        validator = EventValidator(allowed_event_types=["entity.created"])
        data = self._create_valid_event_data()

        # Valid event type
        result = validator.validate(data)
        assert result.is_valid is True

        # Invalid event type
        data["event_type"] = "entity.deleted"
        result = validator.validate(data)
        assert result.is_valid is False

    def test_allowed_tenant_ids_filter(self) -> None:
        """Test tenant ID filtering."""
        validator = EventValidator(allowed_tenant_ids=["tenant-123", "tenant-456"])
        data = self._create_valid_event_data()

        # Valid tenant
        result = validator.validate(data)
        assert result.is_valid is True

        # Invalid tenant
        data["tenant_id"] = "tenant-other"
        result = validator.validate(data)
        assert result.is_valid is False

    def test_require_correlation_id(self) -> None:
        """Test correlation_id requirement."""
        validator = EventValidator(require_correlation_id=True)
        data = self._create_valid_event_data()

        # Without correlation_id
        result = validator.validate(data)
        assert result.is_valid is False
        assert any("correlation_id" in e for e in result.errors)

        # With correlation_id
        data["metadata"] = {"correlation_id": "corr-123"}
        result = validator.validate(data)
        assert result.is_valid is True

    def test_pydantic_schema_validation(self) -> None:
        """Test Pydantic schema validation for registered types."""
        validator = EventValidator()
        data = self._create_valid_event_data()

        # entity_id must be valid UUID for EntityCreatedEvent
        data["entity_id"] = "not-a-uuid"

        result = validator.validate(data)

        assert result.is_valid is False
        assert any("entity_id" in e for e in result.errors)

    def test_unregistered_event_type_warning(self) -> None:
        """Test that unregistered event types generate warnings."""
        validator = EventValidator()
        data = {
            "event_type": "custom.unregistered",
            "source": "mkg-kernel",
            "tenant_id": "tenant-123",
        }

        result = validator.validate(data)

        # Should pass but have warning
        assert result.is_valid is True
        assert any("not registered" in w for w in result.warnings)

    def test_custom_validators(self) -> None:
        """Test custom validator functions."""

        def must_be_article(data: dict) -> list[str]:
            if data.get("entity_type") != "Article":
                return ["entity_type must be 'Article'"]
            return []

        validator = EventValidator(custom_validators=[must_be_article])
        data = self._create_valid_event_data()

        # Valid - is Article
        result = validator.validate(data)
        assert result.is_valid is True

        # Invalid - not Article
        data["entity_type"] = "Product"
        result = validator.validate(data)
        assert result.is_valid is False
        assert any("Article" in e for e in result.errors)


class TestValidateEvent:
    """Tests for validate_event method."""

    def test_validate_event_object(self) -> None:
        """Test validating an event object."""
        validator = EventValidator(allowed_tenant_ids=["tenant-123"])

        event = EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id=uuid4(),
            entity_type="Article",
        )

        result = validator.validate_event(event)

        assert result.is_valid is True

    def test_validate_event_object_tenant_filter(self) -> None:
        """Test tenant filtering on event object."""
        validator = EventValidator(allowed_tenant_ids=["tenant-123"])

        event = EntityCreatedEvent(
            tenant_id="tenant-other",
            entity_id=uuid4(),
            entity_type="Article",
        )

        result = validator.validate_event(event)

        assert result.is_valid is False
        assert any("tenant" in e.lower() for e in result.errors)

    def test_validate_event_object_source_filter(self) -> None:
        """Test source filtering on event object."""
        validator = EventValidator(allowed_sources=["custom-source"])

        event = EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id=uuid4(),
            entity_type="Article",
        )

        result = validator.validate_event(event)

        assert result.is_valid is False  # source is "mkg-kernel", not allowed


class TestRegistryMethods:
    """Tests for registry-related methods."""

    def test_is_registered_type(self) -> None:
        """Test checking if type is registered."""
        validator = EventValidator()

        assert validator.is_registered_type("entity.created") is True
        assert validator.is_registered_type("unknown.type") is False

    def test_get_registered_types(self) -> None:
        """Test getting all registered types."""
        validator = EventValidator()

        types = validator.get_registered_types()

        assert "entity.created" in types
        assert "entity.updated" in types
        assert "entity.deleted" in types


class TestCreateCustomValidator:
    """Tests for create_custom_validator helper."""

    def test_create_custom_validator_passes(self) -> None:
        """Test custom validator that passes."""
        custom = create_custom_validator(
            field="entity_type",
            validator_func=lambda v: v == "Article",
            error_message="Must be Article",
        )

        errors = custom({"entity_type": "Article"})

        assert errors == []

    def test_create_custom_validator_fails(self) -> None:
        """Test custom validator that fails."""
        custom = create_custom_validator(
            field="entity_type",
            validator_func=lambda v: v == "Article",
            error_message="Must be Article",
        )

        errors = custom({"entity_type": "Product"})

        assert errors == ["Must be Article"]

    def test_create_custom_validator_missing_field(self) -> None:
        """Test custom validator with missing field."""
        custom = create_custom_validator(
            field="entity_type",
            validator_func=lambda v: v == "Article",
            error_message="Must be Article",
        )

        # Missing field should not trigger error
        errors = custom({})

        assert errors == []


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_valid_result(self) -> None:
        """Test valid result properties."""
        result = ValidationResult(
            is_valid=True,
            event_type="entity.created",
        )

        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings == []

    def test_invalid_result(self) -> None:
        """Test invalid result properties."""
        result = ValidationResult(
            is_valid=False,
            event_type="entity.created",
            errors=["Error 1", "Error 2"],
            warnings=["Warning 1"],
        )

        assert result.is_valid is False
        assert len(result.errors) == 2
        assert len(result.warnings) == 1
