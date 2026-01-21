"""Unit tests for BaseEventHandler."""

from uuid import uuid4

from mkg_lib_events import EntityCreatedEvent
from mkg_lib_events.handler import (
    BaseEventHandler,
    HandlerResult,
    get_current_correlation_id,
    get_current_tenant,
)


class ConcreteHandler(BaseEventHandler[EntityCreatedEvent]):
    """Concrete implementation for testing."""

    handler_name = "test-handler"
    supported_event_types = ["entity.created"]

    def __init__(self, should_fail: bool = False, error_msg: str = "Test error"):
        super().__init__()
        self.should_fail = should_fail
        self.error_msg = error_msg
        self.processed_events: list[EntityCreatedEvent] = []

    def handle(self, event: EntityCreatedEvent) -> HandlerResult:
        self.processed_events.append(event)

        if self.should_fail:
            return self.failure(self.error_msg, should_retry=True)

        return self.success(metadata={"processed": True})


class ExceptionHandler(BaseEventHandler[EntityCreatedEvent]):
    """Handler that raises exceptions for testing."""

    handler_name = "exception-handler"
    supported_event_types = ["entity.created"]

    def __init__(self, exception: Exception):
        super().__init__()
        self.exception = exception

    def handle(self, event: EntityCreatedEvent) -> HandlerResult:
        raise self.exception


class TestBaseEventHandler:
    """Tests for BaseEventHandler."""

    def _create_event(self, correlation_id: str | None = None) -> EntityCreatedEvent:
        """Helper to create test event."""
        from mkg_lib_events.models.base import EventMetadata

        metadata = EventMetadata(correlation_id=correlation_id)
        return EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id=uuid4(),
            entity_type="Article",
            metadata=metadata,
        )

    def test_successful_processing(self) -> None:
        """Test successful event processing."""
        handler = ConcreteHandler()
        event = self._create_event()

        result = handler.process(event)

        assert result.is_success is True
        assert result.tenant_id == "tenant-123"
        assert result.metadata is not None
        assert result.metadata.get("processed") is True
        assert len(handler.processed_events) == 1

    def test_failed_processing(self) -> None:
        """Test failed event processing."""
        handler = ConcreteHandler(should_fail=True, error_msg="Processing failed")
        event = self._create_event()

        result = handler.process(event)

        assert result.is_success is False
        assert result.error == "Processing failed"
        assert result.should_retry is True

    def test_exception_handling(self) -> None:
        """Test exception handling during processing."""
        handler = ExceptionHandler(exception=ValueError("Test exception"))
        event = self._create_event()

        result = handler.process(event)

        assert result.is_success is False
        assert "Test exception" in str(result.error)
        assert result.should_retry is False  # ValueError is not retryable

    def test_retryable_exception(self) -> None:
        """Test that retryable exceptions set should_retry."""
        handler = ExceptionHandler(exception=ConnectionError("Connection failed"))
        event = self._create_event()

        result = handler.process(event)

        assert result.is_success is False
        assert result.should_retry is True

    def test_tenant_context_set(self) -> None:
        """Test that tenant context is set during processing."""
        captured_tenant: str | None = None

        class ContextCapturingHandler(BaseEventHandler[EntityCreatedEvent]):
            handler_name = "context-handler"
            supported_event_types = ["entity.created"]

            def handle(self, event: EntityCreatedEvent) -> HandlerResult:
                nonlocal captured_tenant
                captured_tenant = get_current_tenant()
                return self.success()

        handler = ContextCapturingHandler()
        event = self._create_event()

        handler.process(event)

        assert captured_tenant == "tenant-123"

    def test_correlation_id_context_set(self) -> None:
        """Test that correlation ID context is set during processing."""
        captured_correlation_id: str | None = None

        class ContextCapturingHandler(BaseEventHandler[EntityCreatedEvent]):
            handler_name = "context-handler"
            supported_event_types = ["entity.created"]

            def handle(self, event: EntityCreatedEvent) -> HandlerResult:
                nonlocal captured_correlation_id
                captured_correlation_id = get_current_correlation_id()
                return self.success()

        handler = ContextCapturingHandler()
        event = self._create_event(correlation_id="corr-456")

        handler.process(event)

        assert captured_correlation_id == "corr-456"

    def test_context_reset_after_processing(self) -> None:
        """Test that context is reset after processing."""
        handler = ConcreteHandler()
        event = self._create_event()

        handler.process(event)

        # Context should be reset
        assert get_current_tenant() is None
        assert get_current_correlation_id() is None

    def test_unsupported_event_type_skipped(self) -> None:
        """Test that unsupported event types are skipped."""

        class FilteredHandler(BaseEventHandler[EntityCreatedEvent]):
            handler_name = "filtered-handler"
            supported_event_types = ["entity.updated"]  # Not entity.created

            def handle(self, event: EntityCreatedEvent) -> HandlerResult:
                return self.success()

        handler = FilteredHandler()
        event = self._create_event()  # entity.created

        result = handler.process(event)

        assert result.is_success is True
        assert result.metadata is not None
        assert result.metadata.get("skipped") is True

    def test_on_success_callback(self) -> None:
        """Test that on_success callback is called."""
        callback_called = False
        callback_event = None
        callback_result = None

        def on_success(event, result):
            nonlocal callback_called, callback_event, callback_result
            callback_called = True
            callback_event = event
            callback_result = result

        handler = ConcreteHandler()
        handler._on_success = on_success
        event = self._create_event()

        handler.process(event)

        assert callback_called is True
        assert callback_event is event
        assert callback_result.is_success is True

    def test_on_error_callback(self) -> None:
        """Test that on_error callback is called on failure."""
        callback_called = False

        def on_error(event, result):
            nonlocal callback_called
            callback_called = True

        handler = ConcreteHandler(should_fail=True)
        handler._on_error = on_error
        event = self._create_event()

        handler.process(event)

        assert callback_called is True


class TestHandlerHelperMethods:
    """Tests for handler helper methods."""

    def test_success_helper(self) -> None:
        """Test success() helper method."""
        handler = ConcreteHandler()

        # Need to be in context for helper to work
        class SuccessTestHandler(BaseEventHandler[EntityCreatedEvent]):
            handler_name = "test"
            supported_event_types = ["entity.created"]

            def handle(self, event: EntityCreatedEvent) -> HandlerResult:
                return self.success(metadata={"key": "value"})

        handler = SuccessTestHandler()
        event = EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id=uuid4(),
            entity_type="Article",
        )

        result = handler.process(event)

        assert result.is_success is True
        assert result.metadata is not None
        assert result.metadata.get("key") == "value"

    def test_failure_helper(self) -> None:
        """Test failure() helper method."""

        class FailureTestHandler(BaseEventHandler[EntityCreatedEvent]):
            handler_name = "test"
            supported_event_types = ["entity.created"]

            def handle(self, event: EntityCreatedEvent) -> HandlerResult:
                return self.failure("Something went wrong", should_retry=True)

        handler = FailureTestHandler()
        event = EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id=uuid4(),
            entity_type="Article",
        )

        result = handler.process(event)

        assert result.is_success is False
        assert result.error == "Something went wrong"
        assert result.should_retry is True

    def test_skip_helper(self) -> None:
        """Test skip() helper method."""

        class SkipTestHandler(BaseEventHandler[EntityCreatedEvent]):
            handler_name = "test"
            supported_event_types = ["entity.created"]

            def handle(self, event: EntityCreatedEvent) -> HandlerResult:
                if event.entity_type != "Product":
                    return self.skip("Only processes Product entities")
                return self.success()

        handler = SkipTestHandler()
        event = EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id=uuid4(),
            entity_type="Article",
        )

        result = handler.process(event)

        assert result.is_success is True
        assert result.metadata is not None
        assert result.metadata.get("skipped") is True
        assert result.metadata.get("reason") == "Only processes Product entities"
