"""Unit tests for DeadLetterHandler."""

from uuid import uuid4

from mkg_lib_events import EntityCreatedEvent
from mkg_lib_events.dlq import DeadLetterHandler, FailedEvent


class TestFailedEvent:
    """Tests for FailedEvent dataclass."""

    def _create_event(self) -> EntityCreatedEvent:
        """Helper to create test event."""
        return EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id=uuid4(),
            entity_type="Article",
        )

    def test_failed_event_creation(self) -> None:
        """Test creating a FailedEvent."""
        event = self._create_event()

        failed = FailedEvent(
            id=uuid4(),
            event=event,
            error_message="Test error",
            error_type="ValueError",
            handler_name="test-handler",
        )

        assert failed.error_message == "Test error"
        assert failed.error_type == "ValueError"
        assert failed.handler_name == "test-handler"
        assert failed.attempt_count == 1

    def test_to_dict(self) -> None:
        """Test converting FailedEvent to dictionary."""
        event = self._create_event()

        failed = FailedEvent(
            id=uuid4(),
            event=event,
            error_message="Test error",
            error_type="ValueError",
            handler_name="test-handler",
            attempt_count=3,
            metadata={"key": "value"},
        )

        result = failed.to_dict()

        assert result["event_type"] == "entity.created"
        assert result["tenant_id"] == "tenant-123"
        assert result["error_message"] == "Test error"
        assert result["error_type"] == "ValueError"
        assert result["handler_name"] == "test-handler"
        assert result["attempt_count"] == 3
        assert result["metadata"] == {"key": "value"}


class TestDeadLetterHandler:
    """Tests for DeadLetterHandler."""

    def _create_event(self) -> EntityCreatedEvent:
        """Helper to create test event."""
        return EntityCreatedEvent(
            tenant_id="tenant-123",
            entity_id=uuid4(),
            entity_type="Article",
        )

    def test_handle_failure_basic(self) -> None:
        """Test basic failure handling."""
        dlq = DeadLetterHandler()
        event = self._create_event()
        error = ValueError("Test error")

        failed_event = dlq.handle_failure(
            event=event,
            error=error,
            handler_name="test-handler",
        )

        assert failed_event.error_message == "Test error"
        assert failed_event.error_type == "ValueError"
        assert failed_event.handler_name == "test-handler"
        assert failed_event.event is event

    def test_handle_failure_with_metadata(self) -> None:
        """Test failure handling with metadata."""
        dlq = DeadLetterHandler()
        event = self._create_event()
        error = ValueError("Test error")

        failed_event = dlq.handle_failure(
            event=event,
            error=error,
            handler_name="test-handler",
            metadata={"custom": "data"},
        )

        assert failed_event.metadata == {"custom": "data"}

    def test_store_callback_called(self) -> None:
        """Test that store callback is called."""
        stored_events: list[FailedEvent] = []

        def store_callback(failed_event: FailedEvent) -> None:
            stored_events.append(failed_event)

        dlq = DeadLetterHandler(on_store=store_callback)
        event = self._create_event()

        dlq.handle_failure(
            event=event,
            error=ValueError("Test"),
            handler_name="test-handler",
        )

        assert len(stored_events) == 1
        assert stored_events[0].error_message == "Test"

    def test_alert_callback_on_threshold(self) -> None:
        """Test that alert callback is called when threshold is reached."""
        alerts: list[FailedEvent] = []

        def alert_callback(failed_event: FailedEvent) -> None:
            alerts.append(failed_event)

        dlq = DeadLetterHandler(on_alert=alert_callback, alert_threshold=3)
        event = self._create_event()

        # First two failures - no alert
        for _ in range(2):
            dlq.handle_failure(
                event=event,
                error=ValueError("Test"),
                handler_name="test-handler",
            )

        assert len(alerts) == 0

        # Third failure - should trigger alert
        dlq.handle_failure(
            event=event,
            error=ValueError("Test"),
            handler_name="test-handler",
        )

        assert len(alerts) == 1

    def test_alert_on_every_failure_threshold_1(self) -> None:
        """Test that every failure triggers alert when threshold is 1."""
        alerts: list[FailedEvent] = []

        def alert_callback(failed_event: FailedEvent) -> None:
            alerts.append(failed_event)

        dlq = DeadLetterHandler(on_alert=alert_callback, alert_threshold=1)
        event = self._create_event()

        for _ in range(3):
            dlq.handle_failure(
                event=event,
                error=ValueError("Test"),
                handler_name="test-handler",
            )

        assert len(alerts) == 3

    def test_error_message_truncation(self) -> None:
        """Test that long error messages are truncated."""
        dlq = DeadLetterHandler(max_error_message_length=50)
        event = self._create_event()
        long_error = ValueError("A" * 100)

        failed_event = dlq.handle_failure(
            event=event,
            error=long_error,
            handler_name="test-handler",
        )

        assert len(failed_event.error_message) == 53  # 50 + "..."
        assert failed_event.error_message.endswith("...")

    def test_get_failure_count(self) -> None:
        """Test getting failure count."""
        dlq = DeadLetterHandler()
        event = self._create_event()

        # No failures yet
        assert dlq.get_failure_count("test-handler", "entity.created") == 0

        # Add some failures
        for _ in range(5):
            dlq.handle_failure(
                event=event,
                error=ValueError("Test"),
                handler_name="test-handler",
            )

        assert dlq.get_failure_count("test-handler", "entity.created") == 5

    def test_reset_failure_count_all(self) -> None:
        """Test resetting all failure counts."""
        dlq = DeadLetterHandler()
        event = self._create_event()

        dlq.handle_failure(
            event=event,
            error=ValueError("Test"),
            handler_name="handler-1",
        )
        dlq.handle_failure(
            event=event,
            error=ValueError("Test"),
            handler_name="handler-2",
        )

        dlq.reset_failure_count()

        assert dlq.get_failure_count("handler-1", "entity.created") == 0
        assert dlq.get_failure_count("handler-2", "entity.created") == 0

    def test_reset_failure_count_specific(self) -> None:
        """Test resetting specific handler failure count."""
        dlq = DeadLetterHandler()
        event = self._create_event()

        dlq.handle_failure(
            event=event,
            error=ValueError("Test"),
            handler_name="handler-1",
        )
        dlq.handle_failure(
            event=event,
            error=ValueError("Test"),
            handler_name="handler-2",
        )

        dlq.reset_failure_count(handler_name="handler-1")

        assert dlq.get_failure_count("handler-1", "entity.created") == 0
        assert dlq.get_failure_count("handler-2", "entity.created") == 1

    def test_handle_batch_failure(self) -> None:
        """Test handling batch failure."""
        stored_events: list[FailedEvent] = []

        def store_callback(failed_event: FailedEvent) -> None:
            stored_events.append(failed_event)

        dlq = DeadLetterHandler(on_store=store_callback)

        events = [self._create_event() for _ in range(3)]

        failed_events = dlq.handle_batch_failure(
            events=events,
            error=ValueError("Batch error"),
            handler_name="batch-handler",
        )

        assert len(failed_events) == 3
        assert len(stored_events) == 3
        assert all(fe.error_message == "Batch error" for fe in failed_events)

    def test_store_callback_error_handled(self) -> None:
        """Test that store callback errors are handled gracefully."""

        def failing_store(failed_event: FailedEvent) -> None:
            raise RuntimeError("Storage failed")

        dlq = DeadLetterHandler(on_store=failing_store)
        event = self._create_event()

        # Should not raise, error should be logged
        failed_event = dlq.handle_failure(
            event=event,
            error=ValueError("Test"),
            handler_name="test-handler",
        )

        assert failed_event is not None

    def test_alert_callback_error_handled(self) -> None:
        """Test that alert callback errors are handled gracefully."""

        def failing_alert(failed_event: FailedEvent) -> None:
            raise RuntimeError("Alert failed")

        dlq = DeadLetterHandler(on_alert=failing_alert, alert_threshold=1)
        event = self._create_event()

        # Should not raise, error should be logged
        failed_event = dlq.handle_failure(
            event=event,
            error=ValueError("Test"),
            handler_name="test-handler",
        )

        assert failed_event is not None
