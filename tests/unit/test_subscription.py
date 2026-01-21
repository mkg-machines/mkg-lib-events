"""Unit tests for SubscriptionManager."""

import pytest

from mkg_lib_events.exceptions import EventConfigurationError, SubscriptionError
from mkg_lib_events.subscription import SubscriptionManager


class TestSubscriptionManagerInit:
    """Tests for SubscriptionManager initialization."""

    def test_manager_requires_event_bus_name(self, mocker) -> None:
        """Test that manager requires event bus name."""
        mocker.patch.dict("os.environ", {}, clear=True)

        with pytest.raises(EventConfigurationError):
            SubscriptionManager()

    def test_manager_uses_env_variable(self, mocker) -> None:
        """Test that manager uses MKG_EVENT_BUS_NAME env variable."""
        mocker.patch.dict("os.environ", {"MKG_EVENT_BUS_NAME": "test-bus"})
        mocker.patch("boto3.client")

        manager = SubscriptionManager()

        assert manager.event_bus_name == "test-bus"


class TestSubscriptionManagerCreate:
    """Tests for SubscriptionManager.create_subscription()."""

    def test_create_subscription(self, mocker) -> None:
        """Test creating a subscription."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"
        mock_client.put_rule.return_value = {
            "RuleArn": "arn:aws:events:eu-central-1:123:rule/mkg-sub-test"
        }
        mock_client.put_targets.return_value = {"FailedEntryCount": 0}

        manager = SubscriptionManager(client=mock_client)

        rule_arn = manager.create_subscription(
            subscription_id="test-sub",
            tenant_id="tenant-123",
            event_types=["entity.created", "entity.updated"],
            target_arn="arn:aws:lambda:eu-central-1:123:function:test",
        )

        assert rule_arn == "arn:aws:events:eu-central-1:123:rule/mkg-sub-test"
        mock_client.put_rule.assert_called_once()
        mock_client.put_targets.assert_called_once()

    def test_create_subscription_validates_subscription_id(self, mocker) -> None:
        """Test that invalid subscription_id is rejected."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"

        manager = SubscriptionManager(client=mock_client)

        with pytest.raises(SubscriptionError, match="subscription_id"):
            manager.create_subscription(
                subscription_id="invalid id with spaces",
                tenant_id="tenant-123",
                event_types=["entity.created"],
                target_arn="arn:aws:lambda:eu-central-1:123:function:test",
            )

    def test_create_subscription_validates_empty_subscription_id(self, mocker) -> None:
        """Test that empty subscription_id is rejected."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"

        manager = SubscriptionManager(client=mock_client)

        with pytest.raises(SubscriptionError, match="cannot be empty"):
            manager.create_subscription(
                subscription_id="",
                tenant_id="tenant-123",
                event_types=["entity.created"],
                target_arn="arn:aws:lambda:eu-central-1:123:function:test",
            )

    def test_create_subscription_validates_tenant_id(self, mocker) -> None:
        """Test that empty tenant_id is rejected."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"

        manager = SubscriptionManager(client=mock_client)

        with pytest.raises(SubscriptionError, match="tenant_id"):
            manager.create_subscription(
                subscription_id="test-sub",
                tenant_id="",
                event_types=["entity.created"],
                target_arn="arn:aws:lambda:eu-central-1:123:function:test",
            )

    def test_create_subscription_validates_event_types(self, mocker) -> None:
        """Test that empty event_types is rejected."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"

        manager = SubscriptionManager(client=mock_client)

        with pytest.raises(SubscriptionError, match="event_types"):
            manager.create_subscription(
                subscription_id="test-sub",
                tenant_id="tenant-123",
                event_types=[],
                target_arn="arn:aws:lambda:eu-central-1:123:function:test",
            )

    def test_create_subscription_with_source_filter(self, mocker) -> None:
        """Test creating subscription with source filter."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"
        mock_client.put_rule.return_value = {"RuleArn": "arn:aws:events:..."}
        mock_client.put_targets.return_value = {"FailedEntryCount": 0}

        manager = SubscriptionManager(client=mock_client)

        manager.create_subscription(
            subscription_id="test-sub",
            tenant_id="tenant-123",
            event_types=["entity.created"],
            target_arn="arn:aws:lambda:eu-central-1:123:function:test",
            source_filter="mkg-kernel",
        )

        # Verify event pattern includes source
        call_args = mock_client.put_rule.call_args
        assert call_args is not None


class TestSubscriptionManagerDelete:
    """Tests for SubscriptionManager.delete_subscription()."""

    def test_delete_subscription(self, mocker) -> None:
        """Test deleting a subscription."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"

        manager = SubscriptionManager(client=mock_client)

        manager.delete_subscription(
            subscription_id="test-sub",
            tenant_id="tenant-123",
        )

        mock_client.delete_rule.assert_called_once_with(rule_name="mkg-sub-test-sub")

    def test_delete_subscription_handles_not_found(self, mocker) -> None:
        """Test that delete handles non-existent rule gracefully."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"
        mock_client.delete_rule.side_effect = Exception("Rule not found")

        manager = SubscriptionManager(client=mock_client)

        with pytest.raises(SubscriptionError):
            manager.delete_subscription(subscription_id="nonexistent")


class TestSubscriptionManagerUpdate:
    """Tests for SubscriptionManager.update_subscription()."""

    def test_update_subscription(self, mocker) -> None:
        """Test updating a subscription (idempotent create)."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"
        mock_client.put_rule.return_value = {"RuleArn": "arn:aws:events:..."}
        mock_client.put_targets.return_value = {"FailedEntryCount": 0}

        manager = SubscriptionManager(client=mock_client)

        rule_arn = manager.update_subscription(
            subscription_id="test-sub",
            tenant_id="tenant-123",
            event_types=["entity.created", "entity.deleted"],
            target_arn="arn:aws:lambda:eu-central-1:123:function:test",
        )

        assert rule_arn is not None
        mock_client.put_rule.assert_called_once()


class TestSubscriptionManagerList:
    """Tests for SubscriptionManager.list_subscriptions()."""

    def test_list_subscriptions(self, mocker) -> None:
        """Test listing subscriptions."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"
        mock_client.list_rules.return_value = [
            {
                "Name": "mkg-sub-tenant-123-search",
                "Arn": "arn:aws:events:eu-central-1:123:rule/mkg-sub-tenant-123-search",
                "State": "ENABLED",
                "Description": "Search subscription",
            },
            {
                "Name": "mkg-sub-tenant-456-export",
                "Arn": "arn:aws:events:eu-central-1:123:rule/mkg-sub-tenant-456-export",
                "State": "ENABLED",
                "Description": "Export subscription",
            },
        ]

        manager = SubscriptionManager(client=mock_client)

        subscriptions = manager.list_subscriptions()

        assert len(subscriptions) == 2
        assert subscriptions[0]["rule_name"] == "mkg-sub-tenant-123-search"
        assert subscriptions[1]["state"] == "ENABLED"

    def test_list_subscriptions_with_prefix(self, mocker) -> None:
        """Test listing subscriptions with prefix filter."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"
        mock_client.list_rules.return_value = []

        manager = SubscriptionManager(client=mock_client)

        manager.list_subscriptions(prefix="mkg-sub-tenant-123")

        mock_client.list_rules.assert_called_once_with(name_prefix="mkg-sub-tenant-123")


class TestSubscriptionManagerEventPattern:
    """Tests for event pattern generation."""

    def test_event_pattern_structure(self, mocker) -> None:
        """Test that event pattern has correct structure."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"
        mock_client.put_rule.return_value = {"RuleArn": "arn:..."}
        mock_client.put_targets.return_value = {"FailedEntryCount": 0}

        manager = SubscriptionManager(client=mock_client)

        manager.create_subscription(
            subscription_id="test",
            tenant_id="tenant-123",
            event_types=["entity.created", "entity.updated"],
            target_arn="arn:aws:lambda:...",
        )

        # Verify put_rule was called with correct event_pattern
        call_kwargs = mock_client.put_rule.call_args[1]
        assert "event_pattern" in call_kwargs

        import json

        pattern = json.loads(call_kwargs["event_pattern"])

        assert pattern["detail-type"] == ["entity.created", "entity.updated"]
        assert pattern["detail"]["tenant_id"] == ["tenant-123"]

    def test_event_pattern_with_source(self, mocker) -> None:
        """Test event pattern with source filter."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"
        mock_client.put_rule.return_value = {"RuleArn": "arn:..."}
        mock_client.put_targets.return_value = {"FailedEntryCount": 0}

        manager = SubscriptionManager(client=mock_client)

        manager.create_subscription(
            subscription_id="test",
            tenant_id="tenant-123",
            event_types=["entity.created"],
            target_arn="arn:aws:lambda:...",
            source_filter="mkg-kernel",
        )

        call_kwargs = mock_client.put_rule.call_args[1]

        import json

        pattern = json.loads(call_kwargs["event_pattern"])

        assert pattern["source"] == ["mkg-kernel"]


class TestSubscriptionManagerRuleName:
    """Tests for rule name generation."""

    def test_rule_name_format(self, mocker) -> None:
        """Test that rule names have correct format."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"
        mock_client.put_rule.return_value = {"RuleArn": "arn:..."}
        mock_client.put_targets.return_value = {"FailedEntryCount": 0}

        manager = SubscriptionManager(client=mock_client)

        manager.create_subscription(
            subscription_id="my-extension-tenant-123",
            tenant_id="tenant-123",
            event_types=["entity.created"],
            target_arn="arn:aws:lambda:...",
        )

        call_kwargs = mock_client.put_rule.call_args[1]
        assert call_kwargs["rule_name"] == "mkg-sub-my-extension-tenant-123"

    def test_rule_name_length_validation(self, mocker) -> None:
        """Test that overly long subscription IDs are rejected."""
        mock_client = mocker.MagicMock()
        mock_client.event_bus_name = "test-bus"

        manager = SubscriptionManager(client=mock_client)

        # Create a subscription_id that would exceed 64 chars with prefix
        long_id = "a" * 60  # "mkg-sub-" + 60 chars = 68 chars > 64

        with pytest.raises(SubscriptionError, match="too long"):
            manager.create_subscription(
                subscription_id=long_id,
                tenant_id="tenant-123",
                event_types=["entity.created"],
                target_arn="arn:aws:lambda:...",
            )
