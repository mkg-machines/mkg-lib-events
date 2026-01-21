"""Subscription manager for MKG Platform.

Creates and manages EventBridge rules for extension event subscriptions.
"""

import re
from typing import Any

from mkg_lib_events.client import EventBusClient
from mkg_lib_events.exceptions import SubscriptionError
from mkg_lib_events.logging import get_logger

logger = get_logger(__name__, component="subscription_manager")


class SubscriptionManager:
    """Manages EventBridge subscriptions for extensions.

    Creates, updates, and deletes EventBridge rules that route events
    to extension Lambda functions or SQS queues.

    Attributes:
        event_bus_name: Name of the EventBridge event bus.

    Example:
        ```python
        manager = SubscriptionManager(event_bus_name="mkg-events")

        # Subscribe to entity events for a tenant
        manager.create_subscription(
            subscription_id="ext-search-tenant-123",
            tenant_id="tenant-123",
            event_types=["entity.created", "entity.updated"],
            target_arn="arn:aws:sqs:eu-central-1:123456789:search-queue",
        )
        ```
    """

    # Rule name pattern for validation
    RULE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")
    MAX_RULE_NAME_LENGTH = 64

    def __init__(
        self,
        event_bus_name: str | None = None,
        client: EventBusClient | None = None,
    ) -> None:
        """Initialize SubscriptionManager.

        Args:
            event_bus_name: Name of the EventBridge event bus.
                Defaults to MKG_EVENT_BUS_NAME environment variable.
            client: Optional pre-configured EventBusClient for testing.
        """
        if client is not None:
            self._client = client
            self.event_bus_name = client.event_bus_name
        else:
            self._client = EventBusClient(event_bus_name=event_bus_name)
            self.event_bus_name = self._client.event_bus_name

        logger.info(
            "subscription_manager_initialized",
            event_bus_name=self.event_bus_name,
        )

    def create_subscription(
        self,
        subscription_id: str,
        tenant_id: str,
        event_types: list[str],
        target_arn: str,
        target_id: str | None = None,
        description: str | None = None,
        source_filter: str | None = None,
    ) -> str:
        """Create an event subscription.

        Creates an EventBridge rule that matches events for the specified
        tenant and event types, routing them to the target ARN.

        Args:
            subscription_id: Unique identifier for this subscription.
                Used as the rule name (must be alphanumeric with - and _).
            tenant_id: Tenant ID to filter events for.
            event_types: List of event types to subscribe to.
            target_arn: ARN of the target (Lambda, SQS, SNS, etc.).
            target_id: Optional target ID. Defaults to subscription_id.
            description: Optional rule description.
            source_filter: Optional source service filter (e.g., "mkg-kernel").

        Returns:
            The ARN of the created rule.

        Raises:
            SubscriptionError: If subscription creation fails.

        Example:
            ```python
            rule_arn = manager.create_subscription(
                subscription_id="search-ext-tenant-123",
                tenant_id="tenant-123",
                event_types=["entity.created", "entity.updated"],
                target_arn="arn:aws:lambda:eu-central-1:123:function:search",
            )
            ```
        """
        self._validate_subscription_id(subscription_id)
        self._validate_tenant_id(tenant_id)
        self._validate_event_types(event_types)

        rule_name = self._build_rule_name(subscription_id)
        event_pattern = self._build_event_pattern(
            tenant_id=tenant_id,
            event_types=event_types,
            source_filter=source_filter,
        )

        logger.info(
            "creating_subscription",
            subscription_id=subscription_id,
            rule_name=rule_name,
            tenant_id=tenant_id,
            event_types=event_types,
            target_arn=target_arn,
        )

        try:
            # Create the rule
            rule_description = description or (
                f"MKG subscription for tenant {tenant_id}: {', '.join(event_types)}"
            )

            response = self._client.put_rule(
                rule_name=rule_name,
                event_pattern=event_pattern,
                description=rule_description,
            )

            rule_arn = response.get("RuleArn", "")

            # Add the target
            target = self._build_target(
                target_id=target_id or subscription_id,
                target_arn=target_arn,
            )

            self._client.put_targets(rule_name=rule_name, targets=[target])

            logger.info(
                "subscription_created",
                subscription_id=subscription_id,
                rule_arn=rule_arn,
                tenant_id=tenant_id,
            )

            return rule_arn

        except Exception as e:
            logger.error(
                "subscription_creation_failed",
                subscription_id=subscription_id,
                tenant_id=tenant_id,
                error=str(e),
            )
            raise SubscriptionError(
                f"Failed to create subscription: {e}",
                rule_name=rule_name,
                tenant_id=tenant_id,
            ) from e

    def delete_subscription(
        self,
        subscription_id: str,
        tenant_id: str | None = None,
    ) -> None:
        """Delete an event subscription.

        Args:
            subscription_id: The subscription ID to delete.
            tenant_id: Optional tenant ID for logging.

        Raises:
            SubscriptionError: If deletion fails.

        Example:
            ```python
            manager.delete_subscription(
                subscription_id="search-ext-tenant-123",
                tenant_id="tenant-123",
            )
            ```
        """
        rule_name = self._build_rule_name(subscription_id)

        logger.info(
            "deleting_subscription",
            subscription_id=subscription_id,
            rule_name=rule_name,
            tenant_id=tenant_id,
        )

        try:
            self._client.delete_rule(rule_name=rule_name)

            logger.info(
                "subscription_deleted",
                subscription_id=subscription_id,
                tenant_id=tenant_id,
            )

        except Exception as e:
            logger.error(
                "subscription_deletion_failed",
                subscription_id=subscription_id,
                tenant_id=tenant_id,
                error=str(e),
            )
            raise SubscriptionError(
                f"Failed to delete subscription: {e}",
                rule_name=rule_name,
                tenant_id=tenant_id,
            ) from e

    def update_subscription(
        self,
        subscription_id: str,
        tenant_id: str,
        event_types: list[str],
        target_arn: str,
        target_id: str | None = None,
        description: str | None = None,
        source_filter: str | None = None,
    ) -> str:
        """Update an existing subscription.

        Updates the rule's event pattern and target. This is idempotent -
        if the subscription doesn't exist, it will be created.

        Args:
            subscription_id: The subscription ID to update.
            tenant_id: Tenant ID for the subscription.
            event_types: Updated list of event types.
            target_arn: Updated target ARN.
            target_id: Optional target ID.
            description: Optional updated description.
            source_filter: Optional source filter.

        Returns:
            The ARN of the updated rule.

        Raises:
            SubscriptionError: If update fails.
        """
        # Put operations are idempotent, so we can just call create
        return self.create_subscription(
            subscription_id=subscription_id,
            tenant_id=tenant_id,
            event_types=event_types,
            target_arn=target_arn,
            target_id=target_id,
            description=description,
            source_filter=source_filter,
        )

    def list_subscriptions(
        self,
        prefix: str | None = None,
    ) -> list[dict[str, Any]]:
        """List existing subscriptions.

        Args:
            prefix: Optional prefix to filter subscriptions.

        Returns:
            List of subscription details.

        Raises:
            SubscriptionError: If listing fails.
        """
        try:
            rules = self._client.list_rules(name_prefix=prefix)

            subscriptions = []
            for rule in rules:
                subscriptions.append(
                    {
                        "subscription_id": rule.get("Name", "").replace("mkg-sub-", ""),
                        "rule_name": rule.get("Name"),
                        "rule_arn": rule.get("Arn"),
                        "state": rule.get("State"),
                        "description": rule.get("Description"),
                    }
                )

            return subscriptions

        except Exception as e:
            logger.error(
                "subscription_list_failed",
                prefix=prefix,
                error=str(e),
            )
            raise SubscriptionError(f"Failed to list subscriptions: {e}") from e

    def _build_rule_name(self, subscription_id: str) -> str:
        """Build EventBridge rule name from subscription ID.

        Args:
            subscription_id: The subscription identifier.

        Returns:
            Rule name with mkg-sub- prefix.
        """
        return f"mkg-sub-{subscription_id}"

    def _build_event_pattern(
        self,
        tenant_id: str,
        event_types: list[str],
        source_filter: str | None = None,
    ) -> dict[str, Any]:
        """Build EventBridge event pattern.

        Args:
            tenant_id: Tenant ID to match.
            event_types: Event types to match.
            source_filter: Optional source service filter.

        Returns:
            EventBridge event pattern dictionary.
        """
        pattern: dict[str, Any] = {
            "detail-type": event_types,
            "detail": {
                "tenant_id": [tenant_id],
            },
        }

        if source_filter:
            pattern["source"] = [source_filter]

        return pattern

    def _build_target(
        self,
        target_id: str,
        target_arn: str,
    ) -> dict[str, Any]:
        """Build EventBridge target definition.

        Args:
            target_id: Target identifier.
            target_arn: Target ARN.

        Returns:
            EventBridge target dictionary.
        """
        return {
            "Id": target_id,
            "Arn": target_arn,
        }

    def _validate_subscription_id(self, subscription_id: str) -> None:
        """Validate subscription ID format.

        Args:
            subscription_id: The subscription ID to validate.

        Raises:
            SubscriptionError: If validation fails.
        """
        if not subscription_id:
            raise SubscriptionError("subscription_id cannot be empty")

        if not self.RULE_NAME_PATTERN.match(subscription_id):
            raise SubscriptionError(
                f"subscription_id must match pattern {self.RULE_NAME_PATTERN.pattern}"
            )

        # Check total rule name length
        rule_name = self._build_rule_name(subscription_id)
        if len(rule_name) > self.MAX_RULE_NAME_LENGTH:
            raise SubscriptionError(
                f"subscription_id too long (rule name would exceed "
                f"{self.MAX_RULE_NAME_LENGTH} characters)"
            )

    def _validate_tenant_id(self, tenant_id: str) -> None:
        """Validate tenant ID.

        Args:
            tenant_id: The tenant ID to validate.

        Raises:
            SubscriptionError: If validation fails.
        """
        if not tenant_id:
            raise SubscriptionError(
                "tenant_id is required for subscription",
                tenant_id=tenant_id,
            )

    def _validate_event_types(self, event_types: list[str]) -> None:
        """Validate event types list.

        Args:
            event_types: List of event types to validate.

        Raises:
            SubscriptionError: If validation fails.
        """
        if not event_types:
            raise SubscriptionError("event_types cannot be empty")

        for event_type in event_types:
            if not event_type:
                raise SubscriptionError("event_type cannot be empty string")
