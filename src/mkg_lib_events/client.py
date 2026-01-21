"""Low-level EventBridge client wrapper.

Provides a thin wrapper around boto3 EventBridge client with consistent
error handling, logging, and configuration.
"""

import os
from typing import Any

import boto3
from botocore.exceptions import ClientError

from mkg_lib_events.exceptions import EventConfigurationError, EventPublishError
from mkg_lib_events.logging import get_logger

logger = get_logger(__name__, component="eventbridge_client")


class EventBusClient:
    """Low-level wrapper for boto3 EventBridge client.

    Provides methods for interacting with AWS EventBridge with consistent
    error handling and logging.

    Attributes:
        event_bus_name: Name of the EventBridge event bus.
        region: AWS region for the EventBridge client.

    Example:
        ```python
        client = EventBusClient(event_bus_name="mkg-events")
        response = client.put_events([entry1, entry2])
        ```
    """

    def __init__(
        self,
        event_bus_name: str | None = None,
        region: str | None = None,
        boto_client: Any | None = None,
    ) -> None:
        """Initialize EventBusClient.

        Args:
            event_bus_name: Name of the EventBridge event bus.
                Defaults to MKG_EVENT_BUS_NAME environment variable.
            region: AWS region. Defaults to AWS_REGION or eu-central-1.
            boto_client: Optional pre-configured boto3 client for testing.

        Raises:
            EventConfigurationError: If event_bus_name is not provided
                and MKG_EVENT_BUS_NAME is not set.
        """
        _event_bus_name = event_bus_name or os.environ.get("MKG_EVENT_BUS_NAME")
        if not _event_bus_name:
            raise EventConfigurationError(
                "event_bus_name must be provided or MKG_EVENT_BUS_NAME must be set"
            )
        self.event_bus_name: str = _event_bus_name

        self.region = region or os.environ.get("AWS_REGION", "eu-central-1")

        if boto_client is not None:
            self._client = boto_client
        else:
            self._client = boto3.client("events", region_name=self.region)

        logger.info(
            "eventbridge_client_initialized",
            event_bus_name=self.event_bus_name,
            region=self.region,
        )

    def put_events(
        self,
        entries: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Send events to EventBridge.

        Args:
            entries: List of event entries in EventBridge format.

        Returns:
            Response from EventBridge PutEvents API.

        Raises:
            EventPublishError: If the API call fails.
        """
        try:
            response: dict[str, Any] = self._client.put_events(Entries=entries)

            failed_count = response.get("FailedEntryCount", 0)
            if failed_count > 0:
                logger.warning(
                    "eventbridge_partial_failure",
                    failed_count=failed_count,
                    total_count=len(entries),
                )

            return response

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_message = e.response.get("Error", {}).get("Message", str(e))

            logger.error(
                "eventbridge_put_events_failed",
                error_code=error_code,
                error_message=error_message,
                entry_count=len(entries),
            )

            raise EventPublishError(
                f"Failed to publish events: {error_code} - {error_message}"
            ) from e

    def put_rule(
        self,
        rule_name: str,
        event_pattern: dict[str, Any],
        description: str | None = None,
        state: str = "ENABLED",
    ) -> dict[str, Any]:
        """Create or update an EventBridge rule.

        Args:
            rule_name: Name of the rule.
            event_pattern: Event pattern for matching events.
            description: Optional rule description.
            state: Rule state (ENABLED or DISABLED).

        Returns:
            Response from EventBridge PutRule API.

        Raises:
            EventPublishError: If the API call fails.
        """
        try:
            import json

            params: dict[str, Any] = {
                "Name": rule_name,
                "EventBusName": self.event_bus_name,
                "EventPattern": json.dumps(event_pattern),
                "State": state,
            }

            if description:
                params["Description"] = description

            response: dict[str, Any] = self._client.put_rule(**params)

            logger.info(
                "eventbridge_rule_created",
                rule_name=rule_name,
                rule_arn=response.get("RuleArn"),
            )

            return response

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_message = e.response.get("Error", {}).get("Message", str(e))

            logger.error(
                "eventbridge_put_rule_failed",
                rule_name=rule_name,
                error_code=error_code,
                error_message=error_message,
            )

            raise EventPublishError(
                f"Failed to create rule: {error_code} - {error_message}"
            ) from e

    def put_targets(
        self,
        rule_name: str,
        targets: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Add targets to an EventBridge rule.

        Args:
            rule_name: Name of the rule.
            targets: List of targets in EventBridge format.

        Returns:
            Response from EventBridge PutTargets API.

        Raises:
            EventPublishError: If the API call fails.
        """
        try:
            response: dict[str, Any] = self._client.put_targets(
                Rule=rule_name,
                EventBusName=self.event_bus_name,
                Targets=targets,
            )

            failed_count = response.get("FailedEntryCount", 0)
            if failed_count > 0:
                logger.warning(
                    "eventbridge_put_targets_partial_failure",
                    rule_name=rule_name,
                    failed_count=failed_count,
                )

            return response

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_message = e.response.get("Error", {}).get("Message", str(e))

            logger.error(
                "eventbridge_put_targets_failed",
                rule_name=rule_name,
                error_code=error_code,
                error_message=error_message,
            )

            raise EventPublishError(
                f"Failed to add targets: {error_code} - {error_message}"
            ) from e

    def delete_rule(self, rule_name: str) -> None:
        """Delete an EventBridge rule.

        Args:
            rule_name: Name of the rule to delete.

        Raises:
            EventPublishError: If the API call fails.
        """
        try:
            # First remove all targets
            self.remove_targets(rule_name)

            # Then delete the rule
            self._client.delete_rule(
                Name=rule_name,
                EventBusName=self.event_bus_name,
            )

            logger.info(
                "eventbridge_rule_deleted",
                rule_name=rule_name,
            )

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_message = e.response.get("Error", {}).get("Message", str(e))

            logger.error(
                "eventbridge_delete_rule_failed",
                rule_name=rule_name,
                error_code=error_code,
                error_message=error_message,
            )

            raise EventPublishError(
                f"Failed to delete rule: {error_code} - {error_message}"
            ) from e

    def remove_targets(self, rule_name: str) -> None:
        """Remove all targets from an EventBridge rule.

        Args:
            rule_name: Name of the rule.

        Raises:
            EventPublishError: If the API call fails.
        """
        try:
            # List existing targets
            response = self._client.list_targets_by_rule(
                Rule=rule_name,
                EventBusName=self.event_bus_name,
            )

            targets = response.get("Targets", [])
            if not targets:
                return

            target_ids = [t["Id"] for t in targets]

            self._client.remove_targets(
                Rule=rule_name,
                EventBusName=self.event_bus_name,
                Ids=target_ids,
            )

            logger.info(
                "eventbridge_targets_removed",
                rule_name=rule_name,
                target_count=len(target_ids),
            )

        except ClientError as e:
            # Ignore if rule doesn't exist
            if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                return

            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_message = e.response.get("Error", {}).get("Message", str(e))

            logger.error(
                "eventbridge_remove_targets_failed",
                rule_name=rule_name,
                error_code=error_code,
                error_message=error_message,
            )

            raise EventPublishError(
                f"Failed to remove targets: {error_code} - {error_message}"
            ) from e

    def list_rules(self, name_prefix: str | None = None) -> list[dict[str, Any]]:
        """List EventBridge rules.

        Args:
            name_prefix: Optional prefix to filter rules.

        Returns:
            List of rule definitions.

        Raises:
            EventPublishError: If the API call fails.
        """
        try:
            params: dict[str, Any] = {"EventBusName": self.event_bus_name}
            if name_prefix:
                params["NamePrefix"] = name_prefix

            response: dict[str, Any] = self._client.list_rules(**params)
            rules: list[dict[str, Any]] = response.get("Rules", [])
            return rules

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_message = e.response.get("Error", {}).get("Message", str(e))

            logger.error(
                "eventbridge_list_rules_failed",
                error_code=error_code,
                error_message=error_message,
            )

            raise EventPublishError(
                f"Failed to list rules: {error_code} - {error_message}"
            ) from e
