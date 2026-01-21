"""Retry policy for event processing.

Provides configurable retry logic with various backoff strategies
for handling transient failures in event processing.
"""

import random
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import TypeVar

from mkg_lib_events.logging import get_logger

logger = get_logger(__name__, component="retry_policy")

T = TypeVar("T")


class BackoffStrategy(Enum):
    """Backoff strategy for retries."""

    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    EXPONENTIAL_JITTER = "exponential_jitter"


@dataclass
class RetryConfig:
    """Configuration for retry policy.

    Attributes:
        max_retries: Maximum number of retry attempts.
        base_delay_seconds: Base delay between retries in seconds.
        max_delay_seconds: Maximum delay between retries.
        backoff_strategy: Strategy for calculating delay.
        backoff_multiplier: Multiplier for backoff calculation.
        retryable_exceptions: Tuple of exception types to retry.
        non_retryable_exceptions: Tuple of exceptions that should not be retried.
    """

    max_retries: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL_JITTER
    backoff_multiplier: float = 2.0
    retryable_exceptions: tuple[type[Exception], ...] = field(
        default_factory=lambda: (ConnectionError, TimeoutError, OSError)
    )
    non_retryable_exceptions: tuple[type[Exception], ...] = field(
        default_factory=lambda: (ValueError, TypeError, KeyError)
    )


class BackoffCalculator(ABC):
    """Abstract base class for backoff calculation."""

    @abstractmethod
    def calculate(self, attempt: int, base_delay: float, max_delay: float) -> float:
        """Calculate delay for a given attempt.

        Args:
            attempt: Current attempt number (0-indexed).
            base_delay: Base delay in seconds.
            max_delay: Maximum delay in seconds.

        Returns:
            Delay in seconds.
        """
        pass


class FixedBackoff(BackoffCalculator):
    """Fixed delay between retries."""

    def calculate(self, _attempt: int, base_delay: float, max_delay: float) -> float:
        """Return fixed delay (attempt count is ignored)."""
        return min(base_delay, max_delay)


class LinearBackoff(BackoffCalculator):
    """Linearly increasing delay between retries."""

    def __init__(self, multiplier: float = 1.0) -> None:
        self.multiplier = multiplier

    def calculate(self, attempt: int, base_delay: float, max_delay: float) -> float:
        """Return linearly increasing delay."""
        delay = base_delay * (attempt + 1) * self.multiplier
        return min(delay, max_delay)


class ExponentialBackoff(BackoffCalculator):
    """Exponentially increasing delay between retries."""

    def __init__(self, multiplier: float = 2.0) -> None:
        self.multiplier = multiplier

    def calculate(self, attempt: int, base_delay: float, max_delay: float) -> float:
        """Return exponentially increasing delay."""
        delay = base_delay * (self.multiplier**attempt)
        return min(delay, max_delay)


class ExponentialJitterBackoff(BackoffCalculator):
    """Exponential backoff with random jitter."""

    def __init__(self, multiplier: float = 2.0) -> None:
        self.multiplier = multiplier

    def calculate(self, attempt: int, base_delay: float, max_delay: float) -> float:
        """Return exponential delay with jitter."""
        base = base_delay * (self.multiplier**attempt)
        # Add jitter: random value between 0 and base
        jitter = random.uniform(0, base * 0.5)
        delay = base + jitter
        return min(delay, max_delay)


def _get_backoff_calculator(
    strategy: BackoffStrategy,
    multiplier: float,
) -> BackoffCalculator:
    """Get backoff calculator for strategy.

    Args:
        strategy: Backoff strategy to use.
        multiplier: Multiplier for backoff calculation.

    Returns:
        BackoffCalculator instance.
    """
    calculators = {
        BackoffStrategy.FIXED: FixedBackoff(),
        BackoffStrategy.LINEAR: LinearBackoff(multiplier),
        BackoffStrategy.EXPONENTIAL: ExponentialBackoff(multiplier),
        BackoffStrategy.EXPONENTIAL_JITTER: ExponentialJitterBackoff(multiplier),
    }
    return calculators[strategy]


@dataclass
class RetryResult:
    """Result of a retry operation.

    Attributes:
        is_success: Whether the operation succeeded.
        result: The result value if successful.
        attempts: Number of attempts made.
        total_delay: Total delay in seconds across all retries.
        last_error: Last error if failed.
    """

    is_success: bool
    result: T | None = None
    attempts: int = 0
    total_delay: float = 0.0
    last_error: Exception | None = None


class RetryPolicy:
    """Configurable retry policy for event processing.

    Provides retry logic with configurable backoff strategies,
    maximum attempts, and exception filtering.

    Example:
        ```python
        policy = RetryPolicy(
            config=RetryConfig(
                max_retries=3,
                backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
            )
        )

        result = policy.execute(lambda: process_event(event))

        if result.is_success:
            print(f"Success after {result.attempts} attempts")
        else:
            print(f"Failed after {result.attempts} attempts: {result.last_error}")
        ```
    """

    def __init__(self, config: RetryConfig | None = None) -> None:
        """Initialize retry policy.

        Args:
            config: Retry configuration. Uses defaults if not provided.
        """
        self.config = config or RetryConfig()
        self._backoff = _get_backoff_calculator(
            self.config.backoff_strategy,
            self.config.backoff_multiplier,
        )

    def execute(
        self,
        func: Callable[[], T],
        context: dict[str, str] | None = None,
    ) -> RetryResult:
        """Execute a function with retry logic.

        Args:
            func: Function to execute.
            context: Optional context for logging.

        Returns:
            RetryResult with success status and result or error.
        """
        context = context or {}
        attempts = 0
        total_delay = 0.0
        last_error: Exception | None = None

        while attempts <= self.config.max_retries:
            try:
                result = func()

                if attempts > 0:
                    logger.info(
                        "retry_succeeded",
                        attempts=attempts + 1,
                        total_delay=total_delay,
                        **context,
                    )

                return RetryResult(
                    is_success=True,
                    result=result,
                    attempts=attempts + 1,
                    total_delay=total_delay,
                )

            except Exception as e:
                last_error = e
                attempts += 1

                # Check if we should retry
                if not self._should_retry(e, attempts):
                    logger.warning(
                        "retry_exhausted_or_non_retryable",
                        attempts=attempts,
                        error=str(e),
                        error_type=type(e).__name__,
                        **context,
                    )
                    break

                # Calculate delay
                delay = self._backoff.calculate(
                    attempts - 1,
                    self.config.base_delay_seconds,
                    self.config.max_delay_seconds,
                )
                total_delay += delay

                logger.info(
                    "retry_scheduled",
                    attempt=attempts,
                    max_retries=self.config.max_retries,
                    delay_seconds=delay,
                    error=str(e),
                    **context,
                )

                time.sleep(delay)

        return RetryResult(
            is_success=False,
            result=None,
            attempts=attempts,
            total_delay=total_delay,
            last_error=last_error,
        )

    async def execute_async(
        self,
        func: Callable[[], T],
        context: dict[str, str] | None = None,
    ) -> RetryResult:
        """Execute an async function with retry logic.

        Args:
            func: Async function to execute.
            context: Optional context for logging.

        Returns:
            RetryResult with success status and result or error.
        """
        import asyncio

        context = context or {}
        attempts = 0
        total_delay = 0.0
        last_error: Exception | None = None

        while attempts <= self.config.max_retries:
            try:
                result = await func()

                if attempts > 0:
                    logger.info(
                        "async_retry_succeeded",
                        attempts=attempts + 1,
                        total_delay=total_delay,
                        **context,
                    )

                return RetryResult(
                    is_success=True,
                    result=result,
                    attempts=attempts + 1,
                    total_delay=total_delay,
                )

            except Exception as e:
                last_error = e
                attempts += 1

                if not self._should_retry(e, attempts):
                    logger.warning(
                        "async_retry_exhausted_or_non_retryable",
                        attempts=attempts,
                        error=str(e),
                        error_type=type(e).__name__,
                        **context,
                    )
                    break

                delay = self._backoff.calculate(
                    attempts - 1,
                    self.config.base_delay_seconds,
                    self.config.max_delay_seconds,
                )
                total_delay += delay

                logger.info(
                    "async_retry_scheduled",
                    attempt=attempts,
                    max_retries=self.config.max_retries,
                    delay_seconds=delay,
                    error=str(e),
                    **context,
                )

                await asyncio.sleep(delay)

        return RetryResult(
            is_success=False,
            result=None,
            attempts=attempts,
            total_delay=total_delay,
            last_error=last_error,
        )

    def _should_retry(self, error: Exception, attempts: int) -> bool:
        """Determine if operation should be retried.

        Args:
            error: The exception that occurred.
            attempts: Number of attempts made so far.

        Returns:
            True if should retry, False otherwise.
        """
        # Check if we've exceeded max retries
        if attempts > self.config.max_retries:
            return False

        # Check if it's a non-retryable exception
        if isinstance(error, self.config.non_retryable_exceptions):
            return False

        # Check if it's a retryable exception
        if self.config.retryable_exceptions:
            return isinstance(error, self.config.retryable_exceptions)

        return True
