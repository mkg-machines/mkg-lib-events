"""Unit tests for RetryPolicy."""

import pytest

from mkg_lib_events.retry import (
    BackoffStrategy,
    ExponentialBackoff,
    ExponentialJitterBackoff,
    FixedBackoff,
    LinearBackoff,
    RetryConfig,
    RetryPolicy,
    RetryResult,
)


class TestBackoffCalculators:
    """Tests for backoff calculators."""

    def test_fixed_backoff(self) -> None:
        """Test fixed backoff returns constant delay."""
        backoff = FixedBackoff()

        assert backoff.calculate(0, 1.0, 60.0) == 1.0
        assert backoff.calculate(1, 1.0, 60.0) == 1.0
        assert backoff.calculate(5, 1.0, 60.0) == 1.0

    def test_fixed_backoff_respects_max(self) -> None:
        """Test fixed backoff respects max delay."""
        backoff = FixedBackoff()

        assert backoff.calculate(0, 100.0, 60.0) == 60.0

    def test_linear_backoff(self) -> None:
        """Test linear backoff increases linearly."""
        backoff = LinearBackoff(multiplier=1.0)

        assert backoff.calculate(0, 1.0, 60.0) == 1.0
        assert backoff.calculate(1, 1.0, 60.0) == 2.0
        assert backoff.calculate(2, 1.0, 60.0) == 3.0

    def test_linear_backoff_respects_max(self) -> None:
        """Test linear backoff respects max delay."""
        backoff = LinearBackoff(multiplier=1.0)

        assert backoff.calculate(100, 1.0, 10.0) == 10.0

    def test_exponential_backoff(self) -> None:
        """Test exponential backoff doubles each attempt."""
        backoff = ExponentialBackoff(multiplier=2.0)

        assert backoff.calculate(0, 1.0, 60.0) == 1.0
        assert backoff.calculate(1, 1.0, 60.0) == 2.0
        assert backoff.calculate(2, 1.0, 60.0) == 4.0
        assert backoff.calculate(3, 1.0, 60.0) == 8.0

    def test_exponential_backoff_respects_max(self) -> None:
        """Test exponential backoff respects max delay."""
        backoff = ExponentialBackoff(multiplier=2.0)

        assert backoff.calculate(10, 1.0, 60.0) == 60.0

    def test_exponential_jitter_backoff(self) -> None:
        """Test exponential jitter backoff has variability."""
        backoff = ExponentialJitterBackoff(multiplier=2.0)

        # Run multiple times to verify jitter adds variability
        results = [backoff.calculate(2, 1.0, 60.0) for _ in range(10)]

        # Base is 4.0, jitter adds up to 2.0, so range is 4.0-6.0
        assert all(4.0 <= r <= 6.0 for r in results)
        # With jitter, not all results should be identical
        assert len(set(results)) > 1


class TestRetryConfig:
    """Tests for RetryConfig."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = RetryConfig()

        assert config.max_retries == 3
        assert config.base_delay_seconds == 1.0
        assert config.max_delay_seconds == 60.0
        assert config.backoff_strategy == BackoffStrategy.EXPONENTIAL_JITTER
        assert config.backoff_multiplier == 2.0

    def test_custom_config(self) -> None:
        """Test custom configuration."""
        config = RetryConfig(
            max_retries=5,
            base_delay_seconds=0.5,
            backoff_strategy=BackoffStrategy.FIXED,
        )

        assert config.max_retries == 5
        assert config.base_delay_seconds == 0.5
        assert config.backoff_strategy == BackoffStrategy.FIXED


class TestRetryPolicy:
    """Tests for RetryPolicy."""

    def test_successful_execution_no_retry(self) -> None:
        """Test successful execution doesn't retry."""
        policy = RetryPolicy()
        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = policy.execute(func)

        assert result.is_success is True
        assert result.result == "success"
        assert result.attempts == 1
        assert call_count == 1

    def test_retry_on_failure(self, mocker) -> None:
        """Test retry on transient failure."""
        mocker.patch("time.sleep")  # Don't actually sleep

        config = RetryConfig(
            max_retries=3,
            base_delay_seconds=0.1,
            backoff_strategy=BackoffStrategy.FIXED,
        )
        policy = RetryPolicy(config=config)

        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Connection failed")
            return "success"

        result = policy.execute(func)

        assert result.is_success is True
        assert result.result == "success"
        assert result.attempts == 3
        assert call_count == 3

    def test_exhausted_retries(self, mocker) -> None:
        """Test when all retries are exhausted."""
        mocker.patch("time.sleep")

        config = RetryConfig(
            max_retries=2,
            base_delay_seconds=0.1,
            backoff_strategy=BackoffStrategy.FIXED,
        )
        policy = RetryPolicy(config=config)

        def func():
            raise ConnectionError("Always fails")

        result = policy.execute(func)

        assert result.is_success is False
        assert result.attempts == 3  # Initial + 2 retries
        assert isinstance(result.last_error, ConnectionError)

    def test_non_retryable_exception(self, mocker) -> None:
        """Test that non-retryable exceptions stop immediately."""
        mocker.patch("time.sleep")

        config = RetryConfig(
            max_retries=3,
            non_retryable_exceptions=(ValueError,),
        )
        policy = RetryPolicy(config=config)

        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            raise ValueError("Not retryable")

        result = policy.execute(func)

        assert result.is_success is False
        assert result.attempts == 1
        assert call_count == 1

    def test_retryable_exception_filter(self, mocker) -> None:
        """Test that only retryable exceptions are retried."""
        mocker.patch("time.sleep")

        config = RetryConfig(
            max_retries=3,
            retryable_exceptions=(ConnectionError,),
        )
        policy = RetryPolicy(config=config)

        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("Retryable")
            raise RuntimeError("Not retryable")

        result = policy.execute(func)

        assert result.is_success is False
        assert result.attempts == 2

    def test_context_passed_to_logging(self, mocker) -> None:
        """Test that context is passed for logging."""
        mocker.patch("time.sleep")
        mock_logger = mocker.patch("mkg_lib_events.retry.logger")

        config = RetryConfig(max_retries=1, base_delay_seconds=0.1)
        policy = RetryPolicy(config=config)

        def func():
            raise ConnectionError("Fails")

        policy.execute(func, context={"event_id": "123"})

        # Check that context was passed to logger
        assert any("event_id" in str(call) for call in mock_logger.method_calls)


class TestRetryResult:
    """Tests for RetryResult."""

    def test_success_result(self) -> None:
        """Test success result properties."""
        result = RetryResult(
            is_success=True,
            result="value",
            attempts=1,
            total_delay=0.0,
        )

        assert result.is_success is True
        assert result.result == "value"
        assert result.last_error is None

    def test_failure_result(self) -> None:
        """Test failure result properties."""
        error = ValueError("Test error")
        result = RetryResult(
            is_success=False,
            result=None,
            attempts=3,
            total_delay=5.0,
            last_error=error,
        )

        assert result.is_success is False
        assert result.result is None
        assert result.last_error is error
        assert result.total_delay == 5.0


class TestAsyncRetry:
    """Tests for async retry functionality."""

    @pytest.mark.asyncio
    async def test_async_successful_execution(self, mocker) -> None:
        """Test async successful execution."""
        mocker.patch("asyncio.sleep")

        policy = RetryPolicy()
        call_count = 0

        async def func():
            nonlocal call_count
            call_count += 1
            return "async_success"

        result = await policy.execute_async(func)

        assert result.is_success is True
        assert result.result == "async_success"
        assert result.attempts == 1

    @pytest.mark.asyncio
    async def test_async_retry_on_failure(self, mocker) -> None:
        """Test async retry on failure."""
        mocker.patch("asyncio.sleep")

        config = RetryConfig(max_retries=2, base_delay_seconds=0.1)
        policy = RetryPolicy(config=config)

        call_count = 0

        async def func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Async connection failed")
            return "success"

        result = await policy.execute_async(func)

        assert result.is_success is True
        assert result.attempts == 2
