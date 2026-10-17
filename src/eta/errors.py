"""Error taxonomy that drives retry decisions.

Every failure inside a step is classified as either *retryable* (transient:
timeouts, 5xx, rate limits, crashes) or *permanent* (bad input, 4xx, template
errors). Permanent errors skip remaining retries and go straight to the
dead-letter queue — retrying them would only burn capacity.
"""

from __future__ import annotations


class ETAError(Exception):
    """Base class for all engine errors."""


class ConfigError(ETAError):
    """Invalid sources / workflow configuration."""


class StepError(ETAError):
    retryable: bool = True

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class RetryableError(StepError):
    """Transient failure; the step should be retried with backoff."""

    retryable = True


class PermanentError(StepError):
    """Failure that will not succeed on retry; dead-letter immediately."""

    retryable = False


class LeaseLostError(ETAError):
    """The worker no longer owns the run (lease expired and was reclaimed)."""


class SignatureError(ETAError):
    """Webhook signature verification failed."""
