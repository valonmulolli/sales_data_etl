import functools
import logging
import time
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
    on_retry: Optional[Callable] = None,
):
    """
    Decorator for retrying a function with exponential backoff.

    Args:
        max_attempts (int): Maximum number of retry attempts
        delay (float): Initial delay between retries in seconds
        backoff_factor (float): Multiplier for delay between retries
        exceptions (tuple): Exceptions to catch and retry
        on_retry (Optional[Callable]): Optional callback function on retry

    Returns:
        Decorated function with retry mechanism
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            current_delay = delay

            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempts += 1

                    if attempts == max_attempts:
                        logger.error(
                            f"Function {func.__name__} failed after {max_attempts} attempts"
                        )
                        raise

                    # Call optional retry callback
                    if on_retry:
                        on_retry(attempts, e)

                    logger.warning(
                        f"Retry attempt {attempts} for {func.__name__}. "
                        f"Error: {str(e)}. Waiting {current_delay} seconds."
                    )

                    # Wait before next retry with exponential backoff
                    time.sleep(current_delay)
                    current_delay *= backoff_factor

        return wrapper

    return decorator


def log_retry(attempt: int, exception: Exception):
    """
    Default retry logging callback.

    Args:
        attempt (int): Current retry attempt number
        exception (Exception): Exception that triggered the retry
    """
    logger.info(
        f"Retry attempt {attempt}. "
        f"Exception: {type(exception).__name__} - {str(exception)}"
    )


class RetryableError(Exception):
    """
    Custom exception for errors that should trigger retry mechanism.
    """

    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


def retry_with_backoff(max_retries: int = 3, backoff_factor: float = 2.0):
    """
    Decorator for retrying a function with exponential backoff.

    Args:
        max_retries (int): Maximum number of retry attempts
        backoff_factor (float): Multiplier for delay between retries

    Returns:
        Decorated function with retry mechanism
    """
    return retry(
        max_attempts=max_retries,
        delay=1.0,
        backoff_factor=backoff_factor,
        exceptions=(Exception,),
        on_retry=log_retry,
    )
