"""Configuration validation for the ETL pipeline."""

import logging
import os
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Custom exception for configuration validation errors."""

    def __init__(self, message: str, field: str = None, value: Any = None):
        super().__init__(message)
        self.field = field
        self.value = value


class ConfigValidator:
    """Validates ETL pipeline configuration settings."""

    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def validate_all(self) -> bool:
        """
        Validate all configuration settings.

        Returns:
            bool: True if all validations pass, False otherwise
        """
        self.errors.clear()
        self.warnings.clear()

        try:
            # Import config after validation setup
            from .config import (
                API_CONFIG,
                BATCH_SIZE,
                DB_CONFIG,
                EXTRACTION_CONFIG,
                LOGGING_CONFIG,
                RETRY_CONFIG,
                SOURCE_DATABASE,
                TARGET_DATABASE,
            )

            # Validate database configurations
            self._validate_database_config(DB_CONFIG, "DB_CONFIG")
            self._validate_database_config(TARGET_DATABASE, "TARGET_DATABASE")
            self._validate_database_config(SOURCE_DATABASE, "SOURCE_DATABASE")

            # Validate API configuration
            self._validate_api_config(API_CONFIG)

            # Validate extraction configuration
            self._validate_extraction_config(EXTRACTION_CONFIG)

            # Validate logging configuration
            self._validate_logging_config(LOGGING_CONFIG)

            # Validate retry configuration
            self._validate_retry_config(RETRY_CONFIG)

            # Validate batch size
            self._validate_batch_size(BATCH_SIZE)

            # Validate environment variables
            self._validate_environment_variables()

            # Validate file paths
            self._validate_file_paths()

        except ImportError as e:
            self.errors.append(f"Failed to import configuration: {e}")
            return False

        # Log warnings and errors
        for warning in self.warnings:
            logger.warning(f"Configuration warning: {warning}")

        for error in self.errors:
            logger.error(f"Configuration error: {error}")

        return len(self.errors) == 0

    def _validate_database_config(self, config: Dict[str, Any], config_name: str):
        """Validate database configuration."""
        required_fields = ["host", "port", "database", "user"]

        for field in required_fields:
            if field not in config:
                self.errors.append(f"Missing required field '{field}' in {config_name}")
            elif not config[field]:
                self.errors.append(
                    f"Empty value for required field '{field}' in {config_name}"
                )

        # Validate port number
        if "port" in config and config["port"]:
            try:
                port = int(config["port"])
                if not (1 <= port <= 65535):
                    self.errors.append(f"Invalid port number {port} in {config_name}")
            except (ValueError, TypeError):
                self.errors.append(
                    f"Invalid port value '{config['port']}' in {config_name}"
                )

        # Validate connection string if present
        if "connection_string" in config and config["connection_string"]:
            self._validate_connection_string(config["connection_string"], config_name)

    def _validate_connection_string(self, connection_string: str, config_name: str):
        """Validate database connection string."""
        try:
            parsed = urlparse(connection_string)
            if not parsed.scheme:
                self.errors.append(f"Invalid connection string format in {config_name}")
            elif parsed.scheme not in ["postgresql", "postgres"]:
                self.warnings.append(
                    f"Unexpected database scheme '{parsed.scheme}' in {config_name}"
                )
        except Exception as e:
            self.errors.append(
                f"Failed to parse connection string in {config_name}: {e}"
            )

    def _validate_api_config(self, config: Dict[str, Any]):
        """Validate API configuration."""
        if not config.get("base_url"):
            self.warnings.append("API base_url not configured")
            return

        # Validate URL format
        try:
            parsed = urlparse(config["base_url"])
            if not parsed.scheme or not parsed.netloc:
                self.errors.append("Invalid API base_url format")
        except Exception as e:
            self.errors.append(f"Failed to parse API base_url: {e}")

        # Validate timeout
        timeout = config.get("timeout", 30)
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            self.errors.append("API timeout must be a positive number")

        # Validate authentication
        auth = config.get("auth", {})
        if auth.get("type") == "bearer" and not auth.get("token"):
            self.warnings.append("Bearer token not configured for API authentication")

    def _validate_extraction_config(self, config: Dict[str, Any]):
        """Validate extraction configuration."""
        sources = config.get("sources", {})

        # Validate CSV source configuration
        csv_config = sources.get("csv", {})
        if csv_config:
            path = csv_config.get("path")
            if path and not os.path.exists(path):
                self.warnings.append(f"CSV input path does not exist: {path}")

        # Validate validation rules
        validation = config.get("validation", {})
        required_columns = validation.get("required_columns", [])
        if not isinstance(required_columns, list):
            self.errors.append("required_columns must be a list")

        type_checks = validation.get("type_checks", {})
        if not isinstance(type_checks, dict):
            self.errors.append("type_checks must be a dictionary")

        range_checks = validation.get("range_checks", {})
        if not isinstance(range_checks, dict):
            self.errors.append("range_checks must be a dictionary")

    def _validate_logging_config(self, config: Dict[str, Any]):
        """Validate logging configuration."""
        level = config.get("level", "INFO")
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

        if level.upper() not in valid_levels:
            self.errors.append(
                f"Invalid log level '{level}'. Must be one of {valid_levels}"
            )

        log_file = config.get("file")
        if log_file:
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                try:
                    os.makedirs(log_dir, exist_ok=True)
                    logger.info(f"Created log directory: {log_dir}")
                except Exception as e:
                    self.errors.append(f"Cannot create log directory {log_dir}: {e}")

    def _validate_retry_config(self, config: Dict[str, Any]):
        """Validate retry configuration."""
        max_attempts = config.get("max_attempts", 3)
        if not isinstance(max_attempts, int) or max_attempts < 1:
            self.errors.append("max_attempts must be a positive integer")

        delay = config.get("delay", 2)
        if not isinstance(delay, (int, float)) or delay < 0:
            self.errors.append("delay must be a non-negative number")

        backoff = config.get("backoff", 1.5)
        if not isinstance(backoff, (int, float)) or backoff <= 0:
            self.errors.append("backoff must be a positive number")

    def _validate_batch_size(self, batch_size: int):
        """Validate batch size configuration."""
        if not isinstance(batch_size, int):
            self.errors.append("BATCH_SIZE must be an integer")
        elif batch_size < 1:
            self.errors.append("BATCH_SIZE must be a positive integer")
        elif batch_size > 100000:
            self.warnings.append("BATCH_SIZE is very large, may cause memory issues")

    def _validate_environment_variables(self):
        """Validate required environment variables."""
        required_vars = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME"]

        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)

        if missing_vars:
            self.warnings.append(
                f"Missing environment variables: {', '.join(missing_vars)}"
            )

        # Validate optional but important variables
        optional_vars = {
            "SLACK_WEBHOOK_URL": "Slack alerting will be disabled",
            "ALERT_EMAIL": "Email alerting will be disabled",
            "API_AUTH_TOKEN": "API authentication will be disabled",
        }

        for var, message in optional_vars.items():
            if not os.getenv(var):
                self.warnings.append(f"{var} not set: {message}")

    def _validate_file_paths(self):
        """Validate file paths and directories."""
        from .config import ARCHIVE_PATH, INPUT_PATH, OUTPUT_PATH

        paths_to_check = [
            (INPUT_PATH, "Input path"),
            (OUTPUT_PATH, "Output path"),
            (ARCHIVE_PATH, "Archive path"),
        ]

        for path, name in paths_to_check:
            if path and not os.path.exists(path):
                try:
                    os.makedirs(path, exist_ok=True)
                    logger.info(f"Created directory: {path}")
                except Exception as e:
                    self.errors.append(f"Cannot create {name.lower()}: {e}")

    def get_validation_report(self) -> Dict[str, Any]:
        """
        Get a detailed validation report.

        Returns:
            Dict containing validation results
        """
        return {
            "is_valid": len(self.errors) == 0,
            "errors": self.errors.copy(),
            "warnings": self.warnings.copy(),
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
        }


def validate_configuration() -> bool:
    """
    Convenience function to validate configuration.

    Returns:
        bool: True if configuration is valid, False otherwise
    """
    validator = ConfigValidator()
    return validator.validate_all()


def get_configuration_report() -> Dict[str, Any]:
    """
    Get a detailed configuration validation report.

    Returns:
        Dict containing validation results
    """
    validator = ConfigValidator()
    validator.validate_all()
    return validator.get_validation_report()


def check_required_config() -> None:
    """
    Check required configuration and raise ConfigurationError if invalid.

    Raises:
        ConfigurationError: If configuration is invalid
    """
    validator = ConfigValidator()
    if not validator.validate_all():
        report = validator.get_validation_report()
        error_messages = "\n".join(report["errors"])
        raise ConfigurationError(f"Configuration validation failed:\n{error_messages}")


# Configuration validation decorator
def require_valid_config(func):
    """Decorator to ensure configuration is valid before function execution."""

    def wrapper(*args, **kwargs):
        check_required_config()
        return func(*args, **kwargs)

    return wrapper  # Configuration validation decorator


def require_valid_config(func):
    """Decorator to ensure configuration is valid before function execution."""

    def wrapper(*args, **kwargs):
        check_required_config()
        return func(*args, **kwargs)

    return wrapper
