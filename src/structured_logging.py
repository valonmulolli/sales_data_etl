"""Structured logging for the ETL pipeline with correlation IDs and context tracking."""

import json
import logging
import logging.handlers
import os
import sys
import time
import traceback
import uuid
from contextvars import ContextVar
from datetime import datetime
from functools import wraps
from typing import Any, Dict, Optional, Union

# Context variables for correlation tracking
correlation_id: ContextVar[str] = ContextVar("correlation_id", default=None)
pipeline_run_id: ContextVar[str] = ContextVar("pipeline_run_id", default=None)
user_id: ContextVar[str] = ContextVar("user_id", default=None)


class StructuredFormatter(logging.Formatter):
    """Custom formatter that outputs structured JSON logs."""

    def __init__(self, include_caller_info: bool = True):
        super().__init__()
        self.include_caller_info = include_caller_info

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        # Base log structure
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": correlation_id.get(),
            "pipeline_run_id": pipeline_run_id.get(),
            "user_id": user_id.get(),
        }

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info),
            }

        # Add caller information
        if self.include_caller_info and record.filename:
            log_entry["caller"] = {
                "filename": record.filename,
                "function": record.funcName,
                "line": record.lineno,
            }

        # Add custom fields from record
        if hasattr(record, "structured_data"):
            log_entry.update(record.structured_data)

        # Add process and thread info
        log_entry["process"] = {
            "pid": record.process,
            "thread_id": record.thread,
            "thread_name": record.threadName,
        }

        return json.dumps(log_entry, default=str)


class StructuredLogger:
    """Enhanced logger with structured logging capabilities."""

    def __init__(self, name: str, level: int = logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self._setup_handlers()

    def _setup_handlers(self):
        """Set up logging handlers with structured formatting."""
        # Clear existing handlers
        self.logger.handlers.clear()

        # Console handler with structured formatting
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(StructuredFormatter())
        self.logger.addHandler(console_handler)

        # File handler with rotation
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            os.path.join(log_dir, "structured_etl.log"),
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
        )
        file_handler.setFormatter(StructuredFormatter())
        self.logger.addHandler(file_handler)

    def _log_with_context(self, level: int, message: str, **kwargs):
        """Log message with additional structured context."""
        extra = {"structured_data": kwargs}
        self.logger.log(level, message, extra=extra)

    def info(self, message: str, **kwargs):
        """Log info message with structured data."""
        self._log_with_context(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message with structured data."""
        self._log_with_context(logging.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs):
        """Log error message with structured data."""
        self._log_with_context(logging.ERROR, message, **kwargs)

    def critical(self, message: str, **kwargs):
        """Log critical message with structured data."""
        self._log_with_context(logging.CRITICAL, message, **kwargs)

    def debug(self, message: str, **kwargs):
        """Log debug message with structured data."""
        self._log_with_context(logging.DEBUG, message, **kwargs)

    def exception(self, message: str, **kwargs):
        """Log exception with traceback and structured data."""
        kwargs["exception"] = True
        self._log_with_context(logging.ERROR, message, **kwargs)


class PipelineLogger:
    """Specialized logger for ETL pipeline operations."""

    def __init__(self, pipeline_name: str = "sales_etl"):
        self.pipeline_name = pipeline_name
        self.logger = StructuredLogger(f"pipeline.{pipeline_name}")
        self.start_time = None
        self.metrics = {}

    def start_pipeline(self, **kwargs):
        """Start pipeline execution with correlation tracking."""
        run_id = str(uuid.uuid4())
        pipeline_run_id.set(run_id)

        self.start_time = time.time()
        self.logger.info(
            "Pipeline started",
            pipeline_name=self.pipeline_name,
            run_id=run_id,
            **kwargs,
        )

    def end_pipeline(self, success: bool = True, **kwargs):
        """End pipeline execution with metrics."""
        if self.start_time:
            duration = time.time() - self.start_time
            kwargs["duration_seconds"] = duration
            kwargs["success"] = success

        self.logger.info(
            "Pipeline completed",
            pipeline_name=self.pipeline_name,
            run_id=pipeline_run_id.get(),
            **kwargs,
        )

    def log_extraction(self, source_type: str, record_count: int, **kwargs):
        """Log extraction step."""
        self.logger.info(
            "Data extraction completed",
            step="extract",
            source_type=source_type,
            record_count=record_count,
            **kwargs,
        )

    def log_transformation(self, input_count: int, output_count: int, **kwargs):
        """Log transformation step."""
        self.logger.info(
            "Data transformation completed",
            step="transform",
            input_record_count=input_count,
            output_record_count=output_count,
            **kwargs,
        )

    def log_loading(self, destination: str, record_count: int, **kwargs):
        """Log loading step."""
        self.logger.info(
            "Data loading completed",
            step="load",
            destination=destination,
            record_count=record_count,
            **kwargs,
        )

    def log_validation(self, validation_results: Dict[str, Any], **kwargs):
        """Log validation results."""
        self.logger.info(
            "Data validation completed",
            step="validate",
            validation_results=validation_results,
            **kwargs,
        )

    def log_error(self, error: Exception, step: str = None, **kwargs):
        """Log pipeline error."""
        self.logger.error(
            f"Pipeline error in step: {step or 'unknown'}",
            step=step,
            error_type=type(error).__name__,
            error_message=str(error),
            **kwargs,
        )


def with_correlation_id(func):
    """Decorator to add correlation ID to function execution."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        # Generate correlation ID if not present
        if not correlation_id.get():
            correlation_id.set(str(uuid.uuid4()))

        # Log function entry
        logger = StructuredLogger(func.__module__)
        logger.debug(
            "Function called",
            function_name=func.__name__,
            args_count=len(args),
            kwargs_keys=list(kwargs.keys()),
        )

        try:
            result = func(*args, **kwargs)

            # Log successful completion
            logger.debug("Function completed successfully", function_name=func.__name__)

            return result

        except Exception as e:
            # Log error
            logger.error(
                "Function failed",
                function_name=func.__name__,
                error_type=type(e).__name__,
                error_message=str(e),
            )
            raise

    return wrapper


def log_execution_time(func):
    """Decorator to log function execution time."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger = StructuredLogger(func.__module__)

        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time

            logger.info(
                "Function execution completed",
                function_name=func.__name__,
                execution_time_seconds=execution_time,
                success=True,
            )

            return result

        except Exception as e:
            execution_time = time.time() - start_time

            logger.error(
                "Function execution failed",
                function_name=func.__name__,
                execution_time_seconds=execution_time,
                error_type=type(e).__name__,
                error_message=str(e),
                success=False,
            )
            raise

    return wrapper


def setup_structured_logging(
    level: str = "INFO", log_file: str = None, include_caller_info: bool = True
) -> StructuredLogger:
    """
    Set up structured logging for the application.

    Args:
        level: Logging level
        log_file: Optional custom log file path
        include_caller_info: Whether to include caller information

    Returns:
        Configured structured logger
    """
    # Convert string level to logging constant
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Create main application logger
    logger = StructuredLogger("sales_etl", log_level)

    # Set up custom log file if specified
    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5
        )
        file_handler.setFormatter(StructuredFormatter(include_caller_info))
        logger.logger.addHandler(file_handler)

    logger.info(
        "Structured logging initialized",
        log_level=level,
        log_file=log_file,
        include_caller_info=include_caller_info,
    )

    return logger


def get_correlation_id() -> Optional[str]:
    """Get current correlation ID."""
    return correlation_id.get()


def set_correlation_id(corr_id: str):
    """Set correlation ID for current context."""
    correlation_id.set(corr_id)


def get_pipeline_run_id() -> Optional[str]:
    """Get current pipeline run ID."""
    return pipeline_run_id.get()


def set_pipeline_run_id(run_id: str):
    """Set pipeline run ID for current context."""
    pipeline_run_id.set(run_id)


def log_structured_data(data: Dict[str, Any], level: str = "INFO"):
    """Log structured data with current context."""
    logger = StructuredLogger("structured_data")
    log_method = getattr(logger, level.lower())
    log_method("Structured data log", **data)


# Convenience functions for common logging patterns
def log_database_operation(operation: str, table: str, record_count: int, **kwargs):
    """Log database operation with structured data."""
    logger = StructuredLogger("database")
    logger.info(
        "Database operation completed",
        operation=operation,
        table=table,
        record_count=record_count,
        **kwargs,
    )


def log_api_request(
    method: str, url: str, status_code: int, response_time: float, **kwargs
):
    """Log API request with structured data."""
    logger = StructuredLogger("api")
    logger.info(
        "API request completed",
        method=method,
        url=url,
        status_code=status_code,
        response_time_seconds=response_time,
        **kwargs,
    )


def log_data_quality_check(
    check_name: str, score: float, details: Dict[str, Any], **kwargs
):
    """Log data quality check results."""
    logger = StructuredLogger("data_quality")
    logger.info(
        "Data quality check completed",
        check_name=check_name,
        quality_score=score,
        details=details,
        **kwargs,
    )


def setup_logging():
    """Set up logging for the ETL pipeline."""
    return setup_structured_logging(level="INFO")
