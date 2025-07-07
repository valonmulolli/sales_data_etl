"""
Sales Data ETL Pipeline

A comprehensive ETL pipeline for processing sales data with validation,
monitoring, and quality checks.
"""

__version__ = "1.0.0"
__author__ = "Sales Data ETL Team"

from .cache_manager import CacheManager
from .config_validator import ConfigValidator
from .data_quality_checks import DataQualityChecker
from .data_validator import DataValidator
from .database_setup import init_database

# Import main components for easy access
from .extract import AdvancedSalesDataExtractor
from .health_check import HealthCheckServer
from .load import SalesDataLoader
from .models import Base, DatabaseConnection, SalesRecord
from .monitoring import ETLMonitor
from .structured_logging import PipelineLogger, StructuredLogger
from .transform import SalesDataTransformer

__all__ = [
    "AdvancedSalesDataExtractor",
    "SalesDataTransformer",
    "SalesDataLoader",
    "Base",
    "SalesRecord",
    "DatabaseConnection",
    "CacheManager",
    "DataValidator",
    "DataQualityChecker",
    "ETLMonitor",
    "HealthCheckServer",
    "ConfigValidator",
    "StructuredLogger",
    "PipelineLogger",
    "init_database",
]
