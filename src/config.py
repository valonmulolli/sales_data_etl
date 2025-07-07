"""Configuration management for Sales Data ETL Pipeline."""

import os
from typing import Any, Dict, List

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Input and Output Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH = os.path.join(BASE_DIR, "data")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "output")
ARCHIVE_PATH = os.path.join(BASE_DIR, "data", "archive")

# Batch Processing Configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))

# Database Configuration
DB_CONFIG = {
    "connection_string": os.getenv(
        "DATABASE_URL", "postgresql://user:password@localhost/sales_db"
    ),
    "schema": os.getenv("DB_SCHEMA", "public"),
    "pool_size": int(os.getenv("DB_POOL_SIZE", 5)),
    "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", 10)),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "sales_db"),
    "user": os.getenv("DB_USER", "etl_user"),
    "password": os.getenv("DB_PASSWORD", "etl_password"),
}

# Source Database Configuration
SOURCE_DATABASE = {
    "type": os.getenv("SOURCE_DB_TYPE", "postgresql"),
    "host": os.getenv("SOURCE_DB_HOST", "localhost"),
    "port": os.getenv("SOURCE_DB_PORT", "5432"),
    "database": os.getenv("SOURCE_DB_NAME", "source_sales_db"),
    "user": os.getenv("SOURCE_DB_USER", "source_user"),
    "password": os.getenv("SOURCE_DB_PASSWORD", ""),
}

# Target Database Configuration
TARGET_DATABASE = {
    "type": os.getenv("TARGET_DB_TYPE", "postgresql"),
    "host": os.getenv("TARGET_DB_HOST", "postgres"),
    "port": os.getenv("TARGET_DB_PORT", "5432"),
    "database": os.getenv("TARGET_DB_NAME", "sales_db"),
    "user": os.getenv("TARGET_DB_USER", "etl_user"),
    "password": os.getenv("TARGET_DB_PASSWORD", "etl_password"),
    "schema": os.getenv("TARGET_DB_SCHEMA", "public"),
}

# API Configuration
API_CONFIG = {
    "base_url": os.getenv("API_BASE_URL", "https://api.example.com/sales"),
    "endpoints": {"sales_data": "/sales-data", "product_info": "/products"},
    "auth": {
        "type": os.getenv("API_AUTH_TYPE", "bearer"),
        "token": os.getenv("API_AUTH_TOKEN", ""),
    },
    "timeout": int(os.getenv("API_TIMEOUT", 30)),
}

# Extraction Configuration
EXTRACTION_CONFIG = {
    "sources": {
        "csv": {"path": INPUT_PATH, "default_filename": "sales_data.csv"},
        "database": DB_CONFIG,
        "api": API_CONFIG,
    },
    "validation": {
        "required_columns": ["date", "product_id", "quantity", "unit_price"],
        "type_checks": {
            "quantity": "numeric",
            "unit_price": "numeric",
            "date": "datetime",
        },
        "range_checks": {"quantity": (0, 1000), "unit_price": (0, 10000)},
    },
}

# Logging Configuration
LOGGING_CONFIG = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": os.path.join(BASE_DIR, "logs", "etl_pipeline.log"),
}

# Retry Configuration
RETRY_CONFIG = {
    "max_attempts": int(os.getenv("RETRY_MAX_ATTEMPTS", 3)),
    "delay": int(os.getenv("RETRY_DELAY", 2)),
    "backoff": float(os.getenv("RETRY_BACKOFF", 1.5)),
}


def load_config(config_path: str = None) -> Dict[str, Any]:
    """Load configuration from environment variables and optional config file."""
    config = {
        "database": DB_CONFIG,
        "source_database": SOURCE_DATABASE,
        "target_database": TARGET_DATABASE,
        "api": API_CONFIG,
        "extraction": EXTRACTION_CONFIG,
        "logging": LOGGING_CONFIG,
        "retry": RETRY_CONFIG,
        "batch_size": BATCH_SIZE,
        "input_path": INPUT_PATH,
        "output_path": OUTPUT_PATH,
        "archive_path": ARCHIVE_PATH,
    }

    return config


def validate_config() -> Dict[str, Any]:
    """Validate the configuration and return validation results."""
    errors = []
    warnings = []

    # Check required environment variables
    required_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]

    for var in required_vars:
        if not os.getenv(var):
            warnings.append(f"Environment variable {var} not set, using default value")

    # Check database connection
    if not DB_CONFIG.get("password"):
        warnings.append("Database password not set")

    # Check API configuration
    if not API_CONFIG["auth"]["token"]:
        warnings.append("API authentication token not set")

    # Check file paths
    if not os.path.exists(INPUT_PATH):
        warnings.append(f"Input path does not exist: {INPUT_PATH}")

    # Create output directories if they don't exist
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    os.makedirs(ARCHIVE_PATH, exist_ok=True)
    os.makedirs(os.path.dirname(LOGGING_CONFIG["file"]), exist_ok=True)

    return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}
