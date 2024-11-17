"""Configuration settings for the Sales Data ETL pipeline."""

# Database configurations
SOURCE_DATABASE = {
    'host': 'localhost',
    'database': 'sales_source_db',
    'user': 'user',
    'password': 'password'
}

TARGET_DATABASE = {
    'host': 'localhost',
    'database': 'sales_warehouse_db',
    'user': 'user',
    'password': 'password'
}

# File paths
INPUT_PATH = 'data/input'
OUTPUT_PATH = 'data/output'

# Logging configuration
LOG_FILE = 'logs/sales_etl.log'

# Data configurations
BATCH_SIZE = 1000
DATE_FORMAT = '%Y-%m-%d'
