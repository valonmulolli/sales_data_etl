#!/bin/bash

# Enhanced Database Migration Script for Sales Data ETL Pipeline

# Set strict error handling
set -euo pipefail

# Logging configuration
LOG_DIR="./logs"
MIGRATION_LOG="${LOG_DIR}/migration.log"

# Create logs directory if it doesn't exist
mkdir -p "${LOG_DIR}"

# Logging function
log() {
    local level="$1"
    local message="$2"
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo "[${timestamp}] [${level}] ${message}" | tee -a "${MIGRATION_LOG}"
}

# Error handling function
handle_error() {
    local error_message="$1"
    log "ERROR" "${error_message}"
    exit 1
}

# Ensure script is run from the project root
cd "$(dirname "$0")" || handle_error "Failed to change directory"

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate || handle_error "Failed to activate virtual environment"
fi

# Check for Alembic
if ! command -v alembic &> /dev/null; then
    log "INFO" "Alembic not found. Installing..."
    pip install alembic || handle_error "Failed to install Alembic"
fi

# Function to display usage
usage() {
    echo "Database Migration Utility"
    echo "========================="
    echo "Usage:"
    echo "  ./migrate.sh upgrade     # Apply all pending migrations"
    echo "  ./migrate.sh downgrade   # Rollback last migration"
    echo "  ./migrate.sh new         # Create a new migration"
    echo "  ./migrate.sh current     # Show current migration version"
    echo "  ./migrate.sh history     # Show migration history"
    echo "  ./migrate.sh stamp       # Stamp the current revision without running migrations"
    exit 1
}

# Backup database before migration
backup_database() {
    local timestamp=$(date "+%Y%m%d_%H%M%S")
    local backup_dir="./db_backups"
    mkdir -p "${backup_dir}"
    
    log "INFO" "Creating database backup..."
    pg_dump -U "${DB_USER}" -d "${DB_NAME}" > "${backup_dir}/db_backup_${timestamp}.sql" || \
        log "WARNING" "Database backup failed"
}

# Perform migration based on argument
case "${1:-}" in
    upgrade)
        log "INFO" "Upgrading database to latest version..."
        backup_database
        alembic upgrade head || handle_error "Migration upgrade failed"
        log "SUCCESS" "Database successfully upgraded"
        ;;
    downgrade)
        log "INFO" "Rolling back last migration..."
        alembic downgrade -1 || handle_error "Migration downgrade failed"
        log "SUCCESS" "Last migration rolled back"
        ;;
    new)
        log "INFO" "Creating new migration..."
        read -p "Enter migration message: " message
        alembic revision --autogenerate -m "${message}" || handle_error "Failed to create migration"
        log "SUCCESS" "New migration created: ${message}"
        ;;
    current)
        log "INFO" "Showing current migration version..."
        alembic current
        ;;
    history)
        log "INFO" "Showing migration history..."
        alembic history
        ;;
    stamp)
        log "INFO" "Stamping current revision..."
        alembic stamp head || handle_error "Failed to stamp revision"
        ;;
    *)
        usage
        ;;
esac