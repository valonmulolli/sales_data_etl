#!/bin/bash

# Database Migration Script for Sales Data ETL Pipeline

# Ensure script is run from the project root
cd "$(dirname "$0")"

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Check for Alembic
if ! command -v alembic &> /dev/null; then
    echo "Alembic not found. Installing..."
    pip install alembic
fi

# Function to display usage
usage() {
    echo "Usage:"
    echo "  ./migrate.sh upgrade     # Apply all migrations"
    echo "  ./migrate.sh downgrade   # Rollback last migration"
    echo "  ./migrate.sh new         # Create a new migration"
    exit 1
}

# Perform migration based on argument
case "$1" in
    upgrade)
        echo "Upgrading database to latest version..."
        alembic upgrade head
        ;;
    downgrade)
        echo "Rolling back last migration..."
        alembic downgrade -1
        ;;
    new)
        echo "Creating new migration..."
        read -p "Enter migration message: " message
        alembic revision --autogenerate -m "$message"
        ;;
    *)
        usage
        ;;
esac
