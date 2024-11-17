#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
until pg_isready -h $DB_HOST -p $DB_PORT -U $DB_USER; do
  >&2 echo "PostgreSQL is unavailable - sleeping"
  sleep 5
done

>&2 echo "PostgreSQL is up - executing migrations"

# Run Alembic migrations
alembic upgrade head

# Execute the main command (passed as arguments or default)
exec "$@"
