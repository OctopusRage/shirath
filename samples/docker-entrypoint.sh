#!/bin/bash
set -e

echo "Waiting for PostgreSQL to be ready..."
until pg_isready -h "$DB_HOST" -U "$DB_USERNAME" -d "$DB_NAME"; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done
echo "PostgreSQL is ready!"

echo "Waiting for ClickHouse to be ready..."
until curl -s "http://clickhouse:8123/ping" > /dev/null 2>&1; do
  echo "ClickHouse is unavailable - sleeping"
  sleep 2
done
echo "ClickHouse is ready!"

# Create Oban database if it doesn't exist
echo "Setting up Oban database..."
PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USERNAME" -tc \
  "SELECT 1 FROM pg_database WHERE datname = 'shirath'" | grep -q 1 || \
  PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -U "$DB_USERNAME" -c "CREATE DATABASE shirath"

# Run Oban migrations
echo "Running Oban migrations..."
mix ecto.migrate -r Shirath.ObanRepo

echo "Starting Shirath..."
exec "$@"
