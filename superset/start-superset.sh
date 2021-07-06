#!/usr/bin/env bash

echo "Creating default user..."
superset fab create-admin \
   --username admin \
   --firstname Superset \
   --lastname Admin \
   --email admin@superset.com \
   --password admin

echo "Migrating meta database..."
superset db upgrade
superset init

echo "Starting webserver..."
gunicorn \
    --bind "0.0.0.0:8088" \
    --access-logfile '-' \
    --error-logfile '-' \
    --workers 1 \
    --worker-class gthread \
    --threads 20 \
    --timeout 60 \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    "superset.app:create_app()"
