#!/usr/bin/env bash

echo "Waiting superset to launch on superset:8088..."
while ! nc -z superset 8088; do
    sleep 1
done

echo "Creating database..."
superset set-database-uri \
    --database_name "hive" \
    --uri hive://hive:10000

echo "Importing dashboards..."
superset import-dashboards \
    --path ./dashboards/*.json \
    --username admin

echo "Waiting datahub to launch on datahub-gms:8080..."
while ! nc -z datahub-gms 8080; do
    sleep 30
done

echo "Ingesting metadata on datahub..."
python ./lineage_update.py
