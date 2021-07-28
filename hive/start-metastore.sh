#!/usr/bin/env bash

echo "Creating hive schema in postgres..."
schematool -dbType postgres -initSchema

echo "Starting hive metastore..."
hive --service metastore
