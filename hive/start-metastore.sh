#!/usr/bin/env bash

echo "Update schema"
# schematool -dbType postgres -initSchema
echo "Start metastore"
hive --service metastore
