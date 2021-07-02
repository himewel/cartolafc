#!/usr/bin/env bash

echo "Starting upload..."
hdfs dfs \
    -fs hdfs://hadoop:8020 \
    -put $SOURCE $DEST

echo "Listing file at destiny..."
hdfs dfs \
    -fs hdfs://hadoop:8020 \
    -ls $DEST
