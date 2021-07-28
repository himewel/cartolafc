#!/usr/bin/env bash

echo "Waiting namenode to launch on hadoop:8020..."
while ! nc -z hadoop 8020; do
    sleep 1
done

echo "Waiting datanode to launch on hadoop_datanode:9864..."
while ! nc -z hadoop_datanode 9864; do
    sleep 1
done

echo "Creating folders..."
hdfs dfs -mkdir -p /raw /trusted
hdfs dfs -ls /
