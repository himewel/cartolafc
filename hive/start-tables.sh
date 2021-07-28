#!/usr/bin/env bash

echo "Waiting hive server to launch on hive:10000..."
while ! nc -z hive 10000; do
    sleep 1
done

echo "Creating tables..."
for file in ./scripts/*.hql; do
    hive --verbose -f $file
done
