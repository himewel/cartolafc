#!/usr/bin/env bash

echo "Waiting airflow webserver to launch on airflow_webserver:8080..."
while ! nc -z airflow_webserver 8080; do
    sleep 1
done

echo "Clear old .err and .pid files..."
rm $AIRFLOW_HOME/*.pid $AIRFLOW_HOME/*.err

echo "Starting service..."
airflow scheduler
