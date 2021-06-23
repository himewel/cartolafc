#!/usr/bin/env bash

echo "Clear old .err and .pid files..."
rm $AIRFLOW_HOME/*.pid $AIRFLOW_HOME/*.err

echo "Starting service..."
airflow scheduler
