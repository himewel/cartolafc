#!/usr/bin/env bash

mode=$1
case "$mode" in
    airflow)
        echo "Starting Apache Airflow services..."
        docker-compose \
            --file docker-compose.airflow.yaml \
            --file docker-compose.hive.yaml \
            --file docker-compose.datahub.yaml \
            up --detach
        ;;

    superset)
        echo "Starting Apache Superset services..."
        docker-compose \
            --file docker-compose.superset.yaml \
            --file docker-compose.hive.yaml \
            --file docker-compose.datahub.yaml \
            up --detach
        ;;

    *)
        echo "Starting all services..."
        docker-compose \
            --file docker-compose.airflow.yaml \
            --file docker-compose.superset.yaml \
            --file docker-compose.hive.yaml \
            --file docker-compose.datahub.yaml \
            up --detach
        ;;
esac
