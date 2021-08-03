#!/usr/bin/env bash

mode=$1

case "$mode" in
    airflow)
        echo "Starting Apache Airflow services..."
        docker-compose \
            --file docker-compose.airflow.yaml \
            --file docker-compose.datahub.yaml \
            --file docker-compose.hive.yaml \
            up --detach
        ;;

    superset)
        echo "Starting Apache Superset services..."
        docker-compose \
            --file docker-compose.datahub.yaml \
            --file docker-compose.hive.yaml \
            --file docker-compose.superset.yaml \
            up --detach
        ;;

    stop)
        echo "Stopping services..."
        docker-compose \
            --file docker-compose.airflow.yaml \
            --file docker-compose.datahub.yaml \
            --file docker-compose.hive.yaml \
            --file docker-compose.superset.yaml \
            stop
        ;;

    help)
        echo "Choose a group of services to start [airflow|superset|stop]..."
        echo "An empty param will start Airflow, Superset, Datahub, Hive and Hadoop together"
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
