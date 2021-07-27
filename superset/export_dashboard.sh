#!/usr/bin/env bash

docker exec \
    --interactive \
    --tty \
    cartolafc_superset_1 \
        superset export-dashboards > output.json
