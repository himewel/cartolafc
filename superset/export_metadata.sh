#!/usr/bin/env bash

docker exec \
    --interactive \
    --tty \
    cartolafc_superset_1 \
        python ./lineage_update.py
