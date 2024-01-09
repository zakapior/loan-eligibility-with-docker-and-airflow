#!/bin/bash
#
# Creates a Docker container with PostgreSQL database and Adminer as database management tool.
# Intended to be used from within Airflow using BashOperator because of templating.

docker run \
    --name {{ conn.get("jakluz-de3.1.5-postgres").host }} \
    --network airflow_default \
    -v jakluz-de3.1.5-pgdata:/var/lib/postgresql/data \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD={{ conn.get("jakluz-de3.1.5-postgres").password }} \
    -d postgres:16.1

docker run \
    --name jakluz-de3.1.5-adminer \
    --network airflow_default \
    -p 8095:8080 \
    -d adminer:4.8.1