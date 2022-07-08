#!/bin/sh

export PG_PASSWORD=password

# run both containers, bao-presto and postgres
docker compose up
