#!/bin/bash

set -euo pipefail

CONTAINER_NAME="ancientgaming-airflow-scheduler-1"

if [ $# -ne 1 ]; then
  echo "Usage: $0 <file>"
  exit 1
fi

FILE="$1"

docker exec "$CONTAINER_NAME" autopep8 -i -a -a "$FILE"
docker exec "$CONTAINER_NAME" add-trailing-comma "$FILE"
docker exec "$CONTAINER_NAME" isort "$FILE" --settings-path /opt/airflow/.flake8
docker exec "$CONTAINER_NAME" flake8 "$FILE"
