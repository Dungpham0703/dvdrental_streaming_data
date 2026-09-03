#!/bin/bash
set -e

echo "Importing directory into Superset..."
docker exec superset_app superset import_directory /app/dashboards_as_code --overwrite

echo "Open Superset at http://localhost:8088 (Login: admin / admin)"
