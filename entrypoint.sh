#!/usr/bin/env bash
set -euo pipefail


DBT_PROJECT_DIR=${DBT_PROJECT_DIR:-/opt/airflow/dbt_project}

if command -v dbt >/dev/null 2>&1; then
  if [ -d "$DBT_PROJECT_DIR" ] && [ ! -d "$DBT_PROJECT_DIR/dbt_packages" ]; then
    echo "[entrypoint] dbt_packages not found, running 'dbt deps'..."
    dbt deps --project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROJECT_DIR" || true
  fi
fi

exec /entrypoint "$@"
