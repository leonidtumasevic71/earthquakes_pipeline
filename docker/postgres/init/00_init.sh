


#/usr/bin/env bash

set -Eeuo pipefail

: "${POSTGRES_USER:?POSTGRES_USER must be set}"
: "${POSTGRES_DB:?POSTGRES_DB must be set}"
: "${GRAFANA_DB_USER:?GRAFANA_DB_USER must be set}"
: "${GRAFANA_DB_PASSWORD:?GRAFANA_DB_PASSWORD must be set}"

psql \
  -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v grafana_user="$GRAFANA_DB_USER" \
  -v grafana_password="$GRAFANA_DB_PASSWORD" <<'EOSQL'
SELECT format(
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM pg_roles
            WHERE rolname = :'grafana_user'
        )
        THEN 'ALTER ROLE %I WITH LOGIN PASSWORD %L'
        ELSE 'CREATE ROLE %I WITH LOGIN PASSWORD %L'
    END,
    :'grafana_user',
    :'grafana_password'
)
\gexec
EOSQL

