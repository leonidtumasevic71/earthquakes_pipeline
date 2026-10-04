#!/bin/bash

set -e

psql \
  -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-EOSQL

DO \$\$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_roles
        WHERE rolname = 'grafana'
    ) THEN
        CREATE ROLE grafana LOGIN PASSWORD '${GRAFANA_DB_PASSWORD}';
    ELSE
        ALTER ROLE grafana
        WITH LOGIN PASSWORD '${GRAFANA_DB_PASSWORD}';
    END IF;
END
\$\$;

EOSQL