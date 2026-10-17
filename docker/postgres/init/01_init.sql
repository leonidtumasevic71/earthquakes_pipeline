\getenv pipeline_db POSTGRES_DB
\getenv pipeline_user POSTGRES_USER
\getenv grafana_user GRAFANA_DB_USER

CREATE TABLE IF NOT EXISTS public.earthquakes (
    id TEXT PRIMARY KEY,
    time TIMESTAMPTZ,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    depth DOUBLE PRECISION,
    magnitude DOUBLE PRECISION,
    place TEXT
);

GRANT CONNECT ON DATABASE :"pipeline_db" TO :"grafana_user";
GRANT USAGE ON SCHEMA public TO :"grafana_user";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"grafana_user";

ALTER DEFAULT PRIVILEGES FOR ROLE :"pipeline_user" IN SCHEMA public
GRANT SELECT ON TABLES TO :"grafana_user";

