-- Bootstrap the extensions sinex's schema expects on a fresh DB.
-- Runs once at first container start via /docker-entrypoint-initdb.d/.

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_jsonschema;
