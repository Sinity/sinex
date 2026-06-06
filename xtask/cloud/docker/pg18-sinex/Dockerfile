# Postgres 18 with the extensions sinex requires:
#   - timescaledb (hypertables, retention policies)
#   - pgvector   (embeddings)
#   - pg_jsonschema (Supabase release; JSON Schema CHECK constraints)
#
# Source-of-truth for versions/sha256: ../../flake.nix
# (pgJsonschemaOverlay block, postgresForSqlx withPackages).
#
# TODO(operator): this Dockerfile is a sketch. Before publishing to
# ghcr.io/sinity/sinex-pg18:latest, walk the install paths line-by-line
# against the flake to confirm:
#   - timescaledb apt repo + apt package name for pg18 on bookworm
#   - pgvector source tag / build flags
#   - pg_jsonschema deb URL + sha256 (currently 0.3.4 per flake.nix:39)
# The `flake.nix` declarations are authoritative; this image only mirrors
# them so cloud-agent sandboxes can spin up a matching database without Nix.
FROM postgres:18-bookworm

ARG TIMESCALEDB_VERSION=2.17.2
ARG PGVECTOR_VERSION=0.8.0
ARG PG_JSONSCHEMA_VERSION=0.3.4

# Base build deps; purged at the end to keep the image lean.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg lsb-release \
        build-essential git \
        postgresql-server-dev-18 \
        ; \
    \
    # --- TimescaleDB ----------------------------------------------------
    # TODO: confirm the timescale apt repo URL is still the canonical one
    # for pg18 once pg18 leaves preview.
    curl -fsSL https://packagecloud.io/install/repositories/timescale/timescaledb/script.deb.sh | bash; \
    apt-get install -y --no-install-recommends \
        timescaledb-2-postgresql-18=${TIMESCALEDB_VERSION}* \
        ; \
    \
    # --- pgvector -------------------------------------------------------
    cd /tmp; \
    git clone --depth 1 --branch v${PGVECTOR_VERSION} https://github.com/pgvector/pgvector.git; \
    cd pgvector && make && make install && cd /tmp && rm -rf pgvector; \
    \
    # --- pg_jsonschema (Supabase release) -------------------------------
    curl -fsSL -o /tmp/pg_jsonschema.deb \
        "https://github.com/supabase/pg_jsonschema/releases/download/v${PG_JSONSCHEMA_VERSION}/pg_jsonschema-v${PG_JSONSCHEMA_VERSION}-pg18-amd64-linux-gnu.deb"; \
    dpkg -i /tmp/pg_jsonschema.deb || apt-get install -fy; \
    rm /tmp/pg_jsonschema.deb; \
    \
    # --- Cleanup --------------------------------------------------------
    apt-get purge -y build-essential postgresql-server-dev-18 git; \
    apt-get autoremove -y; \
    rm -rf /var/lib/apt/lists/* /tmp/*

COPY init-extensions.sql /docker-entrypoint-initdb.d/00-extensions.sql
