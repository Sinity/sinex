# Postgres 18 with the extensions sinex requires:
#   - timescaledb (hypertables, retention policies)
#   - pgvector   (embeddings)
#   - pg_jsonschema (Supabase release; JSON Schema CHECK constraints)
#
# Source-of-truth for versions: the flake-provisioned dev database
# (flake.nix pgJsonschemaOverlay + postgresForSqlx withPackages). Pins below
# were read from a live flake-built PG 18.4 on 2026-07-10:
#   timescaledb 2.27.2 · vector (pgvector) 0.8.2 · pg_jsonschema 0.3.4
# NB: TimescaleDB gained PostgreSQL 18 support in 2.23.0 — anything older
# (the original 2.17.2 sketch pin) has no PG18 packages and cannot build.
# When the flake's postgres bumps, mirror the new versions here; the image
# exists so cloud-agent sandboxes can run a matching database without Nix.
FROM postgres:18-bookworm

ARG TIMESCALEDB_VERSION=2.27.2
ARG PGVECTOR_VERSION=0.8.2
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
    curl -fsSL https://packagecloud.io/install/repositories/timescale/timescaledb/script.deb.sh | bash; \
    # Pin the loader package too: unpinned, apt takes the repo's newest
    # loader whose control file then demands an extension version the pinned
    # scripts don't provide ("no installation script ... for 2.28.2").
    apt-get install -y --no-install-recommends \
        timescaledb-2-postgresql-18=${TIMESCALEDB_VERSION}* \
        timescaledb-2-loader-postgresql-18=${TIMESCALEDB_VERSION}* \
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

# timescaledb must be in shared_preload_libraries BEFORE the initdb-phase
# server runs the init scripts; appending to the sample config makes initdb
# bake it into the generated postgresql.conf (same technique as Timescale's
# official image).
RUN echo "shared_preload_libraries = 'timescaledb'" >> /usr/share/postgresql/postgresql.conf.sample

COPY init-extensions.sql /docker-entrypoint-initdb.d/00-extensions.sql
