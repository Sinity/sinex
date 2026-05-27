# Cloud-agent sidecar images

Two minimal images that mirror the database and message-bus stack sinex
expects, packaged for use inside Claude Code Web / Codex Cloud sandboxes.

| Image                          | Built from               | Purpose                                          |
| ------------------------------ | ------------------------ | ------------------------------------------------ |
| `ghcr.io/sinity/sinex-pg18`    | `pg18-sinex/Dockerfile`  | Postgres 18 + timescaledb + pgvector + pg_jsonschema |
| `ghcr.io/sinity/sinex-nats`    | `nats-sinex/Dockerfile`  | NATS 2.x with JetStream, single-node             |

Both are referenced by [`docker-compose.cloud.yml`](../docker-compose.cloud.yml).

## Building locally

```bash
docker build -t ghcr.io/sinity/sinex-pg18:latest docker/pg18-sinex
docker build -t ghcr.io/sinity/sinex-nats:latest docker/nats-sinex
```

Tag with a specific version when publishing a stable point:

```bash
docker build -t ghcr.io/sinity/sinex-pg18:18.1-ts2.17 docker/pg18-sinex
```

## Publishing to ghcr.io

Requires a personal access token with `write:packages` scope.

```bash
echo "$GHCR_TOKEN" | docker login ghcr.io -u sinity --password-stdin

docker push ghcr.io/sinity/sinex-pg18:latest
docker push ghcr.io/sinity/sinex-nats:latest
```

Make the package public on the GitHub UI after the first push (otherwise
cloud sandboxes that lack ghcr credentials cannot pull).

## How cloud agents consume the images

The cloud-agent setup script ([`.claude/setup.sh`](../.claude/setup.sh))
does NOT pull these by default — most cloud work is `cargo check` /
`cargo test -p <safe-crate>` and does not need a database. Uncomment the
`docker compose pull` block in the setup script if you want the images
warmed at boot.

To stand up the sidecars on demand inside a sandbox:

```bash
docker compose -f docker-compose.cloud.yml up -d
```

Stop:

```bash
docker compose -f docker-compose.cloud.yml down
```

## Source-of-truth alignment

The Postgres extensions are also installed by `flake.nix`'s
`postgresForSqlx` and `pgJsonschemaOverlay`. When versions change in the
flake, mirror the change here. The Dockerfile carries `TODO` comments at
each extension install pointing back to the flake declaration.
