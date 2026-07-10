# Cloud-agent sidecar images

Two minimal images that mirror the database and message-bus stack sinex
expects, packaged for use inside Claude Code Web / Codex Cloud sandboxes.

| Image                          | Built from               | Purpose                                          |
| ------------------------------ | ------------------------ | ------------------------------------------------ |
| `ghcr.io/sinity/sinex-pg18`    | `pg18-sinex/Dockerfile`  | Postgres 18 + timescaledb + pgvector + pg_jsonschema |
| `ghcr.io/sinity/sinex-nats`    | `nats-sinex/Dockerfile`  | NATS 2.x with JetStream, single-node             |

Both are referenced by [`docker-compose.yml`](../docker-compose.yml).

> **Publication status (verified 2026-07-10):** neither tag is anonymously
> pullable from ghcr.io (403/404 — never published). The compose file
> therefore declares `build:` contexts and sandboxes build the images
> locally during setup (`docker compose up -d --build`). Publishing them
> publicly (workflow below) would turn the tags into a pull fast-path.

## Building locally

```bash
docker build -t ghcr.io/sinity/sinex-pg18:latest xtask/cloud/docker/pg18-sinex
docker build -t ghcr.io/sinity/sinex-nats:latest xtask/cloud/docker/nats-sinex
```

Tag with a specific version when publishing a stable point:

```bash
docker build -t ghcr.io/sinity/sinex-pg18:18.1-ts2.17 xtask/cloud/docker/pg18-sinex
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

[`xtask/cloud/bootstrap.sh`](../bootstrap.sh) (profile `db`) builds and
starts these sidecars on Docker-capable sandboxes so SQLx macros can
validate against a live database. On docker-less sandboxes (Codex Cloud's
`codex-universal` has no daemon) the bootstrap fails hard unless
`DATABASE_URL` already points at a reachable external database — it never
silently skips the requirement.

To stand up the sidecars on demand inside a sandbox:

```bash
docker compose -f xtask/cloud/docker-compose.yml up -d
```

Stop:

```bash
docker compose -f xtask/cloud/docker-compose.yml down
```

## Source-of-truth alignment

The Postgres extensions are also installed by `flake.nix`'s
`postgresForSqlx` and `pgJsonschemaOverlay`. When versions change in the
flake, mirror the change here. The Dockerfile carries `TODO` comments at
each extension install pointing back to the flake declaration.
