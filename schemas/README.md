# Sinex Event Schemas

Rust `EventPayload` registrations are the source of truth for the active schema
set used by the running system.

The JSON files under `schemas/` are the checked-in schema bundle used for:

- repo-local GitOps tests and examples,
- downstream consumers that need raw JSON Schema files,
- reviewable source control history for published schema contracts.

Each generated schema file also embeds `x-sinex-source`,
`x-sinex-event-type`, and `x-sinex-version`, so the bundle is
self-describing even outside the repo's own directory layout.

`xtask` can regenerate this bundle from the live Rust `EventPayload` registry:

```bash
xtask docs schema-bundle
xtask docs schema-bundle --check
```

That path is separate from the runtime Rust -> database schema sync used by
preflight and `sinex-ingestd`.

## Directory Structure

```text
schemas/
├── v1/
│   ├── registry.json
│   ├── fs-watcher/
│   │   ├── file.created.json
│   │   └── ...
│   ├── canonical.terminal/
│   └── ...
└── (future versions live beside v1/)
```

## Runtime Sync Path

The live schema registry is populated from Rust code, not from this directory:

1. `EventPayload` implementations register schema metadata through the Rust
   inventory.
2. preflight / ingest startup runs the in-process discovery path and syncs the
   discovered schemas into `sinex_schemas.event_payload_schemas`.
3. ingestd reloads active schemas from the database and broadcasts schema
   metadata to interested consumers.

See:

- `crate/core/sinex-ingestd/docs/schema_sync.md`
- `crate/lib/sinex-primitives/docs/schema_registry.md`

## GitOps Path

The checked-in JSON bundle matters for the GitOps import surface:

1. Register a repository containing JSON schema files with `sinexctl gitops`.
2. `sinex-ingestd` polls or syncs that repository.
3. Matching JSON files are discovered and upserted into
   `sinex_schemas.event_payload_schemas`.

Discovery is metadata-first. The `schemas/<source>/<event_type>/...` layout is a
convenient export shape and a fallback convention, not the only valid layout.

Useful commands:

```bash
sinexctl gitops list
sinexctl gitops create <repo-url> --branch main --pattern "schemas/**/*.json"
sinexctl gitops sync <source-id>

# xtask wrapper around the same operator surface
xtask gitops list
```

Schema compatibility checks against another branch are wired through CI helpers:

```bash
xtask ci compat --base master --glob schemas
```

## Updating This Bundle

When you change the schema contract for an event:

1. update the Rust `EventPayload` and any related validation/runtime logic,
2. regenerate the checked-in JSON bundle under `schemas/`,
3. run the relevant tests / compatibility checks,
4. review both the Rust-side and JSON-side diff together.

Typical local sequence:

```bash
xtask docs schema-bundle
xtask docs schema-bundle --check
xtask ci compat --base master --glob schemas
```

This directory is therefore a tracked contract surface, not merely scratch
output.
