# Schema GitOps

Schema GitOps is implemented by `sinex-ingestd` plus the gateway/CLI control
plane.

## What It Does

1. You register a Git repository containing JSON schema files.
2. `sinex-ingestd` polls that repository on its background sync loop.
3. Matching schema files are discovered and upserted into
   `sinex_schemas.event_payload_schemas`.
4. The validator reload path and schema broadcast path pick up the updated
   active schemas.

This is the operational complement to the Rust-driven schema sync described in
`schema_sync.md`.

## Schema Source Layout

Schemas can be discovered by either:

1. Path convention: `{source}/{event_type}/{version}.json`
2. Embedded metadata fields:
   - `x-sinex-source`
   - `x-sinex-event-type`
   - `x-sinex-version`

## Managing Sources

The canonical interface is `sinexctl git-ops`:

```bash
# List configured sources
sinexctl git-ops list

# Create a source
sinexctl git-ops create https://github.com/org/my-schemas.git \
  --branch main \
  --pattern "schemas/**/*.json" \
  --interval 60

# Trigger an immediate sync
sinexctl git-ops sync <SOURCE_UUID>
```

## Operational Notes

- Start local infrastructure with `xtask infra start`.
- Run the services with `xtask run core --logs` or the relevant deployed units.
- Check ingestd logs if sync is not happening.
- Gateway handlers own CRUD/trigger operations; ingestd owns the polling and
  import loop.

## See Also

- `schema_sync.md`
- `crate/lib/sinex-schema/docs/gitops-schema-sources-status.md`
