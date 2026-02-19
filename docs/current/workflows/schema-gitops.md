# Schema GitOps Workflow

Sinex supports "GitOps" for event payload schemas. This allows you to define schemas in a Git repository (your "source of truth") and have them automatically synchronized to the Sinex database.

## Overview

1. **Author**: You define JSON schemas in a Git repository.
2. **Sync**: The `ingestd` service periodically pulls the repo.
3. **Discovery**: It scans for files matching a pattern (default: `schemas/**/*.json`).
4. **Registration**: New or updated schemas are registered in the `event_payload_schemas` table.

## Prerequisites

- A running Sinex stack (`xtask infra start`).
- A Git repository (local or remote) containing schemas.

## Schema File Convention

Schemas can be discovered in two ways:

1. **Path Convention**:
    `path/to/{source}/{event_type}/{version}.json`
    Example: `schemas/fs-watcher/file.created/1.0.0.json`

2. **Metadata Fields**:
    If your path structure is different, you must include `x-sinex-*` fields in the JSON schema itself:

    ```json
    {
      "type": "object",
      "x-sinex-source": "fs-watcher",
      "x-sinex-event-type": "file.created",
      "x-sinex-version": "1.0.0",
      ...
    }
    ```

## Managing Sources (CLI)

Use the `xtask gitops` command (wrapper around `sinexctl`) to manage sources.

### Add a Source

```bash
xtask gitops create https://github.com/org/my-schemas.git \
  --branch main \
  --pattern "data-contracts/**/*.json" \
  --interval 5
```

### List Sources

```bash
xtask gitops list
```

### Trigger Immediate Sync

By default, sources invoke sync periodically. You can force a sync immediately:

```bash
xtask gitops sync <SOURCE_ULID>
```

## Troubleshooting

- Check `ingestd` logs: `xtask infra logs sinex-ingestd`
- Ensure the repository URL is accessible from the `ingestd` container/process.
- Validate your JSON schemas are valid JSON draft-07+.
