# TIM-ExoCLIReferenceAndDesign: `exo` Command-Line Interface

*   **Purpose:** Provides the design philosophy and (eventually) a comprehensive command reference for the `exo` CLI, the scriptable backbone for Exocortex interaction.
*   **Source:** Derived from original Vision Document Appendix D and expanded based on STAD/Architectural Module capabilities.
*   **Dependencies:** Relies on backend Exocortex services and database.

## 1. Design Philosophy

*   **Unix Philosophy:** Small, composable commands. Do one thing well.
*   **Scriptability:** Default to structured output (JSON) for easy parsing by other scripts. Human-readable table/text formats via flags.
*   **Discoverability:** Comprehensive `--help` for all commands/subcommands. Rich shell completions (Bash, Zsh, Fish).
*   **Consistency:** Consistent naming, argument patterns, and output structures across subcommands.
*   **Idempotency:** Where applicable, commands that modify state should be idempotent if re-run.
*   **Interaction with Backend:** Primarily via direct PostgreSQL connection (for queries/simple writes) or by sending command-like events to agents/services for complex operations.

## 2. Top-Level Command Structure (Conceptual)

The `exo` CLI is envisioned to have the following top-level subcommands (as outlined previously in STAD generation planning / UG Appendix D):
```
exo [GLOBAL_OPTIONS] <COMMAND> [SUBCOMMAND_OPTIONS] [ARGS...]
```
*   **Global Options:** `--config <PATH>`, `--db-url <URL>`, `--output-format <json|yaml|table|csv>`, `--verbose, -v`, `--quiet, -q`, `--version`, `--help, -h`.

## 3. Key Subcommands (Outline & Examples - Details TBD by Implementation)

*(This section will eventually become a detailed reference, auto-generated or manually written based on the `clap` (Rust) or similar CLI framework definitions. For now, it's an outline matching the STAD's prior CLI command list.)*

### 3.1. `exo log`
    *   Purpose: Manually log a raw event or a predefined meta-event.
    *   Examples:
        *   `exo log desktop.manual_input arbitrary_event_type --payload-json '{"key":"value"}' --tags "manual,debug"`
        *   `exo log meta.friction --description "Struggling with Nix Flake inputs" --intensity 4 --tags "nixos,friction"`
        *   `exo log meta.insight --description "Realized CRDTs solve the PKM sync issue!" --confidence 5 --tags "pkm,design"`

### 3.2. `exo query` & `exo find`
    *   `exo query`: Execute simplified Exocortex Query Language (EQL - TBD) or raw SQL.
        *   `exo query --eql "FROM raw.events WHERE source CONTAINS 'hyprland' AND ts_orig > '1d_ago' LIMIT 10"`
        *   `exo query --sql "SELECT count(*) FROM core.artifacts WHERE artifact_type = 'pkm_note';"`
    *   `exo find`: Unified search across artifacts, events, entities using keywords, semantic similarity, tags.
        *   `exo find "NixOS flakes" --type pkm_note --tags "tutorial"`
        *   `exo find --semantic-similar-to-text "The core concept of ULIDs" --limit 5`

### 3.3. `exo pkm`
    *   Manage PKM notes (interacts with Yjs backend via Exocortex services).
    *   `exo pkm new --title "My Yjs Note" --tags "pkm,yjs"`
    *   `exo pkm get <NOTE_ID_OR_TITLE>` (outputs latest Markdown snapshot)
    *   `exo pkm list [--tags "...")`
    *   `exo pkm tag <NOTE_ID> add|rm <tag>`
    *   `exo pkm link <SOURCE_ID> <TARGET_ID_OR_QUERY>`

### 3.4. `exo web`
    *   Manage web archives.
    *   `exo web archive <URL> [--fidelity <text_only|dom_snapshot|full_warc>] [--tags "research"]` (sends `sinex.web.capture_request`)
    *   `exo web get <URL_OR_ARTIFACT_ID>`

### 3.5. `exo blob`
    *   Interact with `git-annex` managed blobs via `core.blobs`.
    *   `exo blob add /path/to/file.pdf --description "Important PDF" --tags "papers,todo_read"`
    *   `exo blob get <BLOB_ID_OR_ANNEX_KEY_OR_HASH>` (ensures file present, outputs path)
    *   `exo blob info <BLOB_ID_OR_ANNEX_KEY_OR_HASH>`

### 3.6. `exo tag`
    *   Manage `core.tags`.
    *   `exo tag create project.exocortex.documentation --description "Tasks related to Exocortex docs" --parent project.exocortex`
    *   `exo tag list [--hierarchy]`

### 3.7. `exo entity` & `exo relation`
    *   Manage Knowledge Graph (`core.entities`, `core.entity_relations`).
    *   `exo entity create --type person --label "Jane Doe" --properties '{"email":"jane@example.com"}'`
    *   `exo entity link <SOURCE_ENTITY_ID> <TARGET_ENTITY_ID> --type works_on_project`

### 3.8. `exo livingdoc`
    *   Interact with the Living Document.
    *   `exo livingdoc append --text "New idea: ..."`
    *   `exo livingdoc query "nodes related to project X"`
    *   `exo livingdoc extract tasks --from-node <NODE_ID_OR_QUERY>`

### 3.9. `exo agent`
    *   Manage and inspect Exocortex agents.
    *   `exo agent list [--status running]`
    *   `exo agent status <AGENT_NAME>`
    *   `exo agent logs <AGENT_NAME> [--since 1h]`
    *   `exo agent enable|disable|restart <AGENT_NAME>` (interacts with systemd via user or sends command event)

### 3.10. `exo schema`
    *   Inspect `sinex_schemas.event_payload_schemas` and `sinex_schemas.agent_manifests`.
    *   `exo schema list-payloads [--source X --type Y]`
    *   `exo schema get-payload <SCHEMA_ULID_OR_SOURCE_TYPE_VERSION>`

### 3.11. `exo embed`
    *   Manage and query embeddings.
    *   `exo embed find-similar-to-text "query text"`
    *   `exo embed queue-artifact <ARTIFACT_ID>` (for `EmbeddingAgent`)

### 3.12. `exo dlq`
    *   Manage `core.dead_letter_queue`.
    *   `exo dlq list [--status pending_review]`
    *   `exo dlq replay <DLQ_ID>`
    *   `exo dlq update-status <DLQ_ID> --new-status resolved_manual`

### 3.13. `exo system`
    *   System-level operations.
    *   `exo system health`
    *   `exo system backup-db` (triggers `pgBackRest` job)
    *   `exo system integrity-check --component <db|annex|links>`

## 4. Shell Completions

Shell completion scripts (for Bash, Zsh, Fish) will be generated from the CLI definition (e.g., by `clap_complete` in Rust).

