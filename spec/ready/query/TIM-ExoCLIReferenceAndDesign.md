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

## 🚀 PHASE 1 ENHANCEMENT STATUS
**Current Foundation**: 2000+ lines of working functionality with rich output formatting
**Target**: Enhanced query templates + complete autocomplete + interactive building
**Implementation**: Week 2 focus (Monday-Thursday)

## 3. Enhanced Query Templates & Smart Shortcuts

*Alternative to EQL complexity - leveraging existing 95% complete database infrastructure*

### **Smart Query Templates**
```bash
# Smart shortcuts with sophisticated backend
exo recent hyprland                    # Last hour hyprland events
exo errors --agent promotion-worker    # Agent-specific error analysis  
exo activity --around "15:30" --window 10m  # Context-aware time queries
exo related --to-event 01JZBC... --context 5m  # Event correlation

# Template system with parameter substitution
exo query --template debug-session --params "agent=worker,time=2h"
exo query --save-as daily-summary --source hyprland --event-type window.focused
```

### **Dynamic Database Autocomplete**
```bash
# All commands support dynamic completion from live database
exo query --source <TAB>     # Shows: hyprland, filesystem, clipboard...
exo agent status <TAB>       # Shows: sinex-collector, promotion-worker...
exo dlq show 01J<TAB>        # Completes ULID from database
```

### **Interactive Query Building**
```bash
# fzf-powered discoverability using rich database
exo --interactive             # Guided query building
exo explore                   # Visual dashboard-like interface
```

## 3. Key Subcommands (Enhanced Implementation Details)

### 3.1. `exo log`
    *   Purpose: Manually log a raw event or a predefined meta-event.
    *   Examples:
        *   `exo log desktop.manual_input arbitrary_event_type --payload-json '{"key":"value"}' --tags "manual,debug"`
        *   `exo log meta.friction --description "Struggling with Nix Flake inputs" --intensity 4 --tags "nixos,friction"`
        *   `exo log meta.insight --description "Realized CRDTs solve the PKM sync issue!" --confidence 5 --tags "pkm,design"`

### 3.2. `exo query` & `exo find`
    *   `exo query`: Execute simplified Exocortex Query Language (EQL - TBD) or raw SQL.
        *   `exo query --eql "FROM core.events WHERE source CONTAINS 'hyprland' AND ts_orig > '1d_ago' LIMIT 10"`
        *   `exo query --sql "SELECT count(*) FROM core.events WHERE source = 'pkm';"`
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

### 3.12. `exo processor` *(Enhanced - Phase 1 Priority)*
    *   Manage processor checkpoints and Redis streams.
    *   `exo processor list [--status running|stopped]`
    *   `exo processor checkpoint <PROCESSOR_NAME> [--reset]`
    *   `exo processor restart <PROCESSOR_NAME>`
    *   `exo processor stats [--since "1 day ago"]` *(New)*

### 3.13. `exo system` *(Enhanced - Phase 1 Priority)*
    *   System-level operations - leveraging existing 85% complete monitoring infrastructure.
    *   `exo system health [--component database|monitoring|services]`
    *   `exo system stats [--detailed]` *(New)*
    *   `exo system backup [--verify]` *(Enhanced)*
    *   `exo system integrity-check --component <db|annex|links>`

### 3.14. `exo query` *(Enhanced - Phase 1 Core)*
    *   Advanced query interface with templates and SQL support.
    *   `exo query --sql "SELECT COUNT(*) FROM core.events WHERE source = 'fs'"`
    *   `exo query --time-range "last 2 hours" --source fs --event-type file.created`
    *   `exo query --export-csv /tmp/events.csv --limit 1000` *(New)*
    *   `exo query --template debug-session --params "agent=worker,time=2h"` *(New)*

## 4. Query Examples and Common Patterns

### 4.1. Contextual Recall Queries

**Find activity around specific PKM note editing:**
```bash
# Find browser tabs and terminal commands active around note editing
exo query --template activity-around-note --params "note_id=01JZBC...,window=15min"
```

**Recent activity analysis:**
```bash
# Last hour of hyprland window events
exo recent hyprland --time "1 hour" --type window.focused

# Terminal commands in the last day
exo recent terminal --time "1 day" --type command.executed
```

### 4.2. Cross-Domain Correlation

**Find related events by time window:**
```bash
# Events 5 minutes before/after a specific event
exo related --to-event 01JZBC... --context 5m

# All activity during a specific time window
exo activity --around "2024-01-01T15:30:00" --window 10m
```

### 4.3. Pattern Analysis

**Error and health monitoring:**
```bash
# Agent-specific error analysis
exo errors --agent sinex-collector --since "2 hours"

# System health patterns
exo system health --component database --detailed
```

### 4.4. SQL Query Examples

**Complex event correlation:**
```sql
-- Find all events that happened within 5 minutes of a terminal command
WITH command_events AS (
    SELECT event_id, ts_orig, payload->>'command' as command
    FROM core.events 
    WHERE source = 'terminal-satellite' AND event_type = 'command.executed'
    AND ts_orig > NOW() - INTERVAL '1 day'
)
SELECT e.*, ce.command as related_command
FROM core.events e
JOIN command_events ce ON ABS(EXTRACT(EPOCH FROM e.ts_orig - ce.ts_orig)) < 300
WHERE e.source != 'terminal-satellite'
ORDER BY e.ts_orig;
```

**Activity clustering:**
```sql
-- Group events by time windows to identify work sessions
SELECT 
    DATE_TRUNC('hour', ts_orig) as time_window,
    source,
    COUNT(*) as event_count,
    ARRAY_AGG(DISTINCT event_type) as event_types
FROM core.events
WHERE ts_orig > NOW() - INTERVAL '1 week'
GROUP BY time_window, source
HAVING COUNT(*) > 10
ORDER BY time_window DESC;
```

## 5. Enhanced Shell Completions *(Phase 1 Core Feature)*

### **Dynamic Database Completion**
```python
# cli/completion.py
import argcomplete
from rich.completion import Completer

class DatabaseCompleter(Completer):
    def get_completions(self, document, complete_event):
        if document.text.endswith('--source '):
            return query_db("SELECT DISTINCT source FROM core.events ORDER BY source")
        elif document.text.endswith('--event-type '):
            current_source = extract_source_from_command(document.text)
            return query_db(
                "SELECT DISTINCT event_type FROM core.events WHERE source = ? ORDER BY event_type",
                [current_source]
            )
        elif document.text.endswith('--agent '):
            return query_db("SELECT DISTINCT automaton_name FROM core.automaton_checkpoints")

# Integration with argparse/click
@click.option('--source', shell_complete=source_completer)
@click.option('--event-type', shell_complete=event_type_completer) 
@click.option('--agent', shell_complete=agent_completer)
```

### **Installation & Configuration**
```bash
# Generate completion scripts
./cli/exo.py --completion-bash > /etc/bash_completion.d/exo
./cli/exo.py --completion-zsh > ~/.zsh/completions/_exo
./cli/exo.py --completion-fish > ~/.config/fish/completions/exo.fish

# Register completions
echo 'eval "$(register-python-argcomplete exo)"' >> ~/.bashrc
```

