# Sinex CLI Design Document

This document describes the design philosophy and architecture of the `exo` CLI tool.

## Design Philosophy

- **Unix Philosophy**: Small, composable commands. Do one thing well.
- **Scriptability**: Default to structured output (JSON) for easy parsing by other scripts. Human-readable table/text formats via flags.
- **Discoverability**: Comprehensive `--help` for all commands/subcommands. Rich shell completions (Bash, Zsh, Fish).
- **Consistency**: Consistent naming, argument patterns, and output structures across subcommands.
- **Idempotency**: Where applicable, commands that modify state should be idempotent if re-run.
- **Interaction with Backend**: Primarily via direct PostgreSQL connection (for queries/simple writes) or by sending command-like events to agents/services for complex operations.

## Command Structure

```
exo [GLOBAL_OPTIONS] <COMMAND> [SUBCOMMAND_OPTIONS] [ARGS...]
```

**Global Options**: 
- `--config <PATH>` - Configuration file path
- `--db-url <URL>` - Database connection URL
- `--output-format <json|yaml|table|csv>` - Output format
- `--verbose, -v` - Verbose output
- `--quiet, -q` - Quiet mode
- `--version` - Show version
- `--help, -h` - Show help

## Core Commands

### Query Commands
- `exo query` - Execute EQL or raw SQL queries
- `exo find` - Unified search across artifacts, events, entities
- `exo recent` - Quick access to recent events by source

### Data Management
- `exo log` - Manually log events
- `exo pkm` - Manage PKM notes
- `exo web` - Manage web archives
- `exo blob` - Interact with git-annex managed blobs
- `exo tag` - Manage tags
- `exo entity` - Manage knowledge graph entities
- `exo relation` - Manage entity relationships

### System Operations
- `exo agent` - Manage and inspect agents
- `exo processor` - Manage processor checkpoints
- `exo system` - System-level operations
- `exo schema` - Inspect event schemas

### Advanced Features
- `exo embed` - Manage embeddings
- `exo livingdoc` - Interact with Living Document

## Phase 1 Enhancements

### Smart Query Templates
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

### Dynamic Database Autocomplete
All commands support dynamic completion from live database:
- Sources: `exo query --source <TAB>`
- Event types: `exo query --event-type <TAB>`
- Agents: `exo agent status <TAB>`
- ULIDs: `exo dlq show 01J<TAB>`

### Interactive Query Building
```bash
exo --interactive             # Guided query building
exo explore                   # Visual dashboard-like interface
```

## Implementation Status

✅ Current Foundation:
- 2000+ lines of working functionality
- Rich output formatting
- Basic query interface

🚧 Phase 1 Enhancements (Week 2 Focus):
- [ ] Enhanced query templates
- [ ] Complete autocomplete system
- [ ] Interactive query builder
- [ ] Performance optimizations

## Query Patterns

### Contextual Recall
```bash
# Find activity around specific PKM note editing
exo query --template activity-around-note --params "note_id=01JZBC...,window=15min"

# Recent activity analysis
exo recent hyprland --time "1 hour" --type window.focused
exo recent terminal --time "1 day" --type command.executed
```

### Cross-Domain Correlation
```bash
# Events around a specific event
exo related --to-event 01JZBC... --context 5m

# Activity during time window
exo activity --around "2024-01-01T15:30:00" --window 10m
```

### Pattern Analysis
```bash
# Error monitoring
exo errors --agent sinex-collector --since "2 hours"

# System health
exo system health --component database --detailed
```

## Shell Completion

Dynamic completion powered by database queries:

```python
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
```

Installation:
```bash
./cli/exo.py --completion-bash > /etc/bash_completion.d/exo
./cli/exo.py --completion-zsh > ~/.zsh/completions/_exo
./cli/exo.py --completion-fish > ~/.config/fish/completions/exo.fish
```

## Future Enhancements

- WebSocket support for real-time event streaming
- Plugin system for custom commands
- Remote execution capabilities
- Advanced visualization integrations
- Machine learning-powered query suggestions