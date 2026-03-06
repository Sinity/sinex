# Jobs Command

Manage xtask command execution history and job tracking.

## Overview

The `jobs` command provides access to the execution history database, allowing you to query past command runs, analyze performance trends, and clean up old records.

## Subcommands

### `xtask jobs list`

List recent command executions.

**Usage:**
```bash
# List last 10 jobs (default)
xtask jobs list

# List last 50 jobs
xtask jobs list --limit 50

# JSON output for programmatic access
xtask jobs list --json
```

**Parameters:**
- `--limit <N>` - Number of jobs to display (default: 10)

**Output:**
```
Recent command executions:

ID        Command      Status    Duration    Timestamp
─────────────────────────────────────────────────────────
42        test         success   45.2s       2026-01-23 14:30:15
41        check        success   8.1s        2026-01-23 14:25:42
40        lint         failed    12.3s       2026-01-23 14:20:10
39        test         success   47.8s       2026-01-23 13:45:22
```

**JSON Output:**
```json
{
  "jobs": [
    {
      "id": 42,
      "command": "test",
      "status": "success",
      "duration_secs": 45.2,
      "timestamp": "2026-01-23T14:30:15Z"
    }
  ]
}
```

### `xtask jobs prune`

Remove old job records from the history database.

**Usage:**
```bash
# Remove jobs older than 30 days (default)
xtask jobs prune

# Remove jobs older than 7 days
xtask jobs prune --older-than 7

# Remove jobs older than 90 days
xtask jobs prune --older-than 90

# JSON output
xtask jobs prune --json
```

**Parameters:**
- `--older-than <DAYS>` - Age threshold in days (default: 30)

**Output:**
```
Pruning job history...

Removed 145 jobs older than 30 days
Remaining jobs: 1,234
Database size reduced by: 2.3 MB
```

**JSON Output:**
```json
{
  "command": "jobs",
  "status": "success",
  "details": [
    "Removed 145 jobs older than 30 days",
    "Remaining jobs: 1,234",
    "Database size reduced by: 2.3 MB"
  ]
}
```

## Use Cases

### Performance Trend Analysis

Track how command execution times change over time to identify performance regressions.

```bash
# List recent test runs
xtask jobs list --limit 50 --json | jq '[.jobs[] | select(.command == "test")]'

# Calculate average duration
xtask jobs list --limit 100 --json | \
  jq '[.jobs[] | select(.command == "test") | .duration_secs] | add / length'
```

### Failure Investigation

Find patterns in failures to identify flaky tests or environment issues.

```bash
# List recent failures
xtask jobs list --limit 100 --json | \
  jq '[.jobs[] | select(.status == "failed")]'

# Count failures by command
xtask jobs list --limit 100 --json | \
  jq '[.jobs[] | select(.status == "failed") | .command] | group_by(.) | map({command: .[0], count: length})'
```

### Database Maintenance

Keep the history database size manageable by periodically pruning old records.

```bash
# Weekly cleanup (remove jobs >7 days old)
xtask jobs prune --older-than 7

# Monthly cleanup (remove jobs >90 days old)
xtask jobs prune --older-than 90
```

## History Database

**Location:** `<repo>/.sinex/state/xtask-history.db`

**Schema:**
```sql
CREATE TABLE invocations (
    id INTEGER PRIMARY KEY,
    command TEXT NOT NULL,
    status TEXT NOT NULL,  -- 'success', 'failed', 'partial'
    duration_secs REAL,
    timestamp TEXT NOT NULL,
    metadata TEXT  -- JSON blob
);
```

**Size considerations:**
- Average record size: ~200 bytes
- 1,000 jobs ≈ 200 KB
- 10,000 jobs ≈ 2 MB
- Recommended: Prune monthly to keep under 5,000 records

## Integration with Other Commands

The jobs history is automatically populated by all xtask commands:

```bash
# These commands automatically record history
xtask check       # Records to jobs database
xtask test        # Records to jobs database
xtask lint        # Records to jobs database

# View the results
xtask jobs list
```

## Advanced Queries

### Find slowest commands

```bash
xtask jobs list --limit 1000 --json | \
  jq '[.jobs[]] | sort_by(.duration_secs) | reverse | .[0:10]'
```

### Success rate by command

```bash
xtask jobs list --limit 1000 --json | \
  jq '[.jobs[] | group_by(.command)[] | {
    command: .[0].command,
    total: length,
    successes: ([.[] | select(.status == "success")] | length),
    success_rate: (([.[] | select(.status == "success")] | length) / length * 100)
  }]'
```

### Commands run today

```bash
xtask jobs list --limit 1000 --json | \
  jq --arg today "$(date +%Y-%m-%d)" '[.jobs[] | select(.timestamp | startswith($today))]'
```

## Troubleshooting

### History database locked

**Cause:** Another xtask command is running

**Solution:** Wait for the other command to complete, or:
```bash
# Check for running xtask processes
ps aux | grep xtask

# If stuck, remove lock file
rm .sinex/state/xtask-history.db-lock
```

### History database corrupted

**Cause:** Unexpected shutdown during write

**Solution:**
```bash
# Backup existing database
cp .sinex/state/xtask-history.db .sinex/state/xtask-history.db.bak

# Recreate database (history will be lost)
rm .sinex/state/xtask-history.db

# Next xtask command will create a new database
xtask check
```

### Missing job records

**Cause:** Commands run before history tracking was implemented

**Note:** Only commands run after the history feature was added (v0.4.0+) are tracked.

## Performance Notes

**Command overhead:**
- Recording to history: <10ms per command
- Querying history: <50ms for 1,000 records
- Pruning old records: ~100ms per 1,000 deleted records

**Database growth:**
- Typical usage: 50-100 commands/day
- 30 days: ~3,000 records ≈ 600 KB
- 90 days: ~9,000 records ≈ 1.8 MB

**Recommended maintenance:**
- Prune monthly: `--older-than 30`
- Keep last 3 months of history for trend analysis
- Archive old data if long-term analysis needed

## Privacy Notes

**What is recorded:**
- Command name (e.g., "test", "check", "lint")
- Exit status (success/failed)
- Duration in seconds
- Timestamp (UTC)
- Basic metadata (profile name, args count)

**What is NOT recorded:**
- File paths or file contents
- Environment variables
- Command arguments or flags
- Error messages or output

The history database is stored locally and never transmitted.

## See Also

- **History command** - `xtask history` - Advanced history analysis
- **Doctor command** - `xtask status --doctor` - Environment diagnostics
- **State directory** - `.sinex/state/` - All persistent state files
