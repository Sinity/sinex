# Jobs Command

`xtask jobs` is the operational surface for background work. It is distinct from
`xtask history`, which is the durable execution record.

## Mental Model

- A **job** is a background process handle you can list, inspect, wait on, cancel, or read output from.
- An **invocation** is the durable history record linked to that job.
- `xtask jobs status --json` exposes both the job handle and the linked invocation id.
- Progress shown by `jobs` comes from the canonical invocation progress snapshot.

Use `jobs` when you care about a running or recently-finished background process.
Use `history` when you care about analysis, diagnostics, tests, stages, or longer-lived records.

## Core Commands

### `xtask jobs list`

List recent background jobs.

```bash
xtask jobs list
xtask jobs list --limit 50
xtask jobs list --active
xtask jobs list --json
```

JSON rows include:

- `id`
- `invocation_id`
- `command`
- `args`
- `status`
- `pid`
- `started_at`
- `exit_code`
- `progress`

### `xtask jobs status <job-id>`

Inspect one background job and its linked invocation progress.

```bash
xtask jobs status 42
xtask jobs status 42 --json
xtask jobs status 42 --follow
```

The JSON response includes:

- `id`
- `invocation_id`
- `command`
- `args`
- `status`
- `pid`
- `started_at`
- `exit_code`
- `progress`
- `stages`

`--follow` tails stdout until the job completes.

### `xtask jobs output <job-id>`

Read live or archived stdout/stderr for one job.

```bash
xtask jobs output 42
xtask jobs output 42 --stderr
xtask jobs output 42 --json
```

### `xtask jobs wait <job-id>`

Block until a job completes and return final status plus final linked progress.

```bash
xtask jobs wait 42
xtask jobs wait 42 --timeout 60
xtask jobs wait 42 --json
```

### `xtask jobs cancel <job-id>`

Cancel a running job.

```bash
xtask jobs cancel 42
```

### `xtask jobs prune`

Delete old terminal job handles and archived logs.

```bash
xtask jobs prune --older-than 7
```

This cleans up the operational job layer. Use `xtask history prune` for durable invocation history.

## Jobs and History Together

Typical workflow:

```bash
xtask check --bg --json
xtask jobs status <job_id> --json
xtask jobs wait <job_id> --json
xtask history progress --invocation <invocation_id> --json
xtask history eta check
```

The important distinction is:

- `jobs` answers "what is this background process doing?"
- `history` answers "what happened across executions?"

## Storage

Background-job metadata and archived logs live in the xtask history database under the
job/invocation tables. Live stdout/stderr is also mirrored on disk under the workspace
state directory while the job is still running.

## Troubleshooting

### Job not found

Likely causes:

- wrong `job_id`
- the job handle was pruned
- the command returned a fresh historical result instead of starting a new background job

If you only have an invocation id, use `xtask history ...`, not `xtask jobs ...`.

### Output disappeared after completion

This is expected. Completed job output may be archived into the database rather than left
in the live stdout/stderr file.
