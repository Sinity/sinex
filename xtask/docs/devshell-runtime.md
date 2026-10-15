# Development services

Worktree-local PostgreSQL and NATS run only through the declared AgentCTL service operation:

```bash
checkout_root="$(git rev-parse --show-toplevel)"
workspace_id="$(agentctl workspace list --project sinex | jq -er --arg path "$checkout_root" '.payload.value.workspaces[] | select(.path == $path) | .workspace_id')"
agentctl job start sinex dev_services --workspace "$workspace_id"
agentctl job get <job-id>
agentctl job cancel <job-id>
```

The devshell derives PostgreSQL and preferred NATS ports from the checkout hash. PostgreSQL startup refuses an occupied port. NATS startup holds a per-user lock from port selection through readiness, reuses a free persisted assignment, or selects another available port from its pool. The assignment is stored in checkout state. `xtask infra lease-services` initializes that state, applies the schema, starts both services, publishes their endpoints as one JSON line, and remains foreground.

The descriptor uses `tree+environment` caching for the service operation. Matching starts in the same checkout share one running AgentCTL job and one systemd service, so cancellation is shared. Different worktrees remain isolated and may run concurrently. Systemd owns the job cgroup, process-tree cancellation, logs, and terminal result. Xtask keeps checkout service state and the NATS port assignment; it does not run a detached service supervisor.

The devshell never auto-starts services. Production `sinexd` remains outside this route.

## Compilation budget

Declared operations inherit the optional `CARGO_BUILD_JOBS` and `SINEX_CARGO_TIMEOUT` settings. For a cold, single-worker build, run `CARGO_BUILD_JOBS=1 SINEX_CARGO_TIMEOUT=1800 agentctl job start sinex check_default --workspace <checkout>`. The timeout is in seconds and applies to each Cargo command; the default is 600. This does not change test deadlines or the operation's outer deadline.
