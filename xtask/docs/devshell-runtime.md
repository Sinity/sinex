# Development services

Worktree-local PostgreSQL and NATS run only through the declared AgentCTL service operation:

```bash
checkout_root="$(git rev-parse --show-toplevel)"
workspace_id="$(agentctl workspace list --project sinex | jq -er --arg path "$checkout_root" '.payload.value.workspaces[] | select(.path == $path) | .workspace_id')"
agentctl job start sinex dev_services --workspace "$workspace_id"
agentctl job get <job-id>
agentctl job cancel <job-id>
```

The devshell derives one PostgreSQL and one NATS port from the checkout hash and exports them as `SINEX_DEV_POSTGRES_PORT` and `SINEX_DEV_NATS_PORT`, so two worktrees never contend and nothing has to allocate ports for them. `xtask infra lease-services` reads that pair, refuses a port another process already holds, initializes the worktree state, applies the schema, starts both services, publishes their endpoints as one JSON line, and remains foreground.

The descriptor uses `tree+environment` caching for the service operation. Matching starts in the same checkout share one running AgentCTL job and one systemd service, so cancellation is shared. Different worktrees remain isolated and may run concurrently. Systemd owns the job cgroup, process-tree cancellation, logs, and terminal result. Xtask does not keep PIDs, locks, owner files, or a detached service supervisor.

The devshell never auto-starts services. Production `sinexd` remains outside this route.
