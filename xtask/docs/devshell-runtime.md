# Development services

Worktree-local PostgreSQL and NATS run only through the declared AgentCTL service operation:

```bash
agentctl job start sinex dev_services --workspace <workspace-id>
agentctl job get <job-id>
agentctl job cancel <job-id>
```

For a pre-push from the leased checkout, pass the exact active job ID for validation:

```bash
SINEX_PRE_PUSH_AGENTCTL_LEASE_ID=<job-id> git push
```

The pre-push hook re-reads the lease from AgentCTL, checks its checkout, operation, state, bounded port slots, and reachability, then propagates the validated PostgreSQL port, PostgreSQL socket, and NATS port. A missing or stale lease ID fails closed; omitting the variable preserves the ordinary non-AgentCTL path.

The descriptor uses `tree+environment` caching for the service operation. Matching starts in the same checkout share one running AgentCTL job, one systemd service, and one bounded loopback service lease, so cancellation is shared. Different worktrees remain isolated and may run concurrently. `xtask infra lease-services` initializes the worktree state, applies the schema, waits for both services, then remains foreground. Systemd owns the job cgroup, process-tree cancellation, logs, terminal result, and lease release. Xtask does not keep PIDs, locks, owner files, or a detached service supervisor.

The devshell never auto-starts services. Use the lease metadata returned by AgentCTL when a consumer needs the assigned database or NATS address. Production `sinexd` remains outside this route.
