# Development services

Worktree-local PostgreSQL and NATS run only through the declared AgentCTL service operation:

```bash
agentctl job start sinex dev_services --workspace <workspace-id>
agentctl job get <job-id>
agentctl job cancel <job-id>
```

The descriptor leases bounded loopback ports and `xtask infra lease-services` initializes the worktree state, applies the schema, waits for both services, then remains foreground. Systemd owns the job cgroup, process-tree cancellation, logs, terminal result, and lease release. Xtask does not keep PIDs, locks, owner files, or a detached service supervisor.

The devshell never auto-starts services. Use the lease metadata returned by AgentCTL when a consumer needs the assigned database or NATS address. Production `sinexd` remains outside this route.
