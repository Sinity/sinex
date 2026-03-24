## Infrastructure & Reset

```bash
xtask infra start              # Start Postgres + NATS
xtask infra stop               # Stop infrastructure
xtask infra status             # Show status
```

### Reset (Developer State Wipe)

```bash
xtask reset --yes              # Everything: db + nats + preflight + jobs + target
xtask reset --yes --db         # Drop and recreate database only
xtask reset --yes --nats       # Wipe NATS JetStream data only
xtask reset --yes --schema     # Force schema reapply (hash file only, no data loss)
xtask reset --yes --contracts  # Force contract redeploy (hash file only)
xtask reset --yes --target     # Wipe target/ (force clean recompilation)
xtask reset --yes --tls        # Regenerate TLS certificates
```

DB schema applies automatically via preflight on every `xtask check`/`build`/`test`. TLS certs are generated lazily when absent.
