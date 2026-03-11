## Infrastructure Management

```bash
xtask infra start              # Start Postgres + NATS
xtask infra stop               # Stop infrastructure
xtask infra status             # Show infrastructure status
xtask infra logs               # View logs
```

---

## Reset (Developer State Wipe)

```bash
xtask reset --yes              # Everything: db + nats + preflight + jobs + target
xtask reset --yes --db         # Drop and recreate database only
xtask reset --yes --nats       # Wipe NATS JetStream data only
xtask reset --yes --blobs      # Wipe git-annex blobstore
xtask reset --yes --preflight  # Wipe entire .sinex/preflight/ directory
xtask reset --yes --contracts  # Delete contracts-hash + preflight-cache (force redeploy)
xtask reset --yes --schema     # Delete schema-apply-hash + preflight-cache (force reapply)
xtask reset --yes --history    # Delete xtask history SQLite DB
xtask reset --yes --history --seed  # Wipe and reseed with synthetic data
xtask reset --yes --jobs       # Delete background job records
xtask reset --yes --target     # Wipe target/ directory (force clean recompilation)
xtask reset --yes --tls        # Regenerate TLS certificates
```

`--contracts` and `--schema` are surgical: they delete only the hash files that gate
preflight re-deployment, forcing a re-run without data loss.

---

## Database Operations

Database management commands (`xtask db`) have been dissolved. Preflight handles schema
apply automatically on every `xtask check`, `xtask build`, and `xtask test`.
To force a database reset: `xtask reset --yes --db`.

---

## TLS Operations

```bash
xtask reset --yes --tls        # Regenerate TLS certificates in .tls/
```

TLS certs are generated lazily by preflight when `.tls/server.pem` is absent. Use
`xtask reset --yes --tls` to force regeneration.
