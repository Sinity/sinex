## Infrastructure Management

```bash
xtask infra start              # Start Postgres + NATS
xtask infra stop               # Stop infrastructure
xtask infra status             # Show infrastructure status
xtask infra reset              # Wipe data and restart
xtask infra logs               # View logs
xtask infra snapshot           # Save infrastructure state
xtask infra env                # Print environment variables
```

---

## Database Operations

```bash
xtask db setup                 # Create DB + run migrations
xtask db migrate               # Apply pending migrations
xtask db status --json         # Check connectivity
xtask db reset                 # Reset database
```

**Note:** Database and migrations are auto-started/applied by preflight (default ON for tests/check).

---

## Event Payload Contracts

```bash
xtask contracts generate       # Generate JSON schemas from types
xtask contracts check-ready    # Verify tables exist
xtask contracts deploy         # Deploy schemas to database
xtask contracts deploy --dry-run  # Preview changes without deploying
xtask contracts compat         # Check backward compatibility
xtask contracts info           # Show schema information
```

**Note:** Contracts are auto-deployed when payload schemas change (via preflight).

---

## TLS Operations

```bash
xtask xtr tls generate-dev-certs   # Generate CA/server/client certificates
xtask xtr tls generate-ca          # Generate only a CA certificate
xtask xtr tls generate-client-cert # Generate additional client certs
xtask xtr tls check                # Verify TLS configuration
xtask xtr tls setup-env            # Generate .env.tls file
```
