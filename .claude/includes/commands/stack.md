## Infrastructure Management

```bash
cargo xtask infra start              # Start Postgres + NATS
cargo xtask infra stop               # Stop infrastructure
cargo xtask infra status             # Show infrastructure status
cargo xtask infra reset              # Wipe data and restart
cargo xtask infra logs               # View logs
cargo xtask infra snapshot           # Save infrastructure state
cargo xtask infra env                # Print environment variables
```

---

## Database Operations

```bash
cargo xtask db setup                 # Create DB + run migrations
cargo xtask db migrate               # Apply pending migrations
cargo xtask db status --json         # Check connectivity
cargo xtask db reset                 # Reset database
```

**Note:** Database and migrations are auto-started/applied by preflight (default ON for tests/check).

---

## Event Payload Contracts

```bash
cargo xtask contracts generate       # Generate JSON schemas from types
cargo xtask contracts check-ready    # Verify tables exist
cargo xtask contracts deploy         # Deploy schemas to database
cargo xtask contracts deploy --dry-run  # Preview changes without deploying
cargo xtask contracts compat         # Check backward compatibility
cargo xtask contracts info           # Show schema information
```

**Note:** Contracts are auto-deployed when payload schemas change (via preflight).

---

## TLS Operations

```bash
cargo xtask xtr tls generate-dev-certs   # Generate CA/server/client certificates
cargo xtask xtr tls check                # Verify TLS configuration
cargo xtask xtr tls generate-client-cert # Generate additional client certs
cargo xtask xtr tls setup-env            # Generate .env.tls file
```
