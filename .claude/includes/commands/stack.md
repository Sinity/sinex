## Stack Management

```bash
cargo xtask stack start              # Start Postgres + NATS
cargo xtask stack stop               # Stop the stack
cargo xtask stack status             # Show stack status
cargo xtask stack doctor             # Run diagnostics
cargo xtask stack reset              # Wipe data and restart
cargo xtask stack logs               # View logs
```

---

## Database Operations

```bash
cargo xtask db setup                 # Create DB + run migrations
cargo xtask db migrate               # Apply pending migrations
cargo xtask db status --json         # Check connectivity
cargo xtask db reset                 # Reset database
cargo xtask contracts generate       # Generate JSON schemas from types
cargo xtask contracts check-ready    # Verify tables exist
cargo xtask contracts deploy         # Deploy schemas to database
```

---

## TLS Operations

```bash
cargo xtask tls generate-dev-certs   # Generate CA/server/client certificates
cargo xtask tls check                # Verify TLS configuration
cargo xtask tls generate-client-cert # Generate additional client certs
cargo xtask tls setup-env            # Generate .env.tls file
```
