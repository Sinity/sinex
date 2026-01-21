# NATS Authentication Setup

## Overview

For production deployments, Sinex supports granular NATS authentication using separate credentials for different component types.

## User Roles

### Ingestor

- **Permissions**: Publish only to source material and raw event streams
- **Subjects**: `*.source_material.>`, `*.events.raw.>`
- **Usage**: Terminal, filesystem, document, and system ingestors

### Automaton  

- **Permissions**: Subscribe and publish to event streams
- **Subjects**: Subscribe/publish to `*.events.>`
- **Usage**: Health, analytics, search, content, PKM automata

### Gateway

- **Permissions**: Full access (admin)
- **Subjects**: All (`>`)
- **Usage**: Gateway service, administrative tools

## Setup

### 1. Generate Credentials

```bash
# Using nk tool (NATS key generator)
nk -gen user --name sinex-ingestor \
  --allow-pub "*.source_material.>" \
  --allow-pub "*.events.raw.>" \
  > /run/sinex-prod/nats/ingestor.creds

nk -gen user --name sinex-automaton \
  --allow-sub "*.events.>" \
  --allow-pub "*.events.>" \
  > /run/sinex-prod/nats/automaton.creds

nk -gen user --name sinex-gateway \
  --allow-pub ">" --allow-sub ">" \
  > /run/sinex-prod/nats/gateway.creds
```

### 2. Configure Services

Set the `NATS_CREDS` environment variable for each service:

```bash
# Ingestors
NATS_CREDS=/run/sinex-prod/nats/ingestor.creds

# Automata
NATS_CREDS=/run/sinex-prod/nats/automaton.creds

# Gateway
NATS_CREDS=/run/sinex-prod/nats/gateway.creds
```

## Environment-Specific Credentials

Credentials should be environment-specific:

- **Dev**: `/run/sinex-dev/nats/*.creds`
- **Staging**: `/run/sinex-staging/nats/*.creds`  
- **Prod**: `/run/sinex-prod/nats/*.creds`

## Security Notes

- Credentials contain private keys - protect with file permissions (600)
- Rotate credentials periodically
- Use different credentials per environment
- Consider using secrets management (HashiCorp Vault, etc.)

## See Also

- NATS documentation: <https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_intro>
- Sinex environment configuration: `sinex-core/src/environment.rs`
