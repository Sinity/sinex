# Sinex Gateway Library

Provides the service container, replay system, and related functionality for the Sinex Gateway.

## Architecture Overview

The Sinex Gateway acts as the central API hub for the Sinex event capture system:

```text
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CLI Tools     │────│  JSON-RPC API   │────│ Service Layer   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                               │                        │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Browser Ext.    │────│ Native Messaging │    │ Database Layer  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Core Components

- **RPC Server** – JSON-RPC 2.0 API for CLI communication (TLS-only).
- **Native Messaging** – Browser extension communication protocol.
- **Replay State Machine** – Distributed replay operation management.
- **Cascade Analyzer** – Dependency graph analysis for safe operations.
- **Service Container** – Dependency injection and service lifecycle.

## Usage Examples

Starting the gateway server:

```rust,no_run
use sinex_gateway::{rpc_server, ServiceContainer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let services = ServiceContainer::new(Some("postgres://user:pass@localhost/sinex_dev".into())).await?;
    rpc_server::run(None, services).await?;
    Ok(())
}
```

## Error Handling Patterns

All operations return `color_eyre::Result<T>` for comprehensive error context. Errors are logged and
sanitized before being returned to clients.
