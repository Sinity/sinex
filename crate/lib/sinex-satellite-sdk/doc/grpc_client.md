# gRPC Client Guidance

gRPC client for communicating with sinex-ingestd

This module provides a robust gRPC client with the following reliability features:
- **Timeouts**: All operations have configurable timeouts (30s for normal ops, 5s for health)
- **Circuit Breaker**: Prevents cascade failures by failing fast when service is down
- **Retry Logic**: Exponential backoff retry (1s, 2s, 4s, 8s) for transient failures
- **Connection Management**: Automatic reconnection and connection pooling via tonic

## Usage
```rust
// Use environment-namespaced default socket (recommended for most cases)
let client = IngestClient::default().await?;

// Or connect to explicit path
let client = IngestClient::new("/run/sinex-dev/ingest.sock").await?;

// Use custom configuration for specific requirements
let config = GrpcClientConfig {
operation_timeout: Duration::from_secs(60),
health_timeout: Duration::from_secs(3),
max_retries: 5,
circuit_breaker_threshold: 10,
circuit_breaker_recovery: Duration::from_secs(60),
};
let socket_path = IngestClient::default_socket_path();
let client = IngestClient::with_config(&socket_path, config).await?;
```
