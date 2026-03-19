# Configuration Newtype Guide

## Overview
Sinex uses newtype wrappers for time and size values to provide type safety and prevent unit confusion. These types wrap `u64` primitives and provide explicit semantics about what the value represents.

## Available Types

### Seconds
Represents time durations in seconds.

**Module:** `sinex_core::types::Seconds`

**Usage:**
```rust
use sinex_core::types::Seconds;

// From raw seconds
let timeout = Seconds::from_secs(30);

// Access underlying value
let secs: u64 = timeout.as_secs();

// Convert to std::time::Duration
let duration = timeout.as_duration();

// In configuration files:
timeout = 30        # Integer value in seconds
```

**Display Format:**
```rust
println!("{}", Seconds::from_secs(30));  // Prints: "30s"
```

**Deserialization:**
Currently supports integer-only deserialization via serde. The value is interpreted as seconds.

```toml
[service]
timeout = 30        # Deserialized as Seconds(30)
```

### Milliseconds
Represents time durations in milliseconds.

**Module:** `sinex_core::types::Milliseconds`

**Usage:**
```rust
use sinex_core::types::Milliseconds;

// From raw milliseconds
let interval = Milliseconds::from_millis(500);

// Access underlying value
let ms: u64 = interval.as_millis();

// Convert to std::time::Duration
let duration = interval.as_duration();

// In configuration files:
interval = 500      # Integer value in milliseconds
```

**Display Format:**
```rust
println!("{}", Milliseconds::from_millis(500));  // Prints: "500ms"
```

### Bytes
Represents data sizes in bytes.

**Module:** `sinex_core::types::Bytes`

**Usage:**
```rust
use sinex_core::types::Bytes;

// From raw bytes
let size = Bytes::from_bytes(1024);

// From mebibytes (MiB)
let limit = Bytes::from_mebibytes(5);  // 5 * 1024 * 1024 bytes

// Access underlying value
let bytes: u64 = size.as_u64();
let bytes_usize: usize = size.as_usize();  // For indexing

// In configuration files:
size = 1024         # Integer value in bytes
size = 5242880      # 5 MiB in raw bytes
```

**Display Format:**
```rust
println!("{}", Bytes::from_bytes(1024));  // Prints: "1024 bytes"
```

**Deserialization:**
Currently supports integer-only deserialization via serde. The value is interpreted as bytes.

```toml
[service]
max_payload_size = 5242880  # Deserialized as Bytes(5242880)
```

## Best Practices

### For NEW Code
- ✅ Use `Seconds` for all timeout/interval/age configs
- ✅ Use `Milliseconds` when sub-second precision is required
- ✅ Use `Bytes` for all size/limit configs
- ✅ Document units in config comments and examples
- ❌ Don't use raw `u64` for time or size in public APIs

### For EXISTING Code
- Current usage with raw `u64` is acceptable
- No mass migration required (deferred to Phase 5)
- Convert opportunistically when touching config code
- When adding new fields to existing structs, consider using newtypes

### Type Selection Guide
- **Seconds** - Use for: timeouts, TTLs, ages, cache expiry (>= 1 second granularity)
- **Milliseconds** - Use for: polling intervals, request latency, fine-grained timing
- **Bytes** - Use for: buffer sizes, payload limits, memory caps, file sizes

## Examples

### Service Configuration
```rust
use sinex_core::types::{Seconds, Bytes};

#[derive(Deserialize)]
struct ServiceConfig {
    /// Request timeout in seconds
    timeout: Seconds,

    /// Maximum payload size in bytes
    max_payload_size: Bytes,

    /// Cache TTL in seconds
    cache_ttl: Seconds,
}
```

### TOML Config File
```toml
[service]
# Timeout after 30 seconds
timeout = 30

# Maximum 5 MiB payload (in bytes)
max_payload_size = 5242880

# Cache expires after 1 hour (in seconds)
cache_ttl = 3600
```

### Environment Variables
```bash
# Set via environment (values are parsed as raw integers)
export SINEX_TIMEOUT=30              # Seconds
export SINEX_MAX_PAYLOAD=5242880     # Bytes (5 MiB)
export SINEX_CACHE_TTL=3600          # Seconds (1 hour)
```

### Runtime Usage
```rust
use sinex_core::types::{Seconds, Bytes};

fn configure_service(config: &ServiceConfig) {
    // Convert to std types as needed
    let timeout_duration = config.timeout.as_duration();
    let max_bytes = config.max_payload_size.as_usize();

    // Display for logging
    info!("Service timeout: {}", config.timeout);  // "30s"
    info!("Max payload: {}", config.max_payload_size);  // "5242880 bytes"
}
```

## Type Safety Benefits

### Prevents Unit Confusion
```rust
// ❌ Without newtypes - easy to mix up units
fn set_timeout(timeout: u64) { /* is this seconds? milliseconds? */ }
fn set_size(size: u64) { /* is this bytes? kilobytes? */ }

// ✅ With newtypes - compiler enforces correctness
fn set_timeout(timeout: Seconds) { /* always seconds */ }
fn set_size(size: Bytes) { /* always bytes */ }
```

### Self-Documenting Code
```rust
// ❌ Unclear intent
let retry_delay = 5;
let buffer_limit = 1024;

// ✅ Clear semantics
let retry_delay = Seconds::from_secs(5);
let buffer_limit = Bytes::from_bytes(1024);
```

### Compile-Time Validation
```rust
// ❌ Type mismatch caught at compile time
fn accept_timeout(timeout: Seconds) {}
fn accept_size(size: Bytes) {}

accept_timeout(Bytes::from_bytes(30));  // Compile error!
accept_size(Seconds::from_secs(1024));  // Compile error!
```

## Validation
As of Phase 1.4, newtypes include optional validation methods:
- `Seconds::validate()` - Range 0..=86400 (24 hours max)
- `Bytes::validate()` - Range 0..=1 GiB (configurable per use case)

Invalid values return clear error messages.

```rust
use sinex_core::types::Seconds;

// Validation example (Phase 1.4+)
let timeout = Seconds::from_secs(30);
timeout.validate()?;  // Ok

let too_long = Seconds::from_secs(100000);
too_long.validate()?;  // Error: exceeds 24 hours
```

## Future Enhancements (Phase 5)

Planned improvements deferred to Phase 5:
- **Human-readable string parsing**: `"30s"`, `"5MiB"`, `"2h"` syntax
- **Serde deserialize_with**: Support both integer and string formats
- **Additional units**: `from_hours()`, `from_kibibytes()`, etc.
- **Migration tooling**: Automated conversion of existing `u64` configs
- **Enhanced validation**: Configurable ranges, semantic constraints

For now, use integer values in configuration files and calculate conversions manually:
```rust
// Current (Phase 1.4)
let five_minutes = Seconds::from_secs(5 * 60);
let five_mib = Bytes::from_mebibytes(5);

// Future (Phase 5)
let five_minutes = Seconds::from_str("5m")?;
let five_mib = Bytes::from_str("5MiB")?;
```

## Related Documentation
- [NixOS Module Surface](../../../../nixos/modules/README.md) - deployment configuration surface
- [Validation Ranges](../../exploration/validation-ranges.md) - Details on validation implementation
- API docs: `cargo doc --package sinex-core --open` (see `types` module)
