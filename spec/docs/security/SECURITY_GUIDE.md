# Sinex Security Guide

## Overview

This guide documents the security measures implemented in Sinex to protect against various attack vectors discovered during adversarial testing. All vulnerabilities mentioned have been addressed through the comprehensive validation framework.

## Security Architecture

### 1. Defense in Depth

Sinex implements multiple layers of security:

```
Input → Validation → Normalization → Processing → Storage
  ↓         ↓             ↓              ↓          ↓
Monitor   Monitor      Monitor        Monitor    Monitor
```

### 2. Core Security Components

#### sinex-validation Crate
- **PathValidator**: Protects against path traversal and null byte injection
- **JsonValidator**: Prevents JSON-based DoS attacks
- **UnicodeNormalizer**: Blocks Unicode-based bypasses
- **SafeCommand**: Prevents command injection
- **SecurityDashboard**: Real-time threat monitoring

## Vulnerability Mitigation

### 1. Path Security

**Threats Mitigated:**
- Null byte injection (CWE-158)
- Path traversal (CWE-22)
- Unicode normalization bypass (CWE-176)

**Implementation:**
```rust
use sinex_validation::PathValidator;

let validator = PathValidator::default();
match validator.validate(user_path) {
    Ok(safe_path) => {
        // Path is safe to use
    }
    Err(e) => {
        // Log security event and reject
    }
}
```

**Security Rules:**
- All paths containing null bytes (`\0`) are rejected
- Parent directory references (`../`) are blocked
- Unicode direction overrides are detected and blocked
- Paths are canonicalized to prevent symbolic link attacks

### 2. JSON Security

**Threats Mitigated:**
- Billion laughs attack (CWE-776)
- Hash collision DoS (CWE-328)
- Circular references (CWE-835)
- Resource exhaustion (CWE-400)

**Implementation:**
```rust
use sinex_validation::{JsonValidator, JsonLimits};

let limits = JsonLimits {
    max_size: 10 * 1024 * 1024,      // 10MB
    max_depth: 32,                    // 32 levels
    max_keys_per_object: 1000,        // 1000 keys
    max_array_length: 10000,          // 10k items
    max_string_length: 1024 * 1024,   // 1MB
};

let validator = JsonValidator::new(limits);
```

**Security Features:**
- SipHash for DoS-resistant object key hashing
- Circular reference detection before processing
- Size and depth limits enforced during parsing
- Suspicious key patterns detected (hash collision attempts)

### 3. Unicode Security

**Threats Mitigated:**
- Homoglyph attacks (visual spoofing)
- Zero-width character smuggling
- Right-to-left override attacks
- Mixed script confusion

**Implementation:**
```rust
use sinex_validation::UnicodeNormalizer;

let normalizer = UnicodeNormalizer::default();
let safe_string = normalizer.normalize(user_input)?;
```

**Security Rules:**
- All strings normalized to NFC form
- Zero-width characters rejected
- Direction override characters blocked
- Mixed script detection (e.g., Latin + Cyrillic)

### 4. Command Execution Security

**Threats Mitigated:**
- Command injection (CWE-78)
- Environment variable manipulation
- Shell metacharacter attacks

**Implementation:**
```rust
use sinex_validation::SafeCommand;

let mut cmd = SafeCommand::new("process_file");
cmd.arg(filename); // Validated separately, never concatenated
let output = cmd.execute()?;
```

**Security Features:**
- No shell interpolation - direct process execution
- Arguments passed as separate parameters
- Environment variables whitelisted
- Shell metacharacters detected and blocked

### 5. ULID Security

**Threats Mitigated:**
- ID collisions in high-frequency scenarios
- Time-based ordering issues
- Cross-process uniqueness problems

**Implementation:**
```rust
use sinex_ulid::monotonic::MonotonicUlidGenerator;

let generator = MonotonicUlidGenerator::new();
let ulid = generator.generate(); // Guaranteed unique and ordered
```

**Security Features:**
- Monotonic counter for same-millisecond uniqueness
- Process ID embedded for multi-process safety
- Thread-safe generation with proper locking
- Overflow detection and handling

## Security Monitoring

### Real-Time Dashboard

The security dashboard provides visibility into attack attempts:

```rust
use sinex_validation::dashboard::{DASHBOARD, ExportFormat};

// Get recent security events
let events = DASHBOARD.get_recent_events(100);

// Get statistics for the last hour
let stats = DASHBOARD.get_stats(Duration::from_secs(3600));

// Export for analysis
let json_export = DASHBOARD.export_events(ExportFormat::Json)?;
```

### Alert Thresholds

Default thresholds (per minute):
- Null byte attempts: 10
- Path traversal attempts: 10
- Command injection attempts: 5
- JSON attacks: 20

### Security Metrics

Monitor these key metrics:
- Total security events by severity
- Attack patterns over time
- Top attack vectors
- Source IP analysis (when available)

## Best Practices

### 1. Input Validation

**Always validate at the boundary:**
```rust
// In your event handler
pub async fn handle_event(payload: String) -> Result<()> {
    let mut validator = Validator::default();
    let validated = validator.validate_event_payload(&payload)?;
    // Process validated data
}
```

### 2. Fail Securely

**Reject suspicious input early:**
```rust
if input.contains('\0') {
    log_security_event(SecurityEvent::NullByteRejected { 
        path: input.to_string() 
    });
    return Err(SecurityError::InvalidInput);
}
```

### 3. Monitor and Alert

**Set up monitoring for your deployment:**
```rust
// Check dashboard stats periodically
tokio::spawn(async {
    loop {
        let stats = DASHBOARD.get_stats(Duration::from_secs(300));
        if stats.critical_events > 0 {
            send_security_alert(&stats).await;
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
});
```

### 4. Regular Updates

- Keep dependencies updated
- Review security advisories
- Run adversarial tests in CI/CD
- Monitor for new attack patterns

## Incident Response

### 1. Detection

Security events are automatically logged with:
- Timestamp
- Event type and severity
- Attack details
- Context (when available)

### 2. Response Steps

1. **Immediate Response**
   - Block source IP (if applicable)
   - Increase monitoring sensitivity
   - Review recent events for patterns

2. **Investigation**
   - Export security events for analysis
   - Correlate with application logs
   - Identify attack vector and impact

3. **Remediation**
   - Apply additional validation rules
   - Update security thresholds
   - Deploy patches if needed

4. **Post-Incident**
   - Document lessons learned
   - Update security tests
   - Review and improve defenses

## Testing Security

### Run Security Tests

```bash
# Run all adversarial tests
cargo test --test adversarial

# Run specific security test category
cargo test --test adversarial security_attacks

# Run with detailed output
cargo test --test adversarial -- --nocapture
```

### Security Test Categories

1. **Time & ULID attacks**: Clock manipulation, collision attempts
2. **Security attacks**: Injection, traversal, bypass attempts
3. **JSON attacks**: DoS, circular references, expansions
4. **Resource exhaustion**: Connection pools, memory, CPU
5. **State violations**: Race conditions, corrupted states

## Configuration

### Security Configuration Example

```rust
use sinex_validation::{ValidatorConfig, JsonLimits};

let config = ValidatorConfig {
    validate_paths: true,
    validate_json: true,
    normalize_unicode: true,
    use_secure_json: true,
    detect_circular_refs: true,
    json_limits: JsonLimits {
        max_size: 5 * 1024 * 1024,  // 5MB for strict environment
        max_depth: 20,               // Reduced depth
        max_keys_per_object: 500,    // Fewer keys allowed
        ..Default::default()
    },
};
```

### Alert Configuration

```rust
use sinex_validation::dashboard::{DashboardConfig, AlertThresholds};

let config = DashboardConfig {
    max_events: 50000,
    event_retention: 7200, // 2 hours
    enable_alerts: true,
    alert_thresholds: AlertThresholds {
        null_byte_per_minute: 5,      // More sensitive
        path_traversal_per_minute: 5,
        command_injection_per_minute: 2,
        json_attacks_per_minute: 10,
    },
};
```

## Conclusion

Security is not a feature but a continuous process. The Sinex validation framework provides robust protection against known attack vectors, but vigilance is required:

1. **Monitor** security events continuously
2. **Update** defenses based on new threats
3. **Test** with adversarial inputs regularly
4. **Document** and share security knowledge

For security concerns or to report vulnerabilities, please contact the security team or file a security issue in the project repository.