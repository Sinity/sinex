# Testing Patterns

Testing infrastructure patterns for Sinex.

## Fixture Management System

Global fixture registry with reference counting:

```rust
static FIXTURE_REGISTRY: OnceCell<Arc<Mutex<FixtureRegistry>>> = OnceCell::const_new();

struct FixtureRegistry {
    cache: HashMap<FixtureKey, Arc<dyn Any + Send + Sync>>,
    cleanups: HashMap<CleanupKey, CleanupTask>,
    ref_counts: HashMap<FixtureKey, usize>,
}
```

**Features:**
- Shared fixtures across tests with reference counting
- Automatic cleanup when last reference released
- Parameterized fixtures with caching
- OnceCell ensures singleton initialization safety

## Property-Based Testing

Custom strategies for domain types:

```rust
pub struct SinexStrategies;

impl SinexStrategies {
    pub fn event_source() -> BoxedStrategy<String> {
        prop_oneof![
            Just("filesystem".to_string()),
            Just("shell.kitty".to_string()),
            "[a-z][a-z0-9._]*".prop_map(|s| s.to_string()),
            Just("".to_string()),  // Empty source edge case
        ].boxed()
    }

    pub fn malicious_payload() -> BoxedStrategy<String> {
        prop_oneof![
            Just("..%2f..%2f..%2fetc%2fpasswd".to_string()),  // Path traversal
            Just("/tmp/safe.txt\\0../../../etc/passwd".to_string()),  // Null byte
            Just("<script>alert('xss')</script>".to_string()),  // XSS
        ].boxed()
    }
}
```

**Edge Cases Caught:**
- Security (path traversal, null byte injection, URL encoding)
- Minimal counts (event_count=1, message_count=1, batch_size=1)
- Numeric extremes (tiny floats, huge floats, large file sizes)
- Timing/concurrency (UUIDv7 uniqueness under load)

## Database Test Pool Architecture

64-database parallel testing:
- PostgreSQL advisory locks for slot reservation
- Template database with fingerprinting for fast cloning
- Cleanup verification with residual tracking
- Quarantine mechanism for problematic databases

**Performance:**
- Parallel test execution without conflicts
- Lazy database provisioning
- Fast cloning from template (migration fingerprint caching)

## Test Categories

1. **Unit tests** (fast, isolated, 57 modules)
2. **Integration tests** (database, NATS, 137 files)
3. **Property tests** (randomized, edge cases)
4. **Adversarial tests** (chaos engineering, attack simulation)
5. **Security tests** (path validation, injection, 11+ files)
6. **Performance tests** (load, benchmarks, regression detection)
7. **System tests** (stress, reliability, end-to-end)

## See Also

- Test documentation: [README.md](./README.md)
- Pipeline testing: [pipeline_testing.md](./pipeline_testing.md)
