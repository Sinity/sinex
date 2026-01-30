# Type System Patterns

Sinex's sophisticated type system patterns for compile-time safety.

## Newtypes for Semantic Safety (35+ domain types)

**Pattern:** Prevent accidental mixing of semantically different strings

```rust
// Macro-generated newtypes
define_string_type!(EventSource);      // e.g., "fs-watcher", "terminal"
define_string_type!(EventType);        // e.g., "file.created", "command.executed"
define_string_type!(HostName);         // Where events occurred
define_string_type!(ProcessorName);    // Automaton/node identifiers
define_string_type!(ConsumerGroup);    // For distributed processing
define_string_type!(ConsumerName);     // Instance names
define_string_type!(SchemaName);       // Schema identifiers
define_string_type!(SchemaVersion);    // Semantic versions
```

**Bug Prevented:**
```rust
let source = EventSource::from("fs-watcher");
let event_type = EventType::from("file.created");

// COMPILE ERROR - arguments swapped, caught by compiler!
process_event(event_type, source);  // ❌ Types don't match
```

**Impact:** Semantic confusion impossible at compile time.

---

## Validated Types with Security Guarantees

**Pattern:** Security properties enforced at type boundary

```rust
/// Path type that enforces security properties
define_validated_string_type!(SanitizedPath);

impl SanitizedPath {
    pub fn validate(path: &str) -> Result<Utf8PathBuf, String> {
        // Reject directory traversal
        if path.contains("..") {
            return Err("Path contains directory traversal sequences".into());
        }

        // Lexically normalize
        let cleaned = normalize_path_lexically(path);

        // Double-check after normalization
        if path_contains_traversal(&cleaned) {
            return Err("Path contains directory traversal sequences".into());
        }

        // Reject null bytes (path injection)
        if path.contains('\0') {
            return Err("Path cannot contain null bytes".into());
        }

        Ok(cleaned)
    }
}
```

**Invariant:** Invalid paths **cannot exist in memory**. Validation happens during deserialization via `FromStr`.

**Impact:** Path traversal attacks and injection vectors prevented at type boundary.

---

## Temporal Patterns: Timestamp Wrapper

**Pattern:** Use `Timestamp` wrapper instead of raw library types (`time` or `chrono`) for consistency, built-in serialization, and database integration.

```rust
use sinex_primitives::temporal::Timestamp;

// Preferred: Use the system wrapper
let ts = Timestamp::now();

// Built-in RFC3339 serialization and database integration
let json = serde_json::to_string(&ts)?;
```

**Anti-Pattern:** Using raw `time::OffsetDateTime` or `chrono::DateTime` in public library APIs. This creates inconsistent serialization formats and requires manual database mapping.

**Impact:** Consistent time representation across the entire ecosystem, from database to CLI.

---

## Error Patterns: Structured SinexError

**Pattern:** Use `SinexError` for all domain-specific errors in library code. Use `eyre!` only at application boundaries (CLI, main daemons) or tests.

```rust
use sinex_primitives::error::{SinexError, Result};

fn process() -> Result<()> {
    // Specific variant with context enrichment
    Err(SinexError::validation("Invalid input")
        .with_context("field", "id")
        .with_context("value", input))
}
```

**Anti-Pattern:** Using generic `color_eyre::eyre::eyre!` or `anyhow!` in library code. This erodes the ability to programmatically handle errors and categories them in monitoring.

**Impact:** Highly diagnosable system with structured logs and error reporting.

---

## Making Illegal States Unrepresentable

### NonEmptyVec

```rust
/// A vector guaranteed to contain at least one element
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct NonEmptyVec<T> {
    inner: Vec<T>,
}

impl<T> NonEmptyVec<T> {
    /// Get first element - ALWAYS safe, never panics
    pub fn first(&self) -> &T {
        &self.inner[0]  // Safe - invariant guaranteed by type
    }

    /// is_empty() always returns false
    pub fn is_empty(&self) -> bool {
        false  // Impossible to be empty by construction
    }
}
```

**Usage in Provenance:**
```rust
pub enum Provenance {
    Synthesis {
        source_event_ids: NonEmptyVec<EventId>,  // MUST have >= 1 parent
        operation_id: Option<Id<Operation>>,
    },
}
```

**Illegal State Made Impossible:** A synthesized event with zero parent events cannot exist.

### Type-State Builder

```rust
// Type-state markers
pub struct NoProvenance;
pub struct HasProvenance;

/// Event builder with compile-time validation
pub struct EventBuilder<T, P = NoProvenance> {
    payload: T,
    source: EventSource,
    event_type: EventType,
    provenance: Option<Provenance>,
    _phantom: PhantomData<P>,
}

// Only available when NoProvenance
impl<T> EventBuilder<T, NoProvenance> {
    pub fn with_provenance(self, provenance: Provenance)
        -> EventBuilder<T, HasProvenance>
    {
        // Transition to HasProvenance state
    }
}

// Only available when HasProvenance
impl<T> EventBuilder<T, HasProvenance> {
    pub fn build(self) -> Event<T> {
        // Only callable after provenance is set
    }
}
```

**Illegal State Made Impossible:** Cannot call `.build()` without first calling `.with_provenance()`.

---

## Enum-Based State Machines

**Pattern:** Exhaustive matching enforces complete handling

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplayState {
    Planning,
    Previewed,
    Approved,
    Executing,
    Committing,
    Completed,
    Failed,
    Cancelled,
}

impl ReplayState {
    pub fn can_transition_to(&self, target: ReplayState) -> bool {
        match (self, target) {
            (ReplayState::Planning, ReplayState::Previewed) => true,
            (ReplayState::Planning, ReplayState::Cancelled) => true,
            (ReplayState::Previewed, ReplayState::Approved) => true,
            // ... exhaustive transitions
            _ => false,
        }
    }
}
```

**Benefit:** Adding new state causes **compile errors** at all match sites until handled.

---

## Runtime → Compile-Time Opportunities

Many validations can move from runtime to compile-time:

| Runtime Check | Type-Level Approach | Impact |
|--------------|---------------------|--------|
| Empty string validation | `NonEmptyString` type | Cannot construct empty |
| Payload size limits | `BoundedJson<const MAX: usize>` | Cannot exceed limit |
| Duplicate parent IDs | `UniqueNonEmptyVec<T>` | Cannot insert duplicates |
| State transitions | Type-state state machine | Invalid transitions don't compile |
| ULID drift validation | `ValidatedUlid` | Validation at construction |
| Null byte detection | `NullFreeString` | Cannot construct with nulls |
| Object payload validation | Use `Map<K,V>` directly | Non-objects don't type-check |
| Work tracking | RAII `WorkGuard` | Cannot forget cleanup |

**Example: NonEmptyString**
```rust
/// String guaranteed to be non-empty (ignoring whitespace)
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NonEmptyString {
    inner: String,
}

impl FromStr for NonEmptyString {
    type Err = ValidationError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            Err(ValidationError::EmptyString)
        } else {
            Ok(NonEmptyString { inner: trimmed.to_string() })
        }
    }
}

// Now EventSource and EventType wrap NonEmptyString
define_string_type_validated!(EventSource, NonEmptyString);
define_string_type_validated!(EventType, NonEmptyString);
```

**Impact:** Empty event sources/types **cannot exist**. Runtime check eliminated.

---

## Architectural Insights

### Layered Validation Strategy

**Three-tier validation approach:**

1. **Compile-time**: Types, phantom types, type-state patterns
2. **Deserialization-time**: `FromStr`, `Deserialize` impls, validation attributes
3. **Runtime**: Explicit validation methods for complex invariants

**Strength:** Defense in depth - multiple layers catch different bug classes.

### Zero-Cost Abstractions

Heavy use of:
- **Phantom types** (no runtime overhead, erased at compile time)
- **Transparent serialization** (`#[serde(transparent)]`)
- **Newtypes** (compiled away to raw representation)

**Strength:** Strong type safety with **zero performance cost**.

### Security by Type

Validation-enforced types (`SanitizedPath`, `Blake3Hash`, `NullFreeString`) prevent injection attacks **at the type boundary**, not just as runtime assertions.

**Strength:** Security properties are **structurally guaranteed**, not defensively checked.

### Exhaustiveness Guarantees

Enum-based state machines force exhaustive pattern matching. Adding a new error variant, state, or checkpoint type causes **compile errors** at all match sites until handled.

**Strength:** No silent failures - compiler enforces complete handling.
