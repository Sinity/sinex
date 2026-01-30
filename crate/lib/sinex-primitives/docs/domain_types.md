# Domain Type System

Sinex uses a macro-driven domain type system to enforce type safety across the codebase, preventing the accidental mixing of semantically different strings (e.g., using an `EventSource` where an `EventType` is expected).

## Architecture

Types are defined using two primary macros in `domain.rs`:

| Macro | Purpose | Examples |
|-------|---------|----------|
| `define_string_type!` | Type-safe wrappers for trusted or internal strings. | `EventSource`, `EventType`, `HostName` |
| `define_validated_string_type!` | Types that require structural validation (e.g., path traversal checks). | `SanitizedPath`, `RelativePath`, `AbsoluteUri`, `Blake3Hash` |

## Validation & Construction

### Validated Types
Validated types implement a `validate()` method. To ensure integrity, these types should ideally only be constructed via `FromStr` or `try_new()` patterns.

**Warning**: Legacy constructors like `new()` and `new_unchecked()` bypass validation. They are retained for internal use cases where data is known-good, but should be avoided for user-supplied input.

### SQLx Integration
Domain types are transparently compatible with PostgreSQL types (usually `TEXT` or `VARCHAR`).
- **Unvalidated types**: Decoded directly into the wrapper.
- **Validated types**: Use the `FromStr` path during database decoding, ensuring that even data retrieved from storage adheres to current validation rules.

## Core Domain Types

- **EventSource**: Identifies the origin of an event (e.g., `fs-watcher`, `terminal`).
- **EventType**: Hierarchical identifier for the event kind (e.g., `file.created`, `process.heartbeat`).
- **SanitizedPath**: A filesystem path that has been normalized and verified to prevent traversal attacks.
- **Blake3Hash**: A hex-encoded representation of a BLAKE3 cryptographic hash.

## Unit Newtypes

Sinex uses newtype wrappers for time and size values to provide type safety and prevent unit confusion. These types wrap \`u64\` primitives and provide explicit semantics.

| Type | Purpose | Methods |
|------|---------|---------|
| **Seconds** | Time durations. | \`as_secs()\`, \`as_duration()\` |
| **Milliseconds** | Fine-grained timing. | \`as_millis()\`, \`as_duration()\` |
| **Bytes** | Data sizes. | \`as_u64()\`, \`as_usize()\` |

### Best Practices
- ✅ Use \`Seconds\` for all timeout/interval/age configs.
- ✅ Use \`Bytes\` for all size/limit configs.
- ❌ Don't use raw \`u64\` for time or size in public APIs.
