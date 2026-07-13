# Configuration units

`sinex-primitives` uses small numeric newtypes where a bare integer would hide
its unit. The implementations live in [`src/units.rs`](../src/units.rs).

## Public units

| Type | Meaning | Common constructors | Accessor |
| --- | --- | --- | --- |
| `Seconds` | Whole-second duration | `from_secs`, `from_minutes`, `from_hours` | `as_secs`, `as_duration` |
| `Milliseconds` | Whole-millisecond duration | `from_millis` | `as_millis`, `as_duration` |
| `Microseconds` | Signed microsecond timestamp or duration component | `from_micros` | `as_micros` |
| `Nanoseconds` | Signed nanosecond timestamp or duration component | `from_nanos` | `as_nanos` |
| `Bytes` | Byte count | `from_bytes`, `from_kibibytes`, `from_mebibytes`, `from_gibibytes` | `as_u64`, `as_usize` |

`Seconds` and `Bytes` are re-exported from the crate root. The finer time units
are available from `sinex_primitives::units`.

```rust
use sinex_primitives::{Bytes, Seconds};
use sinex_primitives::units::Milliseconds;

let timeout = Seconds::from_secs(30);
let poll_interval = Milliseconds::from_millis(250);
let payload_limit = Bytes::from_mebibytes(5);

assert_eq!(timeout.as_duration().as_secs(), 30);
assert_eq!(poll_interval.as_millis(), 250);
assert_eq!(payload_limit.as_u64(), 5 * 1024 * 1024);
```

All unit types serialize transparently as integers and parse integer strings.
They do not accept suffixed values such as `"30s"` or `"5MiB"`.

## Validation

Construction and validation are deliberately separate:

- `Seconds::validate()` enforces the shared 24-hour maximum.
- `Bytes::validate()` enforces the shared 1 GiB maximum.
- `from_secs_validated` and `from_bytes_validated` construct and validate in
  one step.

Plain constructors remain useful for values whose domain-specific limit is
checked elsewhere. Public configuration boundaries should use the validated
constructors or call `validate()` explicitly when the shared limits apply.

```rust
use sinex_primitives::{Bytes, Seconds};

let timeout = Seconds::from_secs_validated(30)?;
let payload = Bytes::from_bytes_validated(5 * 1024 * 1024)?;
```

Use a unit newtype in public APIs and configuration whenever the primitive
would make seconds versus milliseconds, or bytes versus a larger unit,
ambiguous. Conversions into `std::time::Duration`, `u64`, or `usize` should
happen at the boundary that requires the primitive.

See the [NixOS module surface](../../../nixos/modules/README.md) for deployment
configuration and `cargo doc --package sinex-primitives --open` for the full
API reference.
