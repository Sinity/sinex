# Event Taxonomy & Domain Enums

Sinex employs a strongly-typed taxonomy for event classification, utilizing Rust enums to replace "stringly-typed" fields. This approach provides significant memory savings and compile-time correctness guarantees.

## Performance Characteristics

Domain enums are designed as **Zero-Cost Abstractions**:
- **Memory Efficiency**: Most enums derive the `Copy` trait and represent a single byte, compared to 24+ bytes for a `String`. This results in a ~97% reduction in memory footprint for these fields.
- **Stack-Only Operations**: Because enums are `Copy`, they can be passed by value without heap allocation or `clone()` overhead, minimizing CPU cycles in high-frequency event loops.
- **Fast Serialization**: Enums serialize to snake_case strings for database and NATS compatibility, but remain compact discriminants during runtime processing.

## Core Taxonomy Categories

The system defines over 20 specialized enums across several domains:

### Filesystem & Storage
- **FileModificationType**: Categorizes changes into `content`, `metadata`, `permissions`, etc.
- **AnnexBackend**: Identifies the hashing backend used by git-annex (e.g., `SHA256E`).

### System & Lifecycle
- **ShutdownReason**: Documents why a node or system stopped (`requested`, `crashed`, `signal`).
- **SystemdActiveState**: Maps to official systemd unit states (`active`, `inactive`, `failed`).
- **SystemdUnitType**: Identifies unit categories like `service`, `socket`, or `mount`.

### Hardware & Devices
- **DeviceType**: Classifies hardware such as `disk`, `network`, `input`, or `gpu`.
- **UdevAction**: Captures kernel device events like `add`, `remove`, and `bind`.

## Parsing Philosophies

The system uses two distinct strategies for parsing external data into domain enums:

1. **Strict Parsing**: Used for fixed domains (e.g., systemd states). Unknown values trigger an error immediately, ensuring data integrity and early failure detection.
2. **Lenient Parsing**: Used for extensible domains (e.g., kernel udev actions). These enums include an `Other` or `Unknown` variant to ensure forward compatibility with future kernel versions without breaking existing ingestion pipelines.

## Implementation Standards

To maintain consistency across the codebase, all domain enums adhere to the following derive set:
- `Debug`, `Clone`, `Copy`: Essential for logging and performance.
- `PartialEq`, `Eq`, `Hash`: Enables use in `HashSet` and `HashMap` for analytics.
- `Serialize`, `Deserialize`: Integrated with `serde` using `snake_case` renaming.
- `JsonSchema`: Automatically generates JSON schemas for the system-wide schema registry.
