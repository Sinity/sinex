# PKM Service

`PkmService` encapsulates personal knowledge management flows: entity creation,
relationship management, and provenance tracking. Downstream callers receive a
single API for graph mutations without dealing with low-level repository calls.

- Validates incoming entities against schema contracts stored in
  `sinex-schema`.
- Emits events with proper ULIDs so historical lineage can be reconstructed.
- Provides read helpers optimised for gateway navigation scenarios.

Additional modelling guidance lives in
`docs/architecture/UserInteraction_And_Query_Architecture.md`.
