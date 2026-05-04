## Audit Reference

A comprehensive 47-report codebase audit was completed on 2026-05-03. The master synthesis
and all agent reports are at `.agent/scratch/deep-audit-2026-05-03/`.

### Key Architectural Findings

The central pattern: **correct foundations, incomplete superstructure.** The core abstractions
(`SinexError` with `ErrorClass`, `Id<T>` phantom types, string newtypes, the privacy engine,
the settlement/receipt system, `EventBuilder` typestate) are well-designed. But they are not
systematically applied. The same design intent that created `NatsSubject` as a validated newtype
didn't reach the config structs. The `IngestorNodeAdapter` handles only checkpoints, leaving
telemetry/health/privacy/watcher-supervision duplicated across 6 ingestors.

### Consolidation Potential

| Area | Net Savings | Mechanism |
|------|-------------|-----------|
| Ingestor adapter | ~1,364 lines | Move shared infrastructure from 6 ingestors to `IngestorNodeAdapter` |
| Config framework | ~3,000 lines | `#[derive(SinexConfig)]` replacing 33 env wrapper functions |
| SDK unification | ~1,228 lines | Universal retry/shutdown/health/telemetry in node SDK |
| Payload codegen | ~53% surface | Feature-gate 55 dead payloads behind `unstable` |

### Remediation Priority

1. Fix data integrity: schema version bump discipline, cascade trigger extension, DB credential redaction
2. Wire existing infrastructure: settlement system, HealthReporter→IngestorNodeAdapter, SelfObserver→all nodes
3. Apply existing types: newtypes on bare String fields, `Id<T>` on bare Uuid fields
4. Consolidate semantic duplication: config loading, retry, shutdown, watcher lifecycle
5. Fill privacy gaps: CWD redaction, DLQ sanitization
6. Performance: eliminate COPY JSON round-trip, reduce clone cascade

### Related GitHub Issues

Most findings are tracked in GitHub: #744, #751, #754, #755, #759, #762, #848, #945, #951,
and new issues #986-#994 created from the audit.
