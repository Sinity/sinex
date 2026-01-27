# Event Replay Subsystem

The Replay Subsystem enables safe, controlled reprocessing of events to support data corrections, schema migrations, and logical updates. it features a PostgreSQL-backed state machine and a dry-run simulation engine.

## Replay State Machine

Replay operations are managed through a state machine that ensures approval workflows and prevents conflicting operations.

- **States**:
  - `Planning`: Replay scope is defined but not finalized.
  - `Previewed`: Dry-run simulation has been performed and results are ready for review.
  - `Approved`: Operator has authorized the replay to proceed.
  - `Executing`: Events are actively being reprocessed.
  - `Completed/Failed`: Terminal states with summary results.

- **Persistence**: Replay state and metadata (scope, summary) are stored in the `core.operations_log` table.

## Dry-Run Simulation

Before a replay is executed, it can be run in "dry-run" mode to predict its impact.

- **Operation Simulation**: Predicts the results of `ARCHIVE`, `DELETE`, or `MODIFY` operations.
- **Cost Estimation**: Provides a heuristic estimate of how long the replay will take based on the number and type of operations.
- **Transparency**: Generates a detailed preview summary, including which events will be affected and any potential integrity warnings.

## Invariant Enforcement

The system defines several critical invariants that are checked during the planning and simulation phases to prevent data corruption.

- **Structural Invariants**: Detects circular dependencies and broken provenance chains (orphaned events).
- **Temporal Invariants**: Identifies out-of-order timestamps and "temporal paradoxes" (events occurring before their causes).
- **Data Quality Invariants**: Checks for schema mismatches, material gaps, and overlapping ingestion slices.
- **Security Invariants**: Verifies event immutability via checksums.

## Structured Logging & Metrics

Replay operations emit high-fidelity structured logs via the `ReplayLogger`:
- **Throttled Progress**: Batch progress is logged periodically to provide visibility without flooding logs.
- **Severity Mapping**: Invariant violations are logged with appropriate levels (e.g., Critical violations trigger `error!` and block the replay).
- **Performance Monitoring**: Operations track events per second and resource usage for future optimization.
