# Core Types Overview

This document summarizes the primary type families exported by `sinex-primitives`.

## Identity and Time

- `uuid::Uuid` is the canonical scalar identifier type at storage and transport boundaries.
- `Id<T>` provides phantom-typed IDs (`Id<Event<_>>`, `Id<SourceMaterial>`, etc.) to prevent domain mixing.
- `Timestamp` is the project-wide time type used across events, repositories, and RPC contracts.

## Domain Wrappers

- `EventSource`, `EventType`, and `HostName` enforce normalized, validated identifiers.
- Domain enums in `domain.rs` (`OperationStatus`, `ReplayOutcome`, `DataTier`, `HealthStatus`, etc.) replace stringly-typed state fields.

## Events and Payloads

- `Event<T>` is the canonical event envelope.
- `Provenance` encodes material vs synthesis lineage.
- `events::payloads::*` contains typed payload structs.
- `EventBuilder` and `DynamicPayload` provide an escape hatch for dynamic payload construction.

## Errors and Results

- `SinexError` is the shared error type with contextual enrichment.
- `Result<T>` aliases and context helpers are provided for ergonomic propagation.

## Validation and Utility Modules

- `validation::*` contains boundary validation for paths, JSON, and input normalization.
- `environment::SinexEnvironment` provides namespace-aware subject/schema/path derivation.
- `units` and `events::enums` provide strongly typed value domains for payload fields.

## Usage Guidance

- Prefer typed wrappers (`Id<T>`, `EventSource`, domain enums) over raw primitives.
- Keep conversions at boundaries (I/O, SQLx bindings, RPC serialization), not in core logic.
- Add new payloads under `events::payloads` and keep taxonomy/docs aligned with schema contracts.
