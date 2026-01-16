# Multi‑Device Synchronization

## Overview
Synchronize selected Sinex data and state across personal devices (desktop, laptop, mobile) while preserving local‑first operation and privacy. Aim for eventual consistency and robust offline behavior.

## Goals
- Local‑first on each device; sync when available
- Strong provenance and ordering across devices
- Minimal surface for conflict; explicit policies where needed

## Architecture
- Core truth remains in Postgres on the primary node; nodes on secondary devices capture locally and sync deltas.
- Device identity: stable `device_id` derived from an ed25519 public key; events include `device_id_hash`.
- Ordering: use ULIDs at ingest; attach HLC/vector clock metadata where needed for cross‑device ordering.
- For file/state sync:
  - Syncthing for general files under explicit folders
  - Git‑annex for large blobs
  - Optional LiteFS for single‑writer local SQLite caches
- CRDTs (e.g., Yjs) for collaborative text (Living Document); transport deltas through the gateway

## Event Types (examples)
- `sync.device_announce`: device_id_hash, capabilities, software_version
- `sync.queue_flushed`: device_id_hash, items, bytes
- `sync.conflict_detected`: kind (file|state), policy (lww|manual), details

## Data Model & Policies
- Device identity: ed25519 public key; display‑safe fingerprint; rotation supported via key handoff event.
- Conflict handling
  - Files: last‑writer‑wins within a bounded window; previous version retained in annex; emit `sync.conflict_detected`.
  - State (CRDT): merges without conflicts; audit via CRDT change log; allow manual squash/rewrite.
- Scoping: opt‑in folders and state domains; explicit allowlist per device.

## RPC Surface (proposal)
- `sync.list_devices`: enumerate known devices and capabilities.
- `sync.set_policy`: set sync scope and conflict policy for a path/domain.
- `sync.queue_status`: return pending bytes/items per device.

## Security & Privacy
- End‑to‑end encryption for cross‑device channels
- Explicit allowlists for synced folders/state; no implicit capture
- Clear conflict resolution policies per content type

## Failure Modes & Resilience
- Clock skew: tolerate via ULIDs + optional HLC; do not trust remote timestamps for ordering.
- Partitioning: queues buffer while offline; surface backpressure via telemetry and CLI.
- Corruption: verify content hashes end‑to‑end; quarantine bad chunks; rebuild from annex if needed.

## Testing & Validation
- Property tests for ordering invariants (ULID/HLC monotonicity, dedupe).
- Integration tests for file sync edge cases (rename/move, partial writes).
- Chaos scenarios: intermittent connectivity, duplicate deliveries, clock adjustments.

## Roadmap
- P1: File/folder sync (Syncthing) + annex content presence hints
- P2: Yjs CRDT deltas for notes via gateway; cross‑device ordering tags (HLC)
- P3: Device presence + policy management UI; conflict dashboards
