# Event Taxonomy v2 — EventSource Demotion

Status: design record for #1082. Implementation deferred to post-Wave-4 per #1126.

## 1. Current State

98 `(source, event_type)` pairs across 16 payload domain files. 30 distinct `source` values, 88 distinct `event_type` values. Full inventory at `.agent/scratch/recon-wave1-lane4-taxonomy-descriptors.md`.

### What `source` conflates (6 overloaded semantics)

| # | Semantics | Current location | Examples |
|---|-----------|-----------------|----------|
| 1 | Schema namespace | Registry key `(source, event_type)` | `fs-watcher:file.created` |
| 2 | Source-unit identity | `SourceUnitDescriptor.id` | `terminal.atuin`, `wm.hyprland` |
| 3 | Runtime producer | `Event<T>.source` column | `sinex.ingestd`, `sinex.gateway` |
| 4 | NATS routing token | Subject template | `events.raw.{source}.{event_type}` |
| 5 | Query filter dimension | `EventQuery.sources` | `--source fs-watcher` |
| 6 | Domain/material family | Implicit grouping | desktop, system, terminal |

## 2. Collision Analysis

88 of 98 pairs are already globally unique by event_type alone. 10 collisions:

| event_type | Sources | Resolution |
|-----------|---------|------------|
| `command.executed` | shell.kitty, shell.atuin, shell.history.{bash,zsh,fish} | Prefix with source domain |
| `device.connected` | dbus, udev | Prefix with source domain |
| `monitoring.started` | system, desktop, terminal | Prefix with source domain |

## 3. Proposed Taxonomy

**Rename rules:**
1. Dot-namespaced by domain: `{domain}.{kind}.{action}`
2. Source-specific prefixes for shared kinds
3. 88 already-unique types stay as-is
4. New types use deep dot notation from the start

**Target fields on `core.events`:**

| Field | Type | Replaces |
|-------|------|----------|
| `event_type` | `EventType` | (source, event_type) — globally unique semantic kind |
| `source_unit_id` | `SourceUnitId` | source-as-identity |
| `producer_id` | `ProducerId` (new) | source-as-runtime |
| `source` | `EventSource` | Retained as compatibility alias, then renamed |

## 4. Migration Stages

1. Add `source_unit_id`, `producer_id` columns (nullable), populate from descriptor lookup
2. Migrate schema registry key to `(event_type, schema_version)`
3. Migrate NATS subjects to `events.raw.{event_type}`
4. Migrate query surfaces: `--source` → `--source-unit`
5. Drop `source` column compatibility

## 5. First Implementation Slice

`sinexctl verify --source-units` (implemented in PR #1142) cross-checks descriptor declarations against payload inventory. This is the first non-doc consumer. Next: add `source_unit_id` to `core.events`.

## 6. Non-Goals

- Do not perform schema migration (design only)
- Do not rename event types casually
- Do not blur material vs synthesis provenance

Refs: #1054, #1081, #1058, #1059, #1064, #1126.
