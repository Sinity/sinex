## Type Enforcement Hierarchy

Six levels of guarantee, from compile-time impossible to convention-only. Know which level you're operating at.

### Level 1: Compile-Time Impossible (Strongest)

The type system makes the wrong thing unrepresentable.

| What it prevents | How |
|-----------------|-----|
| Mixing event IDs with blob IDs | `Id<Event>` vs `Id<Blob>` (phantom type) |
| Building events without provenance | `EventBuilder<T, NoProvenance>` has no `.build()` method |
| Confusing source with event type | `EventSource` vs `EventType` (distinct newtypes) |
| Empty synthesis parent arrays | `NonEmptyVec<EventId>` in `Provenance::Synthesis` |
| Invalid source strings in constants | `EventSource::from_static()` validated at compile time |

### Level 2: Lint Enforced / AST-Grep Catalog

Static analysis catches violations before code compiles or merges.

| Rule | Enforcement |
|------|-------------|
| No `unwrap`/`expect` in library code | `deny(unwrap_used, expect_used)` + `allow-unwrap-in-tests` |
| Blocking forbidden patterns | `xtask check --forbidden` (public entrypoint; uses ripgrep-based checks plus ast-grep error-severity rules) |
| Additional structural/style rules | AST-grep catalog in `.config/ast-grep/rules/` (currently advisory warnings/hints unless marked `error`) |

### Level 3: DB Constraint Enforced

PostgreSQL rejects violations at write time.

| Constraint | What it guards |
|------------|----------------|
| XOR provenance CHECK | `source_material_id` XOR `source_event_ids` (exactly one set) |
| Material FK | `source_material_id` references `raw.source_material_registry` |
| Non-empty synthesis parents | `cardinality(source_event_ids) > 0` |
| Anchor byte non-negative | `CHECK (anchor_byte >= 0)` |
| Audit trigger | DELETE on `core.events` requires `sinex.operation_id` session var |

### Level 4: Runtime Validation

Application code checks at boundaries, but violations can reach the check.

| What's validated | Where | Gap |
|------------------|-------|-----|
| Privacy engine (secret detection) | Ingestor, before NATS publish | No automata use privacy engine — derived events inherit ingestor leaks |
| Schema validation | ingestd, before persistence | Lenient: unknown types pass. payload_schema_id never stored on event row |
| Path traversal protection | `validate_path()` at API boundary | Only called where explicitly used |
| JSON depth/size limits | `validate_json()` at API boundary | Only called where explicitly used |

### Level 5: Convention + Startup Check

Correctness depends on matching two manual lists, but startup panics if they diverge.

| Convention | Verification |
|------------|-------------|
| COPY column list matches schema | `verify_event_copy_contract()` at startup — panics on mismatch |
| EventPayload constants match NATS subjects | Inventory collection at startup |

### Level 6: Convention Only (Weakest)

No automated enforcement. Correctness depends on developer discipline.

| Convention | Risk if violated |
|------------|-----------------|
| `operation_id` honesty | Callers can claim any ID (safety gate, not security) |
| `ts_orig` plausibility | Future timestamps persist without warning |
| Payload-to-material correspondence | Event can claim any anchor_byte — no cross-check with blob content |
| Health check truthfulness | Defaults to `true` — no verification of actual health |
| `node_run_id` tracking | Column exists, table defined, never populated (always NULL) |

### Decision: Which Level to Target

When adding a new invariant:
- **Data corruption risk** -> Level 1 (type system) or Level 3 (DB constraint)
- **Code quality rule** -> Level 2 (lint/AST-grep)
- **External input boundary** -> Level 4 (runtime validation)
- **Internal consistency** -> Level 5 (startup check) minimum
- **Never leave at Level 6** if the invariant matters for correctness
