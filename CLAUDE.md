# CLAUDE.md – Core‑Kernel Bootstrap & Unbreakable TDD Playbook (v0.1)

> This file is **always** injected into your context.  Treat it as immutable gospel unless instructed otherwise.  All references point to canonical specs living under `spec/`. Do **not** create documentation outside that tree.

---

## 0.  Scope & Analogy

Think of Sinex Exocortex like a Linux distro.  This document defines the *kernel*—everything that must exist so higher‑level packages (ingestors, agents, UI, etc.) plug in without yak‑shaving.  When the checklist at §1 turns green, the system is **structurally complete**.

---

## 1.  Golden Path to MVP‑0 (the “complete kernel”)

| Step | Deliverable                                                                                                              | Canonical TIM(s) / ADR(s)                                                                     |
| ---- | ------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------- |
| 1    | **Bootstrap repo**: `flake.nix`, `devShells.default`, `nix flake check` passes                                           | `TIM-ReleaseEngineeringCICD.md`, `TIM-ExocortexDevelopmentPractices.md`          |
| 2    | **PostgreSQL 15 cluster** with extensions `pgx_ulid`, `pgvector`, `pg_jsonschema`, `timescaledb` loaded                  | `TIM-PrimaryKeyImplementation.md`, `TIM-TimescaleDBConfiguration.md`                            |
| 3    | Run migration bundle from **`TIM-EventSubstrateDDL`** → creates `raw.events` hypertable & core schemas                   | `TIM-EventSubstrateDDL.md`                                                    |
| 4    | Migrate **schema‑registry** + CHECK constraint (`pg_jsonschema`)                                                         | `TIM-EventSchemaRegistry.md`, `TIM-EventValidation-pgJsonschema.md`                    |
| 5    | Migrate **promotion queue** + **agent\_manifests**                                                                       | `TIM-EventIngestionProcessing.md` (§2 queue DDL) |
| 6    | Compile shared Rust crates: `sinex-ulid`, `sinex-db`, `sinex-worker` (implements `SELECT … FOR UPDATE SKIP LOCKED` loop) | `TIM-EventIngestionProcessing.md` (§3 worker pattern)                         |
| 7    | Ship **`sinex-promo-worker`** binary + NixOS module; prove end‑to‑end: insert dummy event → promoted                     | `TIM-ExocortexDevelopmentPractices.md` (NixOS module template)     |
| 8    | Implement **agent heartbeat** emitting `agent.heartbeat` events every 60 s                                               | `TIM-AgentManifestManagement.md`                                                              |
| 9    | Provide **`exo` CLI** skeleton (diag, sqlx migrate)                                                                      | `TIM-ExoCLIReferenceAndDesign.md`                                                             |
| 10   | Provide **shellHook message** (see §3) & helper scripts under `/scripts/`                                                | `TIM-ReleaseEngineeringCICD.md`                                                               |
| 11   | CI: GitHub Actions invoking `nix flake check`, `cargo test`, DB integration tests                                        | `TIM-ReleaseEngineeringCICD.md`                                                               |
| 12   | Observability stack (Prometheus scrape of worker/DB; pg\_stat\_statements)                                               | `TIM-ObservabilityStackSetup.md`                                                              |
| 13   | **Backups & DR** with `pgBackRest`; daily PITR verified in CI VM test                                                    | `TIM-PostgreSQLBackupDR_pgBackRest.md`                                                        |

When step 13 passes, the *kernel* is complete.  All higher‑level specs (ingestors, LLM agents, UI) may now be implemented independently.

---

## 2.  Database Topography & Runtime Rules

* **Primary store**: local PostgreSQL cluster at `$PGDATA`.

  * Role `sinex_app` owns all schemas; migrations run as `sinex_migrate`.
* **Test store**: ephemeral DB spun up by `scripts/setup_test_db.sh`; URL exported as `$TEST_DATABASE_URL`.  Used by `#[sqlx::test]`.
* **Vector index** lives in same cluster (`pgvector`)—no external Milvus/Weaviate in MVP‑0, per ADR‑007.
* **TimescaleDB** turns `raw.events` into hypertable & manages compression policies (7‑day threshold default)
* Additional SQLite or LiteFS dbs permitted *only* in device‑sync experiments (see `TIM-MultiDeviceSyncArchitecture.md`).  They are out‑of‑scope for kernel.

### Canonical environment variables

```
export DATABASE_URL="postgres://sinex_app:…@localhost:5432/sinex"
export TEST_DATABASE_URL="postgres://sinex_test:…@localhost:5433/sinex_test"
export SINEX_PROMPT_DB_LOG_LEVEL=info   # propagated to workers
```

---

## 3.  DevShell `shellHook` message (excerpt)

```bash
cat <<'EOF'
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃  Sinnix Exocortex devShell                                ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
┃ psql     : connect → $DATABASE_URL                         ┃
┃ test DB  : ./scripts/setup_test_db.sh                      ┃
┃ migrate  : sqlx migrate run                                ┃
┃ run unit : cargo test --all-features                       ┃
┃ run e2e  : cargo test --test e2e                           ┃
┃ lint     : nix flake check                                 ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
EOF
```

This prints every time `nix develop` is entered and guarantees newcomers (human or AI) see the critical commands.

---

## 4.  Claude‑Driven TDD Loop (compressed)

1. **Read spec** → derive tests (unit, integration, property) before code.
2. Commit → red CI.
3. Implement minimal code → green.
4. Harden via `cargo‑mutants`, `criterion`, fuzz.
5. On ambiguity write to `UNRESOLVED.md`.

Full details live in `CDDG.md` §II–III

---

## 5.  Non‑negotiable invariants (CI will fail if broken)

* No table outside schemas declared in `TIM-EventSubstrateDDL`.
* All new migrations include rollback.
* All code paths log `correlation_id` (ULID) for traceability.
* Every binary exports Prometheus `/metrics` on configurable port.
* Unit‑test coverage ≥ 80 % lines; property tests cover schema boundaries.

---

## 6.  Directory Hygiene

```
/flake.nix                 – single source of build truth
/spec/                     – VISION.md, STAD.md, Arch Modules, TIMs, ADRs
/src/                      – Rust crates & binaries
/migrations/               – sqlx‑compatible .sql files (ordered)
/scripts/                  – helper scripts invoked by shellHook/CI
```

If you need documentation, put it under `/spec/docs/claude/...`—never sprinkle Markdown elsewhere.

---

### END
