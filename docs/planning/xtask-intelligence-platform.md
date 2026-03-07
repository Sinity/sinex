# xtask as a Developer Intelligence Platform — Full Plan

> This document is the canonical record of the full refactoring and enhancement plan for xtask,
> synthesizing the original proposal with all subsequent amendments. It supersedes all prior
> partial addenda. Sections in **bold group headers** are new relative to the original plan;
> sections with inline `[amended]` notes reflect changes from the original.

---

## Sequence

**A → B → E → N → O → K → F1-F3 → C → D → S → V → L → G1-G4 → H → G5-G8 → I → J → P → Q → R → T → U → W → F4-F6**

Rationale: Interface normalization (A) before reduction (B). Dissolution (B, E) before new
commands (N, O). Logging architecture (S) before code quality sweep (V, which includes the
eprintln migration). Library-first queries (L) before the analytics/query commands that use
them (G5+, I, J). Coordinator evolution (R) is independent but benefits from history queries.
VM tests (Q) land before exercise seeding (T) since seeding should include VM test results.

---

## Group A: Interface Normalization [partially amended]

### A1. Normalize `-p/--package` types
`test.rs`: `Option<Vec<String>>` → `Vec<String>`. `docs build`: `Option<String>` → `Vec<String>`.

### A2. Remove `RunCommand::bg` local field shadowing global flag
`run.rs`: remove `self.bg`; replace all uses with `ctx.is_background()`.

### A3. Wire `fix` through coordinator
`fix.rs`: replace direct `ctx.spawn_background` with `coordinator::coordinate_and_spawn()`.
`coordinator.rs`: add "fix" to the coordinated command set.

### A4. Rename `exercise --id` (`-E` → `--id`)
`exercise.rs`: rename the `-E`/`--exercise` flag to `--id` to end semantic collision with `test -E`.

### A5. Add `CommandMetadata::fix()` and `::analysis()` constructors [amended]
`command.rs`: add `fix()` (modifies_state=true) and `analysis()`.
`fix.rs`: use `CommandMetadata::fix()`.
`deps.rs`: use `CommandMetadata::analysis()`.
Also: change `category` field from `Option<String>` to `Option<&'static str>` — eliminates
29 string allocations per command dispatch (from code quality finding B2).

### A6. Remove discarded `_json: bool` from `CommandContext::new()`
`command.rs`: remove `_json: bool` parameter; update the call site in `lib.rs`.
Also: `tls/mod.rs` has `pub fn run(cmd: TlsCommand, _json: bool)` — a second dead `_json` param.
Remove it from the function signature and its call site too.

---

## Group B: Interface Reduction [amended]

### B1. Remove `--affected`/`-A` duality
Remove the `--affected`/`-A` flag from `check.rs`, `build.rs`, `fix.rs`, `test.rs`.
Keep `--all` as the sole override. The default IS affected mode.

Background serialization change per file — each has **different current logic** (research verified):
- `check.rs`: `if this.all { "--all" } else if !this.affected { "--affected=false" }` → keep only `if self.all { "--all" }`
- `build.rs`: same `--affected=false` pattern as check.rs → same fix
- `fix.rs`: **INVERTED BUG** — currently emits `--affected` when `self.affected == true`; fix: emit `--all` when `self.all == true` only
- `test.rs`: currently does **not** serialize `--affected` in the bg path at all → add `if self.all { "--all" }` only

Also: `--package` field naming is **inconsistent across files** (must normalize):
- `check.rs`: field named `packages: Vec<String>` (plural)
- `build.rs`, `fix.rs`: field named `package: Vec<String>` (singular)
- `test.rs`: field named `package: Option<Vec<String>>` (singular, also the A1 type change)
Normalize all four to `packages: Vec<String>` (plural, consistent with check.rs).

### B2. Remove the `xtr` namespace entirely
`xtr` disappears. Each child is promoted:
- `xtask xtr ci` → `xtask ci` with `#[clap(hide = true)]` — **`commands/ci.rs` already exists**; this change only removes the `xtr.rs` routing wrapper, no new file needed
- `xtask xtr patterns` → **removed entirely** (ast-grep can be run directly)
- `xtask xtr completions` → `xtask completions` with `#[clap(hide = true)]`
- `xtask xtr tls` → dissolved per Group E

**CLAUDE.md includes that reference `xtr` — all four must be updated during B2/E cleanup:**
- `.claude/includes/commands/stack.md`: Remove phantom TLS entries (`check`, `setup-env`); update remaining TLS block to reflect E2 (commands now in sinexctl)
- `.claude/includes/commands/extended.md`: Remove `xtask xtr patterns` and `xtask xtr completions` entries
- `.claude/includes/commands/diagnostics.md`: Remove or update the full `xtr ci` reference block (becomes `xtask ci`, hidden)
- `.claude/includes/commands/development.md`: Audit for any remaining `xtr` references

Actual `TlsCommand` variants (verified): `GenerateDevCerts` (hidden), `GenerateClientCert`, `GenerateCa`.
The phantom `xtr tls check` and `xtr tls setup-env` do not exist in code.

### B3. Remove `history last` — folded into `history list`
Remove `HistorySubcommand::Last`. Add `--first` flag to `history list`.

### B4. Remove `history export` — folded into `history list`
Remove `HistorySubcommand::Export`. Add `--no-limit` flag to `history list`.
The export enrichment plan (`--with-diagnostics`, `--with-stages`) transfers here.

### B5. Merge `jobs active` into `jobs list --active`
Remove `JobsSubcommand::Active` or make it a hidden alias. Add `--active` flag to `jobs list`.

### B6. Remove `check --fix-fmt` flag
Remove `fix_fmt: bool` from `CheckCommand`. Expressible as `xtask fix && xtask check`.

### B7. Dissolve vestigial commands [heavily amended — more aggressive than original]

**Delete entirely (no hiding, no forwarding):**
- `xtask db status` — absorbed by `xtask doctor` (was `status --doctor`)
- `xtask db setup` — preflight already does this
- `xtask db apply` — preflight already does this
- `xtask db reset` — replaced by `xtask reset --yes --db` (Group N)
- `xtask db` command entirely — nothing remains after the above
  _(Note: `xtask db migrate` does NOT exist — `db.rs` only has Status/Apply/Setup/Reset)_
- `xtask contracts generate` — unimplemented stub that `bail!`s; delete
- `xtask contracts deploy` — preflight already does this; keep library function, delete CLI
- `xtask contracts check-ready` — moves to `xtask ci check-ready`
- `xtask contracts compat` — moves to `xtask ci compat`
- `xtask infra reset` — replaced by `xtask reset --yes` (Group N)
- `xtask infra env` — absorbed by `xtask doctor`

**Keep:**
- `xtask contracts info` — consider renaming to `xtask schema info`
- `xtask infra start`, `stop`, `status`, `logs` — these remain

**Move:**
- `contracts check-ready` → `xtask ci check-ready`
- `contracts compat` → `xtask ci compat`

---

## Group C: Observability Completion [unchanged]

### C1. Preflight stage tracking
`preflight.rs`: wrap `auto_start_stack()`, `auto_apply_schema()`, `auto_deploy_contracts()`,
`ensure_tls_certs()` with `ctx.start_stage()` / `ctx.finish_stage()`.

### C2. Live stage column for in-flight visibility
`history/db.rs`: add `fn set_live_stage(&self, invocation_id: i64, stage: &str) -> Result<()>`.
Add `live_stage TEXT` column to `invocations` table. **Important**: there is no `ensure_*` lazy
migration pattern in the codebase. Add via `ALTER TABLE invocations ADD COLUMN IF NOT EXISTS live_stage TEXT`
in `init_schema()` — the same file where all other schema init happens (CREATE TABLE IF NOT EXISTS pattern).
`command.rs`: `start_stage()` calls `db.set_live_stage(id, name)`;
`finish_stage()` clears it with `db.set_live_stage(id, "")`.

### C3. `jobs status --json` stage data and phase [amended]
`history/db.rs`: add `get_stage_timings_for_invocation(id: i64) -> Result<Vec<StageTiming>>`.
**Note**: `StageTiming` struct does not exist yet — create it (see G2). The `stage_timings`
table is currently a pure write sink with no read path and no associated Rust struct.
`commands/jobs.rs`:
- Add `"phase"` field from `live_stage` column
- Add `"stages"` field from `get_stage_timings_for_invocation()`
- Fix `status_to_str()` inconsistency: `Success → "success"` not `"completed"` (code quality J4)

### C4. Fix `auto_apply_schema()` silent failure
`preflight.rs`: on apply failure return `Err(eyre!(...))` instead of `Ok(false)`.

### C5. Stage timing summary in command output
`command.rs`: add `completed_stages: RefCell<Vec<(String, f64, bool)>>`.
`finish_stage()` appends to this. At command exit, `ctx.print_stage_summary()` in human mode:
```
Stages: preflight(0.3s) clippy(18.2s) forbidden(0.8s)  →  total 19.5s
```

### C6. `--format jsonl` mode [stretch goal, unchanged]

---

## Group D: Structural Soundness [unchanged]

### D1. FK_VIOLATION string sentinel in ingestd
Replace `error.to_string().contains("FK_VIOLATION")` with typed match on
`sqlx::Error::Database(e)` → `e.code().as_deref() == Some("23503")`.

### D2. Transient error classification in fs-ingestor
Replace message-substring retry classification with `std::io::ErrorKind` matching.

### D3. Context erasure in schema_apply.rs
`.map_err(|e| format!("...: {e}"))` → `SinexError::database(msg).with_std_error(&e)`.

### D4. Hypertable trigger detection
`xtask/src/sandbox/fs/guards.rs:89`: assess typed query via `pg_trigger` system table.

---

## Group E: TLS Dissolution [heavily amended]

The `xtask tls` command and all its subcommands are removed from xtask entirely.

### E1. Remove `generate-dev-certs` as CLI command
Delete `TlsSubcommand::GenerateDevCerts`. The library function stays — preflight calls it.
No user ever needs to call this directly.

### E2. Remove `generate-ca` and `generate-client-cert` from xtask; add to sinexctl [amended]
Both are operational commands (production PKI management), not developer workflow commands.
**They are deleted from xtask CLI entirely — not hidden, not forwarded, not aliased. Zero xtask
CLI surface for these operations after E1-E3.**

They are re-implemented in sinexctl as `sinexctl tls generate-ca` and
`sinexctl tls generate-client-cert`.

**Cross-crate dependency resolution** (sinexctl cannot depend on xtask):
sinexctl lives in `crate/cli/` — a completely separate crate. Adding xtask as a dependency
would pull in build automation, coordinator, sandbox feature, and all of xtask's heavy deps.
That is not acceptable.

Chosen resolution: **sinexctl implements the TLS commands directly using `rcgen`**, not via
`xtask::tls`. The implementation in `xtask/src/tls/generate.rs` is ~50 lines of rcgen usage.
Duplicate it into `crate/cli/src/tls.rs` (sinexctl). This is a deliberate duplication: the
code is short, the crates serve different purposes, and a shared crate is not warranted for
~50 lines of rarely-changed PKI code.

`xtask::tls` library code (`tls/generate.rs`, `tls/verify.rs`) continues to exist for xtask's
own use (preflight calls `generate_dev_certs()`). Do not delete it — only the CLI entry points
are removed from xtask and re-implemented in sinexctl.

### E3. Remove `xtask tls` subcommand entirely
After E1 and E2, nothing remains under `xtask tls`. Delete the subcommand.
Update `flake.nix` shellHook to remove CLI calls; certs are generated by preflight lazily.

### E4. Regenerate command snapshot
After B2 (xtr removal) and E1-E3:
`INSTA_UPDATE=always xtask test -p xtask -E 'test(command_structure_snapshot)'`

---

## Group F: Test Suite Restructuring [unchanged]

### F1. Delete 5 redundant help tests
Remove `test_deps_help`, `test_deps_list_help`, `test_deps_tree_help`, `test_deps_duplicates_help`,
`test_graph_help`. `test_all_commands_help` already covers these recursively.

### F2. Rename/split `tls_tests.rs`
→ `tls_library_tests.rs` (library tests) + `doctor_tests.rs` (doctor assertions, new file).

### F3. Golden JSON fixtures + regression parser tests
`xtask/tests/fixtures/compiler_output/`:
- `machine_applicable_no_byte_offsets.json`
- `multiple_suggestions_machine_wins.json`
- `diagnostic_no_fix.json`

### F4. History DB pipeline integration tests
`xtask/tests/history_integration.rs`:
- `test_recording_chain_for_diagnostics`
- `test_diagnostics_without_byte_offsets_are_queryable`
- `test_stage_recording_roundtrip`
- `test_diagnostic_delta_new`
- `test_live_stage_roundtrip`

### F5. Class-level behavioral invariant tests
`xtask/tests/class_invariants.rs`:
- `test_all_commands_json_output_has_status_field`
- `test_all_bg_commands_produce_queryable_output`
- `test_all_state_modifying_commands_produce_invocation_record`
- `test_all_package_scoped_commands_reject_nonexistent_package`

### F6. New T4 exercises for observability and query contracts
- `t4.preflight_stages_in_history`
- `t4.live_stage_visible_during_run`
- `t4.diagnostic_delta_roundtrip`
- `t4.history_stages_populated`
- `t4.analytics_recommend_runs`

---

## Group G: Queryability Foundation [unchanged from original, with G8 added]

### G1. Diagnostic delta analytics
New DB method `get_diagnostic_delta(from_id, to_id) -> DiagnosticDelta`.
New `history diagnostics` flags: `--new`, `--resolved`, `--persistent N`, `--since`, `--by-code`, `--code`, `--first-seen`.

### G2. `history stages` subcommand
**Starting from scratch**: `stage_timings` table exists and is populated, but has zero read methods
and no `StageTiming` Rust struct anywhere in the codebase. This is a pure greenfield read path.
New Rust struct: `StageTiming { invocation_id: i64, stage_name: String, started_at: String, duration_secs: f64, success: bool }`.
New DB methods: `get_stage_timings_for_invocation`, `get_slowest_stages`, `get_stage_trend`.
New `history stages` subcommand with `--command`, `--invocation`, `--slowest`, `--trend`, `--window`.

### G3. Fix session tracking
Add columns to `invocations` via `ALTER TABLE invocations ADD COLUMN IF NOT EXISTS` in `init_schema()`:
`pre_fix_errors INT`, `pre_fix_warnings INT`, `pre_fix_fixable INT`.
(No `ensure_*` lazy migration pattern — use `init_schema()` `ADD COLUMN IF NOT EXISTS` guards.)
New `history fix` subcommand with `--sessions`, `--effectiveness`.

**Also owned by G3** (must land here, not in G6):
- Extend `DiagnosticCounts` struct to add `fixable: usize` field.
- Add `get_fixable_diagnostic_count() -> Result<usize>` DB method.
- H1 (`post-check fixable hint`) and H2 (`pre-fix before/after summary`) depend on this method
  and are sequenced after G1-G4. If this method isn't created in G3, H1/H2 have no foundation.
- G6 (`status --summary` semantic enrichment) then adds `fixable` to the JSON output — it reuses
  the method created here; it does not create it.

### G4. Package health profiles
New DB method `get_package_health(package, days) -> Vec<PackageHealth>`.
New `history stats` flags: `--package`, `--all-packages`, `--all-commands`.

### G5. Enriched `history list` (absorbs the deleted `export`)
Add: `--no-limit`, `--with-diagnostics`, `--with-stages`, `--with-tests`, `--since`, `--sort-by`, `--offset`.

### G6. `status --summary` semantic enrichment
Add `diagnostics.fixable`, `diagnostics.flaky_tests`, `health_indicator` to summary JSON.
Human summary: `warns:Xe+Yw fixes:Nf`.

### G7. Test analytics extensions
New flags on `history tests`:
- `--grep <text>`: full-text search across stored test output
- `--by-package`: per-package pass rate, test count, avg duration, flaky count
- `--duration-p95`: P95 duration per test
- `--regression`: tests newly failing in last N runs

### G8. `history diagnostics` scope flag normalization
Replace `--all` and `--invocation` flags with `--scope`:
- `history diagnostics` (default): package-scoped supersession
- `history diagnostics --scope all`: accumulated across all invocations
- `history diagnostics --scope <inv_id|latest>`: single invocation

---

## Group H: Smart Defaults and Automagic [unchanged]

### H1. Post-check fixable diagnostic hint
After `check` records diagnostics, if fixable > 0:
```
→ 5 auto-fixable warnings detected. Run: xtask check --fix --smart
```

### H2. Post-fix before/after summary
Before fix: capture `get_current_diagnostic_counts()`. At exit:
```
Before: 12 warnings (5 auto-fixable). Fixes applied.
→ Verify with: xtask check
```
JSON mode: `"pre_fix": {"errors": 0, "warnings": 12, "fixable": 5}`.

### H3. Pre-fix error advisory
If history shows current errors, advise before running fix.

### H4. Post-test inline failure display
Emit compact failure table in human mode (cap at 5 inline), not just "Run with --debug...".

### H5. Coordinator skip scope context
Fresh path message: include what packages were validated:
```
✅ Fresh: last check already validated sinex-db, sinex-schema (job 42, 2m ago)
```

### H6. Affected-mode selection narration
Human mode: emit which packages were selected and why.

### H7. Auto-surface flaky tests after retry patterns
After test run: detect tests that passed on retry, emit advisory.

---

## Group I: Semantic Query Intelligence [unchanged]

### I1. Named Views System (`history view <name>`)
Views: `fixable-now`, `chronic-diagnostics`, `new-diagnostics`, `resolved-last-run`,
`flaky-tests`, `slow-stages`, `hot-packages`, `fix-history`, `recent-regressions`,
`workspace-timeline`, `build-bottlenecks`.
`history view --list` enumerates with descriptions.

### I2. Raw SQL Access (`history shell` + `history query --sql`)
- `history shell`: prints schema, execs `sqlite3` with ergonomic defaults
- `history query --sql <sql>`: executes with `PRAGMA query_only = ON`, returns JSON or table
- `history schema`: dumps annotated CREATE TABLE statements

### I3. Diagnostic Lifecycle Tracking
New `get_diagnostic_lifecycle()` DB method. Status variants: New | Chronic | Recurring | Resolved.
Exposed via `history diagnostics --lifecycle [--code] [--package] [--status]`.

### I4. `history timeline` — Cross-Invocation Chronological View
`get_invocation_timeline()` DB method.
`history timeline [--command] [--days] [--limit]` with stage + diagnostic delta columns.

### I5. `history diff` — Invocation Comparison
Compare two invocations: diagnostic delta, duration delta, stage delta.
`history diff [--from] [--to] [--command]`.

### I6. `history sessions` — Working Session Grouping
Group invocations by gap > 30min. `get_working_sessions(limit, gap_minutes)`.
`history sessions [--limit]` with per-session summary.

### I7. `history invocation <id> --full` — Complete Single-Invocation Picture
`get_invocation_full(id)` DB method joining invocations, stage_timings, build_diagnostics.
`history invocation <id|latest> [--command] [--full]`.

---

## Group J: Analytics Subsystem [unchanged]

### J1. `analytics workspace-health` — Composite Health Score (0-100)
### J2. `analytics hotspots` — Diagnostic Churn Analysis
### J3. `analytics reliability` — Test Reliability Per Package
### J4. `analytics velocity` — Build and Test Time Trends
### J5. `analytics recommend` — Actionable Heuristic Recommendations

Each recommendation includes the exact command to run next.

---

## Group K: Non-Interactive Mode [amended — simplified]

### K1. TTY autodetection (no new flags)
When stdout is not a TTY and no explicit `--format` → default to JSON output automatically.
No `--plain` flag. No `XTASK_FORMAT` env var.
Precedence: `--json` > `--format` > TTY detection > Human default.
Announce on stderr: `"Plain output active (non-TTY).\n"` in human mode when auto-detected.

**Sequencing dependency on E4**: E4 (command snapshot regeneration) is sequenced before K1
in the plan. After K1 lands, nextest (which runs in non-TTY) will auto-select JSON output
for all command invocations, changing the snapshot again. **E4 must be re-run after K1** as
an explicit follow-up step — note this in the E4 task when executing.

---

## **Group L: Library-First Query API** [expanded to L1-L4]

### L1. `DiagnosticQuery` fluent builder
Replaces the proliferating bespoke diagnostic query methods.
```rust
DiagnosticQuery::new()
    .package("sinex-db")
    .fixable()
    .command("check")
    .scope(DiagnosticScope::Current)
    .limit(50)
    .run(db)?
```
Fields translate to SQL WHERE clauses — no in-memory post-filtering.

### L2. `InvocationQuery` fluent builder
```rust
InvocationQuery::new()
    .command("check")
    .succeeded()
    .days(7)
    .with_stages()
    .limit(20)
    .run(db)?
```

### L3. `TestResultQuery` fluent builder [new]
For nextest results. Replaces 8+ bespoke functions in `history/tests.rs`.
```rust
TestResultQuery::new()
    .package("sinex-db")
    .status(TestStatus::Fail)
    .with_output()
    .limit(10)
    .run(db)?
```
Add `test_mode TEXT DEFAULT 'nextest'` column to `test_results` for VM/bench/fuzz extensibility.

### L4. `HistoryAnalysis` cross-dimensional facade [new]
Composes L1-L3 into multi-dimensional views:
```rust
HistoryAnalysis::new(db)
    .package_health("sinex-db")  // -> {test_pass_rate, diagnostic_count, avg_build_time}
    .regression_scan(since)      // -> new diagnostics correlated with new test failures
    .hotspots()                  // -> packages with highest combined churn
```

**Note on generic base**: L1-L4 share a `HistoryQuery<T>` generic base with common time/command/package
filters, and type-specific terminal methods (`run()`, `count()`, `first()`). This prevents
method-per-combination explosion in `HistoryDb`.

---

## **Group N: `xtask reset` — Standalone Top-Level Command** [new]

Absorbs `infra reset` and `db reset`. Promotes reset to first-class concept because
the scope extends beyond "infrastructure" to all developer state.

```bash
xtask reset --yes              # Everything: db + nats + preflight + jobs + target
xtask reset --yes --db         # Drop and recreate database only
xtask reset --yes --nats       # Wipe NATS JetStream data only
xtask reset --yes --blobs      # Wipe git-annex blobstore
xtask reset --yes --preflight  # Wipe entire .sinex/preflight/ directory
xtask reset --yes --contracts  # Delete contracts-hash.txt + preflight-cache.json
xtask reset --yes --schema     # Delete schema-apply-hash.txt + preflight-cache.json
xtask reset --yes --history    # Delete xtask history SQLite DB
xtask reset --yes --history --seed  # Wipe and reseed with synthetic data
xtask reset --yes --target     # Wipe target/ directory (force clean recompilation)
xtask reset --yes --tls        # Regenerate TLS certificates
```

Implementation: each flag maps to a concrete action (file deletion, db drop, etc.).
`--contracts` and `--schema` are surgical: delete only the hash files that gate
preflight re-deployment. This solves "force contract redeploy" without data loss.

`infra reset` is removed. `infra` keeps: `start`, `stop`, `status`, `logs`.
`db` command is removed entirely (see B7).

---

## **Group O: `xtask doctor` — Real Command** [new]

Not an alias. Logic moves from `status --doctor` into `commands/doctor.rs`.
`status --doctor` is **deleted** — no forwarding, no compatibility alias.
`status` continues to work for summary output; doctor is a separate command.

```bash
xtask doctor                   # Full health check (Postgres, NATS, tools, TLS)
xtask doctor --json            # Structured output
xtask doctor --fix             # Auto-remediate: restart stale processes, repair DB
xtask doctor --pipelines       # Health check + pipeline smoke tests
```

`--fix` remediates: kill stale processes, recreate corrupted history DB, start missing infra,
invalidate stale preflight cache. This has different `CommandMetadata` (modifies_state=true)
than the old `status --doctor` (read-only).

---

## **Group P: `run` Command Improvements** [new]

### P1. Rename `run stack` → `run core`
The word "stack" is ambiguous (also used in infra context). `run core` means
"the core sinex application processes" (ingestd + gateway).

```bash
xtask run core                 # Build and run sinex-ingestd + sinex-gateway
xtask run core --watch         # Hot-reload on file changes
xtask run core --tether        # Connect to production NATS
xtask run core --bg            # Run in background via job system
xtask run core --logs          # Interleaved logs with color-coded prefixes (new)
```

### P2. `run core --logs` flag
Tail all process logs interleaved with color-coded process prefixes. Currently requires
separate `xtask infra logs` calls per process.

### P3. Keep infra/run separation
`infra` = Postgres + NATS (background infrastructure, persistent across sessions).
`run` = sinex application binaries (active development, per-session).
These are semantically different lifecycles. Do not merge.

---

## **Group Q: NixOS VM Tests as First-Class** [new]

### Q1. Promote to `xtask test --vm`
Remove `xtask infra vm test` as the entry point. Add `--vm` flag to `xtask test`.
```bash
xtask test --vm                          # All VM test scenarios
xtask test --vm --category smoke         # Basic + preflight only (~5-10min)
xtask test --vm --category integration   # Integration scenarios
xtask test --vm --parallel               # Parallel execution
```

### Q2. Rewrite bash → native Rust
Replace `tests/e2e/nixos-vm/run-vm-tests.sh` (479 lines) with native Rust in `vm.rs`.
Direct history DB writes instead of `.result` / `.log` file outputs.
Note: `CHAOS_TESTS=()` in the existing shell script is **intentionally empty** — chaos/failure-injection
testing is disabled pending a new failure-injection harness. The Rust rewrite should preserve this as
a stub category (defined but empty), ready to be populated when the harness is built.

### Q3. VM results in history DB
Add `test_mode = 'vm'` to `test_results` via the `test_mode` column (L3).
VM test results become queryable alongside unit/integration test results.

### Q4. NixOS compatibility enforcement narration
VM tests already import real NixOS modules — they ARE the compatibility enforcement.
Make this explicit: `xtask test --vm --category smoke` is the fast NixOS compatibility gate.

### Q5. Dirty NixOS module detection
Extend affected-mode to detect dirty `nixos/**/*.nix` files. When NixOS modules are dirty,
`xtask check --full` suggests running VM smoke tests.

### Q6. `xtask check --nix` fast evaluation stage
Add `--nix` flag to `xtask check`. Runs `nix flake check --no-build` (~2-5s, evaluation only).
Included in `--full`. Stage timing recorded as `"nix-check"`.
Only runs if `nix` is on PATH (skip with warning otherwise).

---

## **Group R: Coordinator Evolution** [new — 3 phases]

### R1. Phase 1: Per-Package Fingerprinting + Compilation Prefetching

**Per-package fingerprinting**: Replace whole-workspace `git status --porcelain` hash with
package-scoped file hash. Changing `nixos/README.md` no longer invalidates `check -p sinex-db`.

```rust
fn scoped_tree_fingerprint(scope: &PackageScope) -> Result<String> {
    match scope {
        PackageScope::Workspace => tree_fingerprint(),
        PackageScope::Explicit(pkgs) | PackageScope::Affected(pkgs) => {
            let mut hasher = Sha256::new();
            for pkg in pkgs {
                let prefix = package_to_path(pkg);
                let output = Command::new("git")
                    .args(["diff", "--name-only", "HEAD", "--", &prefix])
                    .output()?;
                hasher.update(&output.stdout);
            }
            Ok(format!("{:x}", hasher.finalize()))
        }
    }
}
```

**Compilation prefetching**: After a successful `check`, query invocation history to compute
transition probability: "what command does this developer run after check?" If `test` follows
`check` in > 70% of recent invocations within 5 minutes, automatically start `cargo test --no-run`
in the background. When the developer types `xtask test`, binary is already compiled.

### R2. Phase 2: Workflow Dependency Graph + `xtask work`

Declare operation dependencies:
```rust
static WORKFLOW: &[(&str, &str)] = &[
    ("test",  "check"),    // test depends on check succeeding
    ("check", "fix"),      // check should follow fix
    ("test",  "schema"),   // tests need schema applied
];
```

`WorkflowGraph::sequence_to(target)` performs topological sort over the tiny DAG.
`xtask work <target>` executes the minimum sequence to reach a desired state, skipping Fresh steps:
```bash
xtask work test   # ensures: infra_start → schema_apply → check → test
                  # skips anything that's already fresh
```

### R3. Phase 3: Predictive Auto-Prefetch from History
SQL query for command transition frequencies from `invocations` table.
Auto-prefetch compilation when probability > 70%.
Opt-in auto-execution via `UserPreferences.toml` (see Group W):
```toml
[coordinator]
auto_sequence = ["check -> test"]
```

### R4. Distributed Coordination via `git notes`
Zero-infrastructure approach: CI writes results as git notes after successful runs.
Local coordinator checks `git notes show HEAD` before re-running. The git repo becomes
the coordination store. This is speculative/optional — the value is "CI validated this
commit, local re-run skipped."

### R5. Instrumentation
All coordinator decisions emit `tracing::info!()` events (Fresh/Attach/Supersede/Queue/Start)
with structured fields: `command`, `scope_key`, `fingerprint`, `job_id`, `decision`.
These are also persisted to the history DB (see Group S).

---

## **Group S: Logging and Tracing Architecture** [new]

### S1. Core architecture
Two output streams, formally separated:
- **stdout**: operational output via `OutputWriter` (what the user asked for)
- **stderr**: internal diagnostics via `tracing` (what's happening internally)

These never mix. Coordinator decisions currently go to stdout via `println!()` — this is
the primary bug to fix. All such calls move to `tracing::info!()`.

### S2. Verbosity control
Add `-v`/`-vv`/`-vvv` via `clap::ArgAction::Count` to `GlobalOpts` (global flag).

| Flag | Level | Visible on stderr |
|------|-------|-------------------|
| (none) | OFF | Silent |
| `-v` | INFO | Stages, coordinator decisions, preflight steps |
| `-vv` | DEBUG | Cargo args, DB queries, fingerprints, PIDs |
| `-vvv` | TRACE | Full state dumps |

`SINEX_LOG` env var remains as power-user per-module override (secondary to `-v`).
Precedence: env var overrides per-module; `-v` sets the floor.

### S3. Subscriber initialization
```rust
// main.rs — configure based on CLI flags (parsed before init)
let base_level = match verbosity {
    0 => LevelFilter::OFF,
    1 => LevelFilter::INFO,
    2 => LevelFilter::DEBUG,
    _ => LevelFilter::TRACE,
};
let filter = EnvFilter::builder()
    .with_default_directive(base_level.into())
    .with_env_var("SINEX_LOG")
    .from_env_lossy();

// JSON formatter when --json + -v, human formatter otherwise
if json_mode && verbosity > 0 {
    registry.with(fmt::layer().json().with_writer(stderr)).with(filter).init();
} else {
    registry.with(fmt::layer().compact().with_target(false).with_writer(stderr))
        .with(filter).init();
}
```

### S4. Key instrumentation points
- `coordinator.rs`: All 6 decision paths emit `tracing::info!()` with structured fields
  (Excluded / Fresh / Attach / Supersede / Queue / Start)
- `command.rs`: `start_stage()` / `finish_stage()` emit `tracing::info!()` (in addition to DB)
- `preflight.rs`: All `eprintln!()` → `tracing::info!()` or `ctx.status_message()`
- `cargo_diagnostics.rs`: Spawn + completion events at `tracing::info!()`
- `history/db.rs`: Query events at `tracing::debug!()`

### S5. `ctx.status_message()` for essential UX feedback
```rust
impl CommandContext {
    /// Essential operational message — always visible in human mode,
    /// emitted as tracing event in JSON mode.
    pub fn status_message(&self, msg: &str) {
        match self.writer.format() {
            OutputFormat::Silent => {}
            OutputFormat::Json => tracing::info!(kind = "status", "{}", msg),
            _ => eprintln!("{msg}"),
        }
    }
}
```

### S6. `eprintln!()` migration
146 `eprintln!()` calls across 30 files, classified:
- Warning/error messages → `tracing::warn!()` / `tracing::error!()`
- Progress/status messages → `ctx.status_message()` (essential) or `tracing::info!()`
- Debug details → `tracing::debug!()`

### S7. Persistence to history DB
Trace events are persisted to SQLite for permanent queryability. See Group S8 for schema.
Not all events — filtered by category:

| Event category | Persistence policy |
|---|---|
| ERROR/WARN | Always persist |
| Coordinator decisions | Always persist (join to invocations) |
| Stage events | Skip — already in `stage_timings` |
| Cargo spawn/completion | Persist summary only |
| Preflight actions | Persist key actions (auto-start, schema apply, contract deploy) |
| DB queries | Never persist (too granular) |
| DEBUG events | Never persist |

### S8. `trace_events` table schema

**Table definition** (add to `init_schema()` as `CREATE TABLE IF NOT EXISTS` — the standard
pattern used throughout `history/db.rs`; there is no `ensure_*` lazy migration pattern):

```sql
CREATE TABLE IF NOT EXISTS trace_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
    ts            TEXT    NOT NULL,   -- ISO-8601 UTC timestamp
    level         TEXT    NOT NULL,   -- "ERROR" | "WARN" | "INFO"
    target        TEXT    NOT NULL,   -- tracing target (Rust module path)
    event_kind    TEXT,               -- classified: "coordinator.decision" | "preflight.action"
                                      --   | "cargo.spawn" | "cargo.complete" | "error" | "warn"
    message       TEXT    NOT NULL,
    fields        TEXT                -- JSON object of additional structured fields
);
CREATE INDEX IF NOT EXISTS trace_events_invocation_idx  ON trace_events(invocation_id);
CREATE INDEX IF NOT EXISTS trace_events_level_idx       ON trace_events(level);
CREATE INDEX IF NOT EXISTS trace_events_event_kind_idx  ON trace_events(event_kind);
CREATE INDEX IF NOT EXISTS trace_events_ts_idx          ON trace_events(ts);
```

**`TraceRecord` struct** (sent over bounded channel to writer thread):
```rust
struct TraceRecord {
    invocation_id: Option<i64>,   // None until start_invocation() resolves
    ts: String,
    level: &'static str,
    target: String,
    event_kind: Option<&'static str>,
    message: String,
    fields: Option<String>,       // serde_json serialized
}
```

**invocation_id sharing mechanism** (this must be specified unambiguously):

After `tracing::subscriber::set_global_default()` in `main.rs`, the registry owns the layer
and there is no public API to reach back inside it. The solution is a **module-level static**
in `history/tracing_layer.rs` that both `main.rs` and `lib.rs` can read independently:

```rust
// history/tracing_layer.rs
pub static CURRENT_INVOCATION_ID: LazyLock<Arc<AtomicI64>> =
    LazyLock::new(|| Arc::new(AtomicI64::new(-1)));
```

- `main.rs` passes `Arc::clone(&*CURRENT_INVOCATION_ID)` to `HistoryTracingLayer::new()`.
- `lib.rs` (`run_command()`) calls `CURRENT_INVOCATION_ID.store(id, Ordering::SeqCst)` after
  `HistoryDb::start_invocation()` returns the new invocation id.
- The layer's `current_invocation_id()` helper: reads `self.invocation_id.load(Ordering::SeqCst)`,
  returns `None` when the value is -1 (pre-invocation trace events).

`-1` is a safe sentinel because SQLite `invocations.id` is AUTOINCREMENT starting at 1.

**`HistoryTracingLayer` struct:**
```rust
pub struct HistoryTracingLayer {
    tx: mpsc::SyncSender<TraceRecord>,
    _writer_handle: thread::JoinHandle<()>,
    /// Arc clone of CURRENT_INVOCATION_ID. Updated externally by lib.rs after start_invocation().
    invocation_id: Arc<AtomicI64>,
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for HistoryTracingLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        if !should_persist(meta.level(), meta.target()) {
            return;
        }
        let mut visitor = FieldExtractor::default();
        event.record(&mut visitor);
        let event_kind = classify_event_kind(meta.target(), meta.level(), &visitor.fields);
        let record = TraceRecord {
            invocation_id: self.current_invocation_id(),
            ts: Utc::now().to_rfc3339(),
            level: meta.level().as_str(),
            target: meta.target().to_string(),
            event_kind,
            message: visitor.message,
            fields: visitor.extra_fields_as_json(),
        };
        // Non-blocking: drop the event if the channel is full (never block the caller)
        let _ = self.tx.try_send(record);
    }
}
```

**`should_persist` filter logic:**
```rust
fn should_persist(level: &Level, target: &str) -> bool {
    match *level {
        Level::ERROR | Level::WARN => true,
        Level::INFO => {
            // Only persist INFO events from high-value targets
            target.starts_with("xtask::coordinator")
            || target.starts_with("xtask::preflight")
            || target.starts_with("xtask::cargo")
        }
        Level::DEBUG | Level::TRACE => false,
    }
}
```

Stage events from `xtask::command` (start_stage/finish_stage) are at INFO but do NOT match
the above targets — they are intentionally excluded here because `stage_timings` already
captures them with microsecond precision.

**`classify_event_kind` — structured classification:**
```rust
fn classify_event_kind(
    target: &str,
    level: &Level,
    fields: &HashMap<String, Value>,
) -> Option<&'static str> {
    if *level == Level::ERROR { return Some("error"); }
    if *level == Level::WARN  { return Some("warn");  }
    if target.starts_with("xtask::coordinator") { return Some("coordinator.decision"); }
    if target.starts_with("xtask::preflight")   { return Some("preflight.action");     }
    if target.starts_with("xtask::cargo") {
        return Some(if fields.contains_key("pid") { "cargo.spawn" } else { "cargo.complete" });
    }
    None
}
```

**Writer thread — batch INSERT with 64-event flush threshold:**
```rust
fn writer_loop(db_path: PathBuf, rx: mpsc::Receiver<TraceRecord>) {
    let conn = match rusqlite::Connection::open(&db_path) {
        Ok(c) => c,
        Err(_) => return,  // history DB not available; drop all records silently
    };
    ensure_trace_events_table(&conn).ok();

    let mut batch: Vec<TraceRecord> = Vec::with_capacity(64);
    loop {
        // Block until first event (or channel closes)
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(record) => {
                batch.push(record);
                // Drain remaining without blocking
                while batch.len() < 64 {
                    match rx.try_recv() {
                        Ok(r) => batch.push(r),
                        Err(_) => break,
                    }
                }
                flush_batch(&conn, &mut batch);
            }
            Err(RecvTimeoutError::Timeout) => {
                if !batch.is_empty() { flush_batch(&conn, &mut batch); }
            }
            Err(RecvTimeoutError::Disconnected) => {
                flush_batch(&conn, &mut batch);
                break;
            }
        }
    }
}

fn flush_batch(conn: &Connection, batch: &mut Vec<TraceRecord>) {
    if batch.is_empty() { return; }
    if let Ok(tx) = conn.transaction() {
        for record in batch.drain(..) {
            tx.execute(
                "INSERT INTO trace_events (invocation_id, ts, level, target, event_kind, message, fields)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    record.invocation_id, record.ts, record.level, record.target,
                    record.event_kind, record.message, record.fields
                ],
            ).ok();
        }
        tx.commit().ok();
    }
}
```

**Channel sizing:** `mpsc::sync_channel(512)`. At ~15 events/invocation and batch flushes
every 200ms, backpressure is theoretical. Try-send on full channel drops the event (never
blocks the cargo/tracing hot path).

**`history trace` subcommand:**
```bash
# All trace events for the most recent invocation
xtask history trace

# Filter by invocation, level, or classified kind
xtask history trace --invocation <id|latest>
xtask history trace --invocation latest --level error
xtask history trace --invocation latest --kind coordinator.decision
xtask history trace --invocation latest --kind preflight.action
xtask history trace --kind error --limit 20   # recent errors across all invocations

# JSON output (for programmatic processing)
xtask history trace --invocation latest --json
# → [{"ts":"...","level":"INFO","event_kind":"coordinator.decision","message":"...","fields":{...}}]
```

Human output format:
```
Trace for check #1234  (2026-03-07 10:30:15)

[INFO ] coordinator.decision  Fresh: last check validated this state (job 1230, 3m ago)
[INFO ] preflight.action      Schema already applied (hash match), skipping
[INFO ] preflight.action      Contracts already deployed (hash match), skipping
[INFO ] cargo.spawn           cargo clippy started (pid=18432)
[INFO ] cargo.complete        cargo clippy finished in 18.2s (exit=0, 3 warnings)
[WARN ] xtask::cargo          3 warnings recorded (MachineApplicable: 2)
```

**Integration with `history invocation --full`** (Group I7):
The `--full` output includes a "Trace" section showing all `trace_events` for that invocation.
Errors and warnings are visually distinguished. Coordinator decision is surfaced prominently.

**Volume estimates and pruning:**
- Typical invocation: 5–15 trace events
- Heavy day (frequent check/test): ~400 events/day
- At 90-day pruning (already enforced by `history prune`): max ~36,000 rows
- SQLite handles this trivially; no separate pruning needed beyond existing `history prune`
- `history prune` adds: `DELETE FROM trace_events WHERE invocation_id IN (SELECT id FROM invocations WHERE started_at < ?)`
  — cascades automatically via the `ON DELETE CASCADE` foreign key

**File locations:**
- `xtask/src/history/tracing_layer.rs` — `HistoryTracingLayer`, `TraceRecord`, `FieldExtractor`, `writer_loop`
- `xtask/src/history/db.rs` — `ensure_trace_events_table()`, `get_trace_events_for_invocation()`
- `xtask/src/commands/history.rs` — `HistorySubcommand::Trace` variant
- `xtask/src/lib.rs` — install layer in `tracing_subscriber` init

### S9. What NOT to build
- JSONL mixed-stream output (breaks pipe compatibility)
- W3C TraceContext propagation (overkill; `invocation_id` serves this)
- OpenTelemetry exporter (no backend; history DB already provides the data)
- Per-command log files (job output files already serve background jobs)
- `#[instrument]` on hot path functions (noise; manual spans at decision points are correct)

---

## **Group T: Exercise Seeding + Synthetic History** [new — replaces Group M]

### T1. `exercise --seed` — Ephemeral History for Exercise Runs
Before running, create a temp directory containing a seeded SQLite history DB.
Override `SINEX_STATE_DIR` in subprocess environment. Clean up after exercise run.
Exercises that query history see rich, realistic output. Real history is never touched.

Exercises automatically set `XTASK_SYNTHETIC_HISTORY=allow` in subprocess env to suppress
the synthetic warning (see T3).

### T2. `xtask history seed` — Persistent Seeding for Manual Exploration
Writes synthetic data to the real history DB path.
`xtask reset --yes --history --seed` combines wipe + reseed.
Seed generator: `xtask/src/history/seed.rs` (analogous to sandbox's `dataset_seeds.rs`).

### T3. Synthetic History Detection and Warning
The seed command writes a `metadata` table into the SQLite file:
```sql
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO metadata (key, value) VALUES ('synthetic', 'true');
```

`HistoryDb::open()` checks for this marker after `init_schema()`. Sets `is_synthetic: bool`.

**Warning behavior** (NOT a hard error):
- Read commands: multi-line stderr warning on first access per process (via `OnceLock`)
- Write commands: synthetic marker is cleared on first real `start_invocation()` — DB
  transitions naturally from "demo" to "real"

**Warning message:**
```
WARNING: History database contains synthetic (seeded) data.
  Database: /realm/project/sinex/.sinex/state/xtask-history.db
  Seeded by: xtask exercise --seed or xtask history seed

  Results from history commands reflect fabricated data, not real usage.
  To start fresh: xtask reset --yes --history
  To suppress:    XTASK_SYNTHETIC_HISTORY=allow
```

**`status --summary`**: Add `[synthetic]` tag when operating against seeded data.

### T4. Seed data parameters (quantitative, not modal)
```bash
xtask exercise --seed                       # default: 30 days, ~100 invocations
xtask exercise --seed --days 90             # 90-day history
xtask exercise --seed --invocations 500     # 500 invocations
xtask exercise --seed --days 90 --invocations 500
xtask exercise --seed --activate            # also sets XTASK_HISTORY_DB for this session
```

Default distributions (hardcoded, realistic):
- 40% check, 30% test, 15% build, 10% fix, 5% other
- ~85% success rate
- ~5% test failure rate
- Diagnostics trending downward (improving workspace)
- 20-30 diagnostics across 5 packages
- Stage timings: clippy ~18s ±10%, preflight ~0.3s

### T5. `XTASK_HISTORY_DB` env var override
Add to `config.rs`: `XTASK_HISTORY_DB` overrides `history_db_path()`. Allows pointing any
xtask invocation at an alternate SQLite file. The exercise `--seed --activate` sets this.
**Implementation note**: config entry point is `Config::from_env()` (static `LazyLock` singleton),
not `Config::load()`. Current `history_db_path()` returns `state_dir.join("xtask-history.db")`
with no env override — `XTASK_HISTORY_DB` is the new override to add.

---

## **Group U: `xtask snapshot` Enhancements** [new]

**Implementation note**: current `snapshot.rs` uses `Command::new("repomix")` synchronously
(`.output()` wait, not async). Default output filename is `context.xml` (not `context.md`).
`SnapshotCommand` fields: `output: Option<PathBuf>`, `include: Vec<String>`, `exclude: Vec<String>`,
`compress: bool`, `remove_comments: bool`. All flags in U1-U5 are additive to this struct.

### U1. `--diagnostics` flag
Auto-include files mentioned in most recent `build_diagnostics` invocation.
Query: distinct `file_path` from recent `build_diagnostics` → pass as `--include` to repomix.

### U2. `--changed` flag
Include files from `git diff --name-only HEAD` (staged + unstaged changes).

### U3. `--context` flag — xtask state injection
Inject structured xtask state as a metadata section in the repomix output:
```
[xtask-context]
recent_checks: [{id: 42, status: failed, errors: 3, when: "2m ago"}]
active_diagnostics: [{file: "src/foo.rs", line: 12, msg: "unused import"}]
coordinator_state: {check: {fresh: true, fingerprint: "abc123"}}
active_jobs: [{id: 43, command: "test", status: "running"}]
```

### U4. `--project-memory` flag
Include CLAUDE.md and `.claude/includes/` in the snapshot.

### U5. `--scope` flag for crate filtering
```bash
xtask snapshot --scope sinex-db          # sinex-db + transitive deps only
xtask snapshot --scope core              # crate/core/* only
xtask snapshot --scope nodes             # crate/nodes/* only
xtask snapshot --scope tests             # tests/ + xtask/tests/ only
```
Uses the existing workspace dependency graph for transitive closure.

The combination `--context --diagnostics --project-memory` creates a "debug packet":
a single file giving an AI agent complete situational awareness with zero exploration.

---

## **Group V: Code Quality Fixes** [new — from systematic analysis]

> **Internal ID notation**: Sub-item labels within V (e.g. `J1`, `G2`, `A1`) are internal
> quality-tracking IDs reflecting the code area being improved (J = jobs.rs, A = affected/command,
> G = general DB, etc.). They are **not cross-references to plan groups J, G, or A**. To avoid
> ambiguity, read these as "V1-J1", "V2-G1" etc. — the V-group prefix is implied.

### V1. Immediate (trivial, high value)
- **J1**: Extract `row_to_background_job()` — eliminates 3 copy-pasted 20-line row mappers
- **J4**: Standardize `InvocationStatus::Success` display: `"success"` everywhere, not `"completed"` in `jobs.rs` — this is a correctness bug in JSON output
- **J7**: Extract `LATEST_PER_PACKAGE_CTE` as `const &str` — shared between `get_current_diagnostics` and `get_current_diagnostic_counts`
- **J3**: Deduplicate `format_time` / `DISPLAY_TIME_FORMAT` between `jobs.rs` and `history.rs`
- **A3**: Use `config().hostname` consistently (9× duplication of `gethostname()` call)
- **A2**: Add `tracing::warn!()` to `InvocationStatus::from_str` catch-all branch
- **B2**: Change `CommandMetadata::category` from `Option<String>` to `Option<&'static str>`

### V2. Medium effort
- **A1**: Change `record_diagnostic()` from 13 positional params to `&CompilerDiagnostic`
- **G2**: Add `#[derive(Serialize)]` to `BackgroundJob` — eliminates 4 hand-built JSON blocks in `jobs.rs`
- **D2**: Replace 500ms poll loop in `wait_for_any_child_exit` with `tokio::select!` over `child.wait()`
- **J2**: Consolidate `get_recent()` SQL duplication via single parameterized query
- **G1**: Add `ctx.print(msg)` helper on `CommandContext` — eliminates `if ctx.is_human() { println!() }` across 472 sites (gradual migration, coincides with S6 eprintln migration)

### V3. Infrastructure gaps
- **H1**: Add unit tests for `coordinator.rs` — critical decision logic with zero tests
  (prerequisite for coordinator evolution in Group R)
- **H2**: Split `exercise.rs` (114KB / ~2850 lines) into `exercise/tier1.rs` through `exercise/tier4.rs`
- **F1**: Add `run_cargo_build()` to `cargo_diagnostics.rs` with timeout — build currently
  has no timeout protection and can hang on `target/` lock
- **E1/E2**: Extract `fn cargo_run_command()` within `run.rs` — 5 separate `Command::new("cargo")`
  sites with duplicated env-var setup

### V4. Dependency cleanup
- **I3**: Either adopt `parking_lot::Mutex` consistently or remove the dep (currently unused)
- **J6**: Move `Confidence` type from `history/tests.rs` public API to sandbox/test infrastructure
- **I1**: Feature-gate `proptest` and `insta` (currently in `[dependencies]`, compiled into binary)

---

## **Group W: UserPreferences Configuration** [new]

### W1. `~/.config/xtask/preferences.toml`
Extend `Config` to load from XDG config path. `toml` is already a workspace dep.

```rust
#[derive(Debug, Default, serde::Deserialize)]
struct UserPreferences {
    notify_on_completion: bool,
    #[serde(default)]
    coordinator: CoordinatorPrefs,
}

#[derive(Debug, Default, serde::Deserialize)]
struct CoordinatorPrefs {
    auto_sequence: Vec<String>, // e.g., ["check -> test"]
}
```

Precedence: CLI flag > env var > preferences file > default.

### W2. NixOS home-manager integration
```nix
xdg.configFile."xtask/preferences.toml".text = ''
  notify_on_completion = true
  [coordinator]
  auto_sequence = ["check -> test"]
'';
```

### W3. Background job notifications
When `preferences.notify_on_completion = true`, completed `--bg` jobs emit a desktop
notification via `notify-send` (NixOS). Falls back to no-op if unavailable.
Notification includes: command name, status, duration.

---

## Critical Files Summary

| Area | Key files |
|------|-----------|
| Interface normalization (A) | `commands/test.rs`, `build.rs`, `fix.rs`, `run.rs`, `exercise.rs`, `docs.rs` |
| Interface reduction (B) | `commands/check.rs`, `history.rs`, `jobs.rs`, `db.rs`, `contracts.rs`, `infra.rs`, `command.rs` |
| TLS dissolution (E) | `tls/mod.rs`, `flake.nix`; sinexctl for new home |
| Reset (N) | New `commands/reset.rs`; delete `commands/db.rs`, modify `commands/infra.rs` |
| Doctor (O) | New `commands/doctor.rs`; delete `status --doctor` logic from `commands/status.rs` |
| Run (P) | `commands/run.rs` — rename stack→core, add --logs |
| VM tests (Q) | `commands/vm.rs` — rewrite; `commands/test.rs` — add --vm flag |
| Coordinator (R) | `coordinator.rs` — scope fingerprinting, workflow graph, prefetch |
| Logging (S) | `main.rs`, `command.rs`, `coordinator.rs`, `preflight.rs`, `cargo_diagnostics.rs`, `history/db.rs` |
| Exercise seeding (T) | `commands/exercise.rs`, new `history/seed.rs`, `config.rs` |
| Snapshot (U) | `commands/snapshot.rs` |
| Code quality (V) | `history/db.rs` (split + fixes), `commands/jobs.rs`, `coordinator.rs` |
| Preferences (W) | `config.rs` |
| History query (L, G) | New `history/query.rs`; extend `history/db.rs` |
| Analytics (J) | New `commands/analytics/` |
| History commands (I) | Extend `commands/history.rs` → split into `commands/history/` |
| Test suite (F) | `xtask/tests/` |

## Post-Plan Module Structure

```
xtask/src/
  main.rs                      # Binary entrypoint (tracing init, Cli::parse)
  lib.rs                       # Dispatch, CLI enum, top-level routing
  config.rs                    # Config + UserPreferences (TOML fallback)
  output.rs                    # OutputFormat, OutputWriter, CommandResult (UNIFIED)
  command.rs                   # XtaskCommand trait, CommandContext, CommandMetadata
  coordinator.rs               # JobCoordinator, WorkflowGraph
  scope.rs                     # [NEW] PackageScope — shared by check/fix/build/test
  affected.rs                  # Git diff → affected packages
  preflight.rs                 # Auto-start infra, schema apply, preflight checks
  process.rs                   # ProcessBuilder
  cargo_diagnostics.rs         # Cargo JSON output parsing + timeout
  tools.rs                     # External tool detection

  history/
    mod.rs                     # HistoryDb struct, open/init
    invocations.rs             # [split] Invocation CRUD
    jobs.rs                    # [split] Background job DB methods
    diagnostics.rs             # [split] Diagnostic recording + querying
    test_results.rs            # [split] Test result recording + querying
    types.rs                   # All row types
    query.rs                   # [NEW] HistoryQuery<T> builders (L1-L4)
    seed.rs                    # [NEW] HistorySeedCatalog
    tracing_layer.rs           # [NEW] HistoryTracingLayer, CURRENT_INVOCATION_ID, writer_loop (S8)

  commands/
    check.rs
    build.rs
    test.rs
    fix.rs
    run.rs                     # includes run core (was run stack)
    infra.rs                   # start, stop, status, logs only
    reset.rs                   # [NEW] xtask reset
    status.rs                  # summary + watch only (--doctor removed)
    doctor.rs                  # [NEW] xtask doctor (extracted from status)
    snapshot.rs                # enhanced with U1-U5 flags
    exercise/
      mod.rs
      tier1.rs
      tier2.rs
      tier3.rs
      tier4.rs
    jobs.rs
    deps.rs
    docs.rs
    contracts.rs               # contracts info only (rest dissolved)
    ci.rs                      # hidden: check-ready, compat
    completions.rs             # hidden
    vm.rs                      # [rewritten] NixOS VM tests
    work.rs                    # [NEW] xtask work <target> — workflow sequencer (R2)
    history/
      mod.rs                   # routing
      list.rs                  # list, stats, prune
      diagnostics.rs           # diagnostics subcommand
      tests.rs                 # tests subcommand
      views.rs                 # [NEW] view, stages, diff, timeline
      sessions.rs              # [NEW] sessions, invocation
      query.rs                 # [NEW] query, schema, shell
      trace.rs                 # [NEW] trace event queries (S8)
    analytics/
      mod.rs                   # [NEW]
      workspace.rs             # workspace-health, hotspots, reliability, velocity, recommend

  sandbox/                     # existing, unchanged
  tls/                         # library functions only (no CLI)
  bench/, nextest/, graph/     # existing
```

---

## Reusable Infrastructure

| What | Where | Used by |
|------|-------|---------|
| `PackageScope` | `scope.rs` (new) | check, build, fix, test |
| `BackgroundCapable` trait | `command.rs` (new) | All --bg commands |
| `ctx.print(msg)` helper | `command.rs` | All human-output commands |
| `ctx.status_message()` | `command.rs` | preflight, coordinator (essential UX) |
| `HistoryQuery<T>` base | `history/query.rs` | L1-L4 builders |
| `HistorySeedCatalog` | `history/seed.rs` | exercise --seed, xtask history seed |
| `get_diagnostic_delta()` | `history/diagnostics.rs` | G1, I4, I5 |
| `get_stage_timings_for_invocation()` | `history/invocations.rs` | C3, G2, I4, I5 |
| `get_diagnostic_lifecycle()` | `history/diagnostics.rs` | I3, J2, J5 |
| `get_invocation_timeline()` | `history/invocations.rs` | I4 |
| `get_working_sessions()` | `history/invocations.rs` | I6 |
| `WorkflowGraph` | `coordinator.rs` | R2, xtask work |
| `scoped_tree_fingerprint()` | `coordinator.rs` | R1 |
| `HistoryTracingLayer` | `history/tracing_layer.rs` | S8 (trace persistence) |
| `UserPreferences` | `config.rs` | W1, R3, W3 |
| `row_to_background_job()` | `history/jobs.rs` | V1-J1 |
| `LATEST_PER_PACKAGE_CTE` | `history/diagnostics.rs` | V1-J7 |

---

## Codebase Implementation Notes

> Verified facts from codebase research. Use these to avoid false assumptions during implementation.

### `history/db.rs` — Current State (~1900 lines)
- **No `StageTiming` struct** — `stage_timings` table exists and is fully populated, but there
  is zero read path (no struct, no query methods). All G2/C3/I4/I5 work on stage data is greenfield.
- **No `ensure_*` migration functions** — schema is initialized solely in `init_schema()` via
  `CREATE TABLE IF NOT EXISTS`. For new columns: use `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.
- `InvocationStatus::from_str` is **private** (`fn`, not `pub fn`) — V1-A2's `tracing::warn!()`
  instrumentation applies to the private function body only.
- `BackgroundJob` row mapping is copy-pasted 3× manually — no `#[derive(Serialize)]`.
- `latest_per_package` CTE is duplicated between `get_current_diagnostics()` and
  `get_current_diagnostic_counts()` — extract as `LATEST_PER_PACKAGE_CTE` const (V1-J7).
- `DiagnosticCounts` struct fields: `errors: usize, warnings: usize` — no `fixable` field yet
  (add for G6/H1/H2 use cases).
- `record_diagnostic()` has 13 positional parameters — V1-A1 reduces to `&CompilerDiagnostic`.

### `commands/check.rs` — Affected Flag Details
- `affected: bool` uses `default_value_t = true, ArgAction::Set` (survives CLI parse as bool)
- Field is named `packages: Vec<String>` (plural) — unlike `build.rs`, `fix.rs`, `test.rs` which use `package` (singular)
- Background serialization: `if this.all { "--all" } else if !this.affected { "--affected=false" }`
- Calls `coordinator::coordinate_and_spawn("check", &args, ctx)` ✓

### `commands/fix.rs` — Inverted Background Serialization Bug
- Background serialization currently: `if self.affected { args.push("--affected") }` — emits
  flag when affected is true, which is the opposite of check/build behavior. This is a latent bug
  where re-spawned fix processes receive `--affected` unnecessarily. B1 cleanup also fixes this.
- Uses `ctx.spawn_background("fix", &args)` directly — does NOT use `coordinate_and_spawn`. A3 wires it through.

### `commands/test.rs`
- `package: Option<Vec<String>>` — A1 changes to `Vec<String>`
- Background serialization does **not** serialize `--affected` at all — B1 adds `if self.all { "--all" }`

### `coordinator.rs` — Key Facts
- `coordinate_and_spawn(command: &str, args: &[String], ctx: &CommandContext)` — confirmed signature
- `tree_fingerprint()` uses whole-workspace `git status --porcelain` SHA256 (not package-scoped)
- Zero unit tests for the critical 6-outcome decision logic — V3-H1 addresses this
- `scope_key()` includes `-p`/`--package`/`--all` but excludes lint flags — correct

### `commands/run.rs`
- `RunCommand` has `pub bg: bool` with `#[arg(long, global = true)]` — the A2 shadowing issue
- `RunSubcommand::Stack { instance_id }` routes via `run_bundle(["ingestd", "gateway"], ...)`
  — P1 renames `Stack` → `Core` here
- 5 `Command::new("cargo")` sites — V3-E1 extracts `fn cargo_run_command()`

### `tls/mod.rs`
- `TlsCommand` variants: `GenerateDevCerts` (hidden=true), `GenerateClientCert`, `GenerateCa`
- `pub fn run(cmd: TlsCommand, _json: bool)` — dead `_json` param (second A6 site)
- Library code is in `tls/generate.rs` and `tls/verify.rs` (separate from the CLI in `tls/mod.rs`)
- **CLAUDE.md documents `xtask xtr tls check` and `xtask xtr tls setup-env` but neither exists**

### `commands/db.rs`
- Only 4 subcommands: `Status`, `Apply`, `Setup`, `Reset`
- **No `Migrate` subcommand** — the original B7 list was wrong to include it

### `command.rs` — `CommandContext`
- `CommandContext::new(writer, _json: bool, background, invocation_id)` — `_json` not stored
- `CommandMetadata.category: Option<String>` — A5 also changes to `Option<&'static str>`
- `StageHandle` struct exists but NO `completed_stages: RefCell<Vec<...>>` field yet — C5 adds it

### `commands/ci.rs`
- **Already exists** at `xtask/src/commands/ci.rs` as a subcommand under xtr
- B2 removes the `xtr.rs` wrapper; ci.rs itself needs no rewrite, just re-routing in `lib.rs`

### `config.rs`
- Entry point: `Config::from_env()` (static `LazyLock` singleton) — never `Config::load()`
- `history_db_path()` = `state_dir.join("xtask-history.db")` — no env override today
- T5 adds `XTASK_HISTORY_DB` override here

### `commands/snapshot.rs`
- Uses `Command::new("repomix")` synchronously via `.output()` (blocking wait)
- Default output filename: `context.xml` (not `context.md`)
- `SnapshotCommand` fields: `output`, `include`, `exclude`, `compress`, `remove_comments`

### `tests/e2e/nixos-vm/run-vm-tests.sh`
- 11 test scenarios across 4 categories: smoke (5), integration (3), performance (2), chaos (1 empty)
- `CHAOS_TESTS=()` is intentionally empty — disabled pending new failure-injection harness
- Q2's Rust rewrite should preserve CHAOS_TESTS as an empty stub category

### `crate/cli/` (sinexctl)
- No `tls` subcommand — E2 adds `sinexctl tls generate-ca` and `sinexctl tls generate-client-cert`
- Pattern for non-gateway commands (E2 should follow): early-exit before creating gateway client
  — see `db.rs`, `config.rs`, `completions.rs` in sinexctl for examples
