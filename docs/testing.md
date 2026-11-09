# Testing

How Sinex is tested: the test database, the synthetic world, the properties tested,
crash, VM and fuzz tests, and benchmarks. The
commands that run them are in [AGENTS.md](../AGENTS.md#building-testing-and-measuring);
the shared test runner, history and database harness are common's
[`devkit`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/devkit/README.md).

## What a test is worth

Every specified behaviour in these pages has a test that fails
when the production path breaks. A behaviour without one is a gap, whichever
codebase it came from. Tests are judged by what they can catch:

- **Independent oracles.** Goldens written from knowledge of the source
  format, not from Sinex's own output; GNU tar reading a PAX material;
  PostgreSQL's own COPY reader decoding what the writer encoded.
- **Lifecycle and admission tests** over real PostgreSQL and NATS (below).
- **Crash tests** at the real durability boundaries (below).
- **Property tests** at serialization boundaries: COPY encoding, manifests,
  producer frames, the query parser.

A test that only restates a generator or a helper, a test of deleted
machinery, or a SQL-substring check where a row comparison can be made has no
value and is not written. The predecessor's test cases are ported as cases,
inputs and expected outcomes checked against the format knowledge, never
re-blessed from new output. A mock that asserts its own setup tests nothing;
tests run against real PostgreSQL and NATS, which devkit makes cheap.

A test checks behaviour a caller can observe, through an interface that stays
stable while the implementation changes, against an expected result known
independently of the code under test. Its assertions are exact: when an
expectation changes, the doc page that changed it is cited in the commit, and
a failing assertion is fixed in the code rather than loosened to pass.
Removed code is gone because it was deleted, so nothing asserts that a name,
variant, flag or format stays absent; what the system refuses or never
exposes on purpose (a secret in an error, an operation outside a grant) is
behaviour and is tested as such. Tests wait for readiness signals rather than
for elapsed time. A time budget only detects a hang: keep it far from the
behaviour under test (a fake child that must be stopped outlives the budget),
and when a run is slow, find the slow step instead of widening the budget.

## The test database

The store's SQL is checked at compile time against a live database built from
the current schema; there is no offline query cache. The database is named
after a hash of the DDL the schema crate generates (tables, CHECK lists of
closed value sets, the per-kind views and the payload fields they expose)
combined with the PostgreSQL server version. A change that alters DDL
produces a new database and recompiles only the query-checking crates. After
preparing that database, xtask supplies its content-addressed name as
`SINEX_SCHEMA_IDENTITY`; checked-query builds track that identity instead of
the server's transport address. Restarting a disposable server at another
socket therefore reuses the same checked queries. A parser, model or
payload-value change that leaves DDL alone keeps the same schema identity.

`SINEX_SCHEMA_IDENTITY` belongs to xtask's prepared-schema environment. A
manual build without it tracks `DATABASE_URL`, and entering or leaving the
managed environment also triggers a query check. SQLx still checks queries
against the live prepared database whenever the checked crate compiles.

Each test that needs PostgreSQL gets its own copy of the schema database from
a template (`devkit::pg::test_database()`), so database tests run in
parallel. Disposable PostgreSQL clusters and NATS servers are owned by the
test run and reaped on every exit path.

## Development runs and reports

`cargo xtask run --watch --output DIR` builds the daemon and runs it with an owned PostgreSQL cluster, schema database and NATS server. The output directory must be new; it holds private tokens, runtime process/socket identities and any explicitly selected session spools. The runner never selects the host's services. Readiness requires HTTP 200 from the API. A failed rebuild leaves the previous daemon running and records the build failure; edits during a build trigger another build. `--seconds SECONDS` bounds a smoke run. Cancellation stops the owned processes; retained private output is inspected separately from disposable service data.

Sources are explicit in a development run: `--sources FILE` selects daemon bindings, and repeated `--session-source NAME` selects session captures. The generated session source file preserves configured exclusions and excludes every unselected catalog capture, including media and file watches. The watcher ignores its output directory. A checkout has one development-stack lock, released when the process exits.

`cargo xtask history explain RUN` shows resolved arguments, stages and slow tests. `history compare BEFORE AFTER` compares passed runs with matching command, exact arguments, host and toolchain; `history compare-days DATE DATE` groups UTC days by those same fields. Parallel test durations are separate from wall time. Recorded overlap does not by itself establish resource contention. `NEXTEST_TEST_THREADS` selects test concurrency unless explicit nextest options override it, and the resolved setting is recorded with the run.

Crash cases share a two-slot nextest group, which bounds how many whole backfills compete for the test PostgreSQL at once while the rest of the suite runs beside them. The group changes scheduling only; per-test deadlines stay fixed.

The daemon shutdown test reads the supervisor's actual task-join summary: every task, including a completed task not yet joined, must finish without failed joins or abort requests. It also checks process exit, writer-lock release and unchanged readings and source bytes across restart. Elapsed shutdown time is diagnostic; the production drain and test harness retain their own bounded deadlines.

`cargo xtask coverage --output DIR --format html -- -p CRATE` uses a separate instrumented build. Formats are `html`, `lcov` and `json`; `coverage.json` retains LLVM data and `summary.json` records arguments, line coverage and function regions. `--threshold PERCENT` is an optional caller-selected threshold. Output directories are new; failed children, empty selections and malformed or missing reports are errors. The shared runner mechanics belong to [devkit](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/devkit/README.md).

## The synthetic world

The higher layer (episodes, presence, attention, project attribution, the
resume card; see [higher-layer.md](higher-layer.md)) is tested end to end on
one synthetic world: several projects on two hosts, with shell and terminal
reports with output, browser visits and tab focus, editor and file changes,
overlapping compositor and window-watcher records, physical input and
presence, a suspend and resume, Polylogue messages and tool calls, git
commits, task revisions, pull request, check and deployment observations,
records of unknown time, and a watcher outage. It includes:

- an episode crossing the logical-day boundary and one crossing a daylight
  saving change;
- an agent continuing in one project while the operator switches to another,
  and an agent producing focus and commits while the operator is away (the
  operator's session does not extend over it);
- a task closed before its check passes, a task resumed after five days, two
  tasks in one repository, an interruption while waiting on an agent;
- late evidence joining two provisional sessions, and another edit splitting
  one.

The journey through it, each step with its exact expected evidence:

1. Ask for the last activity in a project, or for a recorded time: the
   supported episode, its stated objective and its coverage come first.
2. Read the previous actions, files, commands and failures, pages, Polylogue
   evidence, the open task and the last verified outcome. The operator and
   background agents are told apart; an unknown next step is marked unknown.
3. Follow a claim through its exact and current citations to stored bytes;
   a page and an agent brief use the same links.
4. Replay the materials and run the derivations again; repeat with late
   evidence, archived and restored inputs, and changed rules. Exact citations
   still describe what was cited; current references and complete output sets
   reflect the revised evidence; removing all evidence of a grouping leaves no
   live grouping.
5. Produce the daily pack and the project card from one snapshot: figures
   conserve time, a window's frame is not focus, outcomes cite exact
   revisions, contradictory evidence survives a small presentation budget.
6. Repeat without the outside analysis tools the higher layer replaces, and
   compare against direct Polylogue, shell and git evidence on an equivalent
   sample; known defects of the older tools are corrected, not matched.

The world passes when every specified fact and outcome is present, none is
invented, and every claim and number resolves to its evidence. Evaluation
results are stored as ordinary answer events, so they replay and compare
across versions of the derivations.

## Properties tested

Integration tests are one binary per crate (`crates/<crate>/tests/integration/`)
with one module per area, so each crate links its test dependencies once. The
query language's shared tests live in common's
[`query-language`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/query-language/README.md)
and run against Sinex's catalog as well as Polylogue's.

These properties hold for every input; the tests named are where to look when
one changes:

- An admission batch commits whole or not at all, and commit positions are dense, in batch order and never regress (`events_and_progress_commit_together`, `commit_seq_is_dense_and_in_batch_order`, `a_second_transaction_waits_for_the_first_to_commit`).
- One writer: a second claim is refused (`only_one_connection_can_claim_the_writer`).
- A crash at every point between segment append, sync, commit and truncate leaves no committed row citing missing bytes, and an ordinary restart reaches the uninterrupted run's state (`sinexd/tests/integration/crash.rs`: `synced_uncommitted_segment_tail_is_truncated_on_restart`, `partial_segment_append_recovers_without_advancing_its_cursor`, the `stage_killed_*`, `replay_killed_*` and `publisher_killed_*` cases).
- A source cursor never passes an unadmitted record, and a refused record keeps its failure tied to its bytes (`source_content_refusals_keep_material_failures_and_advance_the_cursor`).
- Read models rebuilt from events equal the ones maintained incrementally (`search_modes_apply_to_live_indexing_and_its_rebuild`).
- Lifecycle state order holds after every operation: no live event has a non-live parent (`assert_state_order` in `sinex-store/src/lifecycle/tests.rs`).
- Archive moves whole chains; restore is refused while another reading is live; replay archives the readings it replaces and a failed replay keeps the old one (`archive_moves_whole_chains`, `restore_refused_while_other_reading_live`, `replay_archives_replaced_readings_of_a_material`, `a_later_material_read_failure_rolls_back_the_entire_replay_sequence`).
- Online schema work resumes in order after interruption, and a failing upgrade rolls back whole (`work_after_start_resumes_in_order_and_walks_answer_as_filled`, `whole_chain_rolls_back_when_its_last_upgrade_fails`).

The lifecycle suite is the core one: whole chains move together, restore is
refused on a collision, a replay is undone by archiving its chain and
restoring the previous one, tombstoned events never become parents while
replay and re-import create new readings normally, and as-of reads return
what was live at that position ([lifecycle.md](lifecycle.md)).

## Crash tests

Crash tests stop the process at the real durability boundaries and reopen
through the ordinary path, which must treat what it finds as normal
([architecture.md](architecture.md) owns the durability order and the states
a crash may leave):

- between segment append, sync, commit and truncate;
- between an archive change and the recompute of the dirty keys it causes;
- between the transactions of a replay;
- around the publisher's acknowledgement on the confirmed stream.

A kill point earns its own test only when it leaves a different state.
Every statement inside one admission transaction leaves the same one (nothing
of the transaction committed), so a single kill inside the transaction covers
them; the kill before the run is registered and the kill after the segment
sync, with bytes past the committed end, are separate tests.

## VM tests

NixOS VM tests boot the shipped modules
([operations.md](operations.md)) and drive them from outside:

| Test | Covers |
|---|---|
| `module` | module evaluation, service ownership, permissions and option wiring |
| `vm-stack` | the daemon, PostgreSQL, NATS and session capture on one machine, from enabled sources to answered queries |
| `vm-topology` | outside producers on a second machine sending over the NATS ingress |
| `vm-restore` | the backup and restore drill: a database rebuilt from a dump and the stored bytes |

Run `cargo xtask vm --output DIR`, optionally repeating `--scenario NAME`, on committed source so Nix and the run record use the same input. The runner attempts every selected scenario and preserves original build logs plus `results.json`.

The native Rust suite tests GNU tar restoration of sparse files, native names, extended attributes and access ACLs. The Nix package check excludes only that exact setter-dependent restoration test because the builder's syscall filter prevents setting xattrs and ACLs; native assertions remain enabled. Its existing peak-RSS test also runs under native nextest, which gives it the separate process required by its measurement. Package checks do not replace the native suite.

## Fuzzing

Fuzzing runs as tests over the real boundaries: COPY encoding, manifests,
producer frames and the query parser. Bounded replay of each target's corpus
runs inside the ordinary test run, and so do property tests that generate new
query inputs on every run: queries assembled from the language's words, seed
queries with bytes changed, inserted or removed, and arbitrary text. The
nightly `fuzz` workflow runs those properties on master with
`PROPTEST_CASES=200000`; longer runs are a test profile over the same
harness, not a separate command. The harness (`fuzz/`) is a workspace
member, so its tests build in the same feature resolution as every other
test. (ruling 2026-03-12, cc-queued:065c511c-e336-4e59-825b-eeefc71cdaae@2026-03-12T00:31:10Z).

## Benchmarks

Throughput is a product requirement ([architecture.md](architecture.md)), so
performance is measured, never asserted: no test checks elapsed time.

- `cargo xtask bench` stages a fixed sample of real inputs and runs the
  derivations over it, reporting input bytes, stored bytes per source,
  database and blob store size, timings, and sampled memory and load.
  Estimates for full inputs assume time grows linearly with input bytes.
- A change is measured with paired runs, before and after, on the same
  sample and machine.
- Reports carry the hardware-independent figures (bytes and WAL written per
  event, syncs per commit group, utilisation of each pipeline stage)
  alongside the absolute rates of whichever machine ran them. Targets are set
  in those figures; host tuning belongs in deployment configuration.
- Development cost is compared the same way: database setup, test
  compilation and test execution are timed separately, a run on a seeded
  build directory is compared only with another seeded run, and the first
  build after a dependency-profile change is reported apart from warm runs.
