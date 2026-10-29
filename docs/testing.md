# Testing

How Sinex is tested: the test database, the synthetic world, the law suites
and how laws map to tests, crash, VM and fuzz tests, and benchmarks. The
commands that run them are in [AGENTS.md](../AGENTS.md#building-testing-and-measuring);
the shared test runner, history and database harness are common's
[`devkit`](https://github.com/Sinity/polylogue-sinex-common/blob/master/crates/devkit/README.md).

## What a test is worth

Every specified behaviour and every law in these pages has a test that fails
when the production path breaks. A behaviour without one is a gap, whichever
codebase it came from. Tests are judged by what they can catch:

- **Independent oracles.** Goldens written from knowledge of the source
  format, not from Sinex's own output; GNU tar reading a PAX material;
  PostgreSQL's own COPY reader decoding what the writer encoded.
- **Law suites** over real PostgreSQL and NATS (below).
- **Crash tests** at the real durability boundaries (below).
- **Property tests** at serialization boundaries: COPY encoding, manifests,
  producer frames, the query parser.

A test that only restates a generator or a helper, a test of deleted
machinery, or a SQL-substring check where a row comparison can be made has no
value and is not written. The predecessor's test cases are ported as cases,
inputs and expected outcomes checked against the format knowledge, never
re-blessed from new output. A mock that asserts its own setup tests nothing;
tests run against real PostgreSQL and NATS, which devkit makes cheap.

## The test database

The store's SQL is checked at compile time against a live database built from
the current schema; there is no offline query cache. The database is named
after a hash of the DDL the schema crate generates (tables, CHECK lists of
closed value sets, the per-kind views and the payload fields they expose)
combined with the PostgreSQL server version. A change that alters DDL
produces a new database and recompiles only the query-checking crates; a
parser, model or payload-value change that leaves DDL alone keeps the same
`DATABASE_URL`.

Each test that needs PostgreSQL gets its own copy of the schema database from
a template (`devkit::pg::test_database()`), so database tests run in
parallel. Disposable PostgreSQL clusters and NATS servers are owned by the
test run and reaped on every exit path.

## Development runs and reports

`cargo xtask run --watch --output DIR` builds the daemon and runs it with an owned PostgreSQL cluster, schema database and NATS server. The output directory must be new; it holds private tokens, runtime process/socket identities and any explicitly selected session spools. The runner never selects the host's services. Readiness requires HTTP 200 from the API. A failed rebuild leaves the previous daemon running and records the build failure; edits during a build trigger another build. `--seconds SECONDS` bounds a smoke run. Cancellation stops the owned processes; retained private output is inspected separately from disposable service data.

Sources are explicit in a development run: `--sources FILE` selects daemon bindings, and repeated `--session-source NAME` selects session captures. The generated session source file preserves configured exclusions and excludes every unselected catalog capture, including media and file watches. The watcher ignores its output directory. A checkout has one development-stack lock, released when the process exits.

`cargo xtask history explain RUN` shows resolved arguments, stages and slow tests. `history compare BEFORE AFTER` compares passed runs with matching command, exact arguments, host and toolchain; `history compare-days DATE DATE` groups UTC days by those same fields. Parallel test durations are separate from wall time. Recorded overlap does not by itself establish resource contention. `NEXTEST_TEST_THREADS` selects test concurrency unless explicit nextest options override it, and the resolved setting is recorded with the run.

The two multi-batch native interruption scenarios reserve all configured nextest slots while they exercise both histories and their replay continuations. Other crash cases share a two-slot group. These reservations change scheduling; fixture sizes and per-test deadlines stay fixed.

The daemon shutdown law reads the supervisor's actual task-join summary: every task, including a completed task not yet joined, must finish without failed joins or abort requests. It also checks process exit, writer-lock release and unchanged readings and source bytes across restart. Elapsed shutdown time is diagnostic; the production drain and test harness retain their own bounded deadlines.

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

## Laws and their tests

Every area page ends with `## Laws`. A law's full name is `<page>/<key>`
(`lifecycle/restore-refused-while-other-reading-live`); keys are plain words
and stay stable, so inserting a law renames nothing. The conventions for
laws and citations are shared with Polylogue and described in its
[docs index](https://github.com/Sinity/polylogue/blob/master/docs/README.md).

- **Tests carry the key in their name.** Integration tests are one binary per
  area (`crates/<crate>/tests/<area>/`); the test that checks a law is named
  from its key (`restore_refused_while_other_reading_live`), with a suffix
  when several tests check one law. Finding a law's tests is a name search.
- **Shared laws live with the shared code.** The query language's law suite
  is in common's
  [`query-language`](https://github.com/Sinity/polylogue-sinex-common/blob/master/docs/query-language.md)
  and runs against Sinex's catalog as well as Polylogue's;
  [query.md](query.md) lists only Sinex's own query laws.
- **Coverage is a report.** `cargo xtask laws` lists each law key with the
  tests whose names carry it, and the laws that have none. It fails nothing;
  a law without a test is a gap for the area's owner.

The lifecycle suite is the core one: whole chains move together, restore is
refused on a collision, a replay is undone by archiving its chain and
restoring the previous one, tombstoned events never become parents while
replay and re-import create new readings normally, and as-of reads return
what was live at that position ([lifecycle.md](lifecycle.md#laws)).

## Crash tests

Crash tests stop the process at the real durability boundaries and reopen
through the ordinary path, which must treat what it finds as normal
([architecture.md](architecture.md) owns the durability order and the states
a crash may leave):

- between segment append, sync, commit and truncate;
- between an archive change and the recompute of the dirty keys it causes;
- between the transactions of a replay;
- around the publisher's acknowledgement on the confirmed stream.

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
runs inside the ordinary test run; longer runs are a test profile over the
same harness, not a separate command. Ruling, 2026-03-12: "it MUST NOT be a
separate command" (cc-queued:065c511c-e336-4e59-825b-eeefc71cdaae@2026-03-12T00:31:10Z).

## Benchmarks

Throughput is a product requirement ([architecture.md](architecture.md)), so
performance is measured, never asserted: no test checks elapsed time.

- `cargo xtask bench` backfills a fixed sample of real inputs and runs the
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

## Laws

- **schema-database-follows-ddl**: two schemas with identical generated DDL
  and server version select the same test database; any DDL change selects a
  different one.
- **test-databases-isolated**: rows written by one test are never visible to
  another test running in parallel.
- **generated-reference-current**: rendering the declarations produces
  exactly the committed pages under [reference/](reference/README.md); the
  golden test fails when they differ.
- **module-catalog-current**: `sinexd admin catalog` produces exactly the
  committed `nix/catalog.json`.
