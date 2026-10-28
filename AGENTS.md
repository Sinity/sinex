# Sinex

Sinex is a personal event substrate: it captures everything a person's
digital life produces (shell and terminal history, window focus, D-Bus,
input devices, browser history, mail, chat exports, media, audio, files, and
the records of other programs such as Polylogue), keeps the exact source
bytes forever, and reads them into events that can be queried, derived from
and cited. Above the captured events sit stored derived events at every level
(sessions, episodes, figures, answers, findings, context packs), each with
exact parents, so every result can be traced to the bytes it came from.
Agents are primary users and get the same powers as the operator.

This file explains how Sinex is designed and how work is done in this
repository. Claude Code, Codex and any other agent read it; it holds the
working rules that apply here.

## The design of record

- [`docs/`](docs/README.md) is the design of record: each area page owns its
  behaviour, model, formats and laws, and [docs/architecture.md](docs/architecture.md)
  is the orientation across them. The docs describe Sinex as designed, not
  as built; when code and design differ, the design is the target and
  closing the difference is the work.
- What exists is shown by the generated pages in
  [docs/reference/](docs/reference/README.md) and by each module's `//!`
  documentation, which describes the module as it is.
- Plans, open questions, decisions and acceptance criteria live in Beads, the
  task tracker, outside this repository. A Bead points at the doc section it
  builds; docs never point at Beads.

Read the owning page before changing an area. When the page is silent, decide
from the principles below, record the decision on the Bead and in the PR
description, and add the behaviour to the page in the same change.

## The core model

[docs/architecture.md](docs/architecture.md) owns the overview; these are the
concepts an agent needs before opening it.

- **Raw bytes are truth; events are readings.** A material is stored,
  source-shaped bytes; a record is a datapoint inside it; a reading is the
  event one read of a record produced, and at most one reading of a record is
  live. See [materials](docs/materials.md) and [events](docs/events.md).
- **Everything enters by being staged**, history and live input alike, and
  live streams are saved as material as they arrive. See
  [materials](docs/materials.md#staging) and [sources](docs/sources.md).
- **Events are hierarchical.** Derived events at every level are stored and
  citable, with exact parents, computed when their inputs are admitted. See
  [derivations](docs/derivations.md).
- **Deletion is a lifecycle**: live, archived (reversible), tombstoned
  (irreversible); whole chains move together and every act is an operation
  with its actor. See [lifecycle](docs/lifecycle.md).
- **One admission path; NATS is integral.** Writes inside the daemon go to the
  one admission function; writes from outside come through the NATS ingress,
  and every committed event is published in commit order on the confirmed
  stream. See [admission](docs/admission.md) and [producers](docs/producers.md).
- **Privacy is configured by the operator and applied at output**, identically
  on every surface; it never limits capture. See [privacy](docs/privacy.md).
- **One operation table drives every surface** (CLI, socket, HTTP, MCP). See
  [interfaces](docs/interfaces.md).

## Repository layout

| Path | Holds |
|---|---|
| `crates/sinex-macros` | the `EventPayload` and `DbCheck` derives |
| `crates/sinex-model` | identifiers, the three clocks, record locations and keys, kinds as declarations with field hints, typed payloads, events and batches. No I/O |
| `crates/sinex-protocol` | the command table and the producer protocol. No I/O |
| `crates/sinex-transport` | the NATS ingress, the confirmed stream, the one producer client (to be split out; this code is in `sinexd` and the producers today) |
| `crates/sinex-schema` | the schema declared in Rust, generating fresh DDL and structural upgrades, plus named data steps; `sinex-schema apply` |
| `crates/sinex-store` | admission, the lifecycle, liveness, search, privacy classification and output actions, the reader access boundary, query compilation. Knows no source by name |
| `crates/sinex-source-core` | the parser trait, drain, the segment spool driver and shared drivers |
| `crates/sinex-sources` | one declaration per source, its parser, and the live capture loop |
| `crates/sinex-sealing` | field and record sealing as a rule action |
| `crates/sinex-derivations` | derivations at every level as pure code, association rules as data |
| `crates/sinex-analysis` | statistical methods over derived events as pure code |
| `crates/sinex-library` | the media library |
| `crates/sinexd` | the daemon: writer, admission, binding reader, publisher, replay, derivation runner, API, reader web, status |
| `crates/sinexctl`, `crates/sinex-diagnostics` | the CLI and MCP server; diagnostics that work while the daemon is down |
| `crates/sinex-session`, `crates/sinex-capture` | the session producer and the capture-only node, outside producers over the NATS ingress |
| `xtask/` | developer commands, built on `devkit` from polylogue-sinex-common |
| `nix/` | the NixOS module, the Home Manager module for session capture, and the VM tests |
| `integrations/` | integrations shipped for other programs (for example the Kitty watcher) |

`cargo xtask boundaries` enforces which crates may depend on which. Source
and derivation options in `nix/catalog.json` are generated by `sinexd admin
catalog` from the source declarations; a test fails when the file is stale.
The shared crates come from
[polylogue-sinex-common](https://github.com/Sinity/polylogue-sinex-common) at
a pinned tag (its README lists them); code is shared only where the mechanics
are identical.

## Building, testing and measuring

Enter the development shell with `nix develop`. It carries the Rust
toolchain pinned in `rust-toolchain.toml`, nextest, PostgreSQL with
TimescaleDB, `nats-server` and the mold linker. Point git at the repository's
hooks once with `git config core.hooksPath .githooks`.

| Command | Does |
|---|---|
| `cargo xtask check` | formatting, boundaries and clippy against the schema database |
| `cargo xtask compile -p <crate> [--tests]` | compile selected packages, preparing the schema database when needed |
| `cargo xtask test -p <crate> -E 'test(<name>)'` | selected Rust tests under nextest |
| `cargo xtask test` | every Rust test |
| `cargo xtask changed` | the nextest filter for packages changed since the merge base, and their dependents |
| `cargo xtask db ensure` | build the database for the current schema and print its `DATABASE_URL`; `db exec` runs a command with it set |
| `cargo xtask bench ...` | backfill and derivation on a fixed sample of real inputs |
| `cargo xtask status`, `cargo xtask history ...` | what failed in this checkout's last run, and the recorded run history |
| `nix build .#checks.x86_64-linux.vm-stack` | one NixOS VM test (`vm-stack`, `vm-topology`, `vm-restore`) |
| `nix flake check` | the locked package plus Nix checks, including the VM tests |
| `nix build` | the packages |

Package selection limits the build; a name filter alone still builds the
workspace. Every xtask run is recorded in devkit's history database.
Fuzzing runs as tests: bounded corpus replay inside `test`, longer runs as a
test profile over the same harness. The design adds `cargo xtask coverage`,
`vm`, `docs` (renders [docs/reference/](docs/reference/README.md)) and `laws`
(lists each law key and the tests named after it); they do not exist yet.

**SQL is checked against a real schema.** The store's queries are SQLx
checked queries compiled against a live database built from the current
schema ([the test database](docs/testing.md#the-test-database)). `cargo
xtask db ensure` builds it; xtask commands set `DATABASE_URL` themselves.
Devkit runs PostgreSQL over a socket: a per-user service when one is running,
otherwise a throwaway cluster. There is no offline query cache; that is the operator's choice
(2026-01-29 to 2026-05-27), with room for a better-designed one if a real need
appears (2026-10-18,
`cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:08497767-3da2-4255-ad19-a28cf92936dd`).

A host may queue these same commands through its own job runner; the
operator's host does so with AgentCTL, whose descriptor in `.agentctl/`
carries only host tuning. Use it where your environment provides it; the
commands and their results are the same either way.

The pre-push hook (`.githooks/pre-push`, with its own test) runs `cargo fmt
--check` and `cargo xtask boundaries` when Rust inputs changed. There is no
hosted CI for now; `nix flake check` and `cargo xtask test` are written so it
can be added.

**Performance is a product goal.** The pipeline overlaps every stage:
acquisition, parsing on blocking workers, one writer that takes one
classification snapshot and makes one binary COPY and one commit per batch,
bulk transactions bounded by duration with live batches served between them,
and consumers with their own positions. Measure with paired `cargo xtask
bench` runs and report hardware-independent figures (bytes and WAL per
event, syncs per group, stage utilisation) alongside the absolute rates. No
test asserts elapsed time. Designs target the data and the guarantees, not
one machine; host tuning belongs in deployment configuration.

## How work is done here

**Design first.** Before writing code, read the owning page in `docs/`, the
pages it links to, the current code of the area, and the
predecessor's implementation and tests for what you are building (the
earlier Sinex is in this repository's Git history, and its old Beads are its
plans). Write the whole change from the design: complete features, built on
the architecture's abstractions, with the predecessor's behaviour and test
cases ported.

**Edit in bulk.** Change whole modules, crates and call sites in one pass;
verify once a coherent change is complete, not after each small edit.

**Review your own work before verifying it.** Walk through every changed file
and function and narrate, in your notes, what it does, checked against the
task, the owning page, the architecture and the predecessor's behaviour.
Look for everything that is wrong at once: a requirement not met, a mechanism
duplicated, a predecessor left in place, a caller not converted, a limit that
refuses valid input, a test that cannot fail, a name that does not say what
the thing is, a case the design covers and the code does not. Fix all of it,
narrate again, and repeat until a full pass finds nothing. Then verify. The
author makes the work good; a separate reviewer is not the mechanism for
that. "Review should not look for a single wrong thing, it should strive to
list everything wrong. so that there is no forever loop of fixing and finding
new issue" (operator, 2026-09-29, `cc:9ec284a7-6a6a-486b-949f-d8bbdb22ae1e:16cb3a25-318f-455b-82b5-bf0f6f732e30`).

**Verify scoped to the change.** Commit first, so a result belongs to a
commit. Run formatting, clippy for the crates you touched and the focused
tests for the code you changed. The full `cargo xtask check` and `cargo xtask
test` run once, on the exact tree that lands on master. Before treating a
failure as yours, compare with the same test on current master.

**Finish the area.** A task owns an area to a written done condition.
Problems found inside the area are fixed in the same work; a large problem is
a set of smaller ones, done in turn. Things outside the area go into the
report or a Bead with what was found, not into the change.

**Git and pull requests.** Work on a branch; commit with plain messages whose
body says why. Stage explicit paths. Rebase onto `origin/master` before the
final verification and again before handing off; resolve conflicts by taking
master's names and structure and reapplying your change. Schema upgrade
numbers are assigned when a change lands, in landing order. Prefer a few
large, coherent PRs, usually one per area. Whoever lands runs the full check
and test on the exact landing tree and merges with
`gh pr merge --squash --match-head-commit <sha>`. Hooks stay enabled.

**PR descriptions** are plain prose: what changed and why, the design
decisions taken and their reasons, what was verified (commands and run IDs)
and what was not, and before and after figures for anything performance
related.

**Decide technical details yourself.** Low-level design choices are yours,
made from the principles here and the docs, and recorded on the
Bead and in the PR. "IDK how these are for me, I do not know ~anything about
these details" (operator, 2026-09-29,
`cc:9ec284a7-6a6a-486b-949f-d8bbdb22ae1e:b6a95d83-9b7f-4478-a74e-a0a3d3b46683`).
The operator decides product scope and intent, what is captured, privacy
rules taking effect, money (paid APIs, model calls), irreversible actions on
real data (tombstoning, deleting materials), and anything published outward.
When one of those is open, take the conservative option, keep working on the
rest, and put the question in the report and on the Bead.

**A request to check is read-only.** When asked to inspect, analyse or
explain, report and recommend; change only what was asked for.

## Engineering values

These are the operator's standing positions, explained so they can be applied
to cases they do not name. References in the form `cc:<session>:<message>`
locate the operator's words in his session history.

**The architecture is deliberate; accreted mechanisms are not.** The core
(one admission, bytes as truth, NATS as the outside log, schema in Rust,
checked SQL) was designed on purpose and repeatedly re-examined. "I'm almost
certain NATS is integral to the architecture. removing it was proposed by
agents a bunch of times, but every time IIRC, it turend out upon closer
inspection that this would break stuff. again, sinex architecture is quite
deliberate. the crappy "mechanisms" that accrued are not." (2026-10-18,
`cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:be2768ea-de74-48a0-8c8d-26a318191069`).
Understand why a core mechanism exists before changing it.

**Invariants instead of repair.** Code does not produce broken state, so
there is nothing to repair. There are no repair, reconcile, verify-and-fix or
drift-checking commands, no dead-letter stores and no scheduled checks. A
crash leaves only states the ordinary open path treats as normal: an
uncommitted segment tail, an unpunched tombstoned extent, a consumer behind
its saved position, a source position behind its committed rows. When
something is found broken, fix the code that produced it. "We want to
engineer things well, to maintain appropiate invariants, which make "break
glass" solutions redundant because the only way these would be necessary is
a striaght-up bug, which if it happens we could just fix" (2026-07-19,
`cc:af12164b-a2fc-42cb-a548-22277c0875a2:60fc6414-24ab-4628-81dc-66aae9282d5d`).

**One mechanism per concept; a replacement removes its predecessor.** When
something is replaced, the old version, its callers, docs, tests and
everything that only served it go in the same change. There are no
compatibility shims, aliases or fallbacks; a breaking change fixes every
caller. Sinex holds live data, so its stored data evolves by numbered,
data-preserving upgrades generated from the one schema description, with data
transformations as named steps; that is how stored data moves forward, not a
second path. Outside formats (vendor exports of any age) are user data and
stay supported. "No compatibility shims means things should be done right,
not that there shoudl be no compatiblity and things coudl break" (2026-09-28,
`cc:9ec284a7-6a6a-486b-949f-d8bbdb22ae1e:e645e5ee-d29a-402e-bd4a-a58c32ad0c88`).

**Full features, built on a whole architecture.** Build the complete feature
the design describes, on abstractions that make the next features cheap,
rather than a bare-bones version to be replaced later. "Why build some sort of
bare-bones features which will need to be replaced by actual full-fledged
ones anyway?" (2026-10-27,
`cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:3c3dd59f-4560-4fee-95d4-5c43b6c94ca5`).
Design thoroughly, including areas that already nominally work.

**Use the predecessor's knowledge, not its mechanisms.** The earlier Sinex
built much of the operator's design (the lifecycle, field-level privacy,
staging) under layers of control planes and repair. Port its parsers' format
knowledge, test cases and designs; leave behind its replay control plane,
cascade temp-table SQL and integrity finders, material assembly over NATS for
local data, dead-letter and retry tools, checkpoints in NATS KV, dual
delivery paths, leader election and schema drift checkers. "Don't make the
opposite mistake of mindlessly porting things over just becasue they were in
the old codenbase either -- to not port over all these terrible mechanisms,
corrections on top of corrections to broken code" (2026-10-27,
`cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:3f159a64-8c63-4851-850b-0db265a6998f`).

**Sources read external formats.** A source reads data as the outside service
or program produces it, not a format someone invented along the way. Window
managers and terminals are ordinary sources behind interfaces that make
adding others clean. Per-user facts (account names) are configuration;
vendor facts (the zone a service writes its timestamps in) belong to the
source's declaration.

**Small codebase, plain names.** Bulk is the main enemy: the predecessor grew
past half a million lines through kludges on kludges. Prefer one deep
abstraction to several shallow ones, clean interfaces, and no repeated logic.
A name says what the thing is in ordinary words, everywhere: types,
functions, local variables, SQL, wire keys, errors, comments, tests and docs.
When an opaque term is found, replace it throughout the codebase in one
pass. "The jargon is not supposed to be anywhere, period. including stuff
like local variable names etc." (2026-10-24,
`cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:e52f56c3-a9ec-45d8-b719-6b881ee22367`).

**Good design instead of guard machinery.** Quality comes from the
architecture and from the author's own review, not from build-enforced bans,
line budgets, word checks or required PR fields. "These sorts of checks dont'
actually work." (2026-10-27,
`cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:deed5fe0-c9a3-49e3-8f27-c35efdc08301`).

**Valid input is never refused or truncated for size.** History alignment,
replay scopes, query results and transfers have no fixed caps; page sizes are
defaults and transfer units continue. Memory is bounded by streaming and
permits, not by limits on what is stored or asked. "Such arbitrary limits are
themselves straight-up bugs." (2026-09-27,
`cc:62be45f2-6c40-49ce-a88e-bc566948628c:4cc20d7c-9e62-4b7a-9550-7dd97f9d48e9`).

**Tests that are better than the predecessor's.** Every specified behaviour
and every law has a test that fails when the production path breaks. The
valuable kinds: independent oracles (goldens written from format knowledge,
GNU tar for PAX materials, PostgreSQL's own COPY reader), the lifecycle law
suite (whole-chain moves, restore refused on collision, replay undo,
tombstoned events never become parents while replay and re-import create new
readings, as-of reads), crash tests at real durability boundaries, and
property tests at serialization boundaries. Tests run against real
PostgreSQL and NATS, which devkit makes cheap; a mock that asserts its own
setup tests nothing. A failing test points at a bug to fix, not a test to
remove. "We don't want to have tests which are counterproductive or
low-value just to have more tests." (2026-10-27,
`cc:ee52794c-8a9e-43b6-9fbc-9749c7e78e8f:bbb105d6-16f1-418e-b04a-0c5f105832df`).

**A fast development loop.** Code quality comes first; within it, optimise
for speed: bulk edits, targeted test runs, short compile times, a small
memory footprint. When a run slows down, look at slow tests, resource waits
and cleanup before raising timeouts or cutting coverage.

## Code conventions

- Rust, four-space indent, `rustfmt`.
- Tests live in their own files: a module's unit tests in `<module>/tests.rs`
  declared with `#[cfg(test)] mod tests;`, integration tests under the
  crate's `tests/`. A test that checks a law is named after the law's key
  ([docs/testing.md](docs/testing.md#laws-and-their-tests)). A test that needs PostgreSQL calls
  `devkit::pg::test_database()` for its own copy of the schema template, so
  tests run in parallel. CLI and MCP behaviour is tested through the public
  commands, not by widening visibility.
- Comments state constraints and reasons, briefly. History, status, Bead IDs
  and design section numbers belong in Git and Beads.
- Configuration is loaded once, in each binary's `main`: one typed value
  (file, then flags, then environment where meaningful), validated there (the
  zone resolved to a named zone, the day start checked, paths absolute) and
  passed explicitly with a `Clock`. Libraries never read the environment or
  the system zone, and no component has a default for a setting. The logical
  day is one `DaySetting` (named zone plus start) with one implementation,
  used by queries, SQL, derivations, analysis and rendering. Derived outputs
  record the settings they were made under.
- Unknown times stay unknown. Reading order for a key is assigned or checked
  by Sinex, never taken from a producer's clock.
- Stored source material is never changed; the lifecycle (archive, restore,
  replay, tombstone) is how anything about it changes.

## Vocabulary

`docs/vocabulary.md` lists each concept and its one name. Before naming
something, look for it there; use the listed word, or add the new concept in
the same change. Names in the code, the docs and this file agree.

## Data and privacy

The repository is public. Fixtures are synthetic and neutral. Credentials,
real captures, private reports and real databases stay out of Git, and so do
paths into a particular machine and personal account names. Reading the
operator's real data on the development machine for development and
measurement is fine; report figures, not contents.
