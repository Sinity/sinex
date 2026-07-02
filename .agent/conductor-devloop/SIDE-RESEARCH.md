---
created: "2026-07-03T00:06:20+02:00"
purpose: "Canonical register for side-research/subagent work in the Sinex devloop"
status: "active"
project: "sinex"
---

# Side Research Register

This file is the durable conductor-facing index for side-research/subagent work.
Raw reports live under `.agent/scratch/research/`; reconciled implementation
ordering lives in `RESEARCH-BACKLOG.md`. This register exists so a fresh agent
can answer three questions without chat context:

1. What side research exists?
2. Which outputs have already affected the backlog or code?
3. What should the next side agents investigate?

## Current Policy

- Side agents are backlog enrichers by default: read-only, no builds/tests, no
  DB/NATS/GitHub mutations, no nested agents.
- Every side report needs a research key, cited evidence, and a smallest useful
  implementation slice.
- Do not launch another broad side wave until active leases are reconciled into
  this file and `RESEARCH-BACKLOG.md`.
- Research is not self-authorizing. The conductor decides whether each output is
  `implement-now`, `queue`, `watch`, or `discard`.
- Side research should usually run while a proof/build/runtime check is already
  in flight, or when object work is blocked on evidence.

## Reconciled Waves

### 2026-07-03 Focused Recall v2 Support Wave

Raw source: current subagent returns plus the active implementation diff.

Agents:

- Gauss (`019f250c-f843-76e0-9ed8-5952a033794b`) —
  `runtime/browser-acquisition`.
- Helmholtz (`019f250d-0d3d-7ed3-83a5-418b5831efdd`) —
  `runtime/git-acquisition`.
- Nash (`019f250d-1f22-7363-8742-204999bf9a03`) —
  `demo/recall-v2-baseline`.

Status: reconciled into `RESEARCH-BACKLOG.md` on
2026-07-03T01:06:48+02:00. This wave is implementation-driving, not merely
queued.

Findings:

- Browser acquisition is the immediate blocker for the Recall v2 terminal
  proof. The parser already understands qutebrowser, Chromium/Chrome history,
  and JSONL-shaped browser rows, but the dev/Nix wiring was selecting only one
  browser sqlite source and did not map Chromium history to the required
  `visits JOIN urls` projection.
- The live Chrome input is current at
  `/home/sinity/.config/chrome-ws/Default/History`. A copied sqlite snapshot
  accepted the Chromium query and contained 49,140 visits, latest
  2026-07-02T22:15:36Z.
- The browser occurrence key must include profile/material identity as well as
  visit id; `visit_id` alone can collide across browser profiles.
- `git-commit-history` is quiet because it is effectively hosted through a
  static file/directory-style adapter: first scan can enumerate commits, but
  continuous polling has no useful repo-HEAD cursor/fingerprint and emits no
  fresh commits after the initial pass.
- The current Recall v2 artifacts are valuable workbench proof, but they are
  not yet terminal proof of fs+git+shell+browser recall. Browser is absent and
  git freshness is caveated.

Current conductor decision:

- Chrome browser acquisition moved from research recommendation to implemented
  proof on 2026-07-03: job `2001248` ran `browser.history` instance 3 against
  `/home/sinity/.config/chrome-ws/Default/History`, acquired leadership as
  `source-driver-browser.history-3`, finalized live browser-history materials,
  and persisted browser visit events. The proof is recorded in
  `OPERATING-LOG.md`.
- Keep the git quietness fix second unless the Recall v2 artifact can honestly
  narrow its claim without it.
- Do not present Recall v2 as terminal proof until a cold-reader packet shows
  browser participation, or until the packet explicitly narrows the claim.

### 2026-07-02 Sidecar Wave

Raw index: `.agent/scratch/research/2026-07-02-sidecar-wave/00-INDEX.md`

Reports:

- `01-query-algebra.md` — query DSL/algebra silo audit.
- `02-inline-test-cleanup.md` — inline-test cleanup automation/status.
- `03-side-research-orchestration.md` — leased side-agent process model.
- `04-runtime-catchup.md` — catch-up/source-material fragmentation evidence.

Status: reconciled into `RESEARCH-BACKLOG.md`.

Effects:

- Query algebra is queued as a high-leverage silo-collapse substrate slice.
- Inline-test cleanup remains a separate mechanical cleanup thread.
- Side-research leases/status became a devloop process improvement item.
- Runtime catch-up/source-material findings drove the remediation-plan,
  capture-debt, and catch-up-readiness slices.

### 2026-07-02 Sidecar Wave 2

Raw index: `.agent/scratch/research/2026-07-02-sidecar-wave-2/00-INDEX.md`

Agents:

- Chandrasekhar: query algebra / CLI/API DSL.
- Copernicus: runtime catch-up/readiness.
- Descartes: GitHub issue prioritization.
- Raman: side-agent process.

Status: reconciled into `RESEARCH-BACKLOG.md`.

Effects:

- Reinforced query algebra as the next major compositional substrate slice.
- Reinforced bounded remediation policy/readiness as the immediate runtime
  debt thread.
- Reinforced side-research leases/status as necessary before the next broad
  side wave.

## Stale / Superseded Leases

### 2026-07-02 Sidecar Wave 3

Raw index: `.agent/scratch/research/2026-07-02-sidecar-wave-3/00-INDEX.md`

Active leases recorded there:

- `runtime/shadow-list-nats-failure` — why `ops catchup status` caveats miss
  consumer backlog evidence when `shadow.list` fails with a NATS RPC error.
- `security/content-key-path-traversal-2195` — compact high-risk issue audit.

Status: stale as of 2026-07-03T00:32:27+02:00. The wave directory contains only
the launch index and no delivered reports, so these leases should not block
new side research.

Next conductor action:

- Treat both topics as queueable research keys, not active outputs.
- Relaunch either topic only if it becomes the best support lane for the active
  object slice.
- Do not wait on these lease ids before launching a focused side agent.

## Priority Order

### Main-Lane Priorities

1. Recall v2 baseline-arm demo: refresh the packet with the newly proven live
   Chrome browser participation, then repair or honestly narrow git
   participation before presenting terminal proof.
2. Bounded source-material remediation action policy if Recall v2 evidence is
   false or too caveated without it.
3. Query algebra/event-query lowering to collapse CLI flags, root query strings,
   and private request shapes into a shared compositional substrate.
4. Production runtime restore after Recall v2 reaches terminal proof.

### Side-Research Priorities

1. Browser acquisition implementation map: can existing source/parser/config
   ingest current Chrome/browser history, what smallest slice makes it true, and
   what proof demonstrates fresh browser evidence?
2. Cold-reader audit the Recall v2 demo directory: what does it prove, what is
   missing, and what exact source/window would make it stronger?
3. Git source quietness audit: explain stale `git-commit-history` output and
   rank the smallest fix for Recall v2.
4. Query algebra implementation map: smallest PR that unifies one real query
   slice without adding another flag-shaped silo.
5. xtask scope/cost audit: why focused sinexctl checks/tests compile sinexd or
   xtask, and what instrumentation/fix would reduce proof latency.
6. Runtime/source backlog audit: browser history live-output gap,
   `read_only=true` dev-run config mismatch, and source action/control wiring.
7. Inline-test cleanup automation audit: programmatic extraction plan for the
   remaining true inline `#[cfg(test)] mod tests` blocks.
8. Relaunch stale Wave 3 topics only when they support the active slice:
   `runtime/shadow-list-nats-failure` after catch-up status work resumes, and
   `security/content-key-path-traversal-2195` when a security/debt PR is chosen.

## Launch Gate For Next Broad Wave

Before launching more than one new side agent:

- `SIDE-RESEARCH.md` active leases are empty or intentionally stale.
- `RESEARCH-BACKLOG.md` current prioritization names the next object slice.
- `ACTIVE-LOOP.md` names whether side research is supporting Recall v2,
  remediation policy, query algebra, or xtask velocity.
- The side prompts forbid builds/tests/mutations unless explicitly needed.
- Each prompt asks for a short cited report and a first implementation slice,
  not a broad essay.
