# Target-Vision Claim Ledger

Target-vision material is input evidence, not implementation truth. This
ledger process records which ideas have been promoted, which are still raw,
and which have been superseded so future sessions do not re-import stale
greenfield assumptions as current architecture.

This is deliberately lightweight. GitHub remains the implementation tracker;
the ledger only records claim state, evidence, and promotion gates.

## When To Add A Claim

Add a ledger row when a target-vision-derived idea is:

- likely to influence architecture, issue scope, naming, or proof obligations;
- attractive enough that future agents may rediscover and over-promote it;
- referenced by more than one issue, report, or source document;
- superseded by current implementation but still present in raw/reference prose.

Do not add every raw note bullet. Raw source files can stay raw unless they
start steering work.

## Claim Statuses

| Status | Meaning |
|---|---|
| `raw_idea` | Captured in raw/report material; not yet synthesized into a design claim. |
| `design_candidate` | Plausible current design idea, but not issue-ready. |
| `semantic_debt` | Valuable idea, but its concepts are not yet cleanly layered or proven. |
| `issue_backed` | GitHub issue exists and is the implementation/planning authority. |
| `implementation_ready` | Issue has enough scope, invariants, and verification to start. |
| `implemented` | Code/docs landed, but verification or closure evidence is incomplete. |
| `verified` | Implementation evidence exists and closure/proof obligations are recorded. |
| `superseded` | Replaced by a newer design, implementation, or issue family. |
| `rejected` | Deliberately not pursuing; rationale is recorded. |

## Ledger Row Format

Use this compact table shape in a target-vision claim ledger file, issue
comment, or design doc:

```markdown
| Claim ID | Claim | Status | Evidence source | Authority | Proof / promotion gate | Notes |
|---|---|---|---|---|---|---|
| TV-### | Short claim text | issue_backed | target-vision/reference/foo.md | #123 | xtask test ... | Supersedes raw/bar.md wording |
```

Fields:

- `Claim ID`: stable local ID, usually `TV-###`.
- `Claim`: one sentence; no full issue body duplication.
- `Status`: one of the statuses above.
- `Evidence source`: raw/report/reference file or issue comment where the idea came from.
- `Authority`: current source of truth: issue, PR, generated catalog, code path, or doc.
- `Proof / promotion gate`: what must be true before promotion to the next status.
- `Notes`: supersession, non-goals, or semantic-debt warning.

If a claim becomes implementation work, create or update a GitHub issue and
link the ledger row to it. Do not paste the full target-vision prose into the
issue; summarize the claim and link the evidence source.

## Promotion Gates

Promotion is a state change with evidence:

| From | To | Gate |
|---|---|---|
| `raw_idea` | `design_candidate` | A maintainer or agent synthesizes the claim into one sentence and identifies non-goals. |
| `design_candidate` | `issue_backed` | A GitHub issue records scope, authority, acceptance criteria or decision question, and source links. |
| `issue_backed` | `implementation_ready` | Blocking design questions are answered; verification plan is concrete. |
| `implementation_ready` | `implemented` | PR/commit lands the scoped change. |
| `implemented` | `verified` | Verification commands or generated proof artifacts are recorded in the issue/PR closure trail. |
| any active status | `semantic_debt` | The idea is useful but mixes layers or depends on unproven substrate. |
| any active status | `superseded` | Newer code, docs, or issues replace the claim. |
| any active status | `rejected` | Rationale says why not. |

## Seed Claims

These rows are the initial authority for recurring target-vision claims.

| Claim ID | Claim | Status | Evidence source | Authority | Proof / promotion gate | Notes |
|---|---|---|---|---|---|---|
| TV-001 | Bus-first/source-worker staged interpretation is the current ingestion spine; old per-source ingestor prose is historical unless explicitly re-promoted. | verified | `/realm/project/sinex-target-vision/reference/historical-architecture.md`; source-worker execution issues | #1054, #1126, #1223, #1225; `crate/core/sinex-source-worker/` | Production-path source-worker tests and source-unit descriptors stay green. | Supersedes raw/reference wording that assumes one long-lived ingestor crate per source family. |
| TV-002 | Event source/type naming must be treated as semantic taxonomy, not ad hoc strings. | issue_backed | `/realm/project/sinex-target-vision/reference/design-intent.md`; event taxonomy work | `docs/design/event-taxonomy-v2.md`; #744 | Promote only through taxonomy docs, schema registry updates, and call-site verification. | Avoid one-off renames without replay/compatibility reasoning. |
| TV-003 | Staged export parsers are a family under the staged-source parser substrate, not isolated import scripts. | issue_backed | `/realm/project/sinex-target-vision/report/work-ahead.md`; parser backlog | #1070, #1054, `docs/architecture/staged-source-parser-substrate.md` | Each parser needs a source-unit descriptor, occurrence identity, privacy tier, and parser/proof tests. | Parser children should cite #1070 but close on their own evidence. |
| TV-004 | Thick knowledge graph ambitions must stay behind a thinner semantic assertion and provenance substrate. | semantic_debt | `/realm/project/sinex-target-vision/reference/knowledge-graph.md`; `/realm/project/sinex-target-vision/report/intelligence.md` | #1087, #1339 | Activate entity/relation automata with observed events and proof before expanding graph ownership. | Prevents horizon KG prose from overriding current event-native authority boundaries. |
| TV-005 | Broad embeddings are not authoritative until document/chunk evaluation and recorded model-effect replay policy exist. | semantic_debt | `/realm/project/sinex-target-vision/reference/embedding-pipeline.md` | #1076, #1063 | Require recorded model effects, cache/replay policy, and retrieval evaluation before promotion. | Keeps embeddings as derived evidence, not source truth. |
| TV-006 | MCP/agent loops are downstream of read-only/query surfaces and authority boundaries. | semantic_debt | `/realm/project/sinex-target-vision/reference/prescriptive-ideas.md` | #1105, #1086 | Read-only context/query server and proposal/finalizer authority must exist before actuator-like loops. | Do not treat agent-loop prose as runtime control-plane design. |
| TV-007 | Active inference and actuators require explicit instruction/observation event design first. | semantic_debt | `/realm/project/sinex-target-vision/raw/main-spine.md` | #1104 | Instruction events, observation events, safety boundaries, and replay semantics documented. | Horizon material only. |
| TV-008 | Event-native domains and KG ownership are separate concerns until reducers and authority surfaces are defined. | semantic_debt | `/realm/project/sinex-target-vision/reference/design-rationale.md` | #1120, #1206 | Current-state reducers and one-authority-per-concern audit complete. | Avoid duplicate "truth" between domain reducers and graph entities. |
| TV-009 | Living documents are not yet a primitive; artifact/workspace/substrate boundaries must be clarified first. | semantic_debt | `/realm/project/sinex-target-vision/report/vision.md` | #356, #1102 | Define notes/artifacts/typed records and graph boundaries before implementation. | Prevents old living-document prose from becoming implicit product scope. |

## Where New Claims Go

Use this order:

1. If the claim is only useful inside one issue, add a compact ledger row in
   that issue comment.
2. If the claim crosses issues or repeats across sessions, add it to this
   document.
3. If target-vision itself needs a richer local ledger later, create a
   `claim-ledger.md` there and link it from this document. Keep GitHub issues
   as the implementation authority.

## Avoiding Duplication

Issue bodies should contain scope, acceptance criteria, and verification.
Ledger rows should contain state and evidence pointers. If both need the same
paragraph, the paragraph belongs in a design doc and both should link to it.

When closing or superseding a claim-backed issue, update the ledger row or
leave an issue comment with:

- final status;
- commit/PR evidence;
- verification command;
- superseded/rejected rationale when applicable.
