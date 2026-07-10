# Issue Operating Model

> **SUPERSEDED (2026-07-10):** GitHub Issues are retired as the sinex task
> substrate. Beads (`bd`) is now the sole durable source of truth for task
> state — see `CLAUDE.md` and `CONTRIBUTING.md`'s "Planning and Source
> Documents" section. All 7 issues open at retirement time were closed as
> not-planned with bead cross-references; new work is filed with
> `bd create`, not a GitHub issue. This document is kept for historical
> reference (it explains the shape of already-closed issues) and because the
> closure-fabrication guard it describes (`xtask verify closure`,
> `.github/workflows/verify-closure.yml`) still operates on GitHub issues
> pending its bead-native replacement — tracked as sinex-e7e9. Do not file
> new GitHub issues against the kinds/rules below.

This document defines how the sinex GitHub issue tracker was used prior to
retirement. It is the authoritative reference for issue **kinds**, **readiness
states**, and **closure rules** for the closed-issue archive. Drift between
this doc and how issues were filed is itself a bug — fix the doc or fix the
issue.

The model exists because the tracker has accumulated mixed artifact types
(execution tickets, tracking spines, design questions, catalog overlays) under
one GitHub issue shape. Without explicit kinds and rules, composite issues
silently close with live work outstanding, design issues get re-treated as
implementation tickets, and tracking spines accumulate stale prose.

## Issue kinds

Every open issue belongs to **exactly one** of these kinds. The kind appears
near the top of the body (or in a pinned governance comment for older
issues).

| Kind | Purpose | May a PR close it? | Required closure evidence |
|---|---|---|---|
| `execution` | Concrete implementation/doc/test slice | Yes | Acceptance criteria + verification |
| `composite` | Multi-PR issue phase, multiple slices | Yes — only after closure matrix | Closure matrix with every row resolved |
| `decision` | Resolve one architectural/design question | Usually no code PR | Decision note + promoted follow-up issues |
| `tracking-spine` | Navigation/index over children | Usually no | Children closed/routed, or spine intentionally remains open |
| `catalog-spec` | Names/taxonomy/schema/source truth | No, unless converted to a generated artifact | Generated source of truth or doc migration |

The single most consequential rule: **PRs do not close composite issues
without a closure matrix**. See "Composite closure" below.

## Readiness states

Independently of kind, every issue has a readiness state:

| State | Meaning |
|---|---|
| `ready-now` | All inputs known; an implementer can start without further design |
| `hybrid` | Body is mostly specced but one or two design questions remain in comments |
| `needs-decision` | One or more architectural choices block implementation |
| `blocked` | Waiting on a specific other issue to land |
| `tracking-only` | This is a navigation surface; not directly implementable |

Readiness can change. When it does, leave a comment noting the change and
why; readiness shifts are part of the durable issue record.

## Authority

Where does the current truth live for this issue?

| Authority | Meaning |
|---|---|
| `body` | The issue body is current and load-bearing |
| `comment N` | Comment N supersedes the body for current decisions |
| `source doc` | A scratch file or `docs/` file is the authority |
| `generated catalog` | A generated artifact is the consumable projection of code/schema truth |
| `code` | The current code state is the authority — issue prose is descriptive only |

If the body and a comment conflict, the comment wins by default unless the
body has been explicitly refreshed after the comment was written. Issue
templates require an "Authority" field on issue creation.

## Composite closure (the load-bearing rule)

Composite issues — those that bundle multiple PRs' worth of work, or those
that aggregate items from a comment thread — must carry a **closure matrix**
before they can be closed.

```
| Item | Source | Status | Evidence | Follow-up |
|---|---|---|---|---|
| F1 some specific thing | body | satisfied | PR #X + verification command | — |
| F2 another thing | body | routed | — | #Y |
| C5 thing from comment 5 | comment | deferred | reason | future-issue |
| F3 outdated thing | body | stale | superseded by Z | — |
```

**Status values**:

| Value | Meaning |
|---|---|
| `satisfied` | Done, with evidence |
| `routed` | Promoted to a named follow-up issue |
| `deferred` | Explicitly out of scope; rationale recorded |
| `stale` | No longer applicable; supersedence noted |

**Close rule**: a composite issue may not be closed while any matrix row is
blank or in `not-started` state. Rows must resolve to one of the four values
above.

This rule is what prevents the failure mode of "closing PR addresses 3 of 5
composite items while the other 2 silently die in chat history".

## Tracking spine rules

Tracking spines (e.g., `#354`, `#358-#370`) are navigation surfaces, not
implementation tickets. PRs **must not** "close" a tracking spine. Spine
closure happens only when:

- All child issues are closed, routed, or stale; **or**
- The spine is intentionally archived after a phase ends (model: `#308`).

Spine bodies should:

- Start with `Kind: tracking-spine` and `Readiness: tracking-only`.
- Contain a current-children table.
- Contain a "promoted next slice" pointer.
- Contain a "last refreshed" stamp.
- Not contain implementation prose; promote that into execution issues.

## Catalog/spec rules

Catalog/spec issues describe taxonomy, schemas, or names. Live truth belongs in
code or schema definitions, not in issue prose. A generated declaration list is
not verification by itself.

- `schemas/v*/registry.json` — generated JSON schema registry projection.

Catalog/spec issues close when their planned material is either promoted into
an owned source plus generated checked projection, or marked stale. PRs do not
"implement" a catalog/spec issue by adding prose; they implement or update the
source/generator/check path that makes the catalog useful.

## Target-Vision Claim Ledger

Target-vision material is evidence, not implementation truth. Reusable claims
derived from `/realm/project/sinex-target-vision` are tracked in
[`target-vision-claim-ledger.md`](target-vision-claim-ledger.md).

Use the ledger when raw/report/reference prose starts steering architecture,
issue scope, naming, or proof obligations. The ledger records claim status,
evidence source, current authority, promotion gates, and supersession notes.
It is intentionally smaller than an issue body: issues remain the
implementation tracker, while ledger rows prevent stale or seductive horizon
prose from being rediscovered as if it were current design.

## Templates (summary)

The issue templates under `.github/ISSUE_TEMPLATE/` should request:

- **Kind** (always)
- **Readiness** (always)
- **Authority** (always)
- **Parent spine** (always — or `none`)
- **Output kind** (when the issue creates or changes a durable output, view,
  artifact, proposal, judgment, operation record, projection row, or event
  payload)
- **Acceptance criteria** (execution + composite)
- **Closure matrix** (composite)
- **Verification command/evidence** (execution + composite)
- **Blocking question** (decision)
- **Promotion path** (catalog-spec)

A future PR will update the templates. Until then, manual issue authors
should include these sections.

## Operating principles

1. **Truth migrates out of prose.** Where code, schema definitions, or a
   checked generated projection can hold the truth, the issue should point
   there and not restate it.
2. **Outputs are classified before they are added.** New output-producing
   boundaries should point at `sinex_primitives::output_kind` and explain why a
   canonical event is warranted when one is added.
3. **Don't relitigate closed decisions.** Decision issues capture a decision;
   subsequent execution issues cite the decision.
4. **Composites need matrices.** No exceptions. The cost of writing a matrix
   is two minutes; the cost of skipping one has been hours of forensic work
   across audit cycles.
5. **Spines stay thin.** A tracking spine is a map. If it accumulates
   implementation detail, promote that detail into an execution child.
6. **Readiness is a real signal.** `needs-decision` and `tracking-only` are
   first-class safe states. Tagging an issue this way is a feature, not a
   failure.

## How this rolls out

This document is the canonical reference. Issues opened after the rollout
date should follow it. Existing issues are migrated lazily — a comment that
adds Kind/Readiness/Authority is enough; bodies don't have to be rewritten
unless they're load-bearing.

Tracking spines (`#354`, `#358-#370`) and high-traffic composites (`#744`)
have been refreshed with operational notes pointing here.

`#933` continues to be the agent-swarm process record but is no longer the
issue-operating-model authority — this document is.
