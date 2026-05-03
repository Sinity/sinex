# Issue Operating Model

This document defines how the sinex GitHub issue tracker is used. It is the
authoritative reference for issue **kinds**, **readiness states**, and
**closure rules**. Drift between this doc and how issues are filed is itself a
bug — fix the doc or fix the issue.

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
| `generated catalog` | A generated artifact (source-units.json, schemas/) is authoritative |
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

Catalog/spec issues describe taxonomy, schemas, or names. Live truth for any
of these belongs in a **generated catalog** under `docs/` or `schemas/`, not
in issue prose.

- `docs/source-units.json` — source-unit descriptors (live truth)
- `schemas/v*/registry.json` — JSON schema registry (live truth)
- Future: `docs/event-catalog.json` — live event catalog (planned)

Catalog/spec issues close when their planned material is either promoted into
a generated catalog or marked stale. PRs do not "implement" a catalog/spec
issue; they implement a generator that emits the catalog.

## Templates (summary)

The issue templates under `.github/ISSUE_TEMPLATE/` should request:

- **Kind** (always)
- **Readiness** (always)
- **Authority** (always)
- **Parent spine** (always — or `none`)
- **Acceptance criteria** (execution + composite)
- **Closure matrix** (composite)
- **Verification command/evidence** (execution + composite)
- **Blocking question** (decision)
- **Promotion path** (catalog-spec)

A future PR will update the templates. Until then, manual issue authors
should include these sections.

## Operating principles

1. **Truth migrates out of prose.** Where a generated catalog can hold the
   truth, the issue should point to the catalog and not restate it.
2. **Don't relitigate closed decisions.** Decision issues capture a decision;
   subsequent execution issues cite the decision.
3. **Composites need matrices.** No exceptions. The cost of writing a matrix
   is two minutes; the cost of skipping one has been hours of forensic work
   across audit cycles.
4. **Spines stay thin.** A tracking spine is a map. If it accumulates
   implementation detail, promote that detail into an execution child.
5. **Readiness is a real signal.** `needs-decision` and `tracking-only` are
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
