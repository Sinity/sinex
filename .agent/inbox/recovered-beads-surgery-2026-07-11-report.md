# Recovered Beads graph-surgery adjudication

Date: 2026-07-11

## Scope

Reviewed `sinex-beads-graph-surgery.zip`, recovered from the GPT-Pro download
bundle, against the Sinex Beads export at current `origin/master` (`e1d1bf2db`).
The canonical Sinex checkout and its shared Dolt database were kept read-only.
All mutations were replayed in a temporary standalone Beads database initialized
from the live export, then exported to this feature branch.

## Package and drift checks

- `sha256sum -c MANIFEST.sha256`: every packaged file passed.
- `python validate_delta.py beads-delta.jsonl`: 79 independent proposal rows
  passed the package's structural validator.
- `preflight_live_delta.py`: 79 applicable, 0 already satisfied, 0 drift-review.
  This means touched fields matched the package snapshot; it does not establish
  that the proposal was authoritative.

## Adjudication

| Result | Count | Basis |
| --- | ---: | --- |
| Accepted as proposed | 32 | Current source, issue text, or graph policy supports the operation. |
| Edited and accepted | 4 | A soft edge already existed and Beads required replacing it before adding the proposed hard edge. |
| Rejected | 43 | Generated design rewrites asserted `SETTLED DECISION` without an operator ruling or existing Bead authority. |
| Drifted / superseded | 0 | Live touched fields matched the packaged snapshot. |

The 36 accepted rows comprise six merges, eleven invariant-restoring relabels,
eight evidence-backed issue updates, nine dependency additions, and two
dependency removals. The rejected rows are 43 of the 44 low-confidence
`update` operations that attempted to install synthesized implementation
designs. `sinex-pasb` is the exception: graph lint proved that active bug lacked
mandatory acceptance criteria, and line 67 supplied concrete, source-anchored
AC and repair targets. The rejected evidence remains useful claim-time
accelerant, but its proposed wording is not graph authority.

The four edited operations were dependency type upgrades. The package modeled
them as plain `dep-add`, but the installed Beads CLI rejects a second edge to
the same target when a `related` edge already exists. The isolated apply removed
the existing soft edge and then added the proposed `blocks` edge. No unrelated
dependency was removed.

## Artifacts

- `recovered-beads-surgery-2026-07-11-adjudication.jsonl` records the decision
  and reason for every one of the 79 proposal lines.
- `.beads/issues.jsonl` is the isolated post-adjudication graph export.
- The original package remains in
  `/realm/inbox/gpt-pro-sol/recovered-branch-project-explanation-2026-07-11/sinex/`.

## Isolation note

Running `bd where` from the feature worktree resolves
`/realm/project/sinex/.beads/embeddeddolt`, the canonical shared database.
Consequently, all writes used `/realm/tmp/sinex-surgery-beads-db`; only its
export was copied into this branch. This avoids changing the dirty canonical
checkout or publishing partial Dolt state outside the branch review boundary.
