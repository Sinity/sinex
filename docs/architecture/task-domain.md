# Task Domain

**Status:** dissolved into issue tracking. The substantive contract
that lived here — tasks-are-event-native invariant (not KG / markdown /
Taskwarrior rows), layering table (source → interpretation → proposal
→ finalizer → reducer → KG), v1 event family taxonomy, payload
sketches, `domain.task_state` reducer projection schema, provenance
table, Taskwarrior boundary modes (import-source-material-only-in-v1,
no bidirectional sync without conflict policy), relation ownership
table, first-slice fixtures, and the boundaries list — now lives in
[issue #1107 (design(domain): model tasks as event-native workflow
objects)](https://github.com/Sinity/sinex/issues/1107) as a design
comment.

`#1107` is the live tracking issue. The reducer contract that task
state implements is owned by `docs/architecture/domain-reducers.md`.
The proposal/judgment/finalizer substrate that mediates inferred
tasks is `docs/architecture/proposal-judgment-finalizer.md`.

**Related:** `docs/architecture/domain-reducers.md`,
`docs/architecture/proposal-judgment-finalizer.md`,
`docs/architecture/inference-decision-metadata.md`.
