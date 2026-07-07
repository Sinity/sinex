> NOTE (2026-07-08): the conductor-devloop packet referenced below is retired and
> archived at `.agent/archive/devloop-2026-07/`; `devloop-*` scripts no longer exist.
> Beads is the task/devloop substrate. Paths below were rewritten to the archive.

# Inbox Integration Index

This index records Sinex-relevant material moved from `/realm/inbox` into the
active `.agent` scaffold. Large/raw imports live under ignored
`.agent/{demos,artifacts}` or focused research notes under
`.agent/scratch/research/`; this tracked file is the durable routing and
interpretation layer.

## Demo Shelf

Authoritative live shelf: `.agent/demos/sinex/`.

Regeneration command:

```bash
.agent/scripts/devloop-refresh-demos
.agent/scripts/devloop-sync
```

Current generated index files:

- `.agent/demos/sinex/MANIFEST.readable.json`
- `.agent/demos/sinex/SUMMARY_INDEX.json`
- `.agent/demos/sinex/CURATED_CATALOG.md`

Interpretation: the old concern that the readable demo packet only listed the
older capability/recall/query/context/cross-stack set is stale. The current
top-level README and regenerated manifest include runtime-presence, DLQ-pressure,
source-id query bridge, status-DLQ, manifest-source, and live context timeline
artifacts. The remaining process fix is to run the refresh script as part of
the loop closing ritual, not to rely on memory. Chisel owns portable bundle
generation; do not regenerate full `CONCATENATED_READABLE.md` copies here.

## Conductor Packet

Moved content was consolidated into the active packet instead of preserving a
handoff mirror:

- `.agent/archive/devloop-2026-07/context/conductor-sinex.md`
- `.agent/archive/devloop-2026-07/context/2026-06-30-conductor-sinex-assimilation.md`
- `.agent/archive/devloop-2026-07/OPERATING-LOG.md`
- `.agent/archive/devloop-2026-07/PROCESS.md`
- `.agent/archive/devloop-2026-07/TACTICS.md`
- `.agent/archive/devloop-2026-07/VELOCITY.md`
- `.agent/archive/devloop-2026-07/RUNBOOK.md`
- `.agent/archive/devloop-2026-07/ACTIVE-LOOP.md`

The durable interpretation lives in the conductor packet and tracked scaffold,
not in the retired scratch-current active-state pattern:

- `.agent/archive/devloop-2026-07/context/2026-06-30-conductor-sinex-assimilation.md`
- `.agent/archive/devloop-2026-07/context/001-standing-goal.md`
- `.agent/archive/devloop-2026-07/PROCESS.md`
- `.agent/archive/devloop-2026-07/TACTICS.md`

Key doctrine to preserve:

- demonstrable value and real artifacts are the objective function;
- dogfooding is instrumental, not the goal by itself;
- use issues as a parts bin, not the plan;
- build thin lenses over general evidence/query/projection algebra;
- occurrence evidence headlines recall; derived/self-observation is separated;
- verify from the operator seat on the live store.

## ChatGPT Project A Exports

The full copied export directory and temporary excerpts were pruned from
`.agent/artifacts`; keep using source archives or Polylogue for full-fidelity
reconstruction. Do not recreate broad scratch inbox-import trees.

Use as research input, not implementation authority. The capture architecture
session is compact and directly useful for #1043/#1469-style capture work:
raw audio/screen/video as source material, transcript/OCR as derived surfaces,
resource budgets that create visible debt/caveats rather than changing event
meaning, and operator-controlled disclosure policy. The misalignment export is
large; mine it only for specific questions instead of loading it wholesale.

## Patch Inbox

The raw copied patch was pruned from `.agent`; use the source inbox/archive if
the exact patch is needed again.

Current interpretation: this appears to be Polylogue-oriented cleanup language
around demo/verifiability/test wording. Do not apply it to Sinex mechanically.
Its useful lesson for Sinex is naming discipline: demo/readable manifests should
describe executable proof surfaces and avoid presenting registries as behavior
truth.

## Project Artifacts And Devloop Exports

Moved content:

- `.agent/artifacts/sinex/` — former `/realm/inbox/project-artifacts/sinex`.
  Large copied downloads, legacy analyses, scratch backups, and full ChatGPT
  export dumps were pruned; keep only deliberately promoted evidence.
- `.agent/devloops/sinex/` — former `/realm/inbox/project-devloops/sinex`,
  retaining the 2026-06-27 operational report and downloaded conductor source
  packet. Raw copied devloop exports and Chisel upload bundles were removed;
  use Polylogue/Chisel for fresh packaging instead of preserving duplicate
  payloads here.

Use these as evidence/research archives. Do not load large tarballs, bundles,
or JSONL exports wholesale; mine by filename/grep and promote only the
actionable conclusions into `.agent/archive/devloop-2026-07/`, `.agent/scratch/research/`,
issues, or new demo artifacts.

## Related Shelves Not Copied Wholesale

- `/realm/inbox/polylogue-conductor-devloop/` is useful for process inspiration
  and twin-loop scripts, but Sinex already has the relevant scaffold scripts.
- `/realm/inbox/demos_polylogue/` is useful for cross-stack design comparisons;
  keep Polylogue transcript-native strengths separate from Sinex evidence joins.
- `/realm/inbox/codices/` contains old session exports and handoff tarballs;
  use only when reconstructing prior agent decisions.
- `/realm/inbox/quarantine/sinex-snapshots/` contains old large dumps; do not
  ingest into `.agent`.

## Closing Ritual

Before a handoff or final status report:

3. Check that `README.md`, `MANIFEST.readable.json`, `SUMMARY_INDEX.json`, and
   `CURATED_CATALOG.md` mention the newest artifact directories named in
   `.agent/archive/devloop-2026-07/OPERATING-LOG.md`.
4. Record the refresh in `OPERATING-LOG.md`.
