# Scratch Index

Scratch is supporting research only. It is not the active conductor state, not a
handoff packet, and not a generated proof dump shelf.

## Active Loop Moved

Active conductor state now lives in `../conductor-devloop/`.

Do not recreate `scratch/current/`. Scratch is supporting research and temporary
analysis only; current loop logs, focus state, velocity notes, and demo radar
belong in the conductor packet.

## Allowed Steady State

- `README.md` — this routing file.
- `research/*.md` — concise supporting research notes that may feed future
  capability slices.

Generated JSON, logs, raw exports, old handoff packets, and proof payloads do
not belong here. Put them in the active conductor packet when they are live
state, in a named demo packet when they prove a demo, or in a purpose-specific
ignored artifact shelf.

## Current Research

`research/` contains recent targeted research that may feed next capability
slices:

- `research/research-INDEX-2026-06-28.md` — entrypoint for the June research
  wave.
- `research/research-keystone-readiness.md` and
  `research/research-crosssource-demo.md` — most relevant to the conductor
  EvidenceWindow / context reconstruction path.
- `research/research-2184-fragmentation.md` — production material fragmentation
  investigation; do not fix from hypothesis alone.
- `research/source-model-unification-design.md` and `research/recon-*` — source
  model/acquisition substrate notes.

## Rule

New active conductor notes go in `../conductor-devloop/`. Supporting research
goes in `research/`. Generated proof dumps should become demo artifacts or move
to a purpose-specific ignored artifact shelf, not scratch root.
