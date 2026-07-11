# Superseded: GitHub Issue Operating Model

GitHub Issues were retired as Sinex's task substrate on 2026-07-10. Beads
(`bd`) is the sole work-tracking and closure authority; current conventions
live in `CLAUDE.md`, `.agent/CONVENTIONS.md`, and `CONTRIBUTING.md`.

Before claiming a Bead is honestly closed, record a `Closure Evidence
Manifest` in its `close_reason` and run:

```bash
xtask verify closure <bead-id>
```

The verifier reads `bd show <bead-id> --json`, requires the Bead to be closed,
maps every acceptance criterion to a Satisfied, Deferred, or Misframed manifest
row, requires deferred rows to name follow-up Beads, and executes runnable
evidence commands. PR or commit citations are landing evidence, not behavioral
proof by themselves.

The retired GitHub issue kinds and closure workflow remain available in git
history before this pointer; they are not an active coordination surface.
