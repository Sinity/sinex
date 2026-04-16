## Agent Orientation

Pre-release software for a single user. Zero production events. No backwards compatibility concerns.

**Core principles:**
- **Cleanliness beats entropy** — modify directly, never wrap or deprecate. Old patterns get replaced.
- **Completion beats deferral** — finish work end-to-end. "Foundation for" means "do it now."
- **Facts beat assumptions** — no invented constraints. Act on code and docs, not guesses.
- **Async beats blocking** — spawn `--bg` tasks and work while they run.
- **Provenance beats convenience** — every event MUST trace to source material or parent events.

**Never do these:**
- Add backwards compatibility or deprecation shims (pre-release, no users)
- Invent "time constraints" or "safety concerns" (act on facts)
- Commit stubs without saying so (finish or declare incomplete)
- Override user requirements (implement what was asked)

**Tooling agency:** When xtask causes friction (missing capability, bad output format, parse difficulty), fix xtask directly. The tooling serves the agent. Follow existing patterns in `xtask/src/`. Commit atomically as `fix(xtask)` or `feat(xtask)`.
