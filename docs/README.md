# Sinex Documentation

This directory contains the canonical documentation for the Sinex workspace.

Core indexes
- Architecture: `docs/architecture/` (start with `README.md` and `system-overview.md`)
- Integrity & Security: `docs/INTEGRITY.md`, `docs/architecture/security-architecture.md`
- Development: `docs/DEVELOPMENT_GUIDE.md`, `TESTING.md`, `IMPORT_STYLE.md`
- Roadmap: `docs/roadmap/`

Messaging
- The internal message bus is NATS JetStream. Older documents may mention Redis Streams historically; use `docs/architecture/streaming-architecture.md` for current patterns.

Organization
- `architecture/` — System design, data substrate, streaming, security.
- `roadmap/` — Direction, features, and future work.
- `done/` — Past plans and completed design notes.
- `_todo/` — Work-in-progress notes and sketches.
- `archive/` — Frozen legacy docs kept for reference (see policy in `archive/README.md`).
- `trash/` — Should be empty; holds only a README explaining the quarantine policy.

Contributing to docs
- Prefer concise, durable guidance over long narrative analyses.
- Update existing docs rather than adding new root files when possible.
- If you must stage large exploratory docs, place them under `_todo/` and extract the useful bits into canonical docs before merging.

