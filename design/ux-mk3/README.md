# Sinex UX/UI MK3 — Evidence OS Workbench

This is a self-contained upgrade of the Sinex UX design program after reviewing the latest Claude Design output (`sinex mk1.1 (1).zip`) and vetting it against the attached Sinex repository/materials.

The incoming Claude output is useful as a visual inventory: it contains a 24-board TUI/workbench canvas, rich fixtures, and a coherent terminal-ish visual language. MK3 keeps the useful parts, then makes the program stricter and more implementation-ready:

- current vs target capability separation
- exact command/RPC/source-surface grounding
- no CDN/Babel demo dependency as the canonical artifact
- GitHub-ready issue/comment bodies
- shared view DTO spine for CLI, TUI, MCP, and future projections
- detailed state grammar, copy/action matrix, and visual-smoke fixtures

The product thesis: **Sinex is an Evidence OS workbench.** The UI should answer: what happened, what evidence supports it, what is missing, what is private/redacted, what derived interpretation exists, and what action is safe next.

Start with:

- `docs/00-executive-verdict.md`
- `docs/01-claude-output-review.md`
- `docs/02-codebase-grounding.md`
- `docs/03-canonical-design-program.md`
- `index.html`
- `github/issue-index.md`
- `github/issue-manifest.json`

Recommended first implementation slice: `ViewEnvelope + SinexObjectRef + ActionAvailability + EventCardView`, then TUI Workbench v2 and Event Inspector.
