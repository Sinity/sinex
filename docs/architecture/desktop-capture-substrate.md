# Desktop Capture Substrate

**Status:** dissolved into issue tracking. The substantive contract
that lived here — what desktop capture owns vs. doesn't, the
`wm.hyprland` / `desktop.context` / `browser.context` / `editor.neovim`
event surfaces, the workspace-context-changed pairing rule, the
four-trigger `state.captured` semantics, the terminal/editor capture
rules (Lua plugin > title parsing for Neovim), the
separate-runtime-process invariant from the staged-source contract,
the privacy-context table, and the boundaries list — now lives in
[issue #1035 (feat(desktop): context assembly — workspace model,
terminal/browser context, notification surge, focus session
detection)](https://github.com/Sinity/sinex/issues/1035) as a design
comment.

Originating issues `#393`/`#394`/`#395`/`#389` are closed; Hyprland
IPC coverage gap `#1019` is closed. `#1035` is the live tracking issue.

Audio + screen OCR streams (which used to be listed under "what this
does not own") live in `#1043`. Browser native-messaging upgrade path:
`#847`. Runtime topology resolution: `#1054`. Privacy admission
policy: `#1042`.

**Related:** `docs/architecture/staged-source-parser-substrate.md`,
`docs/architecture/runtime-boundaries.md`.
