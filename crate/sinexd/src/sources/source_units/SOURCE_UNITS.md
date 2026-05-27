# Source Units — Wave B Registration Catalog

Every source unit Wave B will register in `sinex-source-worker`, grouped by domain.
Derived from the existing ingestor crates (`crate/nodes/sinex-*-ingestor/src/lib.rs`).

Each Wave B subagent owns one domain column: it moves the source unit's `register_source_unit!`
and `register_source_unit_binding!` blocks from the legacy ingestor crate into the
corresponding `crate/core/sinex-source-worker/src/sources/<domain>/` module tree.

---

## terminal

- `terminal.atuin-history` — SourceUnitRuntime; checkpoint: AppendStream (SQLite via atuin)
- `terminal.bash-history` — SourceUnitRuntime; checkpoint: AppendStream (append-only text)
- `terminal.zsh-history` — SourceUnitRuntime; checkpoint: AppendStream (append-only text)
- `terminal.fish-history` — SourceUnitRuntime; checkpoint: MutableSnapshot (sqlite, anchor: fish_history_row_id)
- `terminal.text-history` — SourceUnitRuntime; checkpoint: AppendStream (generic text history)
- `terminal.monitor` — SourceUnitRuntime; checkpoint: LiveObservation (runtime self-observation)

## browser

- `browser.history` — SourceUnitRuntime; checkpoint: MutableSnapshot (sqlite, anchor: visit_id); privacy: Secret

## document

- `document.staging` — SourceUnitRuntime; checkpoint: AppendStream (staged file drop)

## fs

- `fs` — FileContentDropAdapter; checkpoint: AppendStream (inotify/file-watch events with content staging)

## system

- `system.journald` — SourceUnitRuntime; checkpoint: Journal (systemd journal cursor)
- `system.systemd` — SourceUnitRuntime; checkpoint: Journal (systemd unit state)
- `system.dbus` — SourceUnitRuntime; checkpoint: LiveObservation (D-Bus signal stream)
- `system.udev` — SourceUnitRuntime; checkpoint: LiveObservation (udev event stream)
- `system.monitor` — SourceUnitRuntime; checkpoint: LiveObservation (cgroup/PSI monitor)

## desktop

- `desktop.window-manager` — SourceUnitRuntime; checkpoint: LiveObservation (Hyprland socket)
- `desktop.clipboard` — SourceUnitRuntime; checkpoint: LiveObservation (Wayland clipboard)
- `desktop.activitywatch` — SourceUnitRuntime; checkpoint: MutableSnapshot (sqlite, anchor: bucket_event_timestamp)

---

## Notes on drift from tentative lists

- **`terminal.zsh-history`** — present in source unit registrations (listed in the prompt as tentative). Confirmed.
- **`fs`** — registered as `id: "fs"` (a single unit covering all file events), not split into `file-created/modified/deleted/moved` sub-units as the tentative list suggested. It now uses `FileContentDropAdapter` plus `FilesystemParser`; keep the stable `fs` id.
- **`system.monitor`** — present in registrations, omitted from the tentative list. Confirmed here.
- **`terminal.monitor`** — present in registrations, omitted from the tentative list. Confirmed here.
- **`document.staging`** — the descriptor id is `document.staging`, not `document.file-watch` as tentatively listed.
- **`browser.history`** — single unit covering Firefox + Chromium via pluggable `BrowserSqliteFormat`; no sub-split yet.
