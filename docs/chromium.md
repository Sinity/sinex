# Chromium source

The Chromium collector reads configured Chromium `History` databases and admits page visits (`page.visited`) and persisted downloads (`download.observed`) from source `browser.history.chromium`.

```toml
sources = ["browser.history.chromium"]

[[chromium]]
profile = "personal"
database_generation = "95d3d942-c8a4-444e-8016-29d2e312664c"
path = "/abs/profile/History"
poll_interval_ms = 30000        # 100..=3600000, default 30000
full_interval_ms = 86400000     # 100..=604800000, default one day
snapshot_interval_ms = 3600000  # 100..=604800000, default one hour
```

Up to 16 profiles; profile names and paths must be distinct, and `database_generation` must be a non-nil UUID. `sources` and `[[chromium]]` must agree.

## Identity

The profile and generation UUID are the operator's declaration that native keys continue across reads. Visits are keyed by `(profile, generation, page.visited, visits.id)` and downloads by `(profile, generation, download.observed, guid)`. Numeric download IDs and SQLite rowids are only read positions. Rows with a missing or invalid visit ID or GUID are recorded as record failures.

If the file is replaced or the visit ID sequence resets, the collector fences that generation, also across restarts. Stored data stays readable; declare a new generation to read the replacement.

## Values

`page.visited` keeps the Windows-epoch microsecond time, duration, transition bits, from-visit ID, URL and title. The referrer is the provider's external referrer when present, otherwise the URL of the referenced visit. A zero duration means Chromium has not recorded a positive duration yet; such visits are reread on every poll. Redirect hops are stored but excluded from queries unless `payload.redirect` is named.

`download.observed` records the GUID, numeric ID, ordered URL chain and final URL, paths, start and end times, byte counts, Chromium state and a label. States: 0 in progress, 1 complete, 2 cancelled, 3 Chromium's historical bug state, 4 interrupted. Downloads that are not complete or cancelled are reread on every poll. A zero start or end time is unknown. Download files themselves are never read.

## Reading

Full reads compare every row; they run on the first poll, after reopening, and every `full_interval_ms`. Reduced polls read new visits, zero-duration visits and unfinished downloads (found by GUID), and list download GUIDs to find new ones. A key missing from Chromium is never admitted as a deletion. Batches hold at most 64 rows and 1 MiB; a single larger row makes the poll fail instead of being truncated.

The database is opened read-only and each poll reads inside one transaction. Every `snapshot_interval_ms` the collector stores a full copy of the database through SQLite's backup API, checked with SQLite's integrity check, in the blob store. Snapshots are never parsed into events; rows read during a snapshot poll carry `_from.snapshot`.

When Chromium holds a lock that blocks a direct read, the collector copies the database and its rollback journal into its own directory, checks that file identities, sizes and timestamps did not change during the copy, and lets SQLite recover the copy. It retries a changing pair up to five times. A locked WAL database or a super-journal is reported unavailable; SHM files are never copied.

## Queries

```sh
sinexctl … query 'events where source = browser.history.chromium and kind = page.visited and payload.profile = personal' --json
sinexctl … query 'events where source = browser.history.chromium and kind = page.visited and payload.url ~ "example.invalid"' --json
sinexctl … query 'events where source = browser.history.chromium and kind = page.visited and payload.redirect = true' --json
sinexctl … query 'events where source = browser.history.chromium and kind = download.observed and payload.target_path ~ ".pdf"' --json
sinexctl … show sinex:ev/INTERPRETATION_ID --json
sinexctl … trace sinex:ev/INTERPRETATION_ID --bytes --json
```

See [queries.md](queries.md) for the full raw query syntax. `trace --bytes` returns the masked stored row; MCP trace returns coordinates only. When the database disappears, stored events remain readable with `complete: false` and CLI exit 5.

Replay and exclusion do not cover Chromium. Schema change 4 registers the browser event types ([schema.md](schema.md)).
