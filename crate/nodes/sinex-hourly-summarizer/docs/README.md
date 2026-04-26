# sinex-hourly-summarizer

Aggregates bounded `activity.window.summary` events into hourly
`activity.summary.hourly` rollups.

Current semantics:

- Buckets windows by the UTC hour of `window_end`.
- Preserves parent provenance for every contributing window summary.
- Tracks both event-count distribution and a focus-time breakdown.
- Attributes each window's full duration to that window's `primary_source`.

The open hour does not emit until the first window for the next hour arrives or
the hour is replayed later.
