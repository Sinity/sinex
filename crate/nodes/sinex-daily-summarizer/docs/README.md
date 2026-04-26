# sinex-daily-summarizer

Aggregates `activity.summary.hourly` events into daily
`activity.summary.daily` rollups.

Current semantics:

- Buckets hours by the UTC day of `hour_start`.
- Preserves parent provenance for every contributing hourly summary.
- Carries forward top-source rankings, event counts, and focus-time totals.

The open day does not emit until the first hourly summary of the next UTC day
arrives or the period is replayed later.
