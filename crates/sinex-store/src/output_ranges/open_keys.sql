-- The keys of one derivation ($1) with a live open output of one type ($2)
-- that started at or before $3. The index of open outputs is ordered by key
-- and start, so each step seeks to the next key's earliest open output: one
-- probe per key that has an open output, however many each key has.
WITH RECURSIVE earliest(key, started_at) AS (
  (SELECT x.key, x.started_at FROM derivation.output_ranges x
   WHERE x.derivation = $1 AND x.event_type = $2 AND x.ended_at IS NULL
   ORDER BY x.key, x.started_at LIMIT 1)
  UNION ALL
  SELECT next.key, next.started_at FROM earliest e
  CROSS JOIN LATERAL (
    SELECT x.key, x.started_at FROM derivation.output_ranges x
    WHERE x.derivation = $1 AND x.event_type = $2 AND x.ended_at IS NULL AND x.key > e.key
    ORDER BY x.key, x.started_at LIMIT 1
  ) next
)
SELECT key AS "key!" FROM earliest WHERE started_at <= $3::timestamptz
