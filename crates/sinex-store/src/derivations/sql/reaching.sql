-- The live intervals of the asked kinds that started before the window and
-- reach its start ($3), or have no end. Each asked kind is an event type and,
-- when the window fixes one, the value of its narrowing field. Ended
-- intervals are found by the range index and open ones by their own index,
-- so the read costs what it returns, however much history the kind has.
-- An open interval reaches every window that does not end before it starts.
SELECT i.event_id AS "event_id!", i.event_type AS "event_type!", i.started_at AS "started_at!"
FROM unnest($1::text[], $2::text[]) AS k(event_type, narrowing)
CROSS JOIN LATERAL (
  SELECT l.event_id, l.event_type, l.started_at FROM core.live_intervals l
  WHERE k.narrowing IS NOT NULL AND l.event_type = k.event_type AND l.narrowing = k.narrowing
    AND l.ended_at IS NOT NULL AND tstzrange(l.started_at, l.ended_at, '[]') @> $3::timestamptz
  UNION ALL
  SELECT l.event_id, l.event_type, l.started_at FROM core.live_intervals l
  WHERE k.narrowing IS NULL AND l.event_type = k.event_type
    AND l.ended_at IS NOT NULL AND tstzrange(l.started_at, l.ended_at, '[]') @> $3::timestamptz
  UNION ALL
  SELECT l.event_id, l.event_type, l.started_at FROM core.live_intervals l
  WHERE k.narrowing IS NOT NULL AND l.event_type = k.event_type AND l.narrowing = k.narrowing
    AND l.ended_at IS NULL AND l.started_at < $3::timestamptz
    AND ($4::timestamptz IS NULL OR $4 >= $3)
  UNION ALL
  SELECT l.event_id, l.event_type, l.started_at FROM core.live_intervals l
  WHERE k.narrowing IS NULL AND l.event_type = k.event_type
    AND l.ended_at IS NULL AND l.started_at < $3::timestamptz
    AND ($4::timestamptz IS NULL OR $4 >= $3)
) i
WHERE i.started_at < $3::timestamptz AND ($4::timestamptz IS NULL OR i.started_at < $4)
