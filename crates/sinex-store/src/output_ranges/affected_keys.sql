-- The keys with a live ended output that covers part of an asked range: each
-- range is a derivation, an output type and inclusive bounds ($3, $4). The
-- range index is asked with ordered bounds, as it was built, so neither a
-- stored output nor an asked range that ends before it starts fails; the
-- stored bounds then decide. Each range is one index scan that visits the
-- outputs overlapping it.
SELECT DISTINCT r.derivation AS "derivation!", hit.key AS "key!"
FROM unnest($1::text[], $2::text[], $3::timestamptz[], $4::timestamptz[])
  AS r(derivation, event_type, started_at, ended_at)
CROSS JOIN LATERAL (
  SELECT x.key FROM derivation.output_ranges x
  WHERE x.derivation = r.derivation AND x.event_type = r.event_type AND x.ended_at IS NOT NULL
    AND tstzrange(least(x.started_at, x.ended_at), greatest(x.started_at, x.ended_at), '[]')
        && tstzrange(least(r.started_at, r.ended_at), greatest(r.started_at, r.ended_at), '[]')
    AND x.started_at <= r.ended_at AND x.ended_at >= r.started_at
  OFFSET 0
) hit
