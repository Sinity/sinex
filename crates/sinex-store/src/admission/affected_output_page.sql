-- One page of a derivation's memberships in key order, with whether each
-- member is archived: the members after the cursor ($3, $4), and with $2 set
-- only those of that key. The equality on `derivation` is followed directly by
-- the cursor's row comparison, so the index scan starts at the cursor. An
-- equality on a column the comparison also names would start it at that
-- column's first member, so one key is bounded with <= and >= instead; that
-- also leaves the membership index as the only one in the requested order.
-- The page is bounded before the archive lookup, one index probe per member.
WITH page AS MATERIALIZED (
  SELECT o.derivation, o.key, o.event_id FROM derivation.outputs o
  WHERE o.derivation=$1
    AND ($2::jsonb IS NULL OR o.key<=$2::jsonb)
    AND CASE WHEN $3::jsonb IS NOT NULL THEN (o.key,o.event_id)>($3::jsonb,$4::uuid)
             WHEN $2::jsonb IS NOT NULL THEN o.key>=$2::jsonb
             ELSE true END
  ORDER BY o.derivation,o.key,o.event_id LIMIT 256
)
SELECT p.derivation AS "derivation!", p.key AS "key!", p.event_id AS "event_id!",
  a.event_id IS NOT NULL AS "archived!"
FROM page p
LEFT JOIN LATERAL (
  SELECT event_id FROM audit.archived WHERE event_id=p.event_id OFFSET 0
) a ON true
ORDER BY p.key,p.event_id
