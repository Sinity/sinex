-- Select positions from the kind and event time index before reading payloads.
SELECT s.id, s.event_type, s.ts_orig AS "ts_orig!", s.commit_seq AS "commit_seq!"
FROM core.events s
WHERE s.event_type = ANY($1)
  AND s.ts_orig >= $2 AND s.ts_orig < coalesce($3::timestamptz, 'infinity')
  AND (s.ts_orig, s.commit_seq) > ($2, $4)
  AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
ORDER BY s.ts_orig, s.commit_seq
LIMIT $5
