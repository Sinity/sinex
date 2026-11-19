WITH candidates AS (
  SELECT f.event_id FROM core.library_copy_fields f WHERE $1 = 'content'
    AND f.content_hash = ANY(ARRAY(SELECT sha256(convert_to(v, 'UTF8')) FROM unnest($2::text[]) v))
  UNION ALL
  SELECT f.event_id FROM core.library_copy_fields f WHERE $1 = 'path'
    AND f.path_hash = ANY(ARRAY(SELECT sha256(convert_to(v, 'UTF8')) FROM unnest($2::text[]) v))
)
SELECT reading.payload AS "payload!"
FROM candidates f
CROSS JOIN LATERAL (
  SELECT l.source FROM core.live_equivalence l
  WHERE l.event_id = f.event_id AND l.event_type = $3 LIMIT 1
) live
CROSS JOIN LATERAL (
  SELECT e.payload, e.commit_seq FROM core.events e
  WHERE e.id = f.event_id AND e.source = live.source AND e.event_type = $3
    AND e.payload->>$1::text = ANY($2::text[]) LIMIT 1
) reading
ORDER BY reading.commit_seq
