-- Positions only: payloads are read afterwards, once, by ID, kind and time.
SELECT q.id AS "id!", q.seq AS "commit_seq!", q.event_type AS "event_type!", q.ts_orig FROM (
  SELECT s.id, s.commit_seq AS seq, s.event_type, s.ts_orig
  FROM core.events s
  WHERE s.commit_seq > $2 AND s.event_type = ANY($1)
    AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
  UNION ALL
  SELECT c.event_id AS id, c.commit_seq AS seq, s.event_type, s.ts_orig
  FROM audit.archive_changes c JOIN core.events s ON s.id = c.event_id
  WHERE c.commit_seq > $2 AND NOT c.archived AND s.event_type = ANY($1)
    AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
  ORDER BY seq LIMIT $3
) q
ORDER BY q.seq
