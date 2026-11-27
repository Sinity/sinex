-- Count the same entries as the per-event reader, without loading their
-- payloads or ordering them. A restored input can arrive at a new position.
SELECT
  (SELECT count(*) FROM core.events e
   WHERE e.commit_seq > $2 AND e.event_type = ANY($1::text[])
     AND (SELECT a.event_id FROM audit.archived a WHERE a.event_id = e.id) IS NULL)
  +
  (SELECT count(*) FROM audit.archive_changes c
   WHERE c.commit_seq > $2 AND NOT c.archived
     AND (SELECT a.event_id FROM audit.archived a WHERE a.event_id = c.event_id) IS NULL
     AND (SELECT e.id FROM core.events e WHERE e.id = c.event_id AND e.event_type = ANY($1::text[])) IS NOT NULL)
  AS "waiting!"
