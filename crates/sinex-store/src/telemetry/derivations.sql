WITH running AS (
  SELECT name, coalesce(array_agg(input) FILTER (WHERE input IS NOT NULL), '{}') AS inputs
  FROM unnest($1::text[], $2::text[]) AS live(name, input)
  GROUP BY name
)
SELECT d.name AS "name!",
  (SELECT count(*) FROM derivation.dirty k WHERE k.derivation = d.name) AS "dirty!",
  (SELECT count(*) FROM derivation.waiting w WHERE w.derivation = d.name) AS "waiting!",
  (SELECT count(*) FROM derivation.refusals f WHERE f.derivation = d.name) AS "refused!",
  coalesce(p.commit_seq, 0) AS "position!",
  CASE WHEN live.name IS NOT NULL THEN
    -- Count the same entries as the per-event reader, without loading their
    -- payloads or ordering them. A restored input can arrive at a new position.
    (SELECT count(*) FROM core.events e
     WHERE e.commit_seq > coalesce(p.commit_seq, 0) AND e.event_type = ANY(live.inputs)
       AND (SELECT a.event_id FROM audit.archived a WHERE a.event_id = e.id) IS NULL)
    +
    (SELECT count(*) FROM audit.archive_changes c
     WHERE c.commit_seq > coalesce(p.commit_seq, 0) AND NOT c.archived
       AND (SELECT a.event_id FROM audit.archived a WHERE a.event_id = c.event_id) IS NULL
       AND (SELECT e.id FROM core.events e WHERE e.id = c.event_id AND e.event_type = ANY(live.inputs)) IS NOT NULL)
  END AS "waiting_inputs?"
FROM registry.derivations d LEFT JOIN derivation.positions p ON p.derivation = d.name
LEFT JOIN running live ON live.name = d.name
ORDER BY d.name
