SELECT d.name AS "name!",
  (SELECT count(*) FROM derivation.dirty k WHERE k.derivation = d.name) AS "dirty!",
  (SELECT count(*) FROM derivation.waiting w WHERE w.derivation = d.name) AS "waiting!",
  (SELECT count(*) FROM derivation.refusals f WHERE f.derivation = d.name) AS "refused!",
  coalesce(p.commit_seq, 0) AS "position!",
  CASE WHEN d.kind = 'per_event' THEN
    -- Count the same entries as the per-event reader, without loading their
    -- payloads or ordering them. A restored input can arrive at a new position.
    (SELECT count(*) FROM core.events e
     WHERE e.commit_seq > coalesce(p.commit_seq, 0) AND e.event_type = ANY(d.inputs)
       AND (SELECT a.event_id FROM audit.archived a WHERE a.event_id = e.id) IS NULL)
    +
    (SELECT count(*) FROM audit.archive_changes c JOIN core.events e ON e.id = c.event_id
     WHERE c.commit_seq > coalesce(p.commit_seq, 0) AND NOT c.archived AND e.event_type = ANY(d.inputs)
       AND (SELECT a.event_id FROM audit.archived a WHERE a.event_id = e.id) IS NULL)
  END AS "waiting_inputs?"
FROM registry.derivations d LEFT JOIN derivation.positions p ON p.derivation = d.name
ORDER BY d.name
