SELECT reading.payload AS "payload!"
FROM derivation.outputs o
CROSS JOIN LATERAL (
  SELECT e.payload, e.commit_seq FROM core.events e
  WHERE e.id = o.event_id AND e.source = o.derivation AND e.event_type = $3 LIMIT 1
) reading
WHERE o.derivation = $1 AND o.key = $2
  AND (SELECT 1 FROM core.live_equivalence live WHERE live.event_id = o.event_id LIMIT 1) IS NOT NULL
ORDER BY reading.commit_seq
