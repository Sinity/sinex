SELECT reading.payload AS "payload!"
FROM core.live_equivalence live
CROSS JOIN LATERAL (
  SELECT e.payload FROM core.events e WHERE e.id = live.event_id LIMIT 1
) reading
WHERE live.source = $1 AND live.event_type = $2
  AND live.equivalence_key = core.encode_equivalence_key($3)
