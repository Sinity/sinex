SELECT e.id, e.class,
       (EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id=e.id)
        OR EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id=e.id)) AS "live!"
FROM core.events e WHERE e.id = ANY($1::uuid[])
