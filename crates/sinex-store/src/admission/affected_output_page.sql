SELECT o.derivation, o.key, o.event_id FROM derivation.outputs o
                   LEFT JOIN LATERAL (
                     SELECT event_id FROM audit.archived WHERE event_id=o.event_id OFFSET 0
                   ) a ON true
                   WHERE o.derivation=ANY($1)
                     AND ($2::text IS NULL OR (o.derivation,o.key,o.event_id)>($2,$3::jsonb,$4::uuid))
                     AND a.event_id IS NULL
                   ORDER BY o.derivation,o.key,o.event_id LIMIT 256
