WITH live AS NOT MATERIALIZED (
    SELECT e.id, e.event_type, e.source, e.material_id, e.anchor, e.output_index,
        e.ts_orig, e.reading_order, e.commit_seq, e.equivalence_key, e.payload
    FROM core.events e
    WHERE e.event_type = ANY($1) AND e.ts_orig IS NOT NULL
        AND EXISTS (SELECT 1 FROM unnest($1::text[], $2::jsonb[]) k(event_type, contains)
                    WHERE k.event_type = e.event_type AND e.payload @> k.contains)
        AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
), day_present AS MATERIALIZED (
    SELECT 1 FROM live e
    WHERE ts_orig >= $3 AND ts_orig < $4
        AND EXISTS (SELECT 1 FROM unnest($6::text[], $7::jsonb[]) k(event_type, contains)
                    WHERE k.event_type = e.event_type AND e.payload @> k.contains)
    LIMIT 1
)
SELECT e.id AS "id!", e.event_type AS "event_type!", e.source AS "source!", e.material_id, e.anchor, e.output_index AS "output_index!",
    e.ts_orig AS "at!", e.reading_order AS "reading_order!", e.equivalence_key,
    coalesce((SELECT jsonb_object_agg(field, e.payload->field)
              FROM unnest($5::text[]) field WHERE e.payload ? field), '{}'::jsonb) AS "payload!"
FROM live e
WHERE EXISTS (SELECT 1 FROM day_present)
    AND (NOT $8::bool OR (e.ts_orig >= $3 AND e.ts_orig < $4))
ORDER BY e.ts_orig, e.reading_order, e.commit_seq
