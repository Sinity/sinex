SELECT e.id AS "id!", e.event_type AS "event_type!", e.source AS "source!", e.material_id, e.record_offset, e.output_index AS "output_index!",
    e.ts_orig AS "at!", e.reading_order AS "reading_order!", e.commit_seq AS "commit_seq!", e.equivalence_key,
    coalesce((SELECT jsonb_object_agg(field, e.payload->field)
              FROM unnest($5::text[]) field WHERE e.payload ? field), '{}'::jsonb) AS "payload!"
FROM (SELECT DISTINCT unnest($1::text[]) AS event_type) kind
CROSS JOIN LATERAL (
    SELECT e.* FROM core.events e
    WHERE e.event_type = kind.event_type AND e.ts_orig IS NOT NULL
    AND ($3::timestamptz IS NULL OR e.ts_orig >= $3)
    AND ($4::timestamptz IS NULL OR e.ts_orig < $4)
    AND EXISTS (SELECT 1 FROM unnest($1::text[], $2::jsonb[]) k(event_type, contains)
                WHERE k.event_type = e.event_type AND e.payload @> k.contains)
    AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
    AND ($6::timestamptz IS NULL OR (e.ts_orig, e.commit_seq) < ($6, $7::bigint))
    AND (e.payload @> ANY($9::jsonb[]) OR e.equivalence_key = ANY($10::text[])
         OR e.material_id = ANY($11::uuid[]))
    ORDER BY e.ts_orig DESC, e.commit_seq DESC
    LIMIT $8
) e
ORDER BY e.ts_orig DESC, e.commit_seq DESC
LIMIT $8
