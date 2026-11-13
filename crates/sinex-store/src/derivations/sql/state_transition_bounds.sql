WITH eligible AS MATERIALIZED (
    SELECT e.ts_orig, e.commit_seq,
        (e.payload->>$5 IS NOT NULL) AS known_scope,
        jsonb_build_array(e.source, coalesce(e.payload->>$5, e.id::text)) AS scope,
        jsonb_build_array(e.event_type, coalesce(
            (SELECT e.payload->field
             FROM unnest($6::text[]) WITH ORDINALITY AS f(field, n)
             WHERE e.payload->field IS NOT NULL AND e.payload->field <> 'null'::jsonb
             ORDER BY n LIMIT 1), to_jsonb(e.id::text))) AS subject
    FROM core.events e
    WHERE e.event_type = ANY($1) AND e.ts_orig IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM unnest($1::text[], $2::jsonb[]) AS k(event_type, contains)
            WHERE k.event_type = e.event_type AND e.payload @> k.contains)
        AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
), day_scopes AS MATERIALIZED (
    SELECT DISTINCT scope FROM eligible WHERE ts_orig >= $3 AND ts_orig < $4
), previous AS (
    SELECT e.scope, max(e.ts_orig) AS ts_orig
    FROM eligible e JOIN day_scopes d USING (scope)
    WHERE e.ts_orig < $3
    GROUP BY e.scope
), latest AS MATERIALIZED (
    SELECT DISTINCT ON (e.scope) e.scope, e.subject, e.known_scope
    FROM eligible e JOIN day_scopes d USING (scope)
    WHERE e.ts_orig < $4
    ORDER BY e.scope, e.ts_orig DESC, e.commit_seq DESC
), closers AS (
    SELECT l.scope,
        CASE WHEN NOT l.known_scope THEN $4
             ELSE coalesce(min(e.ts_orig), $7) END AS finish
    FROM latest l LEFT JOIN eligible e
        ON l.known_scope AND e.scope = l.scope AND e.ts_orig >= $4
        AND e.subject IS DISTINCT FROM l.subject
    GROUP BY l.scope, l.known_scope
)
SELECT least($3, (SELECT min(ts_orig) FROM previous)) AS start,
    greatest($4, (SELECT max(finish) FROM closers)) AS finish
