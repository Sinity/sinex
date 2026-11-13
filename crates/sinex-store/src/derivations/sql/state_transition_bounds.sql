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
), observations AS (
    SELECT eligible.*,
        bool_or(ts_orig >= $3 AND ts_orig < $4) OVER (PARTITION BY scope) AS touched,
        first_value(subject) OVER latest AS latest_subject,
        first_value(known_scope) OVER latest AS latest_scope_known
    FROM eligible
    -- Put the latest observation before the exclusive end first. Share that
    -- state across the scope without joining every scope to the history.
    WINDOW latest AS (
        PARTITION BY scope
        ORDER BY (ts_orig < $4) DESC, ts_orig DESC, commit_seq DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
), scopes AS (
    SELECT scope, max(ts_orig) FILTER (WHERE ts_orig < $3) AS previous,
        CASE WHEN NOT latest_scope_known THEN $4
             ELSE coalesce(min(ts_orig) FILTER (
                 WHERE ts_orig >= $4 AND subject IS DISTINCT FROM latest_subject), $7)
        END AS finish
    FROM observations
    WHERE touched
    GROUP BY scope, latest_scope_known
)
SELECT least($3, min(previous)) AS start, greatest($4, max(finish)) AS finish
FROM scopes
