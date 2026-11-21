WITH pair_counts AS MATERIALIZED (
    SELECT e.payload->>'family' AS family,
           count(*) FILTER (WHERE e.payload->>'state' = 'undecided') AS undecided,
           count(*) FILTER (WHERE e.payload->>'state' <> 'undecided') AS decided
      FROM core.visible_events e
     WHERE e.event_type = $3 AND e.payload->>'family' = ANY($1::text[])
       AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
     GROUP BY e.payload->>'family'
), fused_counts AS MATERIALIZED (
    SELECT e.event_type, count(*) AS ambiguous_fused
      FROM core.visible_events e
     WHERE e.event_type = ANY($2::text[]) AND (e.payload->>'ambiguous')::bigint > 1
       AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
     GROUP BY e.event_type
)
SELECT f.family AS "family!", coalesce(p.undecided,0) AS "undecided!",
       coalesce(p.decided,0) AS "decided!", coalesce(c.ambiguous_fused,0) AS "ambiguous_fused!"
  FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS f(family, fused_type, ordinal)
  LEFT JOIN pair_counts p ON p.family = f.family
  LEFT JOIN fused_counts c ON c.event_type = f.fused_type
 ORDER BY f.family, f.ordinal
