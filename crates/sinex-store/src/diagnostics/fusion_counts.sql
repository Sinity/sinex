SELECT f.family AS "family!", coalesce(p.undecided,0) AS "undecided!",
       coalesce(p.decided,0) AS "decided!", coalesce(c.ambiguous,0) AS "ambiguous_fused!"
  FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS f(family, fused_type, ordinal)
  LEFT JOIN LATERAL (
    SELECT sum(count) FILTER (WHERE figure = 'undecided')::bigint AS undecided,
           sum(count) FILTER (WHERE figure = 'decided')::bigint AS decided
    FROM core.visible_fusion_counts WHERE name = f.family AND figure IN ('undecided', 'decided')
  ) p ON true
  LEFT JOIN LATERAL (
    SELECT sum(count)::bigint AS ambiguous FROM core.visible_fusion_counts
    WHERE name = f.fused_type AND figure = 'ambiguous'
  ) c ON true
 ORDER BY f.family, f.ordinal
