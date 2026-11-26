WITH payloads AS MATERIALIZED (
  SELECT id,event_type,payload FROM core.events
  WHERE id=ANY($1::uuid[]) AND event_type=ANY($4::text[])
), ranges AS MATERIALIZED (
  SELECT * FROM unnest($5::text[],$6::text[],$7::text[],$8::text[],
    $9::timestamptz[],$10::timestamptz[],$11::text[],$12::text[])
    AS r(derivation,event_type,start_field,end_field,start,finish,open_field,fence_field)
)
SELECT DISTINCT o.derivation AS "derivation!",o.key AS "key!"
FROM unnest($2::text[],$3::jsonb[],$1::uuid[]) AS o(derivation,key,event_id)
JOIN payloads e ON e.id=o.event_id
WHERE EXISTS (
  SELECT 1 FROM ranges r WHERE r.derivation=o.derivation AND r.event_type=e.event_type
    AND (e.payload->>r.start_field)::timestamptz <= r.finish
    -- An output with no end is bounded by its fence, when it has one.
    AND (coalesce(e.payload->>r.end_field, e.payload->>r.fence_field) IS NULL
      OR coalesce(e.payload->>r.end_field, e.payload->>r.fence_field)::timestamptz >= r.start
      OR coalesce((e.payload->>r.open_field)::boolean,false))
)
