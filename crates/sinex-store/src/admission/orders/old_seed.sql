WITH input AS (
      SELECT * FROM unnest($1::text[],$2::text[],$3::bytea[],$4::text[],$5::uuid[],$6::bigint[],$7::bigint[])
        AS i(source,event_type,key_hash,equivalence_key,material_id,anchor,output_index)),
      missing AS (SELECT i.* FROM input i LEFT JOIN core.producer_orders c USING(source,event_type,key_hash) WHERE c.key_hash IS NULL),
      history AS (SELECT source,event_type,key_hash,max(reading_order) AS reading_order FROM (
        SELECT m.source,m.event_type,m.key_hash,e.reading_order FROM missing m JOIN core.events e
          ON e.source=m.source AND e.event_type=m.event_type AND e.equivalence_key=m.equivalence_key WHERE m.equivalence_key IS NOT NULL
        UNION ALL
        SELECT m.source,m.event_type,m.key_hash,e.reading_order FROM missing m JOIN core.events e
          ON e.source=m.source AND e.event_type=m.event_type AND e.material_id=m.material_id AND e.anchor=m.anchor AND e.output_index=m.output_index WHERE m.equivalence_key IS NULL
      ) readings GROUP BY source,event_type,key_hash)
      SELECT i.source AS "source!",i.event_type AS "event_type!",i.key_hash AS "key_hash!",coalesce(c.reading_order,h.reading_order) AS base
      FROM input i LEFT JOIN core.producer_orders c USING(source,event_type,key_hash) LEFT JOIN history h USING(source,event_type,key_hash)
