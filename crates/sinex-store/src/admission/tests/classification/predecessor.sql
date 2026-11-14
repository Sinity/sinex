WITH s AS (
             SELECT st.*,
               row_number() OVER (
                 PARTITION BY st.source, st.event_type, st.equivalence_key IS NOT NULL,
                   coalesce(st.equivalence_key, st.material_id::text || ':' || st.anchor || ':' || st.output_index)
                 -- Equal reading orders with different content: the
                 -- greater content hash wins, here and against the live
                 -- reading below, so arrival never decides.
                 ORDER BY st.reading_order DESC, st.record_hash DESC, st.ord DESC) AS rank
             FROM core.admission_stage st)
           SELECT s.ord AS "ord!",
             CASE
               WHEN s.rank > 1 THEN 'superseded'
               WHEN coalesce(le.event_id, lc.event_id) IS NULL THEN 'new'
               WHEN coalesce(le.record_hash, lc.record_hash) = s.record_hash THEN 'repeat'
               -- The source dropping a record is a fact, not another
               -- reading of the same content: it replaces even where
               -- repeats are suppressed.
               WHEN s.record_state = 'removed'
                 AND (s.reading_order, s.record_hash)
                   > (coalesce(le.reading_order, lc.reading_order), coalesce(le.record_hash, lc.record_hash)) THEN 'newer'
               WHEN s.replace_rule = 'suppress_repeat' THEN 'repeat'
               WHEN (s.reading_order, s.record_hash)
                   > (coalesce(le.reading_order, lc.reading_order), coalesce(le.record_hash, lc.record_hash)) THEN 'newer'
               ELSE 'older'
             END AS "state!",
             coalesce(le.event_id, lc.event_id) AS live_id
           FROM s
           LEFT JOIN core.live_equivalence le
             ON s.equivalence_key IS NOT NULL AND le.source = s.source
             AND le.event_type = s.event_type AND le.equivalence_key = core.encode_equivalence_key(s.equivalence_key)
           LEFT JOIN core.live_locations lc
             ON s.equivalence_key IS NULL AND s.material_id IS NOT NULL
             AND lc.material_id = s.material_id AND lc.anchor = s.anchor
             AND lc.event_type = s.event_type AND lc.output_index = s.output_index
           ORDER BY s.ord
