-- The writer owns this stage and the live tables for the whole transaction.
-- Keep payloads out of the classification snapshot and encode each logical
-- key once, before any live-key comparison.
WITH s AS MATERIALIZED (
    SELECT st.ord, st.source, st.event_type, st.material_id, st.anchor,
           st.output_index, st.equivalence_key IS NOT NULL AS keyed,
           core.encode_equivalence_key(st.equivalence_key) AS encoded_key,
           st.reading_order, st.record_hash, st.record_state, st.replace_rule,
           row_number() OVER (
               PARTITION BY st.source, st.event_type, st.equivalence_key IS NOT NULL,
                   coalesce(st.equivalence_key, st.material_id::text || ':' || st.anchor || ':' || st.output_index)
               ORDER BY st.reading_order DESC, st.record_hash DESC, st.ord DESC) AS rank
    FROM core.admission_stage st
)
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
           LEFT JOIN LATERAL (
               SELECT event_id, reading_order, record_hash
               FROM core.live_equivalence le
               WHERE s.rank = 1 AND s.keyed
                 AND le.source = s.source AND le.event_type = s.event_type
                 AND le.equivalence_key = s.encoded_key
               -- This lookup is one unique key, never a join over live history.
               OFFSET 0
           ) le ON true
           LEFT JOIN LATERAL (
               SELECT event_id, reading_order, record_hash
               FROM core.live_locations lc
               WHERE s.rank = 1 AND NOT s.keyed AND s.material_id IS NOT NULL
                 AND lc.material_id = s.material_id AND lc.anchor = s.anchor
                 AND lc.event_type = s.event_type AND lc.output_index = s.output_index
               OFFSET 0
           ) lc ON true
           ORDER BY s.ord
