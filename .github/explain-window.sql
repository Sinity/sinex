-- Temporary query measurement on synthetic rows; the transaction rolls back.
\set ON_ERROR_STOP on
BEGIN;
INSERT INTO core.events(id,event_type,source,run_id,parents,ts_orig,ts_quality,commit_seq,payload,reading_order,record_hash)
SELECT uuidv7(),'synthetic.window','synthetic.window',uuidv7(),ARRAY[uuidv7()],
       '2026-03-01T00:00:00Z'::timestamptz+n*interval '1 second','intrinsic',n,
       jsonb_build_object('number',n,'text',repeat(md5(n::text),128)),n,decode(repeat(md5(n::text),2),'hex')
FROM generate_series(1,10000)n;
ANALYZE core.events;
-- These synthetic rows are uncompressed. Inserts and ANALYZE warm the table.
-- Execute both page statements
-- before collecting plans, so these are warmed-buffer observations, not cold reads.
\o /dev/null
-- Inclusive UUID bounds prune compressed row groups; ANY keeps exact membership.
-- Materialize payloads for the selected IDs once, before joining page metadata.
WITH page AS MATERIALIZED (
SELECT p.id, p.matched FROM (
               SELECT q.id, count(*) OVER () AS matched,
                      (sum(q.size) OVER (ORDER BY q.ts_orig, q.commit_seq) - q.size)::bigint AS before
               FROM (
                 SELECT s.id, s.ts_orig, s.commit_seq, pg_column_size(s.payload)::bigint AS size
                 FROM core.events s
                 WHERE s.event_type = ANY(ARRAY['synthetic.window']::text[]) AND s.ts_orig IS NOT NULL
                   AND ('2026-03-01T00:00:00Z'::timestamptz IS NULL OR s.ts_orig >= '2026-03-01T00:00:00Z'::timestamptz)
                   AND ('2026-03-02T00:00:00Z'::timestamptz IS NULL OR s.ts_orig < '2026-03-02T00:00:00Z'::timestamptz)
                   AND EXISTS (SELECT 1 FROM unnest(ARRAY['synthetic.window']::text[], ARRAY['{}'::jsonb]::jsonb[]) AS k(event_type, contains)
                               WHERE k.event_type = s.event_type AND s.payload @> k.contains)
                   AND (NULL::timestamptz IS NULL OR (s.ts_orig, s.commit_seq) > (NULL::timestamptz, NULL::bigint))
                   AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
                 ORDER BY s.ts_orig, s.commit_seq
                 LIMIT 256::bigint
               ) q
             ) p WHERE p.before < 1048576::bigint
), payload AS MATERIALIZED (
  SELECT payload_event.* FROM core.events payload_event
  WHERE payload_event.id >= (SELECT id FROM page ORDER BY id LIMIT 1)
    AND payload_event.id <= (SELECT id FROM page ORDER BY id DESC LIMIT 1)
    AND payload_event.id = ANY(ARRAY(SELECT id FROM page))
)
SELECT e.id AS "id!", e.event_type AS "event_type!", e.source AS "source!", e.material_id, e.record_offset, e.output_index AS "output_index!", e.parents,
             e.ts_orig, e.ts_quality AS "ts_quality!", e.payload AS "payload!", e.equivalence_key, e.reading_order AS "reading_order!", e.record_hash AS "record_hash!",
             e.record_state AS "record_state!", e.class AS "class!", e.commit_seq AS "commit_seq!", e.kind_version AS "kind_version!", e.validation AS "validation!",
             (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule, page.matched AS "matched!"
           FROM page JOIN payload e ON e.id = page.id
           ORDER BY e.ts_orig, e.commit_seq;
-- Positions only; the shared event reader fetches payloads in bounded groups.
WITH page AS MATERIALIZED (
SELECT p.id, p.commit_seq, p.event_type, p.ts_orig, p.matched FROM (
               SELECT q.id, q.commit_seq, q.event_type, q.ts_orig, count(*) OVER () AS matched,
                      (sum(q.size) OVER (ORDER BY q.ts_orig, q.commit_seq) - q.size)::bigint AS before
               FROM (
                 SELECT s.id, s.event_type, s.ts_orig, s.commit_seq, pg_column_size(s.payload)::bigint AS size
                 FROM core.events s
                 WHERE s.event_type = ANY(ARRAY['synthetic.window']::text[]) AND s.ts_orig IS NOT NULL
                   AND ('2026-03-01T00:00:00Z'::timestamptz IS NULL OR s.ts_orig >= '2026-03-01T00:00:00Z'::timestamptz)
                   AND ('2026-03-02T00:00:00Z'::timestamptz IS NULL OR s.ts_orig < '2026-03-02T00:00:00Z'::timestamptz)
                   AND EXISTS (SELECT 1 FROM unnest(ARRAY['synthetic.window']::text[], ARRAY['{}'::jsonb]::jsonb[]) AS k(event_type, contains)
                               WHERE k.event_type = s.event_type AND s.payload @> k.contains)
                   AND (NULL::timestamptz IS NULL OR (s.ts_orig, s.commit_seq) > (NULL::timestamptz, NULL::bigint))
                   AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
                 ORDER BY s.ts_orig, s.commit_seq
                 LIMIT 256::bigint
               ) q
             ) p WHERE p.before < 1048576::bigint
)
SELECT id AS "id!", commit_seq AS "commit_seq!", event_type AS "event_type!", ts_orig, matched AS "matched!"
FROM page
ORDER BY ts_orig, commit_seq;
\o
\echo before_window_page
EXPLAIN (ANALYZE, BUFFERS) -- Inclusive UUID bounds prune compressed row groups; ANY keeps exact membership.
-- Materialize payloads for the selected IDs once, before joining page metadata.
WITH page AS MATERIALIZED (
SELECT p.id, p.matched FROM (
               SELECT q.id, count(*) OVER () AS matched,
                      (sum(q.size) OVER (ORDER BY q.ts_orig, q.commit_seq) - q.size)::bigint AS before
               FROM (
                 SELECT s.id, s.ts_orig, s.commit_seq, pg_column_size(s.payload)::bigint AS size
                 FROM core.events s
                 WHERE s.event_type = ANY(ARRAY['synthetic.window']::text[]) AND s.ts_orig IS NOT NULL
                   AND ('2026-03-01T00:00:00Z'::timestamptz IS NULL OR s.ts_orig >= '2026-03-01T00:00:00Z'::timestamptz)
                   AND ('2026-03-02T00:00:00Z'::timestamptz IS NULL OR s.ts_orig < '2026-03-02T00:00:00Z'::timestamptz)
                   AND EXISTS (SELECT 1 FROM unnest(ARRAY['synthetic.window']::text[], ARRAY['{}'::jsonb]::jsonb[]) AS k(event_type, contains)
                               WHERE k.event_type = s.event_type AND s.payload @> k.contains)
                   AND (NULL::timestamptz IS NULL OR (s.ts_orig, s.commit_seq) > (NULL::timestamptz, NULL::bigint))
                   AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
                 ORDER BY s.ts_orig, s.commit_seq
                 LIMIT 256::bigint
               ) q
             ) p WHERE p.before < 1048576::bigint
), payload AS MATERIALIZED (
  SELECT payload_event.* FROM core.events payload_event
  WHERE payload_event.id >= (SELECT id FROM page ORDER BY id LIMIT 1)
    AND payload_event.id <= (SELECT id FROM page ORDER BY id DESC LIMIT 1)
    AND payload_event.id = ANY(ARRAY(SELECT id FROM page))
)
SELECT e.id AS "id!", e.event_type AS "event_type!", e.source AS "source!", e.material_id, e.record_offset, e.output_index AS "output_index!", e.parents,
             e.ts_orig, e.ts_quality AS "ts_quality!", e.payload AS "payload!", e.equivalence_key, e.reading_order AS "reading_order!", e.record_hash AS "record_hash!",
             e.record_state AS "record_state!", e.class AS "class!", e.commit_seq AS "commit_seq!", e.kind_version AS "kind_version!", e.validation AS "validation!",
             (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule, page.matched AS "matched!"
           FROM page JOIN payload e ON e.id = page.id
           ORDER BY e.ts_orig, e.commit_seq;
\echo after_window_positions
EXPLAIN (ANALYZE, BUFFERS) -- Positions only; the shared event reader fetches payloads in bounded groups.
WITH page AS MATERIALIZED (
SELECT p.id, p.commit_seq, p.event_type, p.ts_orig, p.matched FROM (
               SELECT q.id, q.commit_seq, q.event_type, q.ts_orig, count(*) OVER () AS matched,
                      (sum(q.size) OVER (ORDER BY q.ts_orig, q.commit_seq) - q.size)::bigint AS before
               FROM (
                 SELECT s.id, s.event_type, s.ts_orig, s.commit_seq, pg_column_size(s.payload)::bigint AS size
                 FROM core.events s
                 WHERE s.event_type = ANY(ARRAY['synthetic.window']::text[]) AND s.ts_orig IS NOT NULL
                   AND ('2026-03-01T00:00:00Z'::timestamptz IS NULL OR s.ts_orig >= '2026-03-01T00:00:00Z'::timestamptz)
                   AND ('2026-03-02T00:00:00Z'::timestamptz IS NULL OR s.ts_orig < '2026-03-02T00:00:00Z'::timestamptz)
                   AND EXISTS (SELECT 1 FROM unnest(ARRAY['synthetic.window']::text[], ARRAY['{}'::jsonb]::jsonb[]) AS k(event_type, contains)
                               WHERE k.event_type = s.event_type AND s.payload @> k.contains)
                   AND (NULL::timestamptz IS NULL OR (s.ts_orig, s.commit_seq) > (NULL::timestamptz, NULL::bigint))
                   AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
                 ORDER BY s.ts_orig, s.commit_seq
                 LIMIT 256::bigint
               ) q
             ) p WHERE p.before < 1048576::bigint
)
SELECT id AS "id!", commit_seq AS "commit_seq!", event_type AS "event_type!", ts_orig, matched AS "matched!"
FROM page
ORDER BY ts_orig, commit_seq;
CREATE TEMP TABLE selected_events AS -- Positions only; the shared event reader fetches payloads in bounded groups.
WITH page AS MATERIALIZED (
SELECT p.id, p.commit_seq, p.event_type, p.ts_orig, p.matched FROM (
               SELECT q.id, q.commit_seq, q.event_type, q.ts_orig, count(*) OVER () AS matched,
                      (sum(q.size) OVER (ORDER BY q.ts_orig, q.commit_seq) - q.size)::bigint AS before
               FROM (
                 SELECT s.id, s.event_type, s.ts_orig, s.commit_seq, pg_column_size(s.payload)::bigint AS size
                 FROM core.events s
                 WHERE s.event_type = ANY(ARRAY['synthetic.window']::text[]) AND s.ts_orig IS NOT NULL
                   AND ('2026-03-01T00:00:00Z'::timestamptz IS NULL OR s.ts_orig >= '2026-03-01T00:00:00Z'::timestamptz)
                   AND ('2026-03-02T00:00:00Z'::timestamptz IS NULL OR s.ts_orig < '2026-03-02T00:00:00Z'::timestamptz)
                   AND EXISTS (SELECT 1 FROM unnest(ARRAY['synthetic.window']::text[], ARRAY['{}'::jsonb]::jsonb[]) AS k(event_type, contains)
                               WHERE k.event_type = s.event_type AND s.payload @> k.contains)
                   AND (NULL::timestamptz IS NULL OR (s.ts_orig, s.commit_seq) > (NULL::timestamptz, NULL::bigint))
                   AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = s.id)
                 ORDER BY s.ts_orig, s.commit_seq
                 LIMIT 256::bigint
               ) q
             ) p WHERE p.before < 1048576::bigint
)
SELECT id AS "id!", commit_seq AS "commit_seq!", event_type AS "event_type!", ts_orig, matched AS "matched!"
FROM page
ORDER BY ts_orig, commit_seq;
\echo after_window_rows_in_groups_of_64
-- Substitute each group's IDs and bounds before planning, as the client does.
SELECT format($plan$
EXPLAIN (ANALYZE, BUFFERS)
-- Inclusive ID and time bounds and the kinds skip compressed batches by their
-- order and segment columns; the ID list keeps exact membership.
SELECT e.id, e.event_type, e.kind_version, e.validation, e.source, e.run_id, e.material_id,
       e.record_offset, e.output_index, e.parents, e.ts_orig, e.ts_quality, e.ts_coided,
       e.ts_persisted, e.payload, e.equivalence_key, e.reading_order, e.record_hash,
       e.record_state, e.class, e.commit_seq,
       (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule
FROM core.events e
WHERE e.id = ANY(%L::uuid[]) AND e.id >= %L::uuid AND e.id <= %L::uuid AND e.event_type = ANY(ARRAY['synthetic.window']::text[])
  AND e.ts_orig >= %L::timestamptz AND e.ts_orig <= %L::timestamptz
$plan$,
    array_agg("id!" ORDER BY ts_orig, "commit_seq!"),
    (array_agg("id!" ORDER BY "id!"))[1],
    (array_agg("id!" ORDER BY "id!" DESC))[1],
    min(ts_orig), max(ts_orig))
FROM (
    SELECT *, (row_number() OVER (ORDER BY ts_orig, "commit_seq!") - 1) / 64 AS group_number
    FROM selected_events
) numbered_events
GROUP BY group_number
ORDER BY group_number
\gexec
ROLLBACK;
