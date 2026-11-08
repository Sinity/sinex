-- Inclusive UUID bounds prune compressed row groups; ANY keeps exact membership.
-- Materialize payloads for the selected IDs once, before joining page metadata.
WITH page AS MATERIALIZED (
SELECT id FROM(SELECT id,commit_seq,sum(size) OVER(ORDER BY commit_seq,id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)-size AS before
              FROM(SELECT e.id,e.commit_seq,pg_column_size(e)::bigint AS size FROM core.events e
                WHERE e.id=ANY($1) AND (e.commit_seq>$2 OR (e.commit_seq=$2 AND $5::uuid IS NOT NULL AND e.id>$5)) AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)
                ORDER BY e.commit_seq,e.id LIMIT $3) selected) bounded WHERE before<$4::bigint
), payload AS MATERIALIZED (
  SELECT payload_event.* FROM core.events payload_event
  WHERE payload_event.id >= (SELECT id FROM page ORDER BY id LIMIT 1)
    AND payload_event.id <= (SELECT id FROM page ORDER BY id DESC LIMIT 1)
    AND payload_event.id = ANY(ARRAY(SELECT id FROM page))
)
SELECT e.id AS "id!",e.event_type AS "event_type!",e.source AS "source!",e.material_id,e.anchor,e.output_index AS "output_index!",e.parents,
          e.ts_orig,e.ts_quality AS "ts_quality!",e.payload AS "payload!",e.equivalence_key,e.reading_order AS "reading_order!",e.record_hash AS "record_hash!",
          e.record_state AS "record_state!",e.class AS "class!",e.commit_seq AS "commit_seq!",e.kind_version AS "kind_version!",e.validation AS "validation!",
          (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule
          FROM page JOIN payload e ON e.id = page.id
          ORDER BY e.commit_seq,e.id
