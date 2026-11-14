SELECT e.id AS "id!", e.event_type AS "event_type!", e.source AS "source!", e.material_id, e.anchor, e.output_index AS "output_index!", e.parents,
             e.ts_orig, e.ts_quality AS "ts_quality!", e.payload AS "payload!", e.equivalence_key, e.reading_order AS "reading_order!", e.record_hash AS "record_hash!",
             e.record_state AS "record_state!", e.class AS "class!", e.commit_seq AS "commit_seq!", e.kind_version AS "kind_version!", e.validation AS "validation!",
             (SELECT replace_rule FROM registry.event_types k WHERE k.name=e.event_type AND k.version=e.kind_version) AS replace_rule
           FROM core.events e
           WHERE e.source=$1 AND $2=ANY(e.parents)
             AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)
           ORDER BY e.ts_orig,e.commit_seq
