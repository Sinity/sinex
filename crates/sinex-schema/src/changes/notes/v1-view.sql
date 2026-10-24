SELECT id,
    source,
    ts_orig,
    ts_quality,
    ts_coided,
    record_state,
    payload -> 'vault'::text AS vault,
    payload -> 'path'::text AS path,
    payload -> 'text'::text AS text,
    payload -> 'size'::text AS size,
    payload -> 'modified_at'::text AS modified_at,
    payload -> 'vanished_at'::text AS vanished_at
   FROM core.visible_events e
  WHERE event_type = 'note.document'::text AND NOT (EXISTS ( SELECT 1
           FROM audit.archived a
          WHERE a.event_id = e.id));
