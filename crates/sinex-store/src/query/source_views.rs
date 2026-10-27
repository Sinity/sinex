//! Source views expose the evidence held, including where continuity is unknown.

pub(super) fn field(name: &str) -> Option<String> {
    Some(match name {
        "continuity" => "(CASE WHEN {t}.driver='live-stream' AND EXISTS (SELECT 1 FROM core.visible_events e WHERE e.source={t}.name) THEN 'run_lifetimes' ELSE 'unknown' END)".into(),
        "gaps" => "(SELECT coalesce(jsonb_agg(jsonb_build_object('from',prior_end,'to',started_at,'reason','between recorded live runs') ORDER BY started_at), '[]'::jsonb) FROM (
            SELECT started_at, max(ended_at) OVER (ORDER BY started_at,ended_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_end FROM (
              SELECT r.id,r.started_at,coalesce(r.ended_at,max(e.ts_orig)) AS ended_at
              FROM core.visible_events e JOIN core.runs r ON r.id=e.run_id
              WHERE e.source={t}.name AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)
              GROUP BY r.id,r.started_at,r.ended_at
            ) runs
          ) spans WHERE {t}.driver='live-stream' AND prior_end < started_at)".into(),
        "drift" => "(SELECT jsonb_build_object('status',CASE WHEN count(*)=0 THEN 'unknown' ELSE 'changes_recorded' END,
            'reason','only payload field changes explicitly recorded by parsers are known',
            'events',coalesce(jsonb_agg(jsonb_build_object('event',e.id,'when',e.ts_orig,'unknown_fields',e.payload->'unknown_fields') ORDER BY e.ts_orig,e.id),'[]'::jsonb))
            FROM core.visible_events e WHERE e.source={t}.name AND jsonb_typeof(e.payload->'unknown_fields')='array'
            AND e.payload->'unknown_fields' <> '[]'::jsonb
            AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id))".into(),
        "record_failures" => "(SELECT coalesce(jsonb_agg(jsonb_build_object('material',m.id,'offset',f.anchor,'when',f.failed_at,'reason',f.reason) ORDER BY f.failed_at,f.id),'[]'::jsonb)
            FROM raw.failures f JOIN raw.materials m ON m.id=f.material_id JOIN raw.inputs i ON i.id=m.input_id
            JOIN raw.bindings b ON b.binding=i.binding WHERE b.source={t}.name)".into(),
        _ => return None,
    })
}
