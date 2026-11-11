//! Add stat-cache metadata without moving a source's committed read position.
pub(super) fn statements() -> Vec<String> {
    vec![
        "UPDATE raw.positions SET cursor = cursor || jsonb_build_object(
           'ctime_ns', coalesce(cursor->'ctime_ns', '0'::jsonb),
           'looked_ns', coalesce(cursor->'looked_ns', '0'::jsonb))
         WHERE cursor ?& ARRAY['offset','size','mtime_ns','inode','window','material','part_index']
           AND (NOT cursor ? 'ctime_ns' OR NOT cursor ? 'looked_ns')".into(),
        "UPDATE raw.positions SET cursor = jsonb_set(cursor, '{inner}',
           (cursor->'inner') || jsonb_build_object(
             'ctime_ns', coalesce(cursor#>'{inner,ctime_ns}', '0'::jsonb),
             'looked_ns', coalesce(cursor#>'{inner,looked_ns}', '0'::jsonb)))
         WHERE (cursor->'inner') ?& ARRAY['offset','size','mtime_ns','inode','window','material','part_index']
           AND (NOT (cursor->'inner') ? 'ctime_ns' OR NOT (cursor->'inner') ? 'looked_ns')".into(),
    ]
}
