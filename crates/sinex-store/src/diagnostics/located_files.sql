-- Reading order is acquisition order. A replay of an old listing must not
-- turn its old path/hash back into the newest filesystem observation.
WITH readings AS MATERIALIZED (
    SELECT e.id, e.reading_order, e.commit_seq,
           e.payload->>'host' AS host,
           e.payload->>'root' AS root,
           e.payload->>'path' AS path,
           e.payload->>'native_path_bytes' AS native_path,
           m.manifest->>'native_path_bytes' AS native_root,
           e.payload->>'content' AS content,
           e.payload->'located' = 'true'::jsonb AS located,
           e.payload->>'removed_at' AS removed_at,
           EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id) AS archived
    FROM core.events e
    JOIN core.live_locations l ON l.event_id=e.id
    JOIN raw.materials m ON m.id=e.material_id
    WHERE l.event_type='document.captured' AND e.event_type='document.captured'
), located_contents AS (
    SELECT DISTINCT content FROM readings
    WHERE located AND NOT archived AND content IS NOT NULL
), current_paths AS (
    SELECT DISTINCT ON (host, root, native_root, path, native_path)
           root, native_root, path, native_path, content, removed_at
    FROM readings
    ORDER BY host, root, native_root, path, native_path,
             reading_order DESC, commit_seq DESC, id DESC
)
SELECT c.content AS "content!", p.root, p.native_root, p.path, p.native_path
FROM located_contents c
LEFT JOIN current_paths p ON p.content=c.content AND p.removed_at IS NULL
