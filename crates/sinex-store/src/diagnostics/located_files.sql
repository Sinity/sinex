-- Reading order is acquisition order. A replay of an old listing must not
-- turn its old path/hash back into the newest filesystem observation.
WITH event_facts AS MATERIALIZED (
    SELECT e.id, e.material_id, e.reading_order, e.commit_seq,
           e.payload->>'host' AS host,
           e.payload->>'root' AS root,
           e.payload->>'path' AS path,
           e.payload->>'native_path_bytes' AS native_path,
           e.payload->>'content' AS content,
           e.payload->'located' = 'true'::jsonb AS located,
           e.payload->>'removed_at' AS removed_at,
           EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id) AS archived
    FROM core.events e
    JOIN core.live_locations l ON l.event_id=e.id
    WHERE l.event_type='document.captured' AND e.event_type='document.captured'
), listing_roots AS MATERIALIZED (
    -- A listing can describe thousands of files. Decode its manifest once,
    -- rather than detoasting that entire manifest for every reading.
    SELECT m.id, m.manifest->>'native_path_bytes' AS native_root
    FROM raw.materials m
    JOIN (SELECT DISTINCT material_id FROM event_facts) f ON f.material_id=m.id
), readings AS MATERIALIZED (
    SELECT e.*, r.native_root
    FROM event_facts e JOIN listing_roots r ON r.id=e.material_id
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
