-- Seek past every member of the previous key with (derivation, key, event_id).
WITH RECURSIVE keys(n, key) AS (
    (SELECT 1, key FROM derivation.outputs
     WHERE derivation=$1 AND ($2::jsonb IS NULL OR key>$2)
     ORDER BY key LIMIT 1)
    UNION ALL
    SELECT k.n+1, next.key FROM keys k
    CROSS JOIN LATERAL (
        SELECT key FROM derivation.outputs
        WHERE derivation=$1 AND key>k.key
        ORDER BY key LIMIT 1
    ) next
    WHERE k.n<256
)
SELECT key AS "key!" FROM keys ORDER BY n
