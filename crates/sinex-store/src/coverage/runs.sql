WITH source_runs AS MATERIALIZED (
    SELECT DISTINCT run_id FROM core.events WHERE source = $1
)
SELECT r.started_at, r.ended_at,
    (SELECT max(e.ts_orig) FROM core.events e
     WHERE e.source = $1 AND e.run_id = r.id AND e.commit_seq <= $2) AS last_record
FROM source_runs s JOIN core.runs r ON r.id = s.run_id
WHERE EXISTS (
    SELECT 1 FROM core.events e
    WHERE e.source = $1 AND e.run_id = r.id AND e.commit_seq <= $2
)
ORDER BY r.started_at
