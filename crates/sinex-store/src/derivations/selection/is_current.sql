SELECT
    (SELECT value FROM core.counters WHERE name = 'tombstoned') = $4
    AND NOT EXISTS (
        SELECT 1 FROM core.events e
        WHERE e.commit_seq > $2 AND e.event_type = ANY($1)
    )
    AND NOT EXISTS (
        SELECT 1 FROM audit.archive_changes a
        CROSS JOIN LATERAL (
            SELECT e.event_type FROM core.events e WHERE e.id = a.event_id
            -- Probe only events named by the newer change range, never join
            -- that range to the complete compressed event history.
            OFFSET 0
        ) e
        WHERE a.change_seq > $3 AND e.event_type = ANY($1)
    ) AS "current!"
