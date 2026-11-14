-- All five values belong to this statement's one MVCC snapshot. Capturing
-- the range boundaries in a later statement could hide an intervening input.
SELECT
    (SELECT coalesce(max(commit_seq), 0)
     FROM (
         SELECT commit_seq FROM core.events WHERE event_type = ANY($1)
         -- Keep the kind-index scan below the aggregate. Minmax lowering
         -- otherwise searches each chunk's global feed through unrelated rows.
         OFFSET 0
     ) selected) AS "commit",
    (SELECT coalesce(max(a.change_seq), 0)
     FROM audit.archive_changes a JOIN core.events e ON e.id = a.event_id
     WHERE e.event_type = ANY($1)) AS changes,
    (SELECT value FROM core.counters WHERE name = 'tombstoned') AS tombstoned,
    (SELECT value FROM core.counters WHERE name = 'commit_seq') AS through_commit,
    (SELECT coalesce(max(change_seq), 0) FROM audit.archive_changes) AS through_changes
