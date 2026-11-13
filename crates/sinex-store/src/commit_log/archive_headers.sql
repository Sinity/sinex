SELECT a.change_seq,a.event_id,a.commit_seq,a.archived,a.reason,a.replaced_by,a.operation_id,a.recorded_at,
  coalesce(e.event_type,t.event_type) AS event_type,coalesce(e.class,t.class) AS class
FROM audit.archive_changes a
LEFT JOIN (SELECT e.id,e.event_type,e.class FROM core.events e WHERE e.id=ANY($2)) e ON e.id=a.event_id
LEFT JOIN core.event_tombstones t ON t.event_id=a.event_id
WHERE a.change_seq=ANY($1)
