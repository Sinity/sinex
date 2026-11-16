-- A scalar head bounds every branch, so chunk skipping on the commit position
-- prunes compressed chunks. Payloads are read afterwards, once, by ID.
WITH head AS MATERIALIZED (
  SELECT value FROM core.counters WHERE name='commit_seq'
), event_keys AS MATERIALIZED (
  SELECT e.commit_seq,e.id,e.event_type,e.ts_orig FROM core.events e
  WHERE e.commit_seq > $1 AND e.commit_seq <= (SELECT value FROM head)
    AND (SELECT value FROM head)>$1
  ORDER BY e.commit_seq,e.id LIMIT $2
)
SELECT p.commit_seq AS "commit_seq!", p.id, p.event_type, p.ts_orig FROM (
  (SELECT commit_seq,id,event_type,ts_orig FROM event_keys)
  UNION ALL
  (SELECT l.commit_seq,NULL::uuid,NULL::text,NULL::timestamptz FROM core.commit_log l
   WHERE l.commit_seq>$1 AND l.commit_seq <= (SELECT value FROM head)
     AND (SELECT value FROM head)>$1
   ORDER BY l.commit_seq LIMIT $2)
) p
ORDER BY p.commit_seq LIMIT $2
