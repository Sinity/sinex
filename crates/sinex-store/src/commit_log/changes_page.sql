-- Select bounded scalar keys from each category before reading event payloads.
-- The scalar head belongs to the reader's repeatable-read snapshot.
WITH event_keys AS MATERIALIZED (
  SELECT e.commit_seq,0::smallint AS kind,0::bigint AS change_seq,e.id AS event_id
  FROM core.events e WHERE e.commit_seq >= $1 AND e.commit_seq <= $7
    AND (e.commit_seq,0::smallint,0::bigint,e.id)>($1,$2,$3,$4)
    AND NOT EXISTS(SELECT 1 FROM core.event_tombstones t WHERE t.event_id=e.id)
  ORDER BY e.commit_seq,e.id LIMIT $5
), archive_keys AS MATERIALIZED (
  SELECT a.commit_seq,1::smallint AS kind,a.change_seq,a.event_id
  FROM audit.archive_changes a WHERE a.commit_seq >= $1 AND a.commit_seq <= $7
    AND (a.commit_seq,1::smallint,a.change_seq,a.event_id)>($1,$2,$3,$4)
  ORDER BY a.commit_seq,a.change_seq,a.event_id LIMIT $5
), tombstone_keys AS MATERIALIZED (
  SELECT t.commit_seq,2::smallint AS kind,0::bigint AS change_seq,t.event_id
  FROM core.event_tombstones t WHERE t.commit_seq >= $1 AND t.commit_seq <= $7
    AND (t.commit_seq,2::smallint,0::bigint,t.event_id)>($1,$2,$3,$4)
  ORDER BY t.commit_seq,t.event_id LIMIT $5
), positions AS MATERIALIZED (
  SELECT commit_seq FROM event_keys
  UNION SELECT commit_seq FROM archive_keys
  UNION SELECT commit_seq FROM tombstone_keys
  UNION
  (SELECT commit_seq FROM core.commit_log WHERE commit_seq >= $1 AND commit_seq <= $7
     AND (commit_seq,9::smallint,0::bigint,'00000000-0000-0000-0000-000000000000'::uuid)>($1,$2,$3,$4)
   ORDER BY commit_seq LIMIT $5)
  UNION
  (SELECT DISTINCT original_commit_seq FROM core.event_tombstones
   WHERE original_commit_seq >= $1 AND original_commit_seq <= $7
     AND (original_commit_seq,9::smallint,0::bigint,'00000000-0000-0000-0000-000000000000'::uuid)>($1,$2,$3,$4)
   ORDER BY original_commit_seq LIMIT $5)
  UNION SELECT $1::bigint WHERE $1>0 AND $1 <= $7
  UNION SELECT $7::bigint WHERE $7 >= $1 AND $7 > 0
), changes AS NOT MATERIALIZED (
  SELECT * FROM event_keys
  UNION ALL SELECT * FROM archive_keys
  UNION ALL SELECT * FROM tombstone_keys
  UNION ALL SELECT p.commit_seq,9::smallint,0::bigint,'00000000-0000-0000-0000-000000000000'::uuid
  FROM positions p WHERE (p.commit_seq,9::smallint,0::bigint,'00000000-0000-0000-0000-000000000000'::uuid)>($1,$2,$3,$4)
), selected AS MATERIALIZED (
  SELECT * FROM changes ORDER BY commit_seq,kind,change_seq,event_id LIMIT $5
), sized AS MATERIALIZED (
  SELECT s.*,CASE WHEN s.kind=0 THEN CASE WHEN $8::boolean THEN e.size ELSE 512::bigint END WHEN s.kind=9 THEN 64::bigint ELSE 256::bigint END AS size
  FROM selected s LEFT JOIN LATERAL (
    SELECT pg_column_size(e)::bigint+octet_length(e.payload::text)::bigint+
      coalesce(cardinality(e.parents),0)::bigint*40+1024 AS size
    FROM core.events e WHERE $8::boolean AND s.kind=0 AND e.id=s.event_id OFFSET 0
  ) e ON true
), bounded AS (
  SELECT s.*,sum(size) OVER(ORDER BY commit_seq,kind,change_seq,event_id)-size AS before FROM sized s
)
SELECT commit_seq AS "position!",kind AS "kind!",change_seq AS "change_seq!",event_id AS "event_id!"
FROM bounded WHERE before < $6::bigint ORDER BY commit_seq,kind,change_seq,event_id
