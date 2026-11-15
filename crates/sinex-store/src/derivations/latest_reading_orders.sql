WITH selected_runs AS MATERIALIZED (
  SELECT id,name FROM core.runs WHERE name=ANY($1::text[]) AND events>0
), maxima AS (
  SELECT r.name,max(e.reading_order) AS reading_order
  FROM core.events e JOIN selected_runs r ON r.id=e.run_id GROUP BY r.name
)
SELECT requested.name AS "name!",coalesce(m.reading_order,0) AS "order!"
FROM (SELECT DISTINCT unnest($1::text[]) AS name) requested
LEFT JOIN maxima m ON m.name=requested.name
