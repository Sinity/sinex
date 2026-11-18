SELECT requested.name AS "name!", coalesce(p.reading_order,0) AS "order!"
FROM (SELECT DISTINCT unnest($1::text[]) AS name) requested
LEFT JOIN derivation.positions p ON p.derivation=requested.name
