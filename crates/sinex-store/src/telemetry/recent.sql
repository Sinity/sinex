SELECT payload AS "payload!: sqlx::types::Json<Sample>"
FROM core.visible_events e
WHERE event_type = $1 AND ts_orig > now() - $2 * interval '1 second'
  AND core.class_visible(e.class)
  -- archived.event_id is a non-null primary key: a missing scalar lookup is
  -- exactly nonmembership. Keep the lookup on these samples rather than
  -- joining the recent window to all historical archive rows.
  AND (SELECT a.event_id FROM audit.archived a WHERE a.event_id = e.id) IS NULL
ORDER BY payload->>'component', payload->>'name', ts_orig, id
