DROP INDEX core.events_coverage;
DROP VIEW IF EXISTS event_types.operator_mark, event_types.query_saved;
DELETE FROM registry.event_types WHERE name IN ('operator.mark','query.saved');
DELETE FROM sinex_schema.applied;
INSERT INTO sinex_schema.applied (version) VALUES (2);
