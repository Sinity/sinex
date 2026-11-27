DROP VIEW core.visible_search_text;
DROP TABLE core.search_text;
CREATE TABLE core.search_text (
  event_id uuid PRIMARY KEY,
  text text NOT NULL,
  tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED
);
CREATE INDEX search_text_tsv ON core.search_text USING gin (tsv);
CREATE TABLE core.search_trigram (
  event_id uuid PRIMARY KEY,
  text text NOT NULL
);
CREATE INDEX search_trigram_text ON core.search_trigram USING gin (text gin_trgm_ops);
CREATE TABLE core.search_refusals (
  event_id uuid PRIMARY KEY,
  reason text NOT NULL,
  refused_at timestamptz NOT NULL DEFAULT now()
);
CREATE VIEW core.visible_search_text AS
  SELECT s.event_id, s.text, s.tsv
  FROM core.search_text s JOIN core.events e ON e.id = s.event_id
  WHERE core.class_visible(e.class);
CREATE VIEW core.visible_search_trigram AS
  SELECT s.event_id, s.text
  FROM core.search_trigram s JOIN core.events e ON e.id = s.event_id
  WHERE core.class_visible(e.class);
GRANT SELECT ON core.visible_search_text, core.visible_search_trigram TO sinex_reader;
