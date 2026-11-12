//! Separate trigram text avoids rewriting the full-text heap to change index participation.
use sinex_model::{event_types, kinds::Search};

pub(crate) const TABLE: &str = "CREATE TABLE core.search_trigram (
  event_id uuid PRIMARY KEY,
  text text NOT NULL
)";
pub(crate) const INDEX: &str =
    "CREATE INDEX search_trigram_text ON core.search_trigram USING gin (text gin_trgm_ops)";

/// The preceding stored format, needed only by historical schema upgrades.
pub(crate) const PREVIOUS_VIEW: &str =
    "CREATE VIEW core.visible_search_text AS SELECT s.event_id, s.text, s.tsv, s.trigram_indexed
FROM core.search_text s JOIN core.events e ON e.id = s.event_id
WHERE core.class_visible(e.class)";

pub(crate) fn statements() -> Vec<String> {
    let excluded = event_types::ALL
        .iter()
        .filter(|kind| kind.search != Search::WordsAndTrigram)
        .map(|kind| format!("'{}'", kind.event_type.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",");
    vec![
        // Retire the predecessor before copying. The schema transaction
        // restores it if any later statement fails.
        "DROP INDEX IF EXISTS core.search_text_trigram".into(),
        "DROP INDEX IF EXISTS core.search_text_trigram_partial".into(),
        TABLE.into(),
        format!(
            "INSERT INTO core.search_trigram (event_id, text)
          SELECT s.event_id, s.text FROM core.search_text s
          JOIN core.events e ON e.id=s.event_id
          LEFT JOIN registry.event_types k ON k.name=e.event_type AND k.version=e.kind_version
          WHERE e.class<>'sealed' AND e.event_type<>ALL(ARRAY[{excluded}]::text[])
            AND coalesce(k.search,'words_and_trigram')='words_and_trigram'"
        ),
        INDEX.into(),
        "DROP VIEW core.visible_search_text".into(),
        "ALTER TABLE core.search_text DROP COLUMN trigram_indexed".into(),
        "DROP TABLE IF EXISTS sinex_schema.search_trigram_rewrite".into(),
    ]
}

/// Descriptions before this forward step keep exactly their stored shape.
pub(crate) fn previous(tables: &mut Vec<String>, visibility: &mut Vec<String>) {
    tables.retain(|sql| sql != TABLE && sql != INDEX);
    for sql in tables.iter_mut() {
        if sql.starts_with("CREATE TABLE core.search_text (") {
            *sql = sql.replace(
                "tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED\n)",
                "tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED,\n  trigram_indexed boolean NOT NULL DEFAULT true\n)",
            );
        }
    }
    tables.push("CREATE INDEX search_text_trigram ON core.search_text USING gin (text gin_trgm_ops) WHERE trigram_indexed".into());
    visibility.retain(|sql| !sql.starts_with("CREATE VIEW core.visible_search_trigram AS"));
    for sql in visibility {
        if sql.starts_with("CREATE VIEW core.visible_search_text AS") {
            *sql = PREVIOUS_VIEW.into();
        }
    }
}

#[cfg(test)]
mod tests;
