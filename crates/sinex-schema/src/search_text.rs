//! Rebuild the search read model under the new declarations at its saved commit.
use crate::{Error, tables};
use serde_json::Value;
use sinex_model::kinds::Search;
use sinex_model::{DbCheck, event_types, search_text};
use sqlx::types::Uuid;
use sqlx::{Executor, PgConnection};
use std::collections::BTreeMap;

pub(crate) async fn upgrade(connection: &mut PgConnection) -> Result<(), Error> {
    let bytes_before: i64 = sqlx::query_scalar(
        "SELECT pg_total_relation_size('core.search_text')
              + pg_total_relation_size('core.search_trigram')
              + pg_total_relation_size('core.search_refusals')",
    )
    .fetch_one(&mut *connection)
    .await?;
    connection
        .execute(
            "DROP VIEW core.visible_search_trigram;
             DROP VIEW core.visible_search_text;
             DROP TABLE core.search_trigram;
             DROP TABLE core.search_text;
             DROP TABLE core.search_refusals",
        )
        .await?;
    connection.execute(tables::SEARCH_TEXT).await?;
    let pieces = copy_search_rows(connection).await?;
    connection.execute(tables::SEARCH_WORDS).await?;
    connection.execute(tables::SEARCH_TRIGRAM).await?;
    let bytes_after: i64 = sqlx::query_scalar("SELECT pg_total_relation_size('core.search_text')")
        .fetch_one(&mut *connection)
        .await?;
    eprintln!(
        "sinex schema upgrade: search prepared {pieces} text pieces, {bytes_before} bytes before and {bytes_after} bytes after"
    );
    Ok(())
}

async fn copy_search_rows(connection: &mut PgConnection) -> Result<u64, Error> {
    let outside: Vec<(String, i64, String)> = sqlx::query_as(
        "SELECT name, version, search FROM registry.event_types
         WHERE declaring_producer <> 'sinex' AND search <> 'none'",
    )
    .fetch_all(&mut *connection)
    .await?;
    let outside: BTreeMap<_, _> = outside
        .into_iter()
        .map(|(name, version, mode)| {
            (
                (name, version),
                Search::from_db(&mode).expect("the search check permits only declared modes"),
            )
        })
        .collect();
    let kinds: Vec<&str> = event_types::ALL
        .iter()
        .filter(|kind| kind.search_fields.is_some())
        .map(|kind| kind.event_type)
        .chain(outside.keys().map(|(name, _)| name.as_str()))
        .collect();
    sqlx::query(
        "DECLARE search_events NO SCROLL CURSOR FOR
         SELECT id, event_type, kind_version, payload FROM core.events
         WHERE event_type = ANY($1) AND class <> 'sealed' AND commit_seq <=
           (SELECT commit_seq FROM core.search_position)",
    )
    .bind(&kinds)
    .execute(&mut *connection)
    .await?;
    let mut count = 0;
    loop {
        let rows: Vec<(Uuid, String, i64, Value)> =
            sqlx::query_as("FETCH FORWARD 256 FROM search_events")
                .fetch_all(&mut *connection)
                .await?;
        if rows.is_empty() {
            break;
        }
        let mut ids = Vec::with_capacity(rows.len());
        let mut parts = Vec::with_capacity(rows.len());
        let mut texts = Vec::with_capacity(rows.len());
        let mut trigrams = Vec::with_capacity(rows.len());
        for (id, name, version, payload) in rows {
            let declared = outside
                .get(&(name.clone(), version))
                .copied()
                .unwrap_or(Search::None);
            if let Some(fields) = search_text::fields(&name, declared) {
                let text = search_text::text(&payload, fields.fields);
                for (part, piece) in search_text::pieces(&text).enumerate() {
                    ids.push(id);
                    parts.push(part as i32);
                    texts.push(piece.to_owned());
                    trigrams.push(fields.mode == Search::WordsAndTrigram);
                }
            }
        }
        count += sqlx::query(
            "INSERT INTO core.search_text (event_id, part, text, trigram)
             SELECT * FROM unnest($1::uuid[], $2::integer[], $3::text[], $4::boolean[])",
        )
        .bind(&ids)
        .bind(&parts)
        .bind(texts)
        .bind(&trigrams)
        .execute(&mut *connection)
        .await?
        .rows_affected();
    }
    connection.execute("CLOSE search_events").await?;
    Ok(count)
}
