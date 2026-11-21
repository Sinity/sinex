//! Find live output keys affected by an input while admission holds the writer.
use super::*;
use std::collections::{BTreeMap, HashSet};

pub(super) async fn affected_keys(
    conn: &mut PgConnection,
    ranges: Vec<AffectedRange>,
) -> Result<HashSet<(String, Value)>> {
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    for range in ranges {
        let filter = range
            .key_match
            .map(|(field, value)| (range.derivation, field, value));
        groups.entry(filter).or_default().push(range);
    }
    let mut affected = HashSet::new();
    for (filter, ranges) in groups {
        let Some((derivation, field, value)) = filter else {
            overlaps(conn, &ranges, None, &mut affected).await?;
            continue;
        };
        let mut after = None;
        loop {
            let page = sqlx::query_scalar::<_, Value>(include_str!("../affected_key_page.sql"))
                .bind(derivation)
                .bind(after.as_ref())
                .persistent(false)
                .fetch_all(&mut *conn)
                .await?;
            let Some(last) = page.last() else { break };
            after = Some(last.clone());
            let matching: Vec<_> = page
                .into_iter()
                .filter(|key| key.get(field).and_then(Value::as_str) == Some(value))
                .collect();
            if !matching.is_empty() {
                overlaps(conn, &ranges, Some(&matching), &mut affected).await?;
            }
        }
    }
    Ok(affected)
}

async fn overlaps(
    conn: &mut PgConnection,
    ranges: &[AffectedRange],
    matching_keys: Option<&[Value]>,
    affected: &mut HashSet<(String, Value)>,
) -> Result<()> {
    let names: Vec<_> = ranges.iter().map(|r| r.derivation.to_owned()).collect();
    let types: Vec<_> = ranges.iter().map(|r| r.output_type.to_owned()).collect();
    let starts: Vec<_> = ranges.iter().map(|r| r.start_field.to_owned()).collect();
    let ends: Vec<_> = ranges.iter().map(|r| r.end_field.to_owned()).collect();
    let from: Vec<_> = ranges.iter().map(|r| r.start).collect();
    let until: Vec<_> = ranges.iter().map(|r| r.end).collect();
    let open: Vec<_> = ranges
        .iter()
        .map(|r| r.open_field.map(str::to_owned))
        .collect();
    let targets: Vec<_> = names
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let output_types: Vec<_> = types
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    #[derive(sqlx::FromRow)]
    struct Output {
        derivation: String,
        key: Value,
        event_id: Uuid,
    }
    #[derive(sqlx::FromRow)]
    struct AffectedKey {
        #[sqlx(rename = "derivation!")]
        derivation: String,
        #[sqlx(rename = "key!")]
        key: Value,
    }
    let mut after: Option<Output> = None;
    loop {
        // Admission holds the session writer claim, excluding lifecycle and
        // replacement writes between the metadata and payload statements.
        // This is a transfer page, not a limit on the affected output set.
        // Fresh parameter-aware plans keep both keyset seeks and compressed
        // ID-array pruning after repeated executions on this connection.
        let page = sqlx::query_as::<_, Output>(include_str!("../affected_output_page.sql"))
            .bind(&targets)
            .bind(after.as_ref().map(|row| row.derivation.as_str()))
            .bind(after.as_ref().map(|row| &row.key))
            .bind(after.as_ref().map(|row| row.event_id))
            .bind(matching_keys)
            .persistent(false)
            .fetch_all(&mut *conn)
            .await?;
        if page.is_empty() {
            break;
        }
        let ids: Vec<_> = page.iter().map(|row| row.event_id).collect();
        let output_derivations: Vec<_> = page.iter().map(|row| row.derivation.clone()).collect();
        let output_keys: Vec<_> = page.iter().map(|row| row.key.clone()).collect();
        let selected = sqlx::query_as::<_, AffectedKey>(include_str!("../affected_outputs.sql"))
            .bind(&ids)
            .bind(&output_derivations)
            .bind(&output_keys)
            .bind(&output_types)
            .bind(&names)
            .bind(&types)
            .bind(&starts)
            .bind(&ends)
            .bind(&from)
            .bind(&until)
            .bind(&open)
            .persistent(false)
            .fetch_all(&mut *conn)
            .await?;
        affected.extend(selected.into_iter().map(|row| (row.derivation, row.key)));
        after = page.into_iter().last();
    }
    Ok(())
}
