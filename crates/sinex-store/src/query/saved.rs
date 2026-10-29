//! Saved question resolution shared by query authoring and stored selections.
use super::*;
use sinex_model::event_types::query::Saved;
use sinex_model::query::saved::SavedQuery;
use std::collections::BTreeMap;

pub async fn saved_records(conn: &mut PgConnection) -> Result<Vec<Saved>> {
    let mut records = Vec::new();
    for row in crate::live::of_types(
        conn,
        &[<Saved as sinex_model::EventPayload>::EVENT_TYPE.into()],
    )
    .await?
    {
        let Some(payload) = row
            .payload
            .filter(|payload| payload.is_object() && payload.get("withheld").is_none())
        else {
            continue;
        };
        records.push(
            serde_json::from_value::<Saved>(payload)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        );
    }
    records.sort_by(|a, b| (&a.name, &a.by).cmp(&(&b.name, &b.by)));
    Ok(records)
}

pub async fn saved_definitions(
    conn: &mut PgConnection,
    requested: &str,
) -> Result<BTreeMap<String, SavedQuery>> {
    let mut all: BTreeMap<String, Vec<(String, SavedQuery)>> = BTreeMap::new();
    for record in saved_records(conn).await? {
        all.entry(record.name.clone()).or_default().push((
            record.by,
            SavedQuery {
                name: record.name,
                query: record.query,
                parameters: record.parameters,
            },
        ));
    }
    let mut out = BTreeMap::new();
    let mut next = Some(requested.to_owned());
    while let Some(name) = next {
        if out.contains_key(&name) {
            break;
        }
        let (query_name, author) = name
            .split_once('@')
            .map_or((name.as_str(), None), |(name, author)| (name, Some(author)));
        let Some(definitions) = all.get(query_name) else {
            break;
        };
        let matches: Vec<_> = definitions
            .iter()
            .filter(|(by, _)| author.is_none_or(|author| by == author))
            .collect();
        let [(_, definition)] = matches.as_slice() else {
            return Err(Error::Invalid(if matches.is_empty() {
                format!("no saved query named {name}")
            } else {
                format!(
                    "several authors have saved query {name}; invoke query:{query_name}@AUTHOR using the by value from query-list"
                )
            }));
        };
        next = sinex_model::query::saved::reference_name(&definition.query);
        out.insert(name, (*definition).clone());
    }
    Ok(out)
}
