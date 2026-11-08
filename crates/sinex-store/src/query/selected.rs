//! The rows and exact provenance of one recall page, for historical previews.
use super::*;
use serde_json::json;
use sinex_model::event_types::analysis::QuerySelection;
use sinex_model::{Class, EventId, Timestamp};
use std::collections::BTreeSet;

pub(super) struct SelectedPage {
    pub answer: Answer,
    pub selection: Option<QuerySelection>,
    pub parents: Vec<EventId>,
    pub class: Class,
}

pub struct SelectedAnswer {
    pub answer: Answer,
    pub parents: Vec<EventId>,
    pub class: Class,
    pub signature: String,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn capture(
    conn: &mut PgConnection,
    text: &str,
    template: &sinex_model::query::native::Template,
    query: &Query,
    ctx: &Context,
    seq: i64,
    grants: &str,
    selection: &recall::Selection,
    rows: &Query,
    account: &Json,
) -> Result<(QuerySelection, Vec<EventId>, Class)> {
    let selector = walk::recall_rows(rows);
    let (sql, values) = {
        let compiled = super::compile_query(conn, ctx, seq, &selector).await?;
        let mut sql = SqlFragment::text(format!(
            "SELECT jsonb_build_object('row',{},'parents',to_jsonb(q.\"_parents\")) FROM (",
            compiled.json_object()
        ));
        sql.append(compiled.answer).push(") q");
        sql.render()
    };
    let inputs: Vec<Json> = sql::bind_scalar(sqlx::query_scalar(&sql), values)
        .fetch_all(&mut *conn)
        .await?;
    let mut parents = BTreeSet::new();
    let mut rows = Vec::with_capacity(inputs.len());
    for input in inputs {
        for id in input["parents"].as_array().into_iter().flatten() {
            let id = id
                .as_str()
                .and_then(|value| value.parse::<uuid::Uuid>().ok())
                .ok_or_else(|| {
                    Error::Invalid("selected query contains invalid event lineage".into())
                })?;
            parents.insert(id);
        }
        rows.push(input["row"].clone());
    }
    parents.extend(selection.anchor_ids());
    let ids: Vec<_> = parents.into_iter().collect();
    let classes: Vec<String> = sqlx::query_scalar("SELECT class::text FROM core.visible_events WHERE id = ANY($1) AND commit_seq <= $2 AND NOT audit.is_archived_at(id, $2)")
        .bind(&ids).bind(seq).fetch_all(&mut *conn).await?;
    if classes.len() != ids.len() {
        return Err(Error::Invalid(
            "selected query provenance changed while being read".into(),
        ));
    }
    let class = classes
        .into_iter()
        .try_fold(Class::Open, |class, stored| {
            serde_json::from_value::<Class>(json!(stored)).map(|next| class.max(next))
        })
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let explicit = query
        .stages
        .iter()
        .any(|stage| matches!(stage, Stage::AsOf(_)));
    let mut grants: Vec<String> = grants
        .split(',')
        .filter(|grant| !grant.is_empty())
        .map(str::to_owned)
        .collect();
    grants.sort();
    grants.dedup();
    let mut retained = selection.retained(&query.source)?;
    retained["_query"] = json!(format!("{query:?}"));
    retained["_template"] = template.identity()?;
    retained["_fingerprint"] = json!(
        blake3::hash(
            serde_json::to_vec(&json!([rows, account]))
                .map_err(|error| Error::Invalid(error.to_string()))?
                .as_slice()
        )
        .to_hex()
        .to_string()
    );
    let selection = QuerySelection {
        query: text.trim().into(),
        template: template.clone(),
        definitions: Default::default(),
        original_question: None,
        now: Timestamp::from_unix_ns(ctx.now.as_nanosecond())
            .map_err(|error| Error::Invalid(error.to_string()))?,
        read_as: explicit.then_some(seq),
        zone: ctx.zone_name(),
        day_start: ctx.day_start.to_string(),
        grants,
        selected: retained,
        declarations: serde_json::to_value(
            ctx.catalog
                .as_ref()
                .map(|catalog| &catalog.declarations)
                .cloned()
                .unwrap_or_default(),
        )
        .map_err(|error| Error::Invalid(error.to_string()))?,
    };
    Ok((selection, ids.into_iter().map(EventId).collect(), class))
}

/// Historical previews use the same selected rows and exact provenance without
/// admitting a new live output over inputs that may already be archived.
pub async fn selected_page(
    conn: &mut PgConnection,
    text: &str,
    ctx: &Context,
    page: &Page,
) -> Result<SelectedAnswer> {
    let template = if sinex_model::query::saved::reference_name(text).is_some() {
        None
    } else {
        Some(sinex_model::query::native::parse(text, ctx)?)
    };
    let selected = run_inner(conn, text, None, ctx, page, true, false, template.as_ref()).await?;
    let signature = selected
        .selection
        .as_ref()
        .and_then(|selection| selection.selected["_fingerprint"].as_str())
        .ok_or_else(|| Error::Invalid("selected query fingerprint is unavailable".into()))?
        .to_owned();
    Ok(SelectedAnswer {
        answer: selected.answer,
        parents: selected.parents,
        class: selected.class,
        signature,
    })
}
