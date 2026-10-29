//! Standing queries use the ordinary binder/compiler and retain its complete order.
use super::*;
use serde::{Deserialize, Serialize};
use sinex_model::{Class, EventId};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignalRow {
    /// Compiler identity for groups and metadata, native coordinates for events.
    pub identity: String,
    pub reference: Option<String>,
    pub event: Option<EventId>,
    pub observed: Option<sinex_model::Timestamp>,
    pub start: Option<sinex_model::Timestamp>,
    pub end: Option<sinex_model::Timestamp>,
    pub result: Json,
}
#[derive(Clone, Debug)]
pub struct SignalAnswer {
    pub rows: Vec<SignalRow>,
    pub class: Class,
    pub complete: bool,
    pub reason: Option<String>,
    pub live: bool,
    pub read_as: i64,
    pub parents: Vec<EventId>,
}

/// The caller supplies one repeatable read snapshot and the rule's current
/// grants. No page boundary or presentation field determines occurrence identity.
pub async fn standing_query(
    conn: &mut PgConnection,
    text: &str,
    ctx: &Context,
    seq: i64,
    own_source: &str,
) -> Result<SignalAnswer> {
    let mut ctx = ctx.clone();
    ctx.catalog = Some(sinex_model::query::QueryCatalog::new(
        sinex_schema::kinds::all(conn).await?.into_values(),
    ));
    let mut bound = if let Some(name) = sinex_model::query::saved::reference_name(text) {
        let definitions = saved::saved_definitions(conn, &name).await?;
        sinex_model::query::saved::bind_saved(text, &ctx, &definitions)?
    } else {
        sinex_model::query::bind(text, &ctx)?
    };
    ctx.catalog
        .as_ref()
        .expect("loaded declarations")
        .bind(&mut bound, &ctx)?;
    let mut seq = seq;
    for stage in &bound.stages {
        match stage {
            Stage::AsOf(AsOf::Seq(position)) => seq = *position,
            Stage::AsOf(AsOf::Time(at)) => seq = seq_at(conn, *at).await?,
            _ => {}
        }
    }
    if seq < 0 || seq > current_seq(conn).await? {
        return Err(Error::Invalid(
            "signal query position is outside the archive".into(),
        ));
    }
    if let Some(reference) = bound.similar_reference()? {
        crate::vectors::validate_reference(conn, reference, seq).await?;
    }
    let live = bound
        .source
        .alias()
        .is_some_and(|alias| matches!(alias.name, "now" | "intervals"));
    if matches!(
        bound.source,
        Source::Recall { .. } | Source::RecallLast { .. } | Source::RecallMark { .. }
    ) {
        let selection = recall::select(conn, &bound.source, &ctx, seq).await?;
        bound.source = Source::Table {
            table: Table::Events,
            alias: None,
        };
        bound.filter = Some(match bound.filter.take() {
            Some(filter) => Expr::And(Box::new(selection.filter()), Box::new(filter)),
            None => selection.filter(),
        });
    }
    // A delivery and its retained evaluation are observations of this mechanism,
    // never new source evidence for another rule.
    if bound.source.table().is_events() {
        let exclude = Expr::Not(Box::new(Expr::Compare {
            lhs: Operand::Field(FieldRef::Field(
                Table::Events.field("event_type").expect("event type field"),
            )),
            op: sinex_model::query::Op::Prefix,
            rhs: sinex_model::query::Value::Text("signal.".into()),
        }));
        bound.filter = Some(match bound.filter.take() {
            Some(filter) => Expr::And(Box::new(filter), Box::new(exclude)),
            None => exclude,
        });
    }
    let exclude_metadata = match bound.source.table() {
        Table::Runs => Some(("producer", "signals")),
        Table::Materials => Some(("parser", "signals")),
        Table::Sources => Some(("source", own_source)),
        _ => None,
    };
    if let Some((field, value)) = exclude_metadata {
        let exclude = Expr::Not(Box::new(Expr::Compare {
            lhs: Operand::Field(FieldRef::Field(
                bound
                    .source
                    .table()
                    .field(field)
                    .expect("declared metadata field"),
            )),
            op: sinex_model::query::Op::Eq,
            rhs: sinex_model::query::Value::Text(value.into()),
        }));
        bound.filter = Some(match bound.filter.take() {
            Some(filter) => Expr::And(Box::new(filter), Box::new(exclude)),
            None => exclude,
        });
    }
    let mut parents = Vec::new();
    if matches!(bound.source,Source::Table{table,..} if table.is_events()) {
        let mut selector = bound.clone();
        if let Some(group) = selector
            .stages
            .iter()
            .position(|stage| matches!(stage, Stage::Group(_)))
        {
            selector.stages.truncate(group);
        }
        selector
            .stages
            .retain(|stage| !matches!(stage, Stage::Fields(_)));
        selector.stages.push(Stage::Fields(vec![FieldRef::Field(
            Table::Events.field("id").expect("event ID field"),
        )]));
        let (sql, params) = {
            let compiled = super::compile_query(conn, &ctx, seq, &selector).await?;
            let mut sql = SqlFragment::text("SELECT q.id FROM (");
            sql.append(compiled.answer).push(") q");
            sql.render()
        };
        let ids: Vec<uuid::Uuid> = sql::bind_scalar(sqlx::query_scalar(&sql), params)
            .fetch_all(&mut *conn)
            .await?;
        parents = ids.into_iter().map(EventId).collect();
    }
    let grouped = bound
        .stages
        .iter()
        .any(|stage| matches!(stage, Stage::Group(_)));
    let event_rows = bound.source.table().is_events() && !grouped;
    let values: Vec<Json> = match &bound.source {
        Source::Walk { walk, reference } => walk::walk(conn, *walk, reference)
            .await?
            .into_iter()
            .map(|row| serde_json::json!({"identity":row["id"],"row":row}))
            .collect(),
        _ => {
            let (sql, params) = {
                let compiled = super::compile_query(conn, &ctx, seq, &bound).await?;
                let mut sql = SqlFragment::text(format!(
                    "SELECT jsonb_build_object('identity',q.\"_row_identity\",'row',{}) FROM (",
                    json_object(&compiled.columns)
                ));
                sql.append(compiled.answer).push(") q");
                sql.render()
            };
            sql::bind_scalar(sqlx::query_scalar(&sql), params)
                .fetch_all(&mut *conn)
                .await?
        }
    };
    let grants = current_grants(conn).await?;
    let mut class = if grants.split(',').any(|grant| grant == "sealed") {
        Class::Sealed
    } else if grants.split(',').any(|grant| grant == "guarded") {
        Class::Guarded
    } else {
        Class::Open
    };
    let owner_key = crate::owner_protocol::key(conn).await?;
    let identity_key = blake3::derive_key("sinex.owner_protocol.signal_occurrence.v1", &owner_key);
    let mut rows = Vec::with_capacity(values.len());
    for mut value in values {
        let identity = value["identity"]
            .as_str()
            .ok_or_else(|| Error::Invalid("signal query lost its native row identity".into()))?
            .to_owned();
        let mut row = SignalRow {
            identity,
            reference: None,
            event: None,
            observed: None,
            start: None,
            end: None,
            result: value["row"].take(),
        };
        if event_rows {
            let id: uuid::Uuid = row
                .identity
                .parse()
                .map_err(|_| Error::Invalid("signal event row has no reading identity".into()))?;
            let details=sqlx::query!(r#"SELECT jsonb_build_object('material',original.material_id,'anchor',original.anchor,'output',original.output_index,'source',original.source,'kind',original.event_type,'key',CASE WHEN preserved.event_id IS NOT NULL THEN preserved.equivalence_key ELSE original.equivalence_key END,'observed',visible.ts_coided,'start',visible.ts_orig,'payload',visible.payload) AS "details!",visible.class::text AS "class!" FROM core.visible_events visible JOIN core.events original ON original.id=visible.id LEFT JOIN privacy.original_events preserved ON preserved.event_id=original.id WHERE visible.id=$1"#,id).fetch_optional(&mut *conn).await?;
            let Some(details) = details else {
                return Err(Error::Invalid(
                    "signal query reading became unavailable".into(),
                ));
            };
            let stored_class = details.class;
            let details = details.details;
            class = class.max(
                serde_json::from_value(serde_json::json!(stored_class))
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            );
            row.event = Some(EventId(id));
            let native = if let Some(key) = details["key"].as_str() {
                serde_json::json!(["key", details["source"], details["kind"], key])
            } else if details["material"].is_string() {
                serde_json::json!([
                    "record",
                    details["material"],
                    details["anchor"],
                    details["output"]
                ])
            } else {
                // Historical keyless derived rows have no invented natural key.
                serde_json::json!(["native_event", id])
            };
            row.identity = format!(
                "native:{}",
                blake3::keyed_hash(
                    &identity_key,
                    &serde_json::to_vec(&native)
                        .map_err(|error| Error::Invalid(error.to_string()))?
                )
            );
            // The immutable interpretation cited at match time survives replay.
            row.reference = Some(format!("sinex:ev/{id}"));
            row.observed = serde_json::from_value(details["observed"].clone()).ok();
            row.start = serde_json::from_value(details["start"].clone()).ok();
            if live {
                for field in ["ended_at", "fenced_at", "end", "observed_until"] {
                    if let Ok(end) = serde_json::from_value(details["payload"][field].clone()) {
                        row.end = Some(end);
                        break;
                    }
                }
                if let Ok(start) = serde_json::from_value(details["payload"]["started_at"].clone())
                {
                    row.start = Some(start);
                }
            }
        }
        rows.push(row);
    }
    let complete = class != Class::Sealed;
    Ok(SignalAnswer {
        rows,
        class,
        complete,
        reason: (!complete).then(|| "sealed_query_requires_available_plaintext_evidence".into()),
        live,
        read_as: seq,
        parents,
    })
}
