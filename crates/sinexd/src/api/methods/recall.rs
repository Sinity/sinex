//! The recall adapter stores one complete selected context before paging it.
use super::*;
use serde::{Deserialize, Serialize};
use sinex_model::Timestamp;
use sinex_model::event_types::analysis::AnalysisRequest;

#[derive(Serialize, Deserialize)]
struct PackSelection {
    pack: String,
}

impl Api {
    pub(super) async fn recall(&self, caller: &Caller, text: &str, page: Page) -> Reply {
        match self.recall_answer(caller, text, page, None).await {
            Ok(answer) => data(answer),
            Err(error) => refused(error),
        }
    }

    pub(super) async fn recall_context(
        &self,
        caller: &Caller,
        text: &str,
        page: Page,
        question: Value,
    ) -> Reply {
        match self.recall_answer(caller, text, page, Some(question)).await {
            Ok(answer) => data(answer),
            Err(error) => refused(error),
        }
    }
    async fn recall_answer(
        &self,
        caller: &Caller,
        text: &str,
        page: Page,
        identity: Option<Value>,
    ) -> Result<Value, String> {
        let mut ctx = self.context();
        let mut conn = self
            .acquire(caller)
            .await
            .map_err(|error| error.to_string())?;
        let size = page.size.unwrap_or(sinex_store::query::DEFAULT_PAGE).max(1);
        if size > i64::MAX as u64 {
            return Err("page size is out of range".into());
        }
        let text = text.trim();
        ctx.catalog = Some(query::QueryCatalog::new(
            sinex_schema::kinds::all(&mut conn)
                .await
                .map_err(|e| e.to_string())?
                .into_values(),
        ));
        let question = match identity {
            Some(value) => value,
            None => sinex_store::query::continuation::query_identity(text, &ctx)
                .map_err(|e| e.to_string())?,
        };
        let question_id =
            sinex_store::query::continuation::identity("recall", &question, &caller.grants)
                .map_err(|e| e.to_string())?;
        let seal = sinex_store::query::continuation::seal_key(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        let prior = if let Some(raw) = &page.cursor {
            match sinex_store::query::continuation::open(&seal, raw, question_id, &ctx) {
                Ok(c) => Some(c),
                Err(error) => {
                    let bound = query::saved::reference_name(text).map(|name| name);
                    let historical = if let Some(name) = bound {
                        let definitions = queries::definitions(&mut conn, &name).await?;
                        query::saved::bind_saved(text, &ctx, &definitions)
                            .map_err(|e| e.to_string())?
                    } else {
                        query::bind(text, &ctx).map_err(|e| e.to_string())?
                    };
                    if historical
                        .stages
                        .iter()
                        .any(|s| matches!(s, query::Stage::AsOf(_)))
                    {
                        return self
                            .recall_preview(&mut conn, caller, text, &ctx, &page, None)
                            .await;
                    }
                    return Err(error.to_string());
                }
            }
        } else {
            None
        };
        let pack = if let Some(cursor) = &prior {
            ctx.now = cursor.context.clock.now;
            let selected: PackSelection = serde_json::from_str(
                cursor
                    .context
                    .versions
                    .get("sinex.recall")
                    .ok_or("invalid recall cursor")?,
            )
            .map_err(|e| e.to_string())?;
            let pack = crate::analysis::load_pack(&mut conn, &selected.pack)
                .await?
                .ok_or("recall cursor's stored context is unavailable")?;
            let selection = serde_json::from_value(pack.payload["analysis"]["selected"].clone())
                .map_err(|error| error.to_string())?;
            sinex_store::query::check_selected_question(&mut conn, text, &selection)
                .await
                .map_err(|e| e.to_string())?;
            pack
        } else {
            let selection = sinex_store::query::resolve_recall(&mut conn, text, &ctx)
                .await
                .map_err(|error| error.to_string())?;
            if selection.read_as.is_some() {
                return self
                    .recall_preview(&mut conn, caller, text, &ctx, &page, Some(&selection))
                    .await;
            }
            let selected = &selection.selected;
            let at = Timestamp::from_unix_ns(ctx.now.as_nanosecond())
                .map_err(|error| error.to_string())?;
            let started_at =
                serde_json::from_value::<Timestamp>(selected["from"].clone()).unwrap_or(at);
            let ended_at =
                serde_json::from_value::<Timestamp>(selected["to"].clone()).unwrap_or(at);
            let mut request = AnalysisRequest {
                request: String::new(),
                operation: "recall".into(),
                recipe: None,
                from: ctx.logical_date(ctx.now).to_string(),
                to: ctx.logical_date(ctx.now).to_string(),
                metrics: vec![],
                min_observed_fraction: None,
                zone: selection.zone.clone(),
                day_start: selection.day_start.clone(),
                grants: selection.grants.clone(),
                started_at,
                ended_at,
                by: caller.actor.into(),
                requested_at: at,
                selection: Some(selection),
            };
            request.request = crate::analysis::recall::request_identity(&request)?;
            let writer = self
                .writer
                .as_ref()
                .ok_or("recall materialization requires sinexd run")?;
            let reference = crate::analysis::recall::materialize(
                &mut conn,
                writer,
                &self.blobs,
                request,
                caller.actor,
                &self.host,
            )
            .await?;
            let pack = crate::analysis::load_pack(&mut conn, &reference)
                .await?
                .ok_or("the selected context pack is not materialized")?;
            pack
        };
        let mut answer = pack.payload["analysis"]["answer"].clone();
        if !answer.is_object() {
            return Err("the stored context is not a recall answer".into());
        }
        let rows = answer["rows"]
            .as_array()
            .ok_or("the stored recall rows are unavailable")?;
        // A stored row coordinate is immutable even when projection omits IDs.
        // It identifies a row in this exact cited pack, never a moving offset.
        let position = |index: usize| query::cursor::OrderPosition {
            values: vec![],
            row_identity: format!("{}/rows/{index:020}", pack.citation),
        };
        let mut chosen = Vec::new();
        let mut last = None;
        let mut more = false;
        for (index, row) in rows.iter().enumerate() {
            let here = position(index);
            if let Some(prior) = &prior
                && !query::cursor::compare_positions(&here, &prior.after, &[])
                    .map_err(|e| e.to_string())?
                    .is_gt()
            {
                continue;
            }
            if chosen.len() as u64 == size {
                more = true;
                break;
            }
            chosen.push(row.clone());
            last = Some(here);
        }
        let next = if more {
            let now = answer["resolved"]["now"]
                .as_str()
                .ok_or("stored recall clock is unavailable")?
                .parse()
                .map_err(|e: jiff::Error| e.to_string())?;
            let mut clock = ctx.clone();
            clock.now = now;
            let metadata = serde_json::to_string(&PackSelection {
                pack: pack.citation.clone(),
            })
            .map_err(|e| e.to_string())?;
            Some(
                sinex_store::query::continuation::encode(
                    &seal,
                    question_id,
                    &clock,
                    std::collections::BTreeMap::from([(
                        "context.pack".into(),
                        pack.citation.clone(),
                    )]),
                    std::collections::BTreeMap::from([("sinex.recall".into(), metadata)]),
                    last.ok_or("recall page did not advance")?,
                )
                .map_err(|e| e.to_string())?,
            )
        } else {
            None
        };
        answer["rows"] = json!(chosen);
        answer["query"] = json!(text);
        answer["page"] = json!({"size":size,"cursor":next});
        let (presentation, _) =
            crate::analysis::render_pack(&mut conn, &pack.citation, 4000).await?;
        answer["machine"] = json!({"context_pack":pack.citation,"read_as":presentation.context.as_of,
            "sections":presentation.sections.iter().filter(|section| section.title=="Machine").collect::<Vec<_>>()});
        answer["text"] = json!(presentation.markdown);
        answer["presentation"] = json!(presentation);
        answer["context_pack"] = json!(pack.citation);
        answer["producer_versions"] = pack.payload["producer_versions"].clone();
        Ok(answer)
    }

    async fn recall_preview(
        &self,
        conn: &mut sqlx::PgConnection,
        caller: &Caller,
        text: &str,
        ctx: &Context,
        page: &Page,
        selection: Option<&sinex_model::event_types::analysis::QuerySelection>,
    ) -> Result<Value, String> {
        let selected = sinex_store::query::selected_page(conn, text, ctx, page, selection)
            .await
            .map_err(|error| error.to_string())?;
        let versions = sinex_store::derivations::producer_versions(conn, &selected.parents)
            .await
            .map_err(|error| error.to_string())?;
        let mut answer =
            serde_json::to_value(&selected.answer).map_err(|error| error.to_string())?;
        let sections = crate::analysis::recall::sections(&answer, &selected.parents, None);
        let day_setting = format!(
            "{} {}",
            selected.answer.resolved.day_start, selected.answer.resolved.zone
        );
        let time_setting_version = blake3::hash(
            serde_json::to_vec(&json!([
                selected.answer.resolved.zone,
                selected.answer.resolved.day_start
            ]))
            .map_err(|error| error.to_string())?
            .as_slice(),
        )
        .to_hex()
        .to_string();
        let presentation = sinex_analysis::presentation::compile(
            sinex_analysis::presentation::Context {
                citation: None,
                producer_versions: Some(versions.clone()),
                title: "Historical context preview".into(),
                evaluated_at: selected.answer.resolved.now.clone(),
                as_of: selected.answer.resolved.commit_seq,
                day_setting,
            },
            sections,
            4000,
        );
        answer["historical_preview"] = json!({"materialized":false,
            "reason":"Historical read contexts use an unmaterialized preview because their selected inputs can already be archived.",
            "read_as":selected.answer.resolved.commit_seq,"zone":selected.answer.resolved.zone,
            "day_start":selected.answer.resolved.day_start,"time_setting_version":time_setting_version,
            "grants":caller.grants.split(',').filter(|grant| !grant.is_empty()).collect::<Vec<_>>(),
            "raw_citations":selected.parents.iter().map(|parent| format!("sinex:ev/{parent}")).collect::<Vec<_>>()});
        answer["context_pack"] = Value::Null;
        answer["text"] = json!(presentation.markdown);
        answer["machine"] = json!({"context_pack":null,"read_as":presentation.context.as_of,"historical_preview":true,
            "sections":presentation.sections.iter().filter(|section| section.title=="Machine").collect::<Vec<_>>()});
        answer["presentation"] = json!(presentation);
        answer["producer_versions"] = versions;
        Ok(answer)
    }
}

pub(super) async fn context_for_page(
    conn: &mut sqlx::PgConnection,
    ctx: &Context,
    page: &Page,
    question: &Value,
    grants: &str,
) -> Result<Context, String> {
    let Some(raw) = &page.cursor else {
        return Ok(ctx.clone());
    };
    let seal = sinex_store::query::continuation::seal_key(conn)
        .await
        .map_err(|e| e.to_string())?;
    let id = sinex_store::query::continuation::identity("recall", question, grants)
        .map_err(|e| e.to_string())?;
    let saved =
        sinex_store::query::continuation::open(&seal, raw, id, ctx).map_err(|e| e.to_string())?;
    Ok(Context::new(
        saved.context.clock.now,
        ctx.zone.clone(),
        ctx.day_start,
    ))
}
