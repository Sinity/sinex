//! The recall adapter stores one complete selected context before paging it.
use super::*;
use serde::{Deserialize, Serialize};
use sinex_model::Timestamp;
use sinex_model::event_types::analysis::AnalysisRequest;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PackCursor {
    version: u8,
    pack: String,
    question: String,
    grants: String,
    zone: String,
    day_start: String,
    offset: u64,
    now: String,
}

impl Api {
    pub(super) async fn recall(&self, caller: &Caller, text: &str, page: Page) -> Reply {
        match self.recall_answer(caller, text, page).await {
            Ok(answer) => data(answer),
            Err(error) => refused(error),
        }
    }

    async fn recall_answer(
        &self,
        caller: &Caller,
        text: &str,
        page: Page,
    ) -> Result<Value, String> {
        let ctx = self.context();
        let mut conn = self
            .acquire(caller)
            .await
            .map_err(|error| error.to_string())?;
        let size = page.size.unwrap_or(sinex_store::query::DEFAULT_PAGE).max(1);
        if size > i64::MAX as u64 {
            return Err("page size is out of range".into());
        }
        let text = text.trim();
        if page
            .cursor
            .as_ref()
            .is_some_and(|cursor| query::cursor::decode::<PackCursor>(cursor).is_err())
        {
            return self
                .recall_preview(&mut conn, caller, text, &ctx, &page, None)
                .await;
        }
        let (pack, offset) = if let Some(cursor) = &page.cursor {
            let cursor: PackCursor = query::cursor::decode(cursor).map_err(str::to_owned)?;
            if cursor.version != 1
                || cursor.question != text
                || cursor.grants != caller.grants
                || cursor.zone != ctx.zone_name()
                || cursor.day_start != ctx.day_start.to_string()
                || cursor.offset == 0
            {
                return Err(
                    "recall cursor belongs to a different question or caller context".into(),
                );
            }
            let pack = crate::analysis::load_pack(&mut conn, &cursor.pack)
                .await?
                .ok_or("recall cursor's stored context is unavailable")?;
            let selection = serde_json::from_value(pack.payload["analysis"]["selected"].clone())
                .map_err(|error| error.to_string())?;
            sinex_store::query::check_selected_question(&mut conn, text, &selection)
                .await
                .map_err(|error| error.to_string())?;
            (pack, cursor.offset)
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
            let selected = request
                .selection
                .as_ref()
                .expect("resolved recall selector");
            let mut scope = selected.selected.clone();
            scope
                .as_object_mut()
                .expect("resolved selector object")
                .remove("_fingerprint");
            request.request = blake3::hash(
                serde_json::to_vec(&json!([
                    request.operation,
                    scope,
                    selected.zone,
                    selected.day_start,
                    selected.grants,
                    selected.read_as,
                    selected.declarations
                ]))
                .map_err(|error| error.to_string())?
                .as_slice(),
            )
            .to_hex()
            .to_string();
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
            (pack, 0)
        };
        let mut answer = pack.payload["analysis"]["answer"].clone();
        if !answer.is_object() {
            return Err("the stored context is not a recall answer".into());
        }
        let rows = answer["rows"]
            .as_array()
            .ok_or("the stored recall rows are unavailable")?;
        let count =
            u64::try_from(rows.len()).map_err(|_| "stored recall row count is out of range")?;
        if offset > count || (offset != 0 && offset == count) {
            return Err("recall cursor is outside its stored context".into());
        }
        let end = offset
            .checked_add(size)
            .ok_or("recall page end overflow")?
            .min(count);
        let next = if end < count {
            Some(
                query::cursor::encode(&PackCursor {
                    version: 1,
                    pack: pack.citation.clone(),
                    question: text.into(),
                    grants: caller.grants.clone(),
                    zone: ctx.zone_name(),
                    day_start: ctx.day_start.to_string(),
                    offset: end,
                    now: answer["resolved"]["now"]
                        .as_str()
                        .ok_or("stored recall clock is unavailable")?
                        .into(),
                })
                .map_err(|error| error.to_string())?,
            )
        } else {
            None
        };
        let chosen = rows[usize::try_from(offset).map_err(|_| "cursor offset is out of range")?
            ..usize::try_from(end).map_err(|_| "cursor end is out of range")?]
            .to_vec();
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

pub(super) fn context_for_page(ctx: &Context, page: &Page) -> Result<Context, String> {
    let Some(raw) = &page.cursor else {
        return Ok(ctx.clone());
    };
    let Ok(cursor) = query::cursor::decode::<PackCursor>(raw) else {
        return sinex_store::query::context_for_page(ctx, page).map_err(|error| error.to_string());
    };
    if cursor.version != 1
        || cursor.zone != ctx.zone_name()
        || cursor.day_start != ctx.day_start.to_string()
    {
        return Err("recall cursor does not match the time zone or day start".into());
    }
    let now = cursor
        .now
        .parse::<jiff::Timestamp>()
        .map_err(|error| error.to_string())?;
    if now > ctx.now {
        return Err("recall cursor clock is in the future".into());
    }
    Ok(Context::new(now, ctx.zone.clone(), ctx.day_start))
}
