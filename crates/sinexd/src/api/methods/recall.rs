//! Recall reads its answer at one commit position and presents it from the
//! rows of that read. An ordinary interactive recall stores nothing; its pages
//! are the query cursor's pages at the same position.
use super::*;
use crate::Result;

impl Api {
    pub(super) async fn recall(&self, caller: &Caller, text: &str, page: Page) -> Result<Reply> {
        self.recall_in(caller, text, page, self.context()).await
    }

    /// Recall under a supplied clock, used when a surface's window is relative
    /// to the time its first page was read.
    pub(super) async fn recall_in(
        &self,
        caller: &Caller,
        text: &str,
        page: Page,
        ctx: Context,
    ) -> Result<Reply> {
        match self.recall_answer(caller, text, page, ctx).await {
            Ok(answer) => Ok(data(answer)),
            Err(error) => Err(error.in_query(text)),
        }
    }

    async fn recall_answer(
        &self,
        caller: &Caller,
        text: &str,
        page: Page,
        mut ctx: Context,
    ) -> Result<Value> {
        let mut conn = self.acquire(caller).await?;
        let text = text.trim();
        ctx.catalog = Some(query::QueryCatalog::new(
            sinex_schema::kinds::all(&mut conn).await?.into_values(),
        ));
        if reads_history(&mut conn, text, &ctx).await? {
            return self
                .recall_preview(&mut conn, caller, text, &ctx, &page)
                .await;
        }
        let read = if query::saved::reference_name(text).is_some() {
            sinex_store::query::run(&mut conn, text, None, &ctx, &page).await
        } else {
            let operation = query::native::parse(text, &ctx)?;
            sinex_store::query::run_operation(&mut conn, text, &operation, &ctx, &page).await
        }?;
        let mut answer = serde_json::to_value(&read)?;
        let parents: Vec<sinex_model::EventId> = read
            .rows
            .iter()
            .filter_map(|row| row["id"].as_str()?.parse::<uuid::Uuid>().ok())
            .map(sinex_model::EventId)
            .collect();
        let versions = sinex_store::derivations::producer_versions(&mut conn, &parents).await?;
        let presentation = sinex_analysis::presentation::compile(
            sinex_analysis::presentation::Context {
                citation: None,
                producer_versions: Some(versions.clone()),
                title: "Recorded context".into(),
                evaluated_at: read.resolved.now.clone(),
                as_of: read.resolved.commit_seq,
                day_setting: format!("{} {}", read.resolved.day_start, read.resolved.zone),
            },
            crate::analysis::recall::sections(&answer, &parents),
            4000,
        );
        answer["machine"] = json!({"context_pack":null,"read_as":presentation.context.as_of,
            "sections":presentation.sections.iter().filter(|section| section.title=="Machine").collect::<Vec<_>>()});
        answer["text"] = json!(presentation.markdown);
        answer["presentation"] = json!(presentation);
        answer["context_pack"] = Value::Null;
        answer["producer_versions"] = versions;
        Ok(answer)
    }

    async fn recall_preview(
        &self,
        conn: &mut sqlx::PgConnection,
        caller: &Caller,
        text: &str,
        ctx: &Context,
        page: &Page,
    ) -> Result<Value> {
        let selected = sinex_store::query::selected_page(conn, text, ctx, page).await?;
        let versions = sinex_store::derivations::producer_versions(conn, &selected.parents).await?;
        let mut answer = serde_json::to_value(&selected.answer)?;
        let sections = crate::analysis::recall::sections(&answer, &selected.parents);
        let day_setting = format!(
            "{} {}",
            selected.answer.resolved.day_start, selected.answer.resolved.zone
        );
        let time_setting_version = blake3::hash(
            serde_json::to_vec(&json!([
                selected.answer.resolved.zone,
                selected.answer.resolved.day_start
            ]))?
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

/// Whether the question names an earlier read position (`as of`), which is
/// answered as a labelled historical preview.
async fn reads_history(conn: &mut sqlx::PgConnection, text: &str, ctx: &Context) -> Result<bool> {
    let bound = if let Some(name) = query::saved::reference_name(text) {
        let definitions = queries::definitions(conn, &name).await?;
        query::saved::bind_saved(text, ctx, &definitions)?
    } else {
        query::native::parse(text, ctx).and_then(|operation| operation.bind(ctx))?
    };
    Ok(bound
        .stages
        .iter()
        .any(|stage| matches!(stage, query::Stage::AsOf(_))))
}

/// The clock a surface's first page was read at, carried by its cursor.
pub(super) async fn context_for_page(
    conn: &mut sqlx::PgConnection,
    ctx: &Context,
    page: &Page,
) -> Result<Context> {
    let Some(raw) = &page.cursor else {
        return Ok(ctx.clone());
    };
    let seal = sinex_store::query::continuation::seal_key(conn).await?;
    let authenticated = sinex_store::query::continuation::authenticate(&seal, raw)?;
    Ok(Context::new(
        authenticated.context().clock.now,
        ctx.zone.clone(),
        ctx.day_start,
    ))
}
