//! Query read positions and declaration availability around shared sealed cursors.
use super::*;
use query_language::{
    cursor::{CursorSeal, OrderPosition},
    identity::QuestionId,
};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    declarations: Vec<(sinex_model::KindRef, String)>,
    bound: String,
    seq: i64,
    total: i64,
    key_types: Vec<String>,
    availability: Json,
}
pub(super) struct Cursor {
    pub declarations: Vec<(sinex_model::KindRef, String)>,
    bound: String,
    pub seq: i64,
    pub total: i64,
    pub key_types: Vec<String>,
    pub after: OrderPosition,
    pub availability: Json,
    pub ctx: Context,
}
fn invalid() -> Error {
    Error::Invalid("invalid or obsolete query cursor; start the query again".into())
}
impl Cursor {
    pub(super) fn decode(
        seal: &CursorSeal,
        raw: &str,
        question: QuestionId,
        ctx: &Context,
    ) -> Result<Self> {
        let c = continuation::open(seal, raw, question, ctx)?;
        let m: Metadata =
            serde_json::from_str(c.context.versions.get("sinex.query").ok_or_else(invalid)?)
                .map_err(|_| invalid())?;
        if m.seq < 0
            || m.total < 0
            || m.bound.len() != 64
            || m.key_types.len() != c.after.values.len()
        {
            return Err(invalid());
        }
        Ok(Self {
            declarations: m.declarations,
            bound: m.bound,
            seq: m.seq,
            total: m.total,
            key_types: m.key_types,
            after: c.after,
            availability: m.availability,
            ctx: Context::new(c.context.clock.now, ctx.zone.clone(), ctx.day_start),
        })
    }
    pub(super) fn check_question(&self, bound: &str) -> Result<()> {
        if self.bound != bound {
            return Err(Error::Invalid(
                "query cursor belongs to a different question or caller context".into(),
            ));
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn encode(
        seal: &CursorSeal,
        question: QuestionId,
        bound: &str,
        ctx: &Context,
        seq: i64,
        total: i64,
        key_types: Vec<String>,
        after: OrderPosition,
        availability: Json,
    ) -> Result<String> {
        let m = Metadata {
            declarations: ctx
                .catalog
                .as_ref()
                .map(|c| c.declarations.clone())
                .unwrap_or_default(),
            bound: bound.into(),
            seq,
            total,
            key_types,
            availability,
        };
        continuation::encode(
            seal,
            question,
            ctx,
            BTreeMap::from([("events".into(), seq.to_string())]),
            BTreeMap::from([(
                "sinex.query".into(),
                serde_json::to_string(&m).map_err(|_| invalid())?,
            )]),
            after,
        )
    }
}
pub async fn context_for_page(
    conn: &mut PgConnection,
    ctx: &Context,
    page: &Page,
    text: &str,
) -> Result<Context> {
    if page.cursor.is_none() {
        ctx.clock()?;
        return Ok(ctx.clone());
    }
    let mut ctx = ctx.clone();
    if ctx.catalog.is_none() {
        ctx.catalog = Some(sinex_model::query::QueryCatalog::new(
            sinex_schema::kinds::all(conn).await?.into_values(),
        ));
    }
    let grants = current_grants(conn).await?;
    let question =
        continuation::identity("query", &continuation::query_identity(text, &ctx)?, &grants)?;
    let seal = continuation::seal_key(conn).await?;
    Ok(Cursor::decode(
        &seal,
        page.cursor.as_deref().expect("cursor present"),
        question,
        &ctx,
    )?
    .ctx)
}
pub(super) fn question(
    text: &str,
    programmatic: Option<String>,
    definitions: Option<&std::collections::BTreeMap<String, sinex_model::query::saved::SavedQuery>>,
    ctx: &Context,
    scope: &(String, String),
) -> Result<String> {
    digest(&serde_json::json!([
        continuation::query_identity(text, ctx)?,
        programmatic,
        definitions,
        ctx.now.to_string(),
        ctx.zone_name(),
        ctx.day_start.to_string(),
        scope
    ]))
}
pub(super) fn digest(value: &Json) -> Result<String> {
    Ok(
        blake3::hash(&serde_json::to_vec(value).map_err(|_| invalid())?)
            .to_hex()
            .to_string(),
    )
}
