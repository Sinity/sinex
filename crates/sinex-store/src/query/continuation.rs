//! Authentication and read-context binding shared by every paged Sinex surface.
use super::*;
pub use query_language::cursor::OrderPosition as Position;
pub use query_language::cursor::{Direction, compare_positions};
use query_language::{
    cursor::{Continuation, CursorContext, CursorSeal, OrderPosition, SealedCursor},
    identity::QuestionId,
};
use std::collections::BTreeMap;

pub async fn seal_key(conn: &mut PgConnection) -> Result<CursorSeal> {
    let key: Vec<u8> = sqlx::query_scalar("SELECT secret FROM core.query_cursor_key WHERE id")
        .fetch_one(conn)
        .await?;
    Ok(CursorSeal::new(key.try_into().map_err(|_| {
        Error::Invalid("invalid query cursor key".into())
    })?))
}
pub fn identity(surface: &str, question: &Json, grants: &str) -> Result<QuestionId> {
    let bytes = serde_json::to_vec(&(surface, question, grants))
        .map_err(|e| Error::Invalid(e.to_string()))?;
    Ok(QuestionId(*blake3::hash(&bytes).as_bytes()))
}
pub fn query_identity(text: &str, ctx: &Context) -> Result<Json> {
    let catalog = sinex_model::query::syntax::catalog(ctx);
    let parsed = sinex_model::query::syntax::parse(text, ctx)?;
    query_language::identity::normal_form(&parsed, &catalog)
        .map_err(|e| Error::Invalid(e.to_string()))
}
pub fn open(
    seal: &CursorSeal,
    raw: &str,
    question: QuestionId,
    ctx: &Context,
) -> Result<Continuation> {
    let opened = seal
        .open(
            &SealedCursor::from_wire(raw),
            question,
            |name| {
                if ctx.zone.iana_name() == Some(name) {
                    Ok(ctx.zone.clone())
                } else {
                    Err(query_language::Error::new(
                        query_language::ErrorKind::StaleCursor,
                        "cursor zone changed",
                        0,
                    ))
                }
            },
            |saved| saved.clock.day.start() == ctx.day_start && saved.clock.now <= ctx.now,
        )
        .map_err(|e| Error::Invalid(e.to_string()))?;
    Ok(opened)
}
pub fn encode(
    seal: &CursorSeal,
    question: QuestionId,
    ctx: &Context,
    positions: BTreeMap<String, String>,
    versions: BTreeMap<String, String>,
    after: OrderPosition,
) -> Result<String> {
    let context = CursorContext {
        question,
        clock: ctx.clock()?,
        resolved_times: vec![],
        random_seed: 0,
        read_positions: positions,
        versions,
    };
    seal.seal(&Continuation { context, after })
        .map(|cursor| cursor.as_str().to_owned())
        .map_err(|e| Error::Invalid(e.to_string()))
}
