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
    if sinex_model::query::native::is_command(text) {
        return sinex_model::query::native::parse(text, ctx)?
            .identity()
            .map_err(Error::Query);
    }
    let catalog = sinex_model::query::syntax::catalog(ctx);
    let parsed = sinex_model::query::syntax::parse(text, ctx)?;
    query_language::identity::normal_form(&parsed, &catalog)
        .map_err(|e| Error::Invalid(e.to_string()))
}
/// Exact format-1 question identity for the bounded provenance operation.
/// This serializes a complete typed walk and never parses historical text.
pub(super) fn original_walk_question(
    operation: &sinex_model::query::native::Template,
) -> Option<Json> {
    use sinex_model::query::native::Operation;
    let Some(Operation::Walk {
        direction,
        reference,
    }) = &operation.operation
    else {
        return None;
    };
    if !operation.relation.transforms.is_empty()
        || !operation.relation.attach.is_empty()
        || !matches!(&operation.relation.source, query_language::ast::Source::Unit {unit,condition:None,..} if unit=="events")
    {
        return None;
    }
    let mut question = serde_json::json!({"source":{"kind":"walk","direction":direction,"reference":reference},"attach":[],"stages":[]});
    fn sort(value: &mut Json) {
        match value {
            Json::Object(fields) => {
                for value in fields.values_mut() {
                    sort(value);
                }
                fields.sort_keys();
            }
            Json::Array(values) => {
                for value in values {
                    sort(value);
                }
            }
            _ => {}
        }
    }
    sort(&mut question);
    Some(question)
}

/// The authenticated question may use the original representation only when
/// the complete current native operation has exactly that original identity.
/// The caller still binds grants, clock and authority before reading its keyset.
pub(super) fn authenticated_operation_question(
    authenticated: &query_language::cursor::AuthenticatedCursor,
    current: Json,
    operation: Option<&sinex_model::query::native::Template>,
    grants: &str,
) -> Result<(Json, QuestionId)> {
    let current_id = identity("query", &current, grants)?;
    if authenticated.context().question != current_id {
        if let Some(original) = operation.and_then(original_walk_question) {
            let original_id = identity("query", &original, grants)?;
            if authenticated.context().question == original_id {
                return Ok((original, original_id));
            }
        }
    }
    Ok((current, current_id))
}

pub fn open(
    seal: &CursorSeal,
    raw: &str,
    question: QuestionId,
    ctx: &Context,
) -> Result<Continuation> {
    bind_authenticated(authenticate(seal, raw)?, question, ctx)
}

pub fn authenticate(
    seal: &CursorSeal,
    raw: &str,
) -> Result<query_language::cursor::AuthenticatedCursor> {
    seal.authenticate(&SealedCursor::from_wire(raw))
        .map_err(|error| Error::Invalid(error.to_string()))
}

pub fn bind_authenticated(
    authenticated: query_language::cursor::AuthenticatedCursor,
    question: QuestionId,
    ctx: &Context,
) -> Result<Continuation> {
    authenticated
        .bind(
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
        .map_err(|error| Error::Invalid(error.to_string()))
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

#[cfg(test)]
mod tests;
