//! A continuation describes one question, clock and visible ordered answer.
use super::*;
use base64::Engine as _;

const VERSION: u8 = 1;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Cursor {
    version: u8,
    question: String,
    now: String,
    zone: String,
    day_start: String,
    pub seq: i64,
    pub offset: u64,
    view: String,
}

fn invalid() -> Error {
    Error::Invalid("invalid or obsolete query cursor; start the query again".into())
}

fn zone(ctx: &Context) -> Result<String> {
    if let Some(name) = ctx.zone.iana_name() {
        return Ok(format!("iana:{name}"));
    }
    ctx.zone
        .to_fixed_offset()
        .map(|offset| format!("offset:{offset}"))
        .map_err(|_| {
            Error::Invalid("query paging requires a named or fixed-offset time zone".into())
        })
}

impl Cursor {
    pub fn decode(raw: &str) -> Result<Self> {
        if raw.len() > 4096 {
            return Err(invalid());
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|_| invalid())?;
        let cursor: Self = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if cursor.version != VERSION
            || cursor.seq < 0
            || cursor.offset == 0
            || cursor.offset > i64::MAX as u64
            || cursor.question.len() != 64
            || cursor.view.len() != 64
        {
            return Err(invalid());
        }
        Ok(cursor)
    }

    fn context(&self, ctx: &Context) -> Result<Context> {
        if self.zone != zone(ctx)? || self.day_start != ctx.day_start.to_string() {
            return Err(Error::Invalid(
                "query cursor does not match the time zone or day start".into(),
            ));
        }
        let now: jiff::Timestamp = self.now.parse().map_err(|_| invalid())?;
        if now > ctx.now {
            return Err(invalid());
        }
        Ok(Context::new(now, ctx.zone.clone(), ctx.day_start))
    }

    pub fn check_question(&self, question: &str) -> Result<()> {
        if self.question != question {
            return Err(Error::Invalid(
                "query cursor belongs to a different question or caller context".into(),
            ));
        }
        Ok(())
    }

    pub fn check_view(&self, view: &str, total: i64) -> Result<()> {
        if self.view != view || self.offset >= total.max(0) as u64 {
            return Err(Error::StaleCursor);
        }
        Ok(())
    }

    pub fn encode(
        question: &str,
        ctx: &Context,
        seq: i64,
        offset: u64,
        view: &str,
    ) -> Result<String> {
        let cursor = Self {
            version: VERSION,
            question: question.into(),
            now: ctx.now.to_string(),
            zone: zone(ctx)?,
            day_start: ctx.day_start.to_string(),
            seq,
            offset,
            view: view.into(),
        };
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&cursor).map_err(|_| invalid())?))
    }
}

/// Restore the first page's clock before binding relative expressions. The
/// current zone and logical day still have to match the original context.
pub fn context_for_page(ctx: &Context, page: &Page) -> Result<Context> {
    match &page.cursor {
        Some(raw) => Cursor::decode(raw)?.context(ctx),
        None => Ok(ctx.clone()),
    }
}

pub(super) fn question(
    text: &str,
    query: &Query,
    ctx: &Context,
    scope: &(String, String),
) -> Result<String> {
    // Include the actual AST: follow adds its commit filter and ordering after
    // parsing. The raw text alone does not describe that question.
    digest(&serde_json::json!([
        text,
        format!("{query:?}"),
        ctx.now.to_string(),
        zone(ctx)?,
        ctx.day_start.to_string(),
        scope
    ]))
}

pub(super) fn digest(value: &Json) -> Result<String> {
    Ok(blake3::hash(
        &serde_json::to_vec(value)
            .map_err(|_| Error::Invalid("query context cannot be encoded".into()))?,
    )
    .to_hex()
    .to_string())
}
