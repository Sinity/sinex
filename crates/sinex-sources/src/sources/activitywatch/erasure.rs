use super::*;
use crate::RecordShape;

/// Matching and span coordinates survive erasure; native text does not.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ErasedSpan {
    version: u8,
    span_id: i64,
    last_row: i64,
    started_ns: i128,
    ended_ns: i128,
    merged: u32,
    window: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RowEffect {
    version: u8,
    bucket: String,
    row: i64,
    started_ns: i128,
    ended_ns: i128,
    window: String,
}

fn digest(domain: &str, text: &[&str]) -> String {
    let mut hash = blake3::Hasher::new_derive_key(domain);
    for part in text {
        hash.update(&(part.len() as u64).to_le_bytes());
        hash.update(part.as_bytes());
    }
    hash.finalize().to_hex().to_string()
}

fn bucket_key(bucket: &str) -> String {
    digest("sinex ActivityWatch erased bucket v1", &[bucket])
}

fn window_key(app: &str, title: &str) -> String {
    digest("sinex ActivityWatch erased window v1", &[app, title])
}

fn hash_valid(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn invalid() -> RecordError {
    RecordError::new("unsupported ActivityWatch erased-window state")
}

pub(super) fn validate(open: &OpenSpans) -> Result<(), RecordError> {
    for (bucket, s) in &open.erased {
        if s.version != 1
            || !hash_valid(bucket)
            || !hash_valid(&s.window)
            || s.merged == 0
            || s.span_id > s.last_row
            || open.spans.keys().any(|b| bucket_key(b) == *bucket)
        {
            return Err(invalid());
        }
        ts(s.started_ns)?;
        ts(s.ended_ns)?;
    }
    Ok(())
}

impl ErasedSpan {
    fn from_span(s: &OpenSpan) -> Self {
        Self {
            version: 1,
            span_id: s.span_id,
            last_row: s.last_row,
            started_ns: s.started_ns,
            ended_ns: s.ended_ns,
            merged: s.merged,
            window: window_key(&s.app, &s.title),
        }
    }

    fn apply(prior: Option<Self>, row: &RowEffect) -> Result<Self, RecordError> {
        ts(row.started_ns)?;
        ts(row.ended_ns)?;
        match prior {
            Some(s) if s.last_row == row.row => Ok(Self {
                ended_ns: s.ended_ns.max(row.ended_ns),
                window: row.window.clone(),
                ..s
            }),
            Some(s)
                if row.row > s.last_row
                    && s.window == row.window
                    && row.started_ns - s.ended_ns <= i128::from(CORRECTIONS.merge_gap_ns) =>
            {
                Ok(Self {
                    last_row: row.row,
                    ended_ns: s.ended_ns.max(row.ended_ns),
                    merged: s.merged.checked_add(1).ok_or_else(invalid)?,
                    ..s
                })
            }
            _ => Ok(Self {
                version: 1,
                span_id: row.row,
                last_row: row.row,
                started_ns: row.started_ns,
                ended_ns: row.ended_ns,
                merged: 1,
                window: row.window.clone(),
            }),
        }
    }
}

pub(super) fn advance(
    open: &mut OpenSpans,
    bucket: &str,
    row: i64,
    started_ns: i128,
    ended_ns: i128,
    app: String,
    title: String,
) -> Result<OpenSpan, RecordError> {
    let key = bucket_key(bucket);
    let prior = open
        .spans
        .get(bucket)
        .map(ErasedSpan::from_span)
        .or_else(|| open.erased.get(&key).cloned());
    let next = ErasedSpan::apply(
        prior,
        &RowEffect {
            version: 1,
            bucket: key.clone(),
            row,
            started_ns,
            ended_ns,
            window: window_key(&app, &title),
        },
    )?;
    open.erased.remove(&key);
    Ok(OpenSpan {
        span_id: next.span_id,
        last_row: next.last_row,
        started_ns: next.started_ns,
        ended_ns: next.ended_ns,
        merged: next.merged,
        app,
        title,
    })
}

pub(super) fn erase(
    raw: &[u8],
    open: &OpenSpans,
    context: &crate::RecordContext<'_>,
) -> Result<crate::Erased<OpenSpans>, RecordError> {
    validate(open)?;
    let row = <Map<String, Value> as RecordShape>::decode_context(raw, context)?;
    let (events, _) = ActivityWatchParser.parse(&row, &OpenSpans::default(), context)?;
    let mut carry = open.clone();
    let Some(Activity::Window(w)) = events.first() else {
        return Ok(crate::Erased {
            carry,
            replay: None,
        });
    };
    let key = bucket_key(&w.bucket);
    // A later unrelated span of this bucket is not this record's state.
    if let Some(s) = carry.spans.get(&w.bucket)
        && s.span_id <= w.span_id
        && w.span_id <= s.last_row
        && window_key(&s.app, &s.title) == window_key(&w.app, &w.title)
    {
        carry.erased.insert(key.clone(), ErasedSpan::from_span(s));
        carry.spans.remove(&w.bucket);
    }
    let replay = serde_json::to_value(RowEffect {
        version: 1,
        bucket: key,
        row: w.span_id,
        started_ns: w.started_at.unix_ns(),
        ended_ns: w.ended_at.unix_ns(),
        window: window_key(&w.app, &w.title),
    })
    .map_err(|_| invalid())?;
    Ok(crate::Erased {
        carry,
        replay: Some(replay),
    })
}

pub(super) fn skip(open: &OpenSpans, transition: &Value) -> Result<OpenSpans, RecordError> {
    validate(open)?;
    if transition.is_null() {
        return Ok(open.clone());
    }
    let effect: RowEffect = serde_json::from_value(transition.clone()).map_err(|_| invalid())?;
    if effect.version != 1 || !hash_valid(&effect.bucket) || !hash_valid(&effect.window) {
        return Err(invalid());
    }
    let mut next = open.clone();
    let bucket = next
        .spans
        .keys()
        .find(|b| bucket_key(b) == effect.bucket)
        .cloned();
    let prior = bucket
        .as_ref()
        .and_then(|b| next.spans.get(b))
        .map(ErasedSpan::from_span)
        .or_else(|| next.erased.get(&effect.bucket).cloned());
    let span = ErasedSpan::apply(prior, &effect)?;
    if let Some(bucket) = bucket {
        next.spans.remove(&bucket);
    }
    next.erased.insert(effect.bucket, span);
    Ok(next)
}
