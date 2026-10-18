//! Credential masking for RPC traffic. A response is masked as a whole before
//! any byte is sent; if it cannot be shown safely, a refusal is sent instead.
use credential_output::Masker;
use serde_json::Value;
use sinex_protocol::rpc::{ErrorKind, RpcError, RpcId, RpcResponse};
use std::io::{self, Write};

// Pages hold at most 100 rows; this caps the masked size of any response.
const RESPONSE_BYTES: usize = 128 * 1024 * 1024;
struct Limited(Vec<u8>);
impl Write for Limited {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) > RESPONSE_BYTES {
            return Err(io::Error::other("response size limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn mask_text(raw: &str) -> Result<String, ()> {
    let mut output = Limited(Vec::new());
    let mut masker = Masker::new();
    masker.push(raw, &mut output).map_err(|_| ())?;
    masker.finish(&mut output).map_err(|_| ())?;
    String::from_utf8(output.0).map_err(|_| ())
}
/// True when masking would leave `raw` as it is.
fn unchanged(raw: &str) -> bool {
    mask_text(raw).is_ok_and(|masked| masked == raw)
}
fn is_identity_key(key: &str) -> bool {
    matches!(
        key,
        "id" | "reference"
            | "ref"
            | "event"
            | "material"
            | "cursor"
            | "native_id"
            | "session"
            | "source"
            | "source_id"
            | "binding"
            | "operation_id"
            | "action_id"
            | "request_id"
            | "replaced_by"
            | "hash"
            | "content_hash"
            | "occurrence_hash"
            | "query"
    )
}
fn display_field(key: &str, presentation: bool) -> bool {
    !is_identity_key(key)
        && (presentation
            || matches!(
                key,
                "payload"
                    | "record"
                    | "command"
                    | "cwd"
                    | "hostname"
                    | "host"
                    | "message"
                    | "reason"
                    | "description"
                    | "privacy"
                    | "snippet"
                    | "text"
            ))
}
/// Mask every string in `value`. Credentials found in display fields are
/// masked; one in a key or an identifier fails the whole value.
/// Returns whether anything was masked.
pub fn mask(value: &mut Value) -> Result<bool, ()> {
    walk(value, false)
}
fn walk(value: &mut Value, presentation: bool) -> Result<bool, ()> {
    match value {
        Value::String(raw) => {
            let masked = mask_text(raw)?;
            let changed = masked != *raw;
            if changed && !presentation {
                return Err(());
            }
            *raw = masked;
            Ok(changed)
        }
        Value::Array(values) => {
            let mut changed = false;
            for value in values {
                changed |= walk(value, presentation)?;
            }
            Ok(changed)
        }
        Value::Object(fields) => {
            let snippet_text = fields.get("text").cloned();
            let is_snippet = fields.contains_key("spans") && fields.contains_key("span_unit");
            let mut changed = false;
            for (key, value) in fields.iter_mut() {
                if mask_text(key)? != *key {
                    return Err(());
                }
                let display = display_field(key, presentation);
                if !presentation && key == "rows" {
                    let rows = value.as_array_mut().ok_or(())?;
                    for row in rows {
                        if !row.is_object() {
                            return Err(());
                        }
                        let detected = walk(row, false)?;
                        row["secret"] = Value::Bool(detected);
                        changed |= detected;
                    }
                    continue;
                }
                let detected = walk(value, display)?;
                if !presentation && key == "row" && value.is_object() {
                    value["secret"] = Value::Bool(detected);
                }
                changed |= detected;
            }
            if is_snippet && snippet_text.as_ref() != fields.get("text") {
                fields.insert("spans".into(), serde_json::json!([]));
                fields.insert("highlights_unavailable".into(), serde_json::json!("masked"));
            }
            Ok(changed)
        }
        _ => Ok(false),
    }
}
/// Cut search snippets to a window around the first match. This runs after
/// masking: cutting first could separate a password from its URL or header.
fn finish_snippets(value: &mut Value) {
    let Some(rows) = value
        .get_mut("result")
        .and_then(|v| v.get_mut("rows"))
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for row in rows {
        let Some(snippet) = row.get_mut("snippet") else {
            continue;
        };
        let Some(text) = snippet["text"].as_str() else {
            continue;
        };
        let first = snippet["spans"]
            .as_array()
            .and_then(|s| s.first())
            .and_then(|s| s[0].as_u64())
            .unwrap_or(0) as usize;
        let mut start = first.saturating_sub(80).min(text.len());
        while !text.is_char_boundary(start) {
            start -= 1;
        }
        let mut end = (start + 400).min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        let truncated = text[start..end].to_string();
        let cut_after = end < text.len();
        let spans: Vec<Value> = snippet["spans"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|s| {
                let a = s[0].as_u64()? as usize;
                let b = s[1].as_u64()? as usize;
                (a >= start && b <= end).then(|| serde_json::json!([a - start, b - start]))
            })
            .collect();
        snippet["text"] = Value::String(truncated);
        snippet["spans"] = Value::Array(spans);
        snippet["cut_before"] = Value::Bool(start > 0);
        snippet["cut_after"] = Value::Bool(cut_after);
    }
}
pub fn refusal(id: Option<RpcId>) -> RpcResponse {
    RpcResponse::failure(
        id,
        RpcError::new(
            ErrorKind::Refused,
            "response refused: a credential appears in a key or identifier, or the response is too large to mask",
            Value::Null,
        ),
    )
}
pub fn encode(response: RpcResponse) -> Vec<u8> {
    let id = response.id().cloned().filter(|id| match id {
        RpcId::String(raw) => unchanged(raw),
        RpcId::Number(_) => true,
    });
    let mut value = serde_json::to_value(&response).expect("RPC response serializes");
    let mut output = Limited(Vec::new());
    if mask(&mut value).is_err() {
        return serde_json::to_vec(&refusal(id)).expect("static refusal serializes");
    }
    finish_snippets(&mut value);
    if serde_json::to_writer(&mut output, &value).is_err() {
        return serde_json::to_vec(&refusal(id)).expect("static refusal serializes");
    }
    output.0
}

pub fn request_safe(id: &RpcId, params: &Value) -> bool {
    let id_safe = match id {
        RpcId::Number(_) => true,
        RpcId::String(raw) => unchanged(raw),
    };
    // Only checks; request values are never changed. Parameter shapes are
    // checked by the protocol validator afterwards.
    fn safe(value: &Value) -> bool {
        match value {
            Value::String(raw) => unchanged(raw),
            Value::Array(values) => values.iter().all(safe),
            Value::Object(fields) => fields
                .iter()
                .all(|(key, value)| unchanged(key) && safe(value)),
            _ => true,
        }
    }
    id_safe && safe(params)
}
#[cfg(test)]
#[path = "output_tests.rs"]
mod tests;
