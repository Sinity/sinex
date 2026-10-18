//! Whole-response presentation admission. No bytes are released before this succeeds.
use credential_output::Masker;
use serde_json::Value;
use sinex_protocol::rpc::{ErrorKind, RpcError, RpcId, RpcResponse};
use std::io::{self, Write};

// Existing pages contain at most 100 rows. This also bounds marker expansion and
// non-query registered responses; recognition never concatenates logical strings.
const RESPONSE_BYTES: usize = 128 * 1024 * 1024;
struct Limited(Vec<u8>);
impl Write for Limited {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) > RESPONSE_BYTES {
            return Err(io::Error::other("presentation budget"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn text(raw: &str) -> Result<String, ()> {
    let mut output = Limited(Vec::new());
    let mut masker = Masker::new();
    masker.push(raw, &mut output).map_err(|_| ())?;
    masker.finish(&mut output).map_err(|_| ())?;
    String::from_utf8(output.0).map_err(|_| ())
}
fn identity(key: &str) -> bool {
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
    !identity(key)
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
pub fn admit(value: &mut Value) -> Result<bool, ()> {
    walk(value, false)
}
fn walk(value: &mut Value, presentation: bool) -> Result<bool, ()> {
    match value {
        Value::String(raw) => {
            let masked = text(raw)?;
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
                if text(key)? != *key {
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
/// Cutting happens only after the complete logical string passed the fixed
/// floor. Cutting raw text first could detach a password from its URL/header.
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
            "credential presentation unavailable: identity, structural key or bounded recognition cannot be represented safely",
            Value::Null,
        ),
    )
}
pub fn encode(response: RpcResponse) -> Vec<u8> {
    let id = response.id().cloned().filter(|id| match id {
        RpcId::String(raw) => text(raw).is_ok_and(|masked| masked == *raw),
        RpcId::Number(_) => true,
    });
    let mut value = serde_json::to_value(&response).expect("RPC response serializes");
    let mut output = Limited(Vec::new());
    if admit(&mut value).is_err() {
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
        RpcId::String(raw) => text(raw).is_ok_and(|masked| masked == *raw),
    };
    // Screening does not decorate or change request values. The protocol
    // validator, called next, owns method-specific parameter shapes.
    fn safe(value: &Value) -> bool {
        match value {
            Value::String(raw) => text(raw).is_ok_and(|masked| masked == *raw),
            Value::Array(values) => values.iter().all(safe),
            Value::Object(fields) => fields
                .iter()
                .all(|(key, value)| text(key).is_ok_and(|masked| masked == *key) && safe(value)),
            _ => true,
        }
    }
    id_safe && safe(params)
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn malformed_request_rows_reach_protocol_validation_without_panicking() {
        use sinex_protocol::rpc::{Method, RpcRequest};
        for params in [
            json!({"rows":[1]}),
            json!({"rows":[[]]}),
            json!({"rows":[null]}),
            json!({"rows":[true]}),
            json!({"rows":["plain"]}),
            json!({"rows":1}),
            json!({"rows":[]}),
            json!([1]),
            json!(null),
        ] {
            let original = params.clone();
            assert!(request_safe(&RpcId::Number(7), &params));
            assert_eq!(params, original);
            let error = RpcRequest::new(RpcId::Number(7), Method::Status, params)
                .validate()
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::BadQuery);
        }
    }
    #[test]
    fn malformed_response_rows_refuse_before_encoding_success() {
        for rows in [
            json!([1]),
            json!([[]]),
            json!([null]),
            json!([true]),
            json!(["plain"]),
            json!(1),
            json!(null),
            json!({}),
        ] {
            let bytes = encode(RpcResponse::success(RpcId::Number(9), json!({"rows":rows})));
            let wire: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(wire["id"], 9);
            assert_eq!(wire["error"]["kind"], "refused");
            assert!(wire.get("result").is_none());
        }
        let bytes = encode(RpcResponse::success(RpcId::Number(9), json!({"rows":[]})));
        let wire: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(wire["result"]["rows"], json!([]));
    }
    #[test]
    fn whole_document_refusal_discards_staged_text_and_ambiguous_keys() {
        let token = format!("ghp_{}", "A".repeat(36));
        for payload in [
            json!({token.clone():"one", "other":"two"}),
            json!({"ordinary":"before", "nested":[{"text":format!("before https://u:{}", "A".repeat(600))}]}),
        ] {
            let bytes = encode(RpcResponse::success(
                RpcId::Number(7),
                json!({"row":{"payload":payload}}),
            ));
            let wire: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(wire["id"], 7);
            assert_eq!(wire["error"]["kind"], "refused");
            assert!(wire.get("result").is_none());
            assert!(!String::from_utf8(bytes).unwrap().contains(&token));
        }
        let mut read = json!({"rows":[{"payload":{"nested":["--secret=x",{"text":"plain"}]}}]});
        admit(&mut read).unwrap();
        assert_eq!(read["rows"][0]["secret"], true);
        assert_eq!(read["rows"][0]["payload"]["nested"][1]["text"], "plain");
    }
    #[test]
    fn values_metadata_and_rows_mask_but_keys_and_identities_refuse() {
        let token = format!("ghp_{}", "A".repeat(36));
        let mut row = json!({"payload":{"command":format!("before {token} after"),"nested":["--secret=x"]},"hostname":token});
        assert!(admit(&mut row).unwrap());
        assert_eq!(
            row["payload"]["command"],
            "before [credential: GitHub token, masked] after"
        );
        for value in [
            json!({token.clone():"x"}),
            json!({"native_id":token.clone()}),
            json!({"page":{"cursor":token.clone()}}),
        ] {
            assert!(admit(&mut value.clone()).is_err());
        }
        let bytes = encode(RpcResponse::success(
            RpcId::String(token.clone()),
            json!({"ok":true}),
        ));
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["id"], Value::Null);
        assert_eq!(value["error"]["kind"], "refused");
        assert!(!String::from_utf8(bytes).unwrap().contains(&token));
    }
}
