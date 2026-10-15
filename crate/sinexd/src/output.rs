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
            ))
}
/// Optional presentation rules must also preserve selectors and control metadata.
pub fn same_bindings(before: &Value, after: &Value) -> bool {
    fn equal(a: &Value, b: &Value, display: bool) -> bool {
        match (a, b) {
            (Value::String(_), Value::String(_)) => display || a == b,
            (Value::Array(a), Value::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| equal(a, b, display))
            }
            (Value::Object(a), Value::Object(b)) => {
                a.len() == b.len()
                    && a.iter().all(|(key, a)| {
                        b.get(key)
                            .is_some_and(|b| equal(a, b, display_field(key, display)))
                    })
            }
            _ => a == b,
        }
    }
    equal(before, after, false)
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
            let mut changed = false;
            for (key, value) in fields.iter_mut() {
                if text(key)? != *key {
                    return Err(());
                }
                let display = display_field(key, presentation);
                if !presentation && key == "rows" {
                    if let Some(rows) = value.as_array_mut() {
                        for row in rows {
                            let detected = walk(row, false)?;
                            row["secret"] = Value::Bool(detected);
                            changed |= detected;
                        }
                        continue;
                    }
                }
                let detected = walk(value, display)?;
                if !presentation && key == "row" && value.is_object() {
                    value["secret"] = Value::Bool(detected);
                }
                changed |= detected;
            }
            Ok(changed)
        }
        _ => Ok(false),
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
    if admit(&mut value).is_err() || serde_json::to_writer(&mut output, &value).is_err() {
        return serde_json::to_vec(&refusal(id)).expect("static refusal serializes");
    }
    output.0
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
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

pub fn request_safe(id: &RpcId, params: &Value) -> bool {
    let id_safe = match id {
        RpcId::Number(_) => true,
        RpcId::String(raw) => text(raw).is_ok_and(|masked| masked == *raw),
    };
    id_safe && admit(&mut params.clone()).is_ok_and(|changed| !changed)
}

/// Validate raw identities/keys before optional presentation rules can alter them.
/// Discard the masked copy; only the final encoder renders the fixed floor.
pub fn validate_read(value: &Value) -> Result<(), ()> {
    admit(&mut value.clone()).map(|_| ())
}
