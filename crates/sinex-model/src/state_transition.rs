//! The interpretation shared by a state fold and its bounded input reader.
use serde_json::Value;

pub const SCOPE_FIELDS: &[&str] = &["scope", "bucket"];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SubjectField {
    /// A nonempty source string, optionally distinguished by a prefix.
    Text {
        field: &'static str,
        prefix: &'static str,
    },
    /// Strings stay literal; every other non-null JSON value is serialized.
    Scalar {
        field: &'static str,
        prefix: &'static str,
    },
}

impl SubjectField {
    pub fn field(self) -> &'static str {
        match self {
            Self::Text { field, .. } | Self::Scalar { field, .. } => field,
        }
    }
    fn read(self, payload: &Value) -> Option<String> {
        let value = payload.get(self.field())?;
        let (prefix, value) = match self {
            Self::Text { prefix, .. } => {
                (prefix, value.as_str().filter(|s| !s.is_empty())?.to_owned())
            }
            Self::Scalar { prefix, .. } => (
                prefix,
                match value {
                    Value::Null => return None,
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                },
            ),
        };
        Some(format!("{prefix}{value}"))
    }
}

pub fn subject(fields: &[SubjectField], payload: &Value) -> Option<String> {
    fields.iter().find_map(|field| field.read(payload))
}

/// Empty source strings are still captured scopes. Other JSON types do not
/// invent a scope; use the next source field or the stable captured identity.
pub fn scope(payload: &Value, captured: impl FnOnce() -> String) -> String {
    SCOPE_FIELDS
        .iter()
        .find_map(|field| payload.get(*field).and_then(Value::as_str))
        .map(str::to_owned)
        .unwrap_or_else(captured)
}

/// Project only the values needed by these keys and the existing record key.
pub fn payload_fields(subjects: &[SubjectField]) -> Vec<&'static str> {
    let mut fields = SCOPE_FIELDS.to_vec();
    fields.push(crate::record::NATIVE_ID);
    for subject in subjects {
        if !fields.contains(&subject.field()) {
            fields.push(subject.field());
        }
    }
    fields
}

/// The existing input-kind rule is top-level value equality, not JSON array
/// containment. Readers may fetch a superset, but cannot invent a stop from it.
pub fn matches<'a>(
    wanted: &str,
    fields: impl IntoIterator<Item = (&'a str, &'a Value)>,
    event_type: &str,
    payload: &Value,
) -> bool {
    wanted == event_type
        && fields
            .into_iter()
            .all(|(field, value)| payload.get(field) == Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn subjects_keep_literal_fallback_and_json_scalar_meaning() {
        let focus = [
            SubjectField::Text {
                field: "window",
                prefix: "",
            },
            SubjectField::Text {
                field: "class",
                prefix: "class:",
            },
        ];
        assert_eq!(
            subject(&focus, &json!({"window":"", "class":"app"})),
            Some("class:app".into())
        );
        assert_eq!(
            subject(&focus, &json!({"window":"class:app", "class":"other"})),
            Some("class:app".into())
        );
        assert_eq!(
            subject(&focus, &json!({"window":false, "class":null})),
            None
        );
        let workspace = [
            SubjectField::Scalar {
                field: "id",
                prefix: "workspace:",
            },
            SubjectField::Scalar {
                field: "name",
                prefix: "workspace:",
            },
        ];
        for (payload, expected) in [
            (json!({"id":null,"name":""}), "workspace:"),
            (json!({"id":2}), "workspace:2"),
            (json!({"id":false}), "workspace:false"),
            (json!({"id":[1,{"aa":true}]}), "workspace:[1,{\"aa\":true}]"),
        ] {
            assert_eq!(subject(&workspace, &payload).as_deref(), Some(expected));
        }
    }
    #[test]
    fn scopes_use_strings_then_bucket_then_the_captured_record() {
        for (payload, expected) in [
            (json!({"scope":"","bucket":"b"}), ""),
            (json!({"scope":false,"bucket":"b"}), "b"),
            (json!({"scope":null,"bucket":3}), "captured"),
        ] {
            assert_eq!(scope(&payload, || "captured".into()), expected);
        }
    }
}
