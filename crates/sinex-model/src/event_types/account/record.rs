//! Generic account rows retain normalized source values without interpreting
//! a provider table. A table name alone does not establish its field schema.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};

/// A fallback for account-export tables with no declared field interpretation.
#[derive(Clone, Debug, PartialEq, Deserialize, crate::EventPayload)]
#[event(
    type = "account.record",
    key = ["service", "table", "native_id"],
    replace = newer,
    time = at,
    quality = intrinsic
)]
pub struct AccountRecord {
    pub service: String,
    pub table: String,
    pub native_id: String,
    /// The row's own time, extracted by the source parser.
    pub at: Option<Timestamp>,
    /// Normalized source columns, including decoded JSON cells.
    pub values: Map<String, Value>,
}

// This wire view owns both serialization and its schema. The marker is
// computed from the fallback kind, never accepted as an interpretation claim.
#[derive(Serialize, JsonSchema)]
struct AccountRecordView<'a> {
    service: &'a str,
    table: &'a str,
    native_id: &'a str,
    at: Option<Timestamp>,
    values: &'a Map<String, Value>,
    #[serde(default = "uninterpreted")]
    uninterpreted: bool,
}
fn uninterpreted() -> bool {
    true
}
impl Serialize for AccountRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        AccountRecordView {
            service: &self.service,
            table: &self.table,
            native_id: &self.native_id,
            at: self.at,
            values: &self.values,
            uninterpreted: true,
        }
        .serialize(serializer)
    }
}
impl JsonSchema for AccountRecord {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "AccountRecord".into()
    }
    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        AccountRecordView::json_schema(generator)
    }
}

#[cfg(test)]
mod tests;
