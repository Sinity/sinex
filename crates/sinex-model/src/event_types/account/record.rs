//! Generic account rows keep normalized source values without typing a provider
//! table. A table name alone does not establish its field schema.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};

/// A fallback for account-export tables with no declared field types.
#[derive(Clone, Debug, PartialEq, Deserialize, crate::EventPayload)]
#[event(version = 2,
    type = "account.row",
    key = ["service", "table", "native_id"],
    replace = newer,
    time = at,
    quality = intrinsic
)]
pub struct AccountRow {
    pub service: String,
    pub table: String,
    pub native_id: String,
    /// The row's own time, extracted by the source parser.
    pub at: Option<Timestamp>,
    /// Normalized source columns, including decoded JSON cells.
    pub values: Map<String, Value>,
}

// This wire view owns both serialization and its schema. The marker is computed
// from the fallback kind, never accepted from the input.
#[derive(Serialize, JsonSchema)]
struct AccountRecordView<'a> {
    service: &'a str,
    table: &'a str,
    native_id: &'a str,
    at: Option<Timestamp>,
    values: &'a Map<String, Value>,
    #[serde(default = "untyped")]
    untyped: bool,
}
fn untyped() -> bool {
    true
}
impl Serialize for AccountRow {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        AccountRecordView {
            service: &self.service,
            table: &self.table,
            native_id: &self.native_id,
            at: self.at,
            values: &self.values,
            untyped: true,
        }
        .serialize(serializer)
    }
}
impl JsonSchema for AccountRow {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "AccountRow".into()
    }
    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        AccountRecordView::json_schema(generator)
    }
}

#[cfg(test)]
mod tests;
