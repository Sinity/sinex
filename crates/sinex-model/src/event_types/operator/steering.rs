//! Known steering rows keep their table's declared fields and original spelling.
//! For a source field, outer `None` means absent and `Some(None)` means null.
//! A column the row does not have stays absent.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Number, Value};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CommitmentStatus {
    Open,
    Done,
    Missed,
    Retired,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActivityKind {
    Task,
    Practice,
    Experiment,
    Leisure,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EnergyTier {
    Good,
    Any,
    Low,
}

fn supplied<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    de: D,
) -> Result<Option<Option<T>>, D::Error> {
    Option::<T>::deserialize(de).map(Some)
}

/// A commitment and forecast, including the written outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Commitment {
    pub id: String,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_start: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_end: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Option<f64>>")]
    pub forecast_p: Option<Option<Number>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Option<CommitmentStatus>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome_at: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome_note: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activity_id: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub review_id: Option<Option<String>>,
}

/// An undated activity or experiment in the steering catalog.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Activity {
    pub id: String,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<Option<ActivityKind>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub est_minutes: Option<Option<i64>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub energy_tier: Option<Option<EnergyTier>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub standing_notes: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hypothesis: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prereg_prediction: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric_ref: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub experiment_status: Option<Option<String>>,
}

/// The written review; generated_summary remains model output.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Review {
    pub id: String,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generated_summary: Option<Option<String>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator_notes: Option<Option<String>>,
}

/// A closed choice of known row shapes. Deserialization is selected by the table,
/// never by trying these variants against the same untagged object.
#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum SteeringValues {
    Commitment(Commitment),
    Activity(Activity),
    Review(Review),
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
enum Table {
    #[serde(rename = "commitments")]
    Commitments,
    #[serde(rename = "activities")]
    Activities,
    #[serde(rename = "reviews")]
    Reviews,
}

/// One steering row, identified by its table and record ID, with typed known
/// values.
#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema, crate::EventPayload)]
#[serde(deny_unknown_fields)]
#[event(type = "steering.row", key = ["table", "record_id"], replace = newer, time = at, quality = intrinsic)]
pub struct SteeringRow {
    table: Table,
    record_id: String,
    at: Option<Timestamp>,
    values: SteeringValues,
}

impl SteeringRow {
    /// Decode the row using its declared table, keeping missing and null
    /// fields.
    pub fn from_row(table: &str, values: Value) -> Result<Self, String> {
        let table = match table {
            "commitments" => Table::Commitments,
            "activities" => Table::Activities,
            "reviews" => Table::Reviews,
            other => return Err(format!("unsupported steering table {other:?}")),
        };
        Self::decode(table, values)
    }

    fn decode(table: Table, values: Value) -> Result<Self, String> {
        let values = match table {
            Table::Commitments => SteeringValues::Commitment(
                serde_json::from_value(values).map_err(|e| e.to_string())?,
            ),
            Table::Activities => {
                SteeringValues::Activity(serde_json::from_value(values).map_err(|e| e.to_string())?)
            }
            Table::Reviews => {
                SteeringValues::Review(serde_json::from_value(values).map_err(|e| e.to_string())?)
            }
        };
        let (id, written) = match &values {
            SteeringValues::Commitment(row) => {
                (&row.id, row.created_at.as_ref().and_then(|v| v.as_deref()))
            }
            SteeringValues::Activity(row) => (&row.id, None),
            SteeringValues::Review(row) => (&row.id, row.ts.as_ref().and_then(|v| v.as_deref())),
        };
        let at = written
            .filter(|s| !s.is_empty())
            .map(|s| {
                time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                    .map(Timestamp)
                    .map_err(|e| format!("invalid steering time {s:?}: {e}"))
            })
            .transpose()?;
        Ok(Self {
            table,
            record_id: id.clone(),
            at,
            values,
        })
    }
}

impl<'de> Deserialize<'de> for SteeringRow {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            table: Table,
            record_id: String,
            at: Option<Timestamp>,
            values: Value,
        }
        let wire = Wire::deserialize(de)?;
        let record = Self::decode(wire.table, wire.values).map_err(serde::de::Error::custom)?;
        if record.record_id != wire.record_id || record.at != wire.at {
            return Err(serde::de::Error::custom(
                "steering identity/time disagrees with its source row",
            ));
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests;
