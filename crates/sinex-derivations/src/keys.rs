//! Typed keys retain the JSON shapes stored for keyed derivations.
use jiff::civil::Date;
use serde::{Deserialize, Serialize};
use sinex_model::Timestamp;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DayKey {
    pub day: Date,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostDayKey {
    pub host: String,
    pub day: Date,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricDayKey {
    pub metric: String,
    pub day: Date,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleDayKey {
    pub state: String,
    pub day: Date,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HourKey {
    pub hour: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DayScopeKey {
    pub day: Date,
    pub scope: String,
}

#[cfg(test)]
mod tests;
