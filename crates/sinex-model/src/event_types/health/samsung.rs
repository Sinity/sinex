use super::{Scalar, supplied};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Common {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub day_time: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_time: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_offset: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datauuid: Option<Option<String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Stress {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_score: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binning_data: Option<Option<Binning>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Steps {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Hrv {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binning_data: Option<Option<Binning>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HeartRate {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heart_rate: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heart_beat_count: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binning_data: Option<Option<Binning>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Oxygen {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spo2: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub low_duration: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Respiratory {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub average: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lower_limit: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upper_limit: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binning_data: Option<Option<Binning>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SkinTemperature {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binning_data: Option<Option<Binning>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Sleep {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deviceuuid: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sleep_score: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sleep_duration: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_recovery: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mental_recovery: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub efficiency: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sleep_cycle: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub movement_awakening: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SleepCombined {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deviceuuid: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sleep_score: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sleep_duration: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub physical_recovery: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mental_recovery: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub efficiency: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sleep_cycle: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub movement_awakening: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_rem_duration: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_light_duration: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deep_score: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rem_score: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wake_score: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SleepStage {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sleep_id: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Snoring {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Movement {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binning_data: Option<Option<Binning>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Nap {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_before: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_after: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vitality_day_time: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Vitality {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activity_score: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_score: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sleep_score: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shr_score: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shrv_score: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activity_level: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Weight {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_fat: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub muscle_mass: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skeletal_muscle_mass: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skeletal_muscle: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub basal_metabolic_rate: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_fat_mass: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_body_water: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Floors {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub floor: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Mood {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mood_type: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emotions: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub factors: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ActivityDay {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_time: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_count: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub walk_time: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_time: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calorie: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Calories {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_calorie: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rest_calorie: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tef_calorie: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_time: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Ecg {
    #[serde(flatten)]
    pub common: Common,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean_heart_rate: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_count: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_frequency: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_key: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_mime: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Bin {
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_min: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_max: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flag: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdnn: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rmssd: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heart_rate: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heart_rate_min: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heart_rate_max: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub respiratory_rate: Option<Option<Scalar>>,
    #[serde(default, deserialize_with = "supplied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activity_level: Option<Option<Scalar>>,
    #[serde(flatten)]
    pub(crate) uninterpreted: Map<String, Value>,
}

/// A companion-file reference, decoded source bins, or an unrecognized object.
#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum Binning {
    Reference(String),
    Rows(Vec<Bin>),
    UninterpretedObject(Map<String, Value>),
}
impl<'de> Deserialize<'de> for Binning {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(de)?;
        match value {
            Value::String(s) => Ok(Self::Reference(s)),
            Value::Array(_) => serde_json::from_value(value)
                .map(Self::Rows)
                .map_err(serde::de::Error::custom),
            Value::Object(object) => Ok(Self::UninterpretedObject(object)),
            _ => Err(serde::de::Error::custom(
                "binning_data must be a reference, bin array or unknown object",
            )),
        }
    }
}

impl Binning {
    pub(crate) fn uninterpreted_fields(&self) -> Vec<String> {
        match self {
            Self::UninterpretedObject(_) => vec!["/values/binning_data".into()],
            Self::Reference(_) => vec![],
            Self::Rows(rows) => rows
                .iter()
                .enumerate()
                .flat_map(|(i, row)| {
                    row.uninterpreted
                        .keys()
                        .map(move |key| format!("/values/binning_data/{i}/{}", super::pointer(key)))
                })
                .collect(),
        }
    }
}
