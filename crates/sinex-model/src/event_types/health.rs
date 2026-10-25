//! Source-normalized health records. The captured material stays the record of
//! truth.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Number, Value};
pub mod fit;
pub mod samsung;
pub mod sleep;

/// CSV normalization preserves numbers and numeric-looking text separately.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum Scalar {
    #[schemars(with = "f64")]
    Number(Number),
    Text(String),
}
fn supplied<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    de: D,
) -> Result<Option<Option<T>>, D::Error> {
    Option::<T>::deserialize(de).map(Some)
}
fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, serde_json::Error> {
    serde_json::from_value(value)
}

/// Known source categories select their fields before decoding. A malformed
/// known field is an error, never a reason to read the row as unknown.
#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum HealthValues {
    SleepSession(Box<sleep::SleepSession>),
    SleepNoise(Box<sleep::SleepNoise>),
    Fit(Box<fit::Fit>),
    Stress(Box<samsung::Stress>),
    Steps(Box<samsung::Steps>),
    Hrv(Box<samsung::Hrv>),
    HeartRate(Box<samsung::HeartRate>),
    Oxygen(Box<samsung::Oxygen>),
    Respiratory(Box<samsung::Respiratory>),
    SkinTemperature(Box<samsung::SkinTemperature>),
    Sleep(Box<samsung::Sleep>),
    SleepCombined(Box<samsung::SleepCombined>),
    SleepStage(Box<samsung::SleepStage>),
    Snoring(Box<samsung::Snoring>),
    Movement(Box<samsung::Movement>),
    Nap(Box<samsung::Nap>),
    Vitality(Box<samsung::Vitality>),
    Weight(Box<samsung::Weight>),
    Floors(Box<samsung::Floors>),
    Mood(Box<samsung::Mood>),
    ActivityDay(Box<samsung::ActivityDay>),
    Calories(Box<samsung::Calories>),
    Ecg(Box<samsung::Ecg>),
    Unknown(Map<String, Value>),
}
impl HealthValues {
    fn decode(provider: &str, category: &str, value: Value) -> Result<Self, serde_json::Error> {
        match (provider, category) {
            ("sleep-as-android", "sleep") => decode(value).map(Box::new).map(Self::SleepSession),
            ("sleep-as-android", "sleep-noise") => {
                decode(value).map(Box::new).map(Self::SleepNoise)
            }
            ("google-fit", _) => decode(value).map(Box::new).map(Self::Fit),
            ("samsung-health" | "samsung-health-cloud", category) => match category {
                "stress" | "Stress Internal Data" => decode(value).map(Box::new).map(Self::Stress),
                "step_daily_trend" | "Step Count" => decode(value).map(Box::new).map(Self::Steps),
                "hrv" | "Health HRV" => decode(value).map(Box::new).map(Self::Hrv),
                "heart_rate" | "tracker.heart_rate" | "Heart Rate" => {
                    decode(value).map(Box::new).map(Self::HeartRate)
                }
                "oxygen_saturation" | "tracker.oxygen_saturation" | "Oxygen Saturation (SpO2)" => {
                    decode(value).map(Box::new).map(Self::Oxygen)
                }
                "Health Respiratory Rate" => decode(value).map(Box::new).map(Self::Respiratory),
                "skin_temperature" | "Samsung Health Skin Temperature" => {
                    decode(value).map(Box::new).map(Self::SkinTemperature)
                }
                "Sleep" => decode(value).map(Box::new).map(Self::Sleep),
                "sleep_combined" | "Sleep Combined" => {
                    decode(value).map(Box::new).map(Self::SleepCombined)
                }
                "Sleep Stage" => decode(value).map(Box::new).map(Self::SleepStage),
                "sleep_snoring" | "Sleep Snoring" => decode(value).map(Box::new).map(Self::Snoring),
                "Health Movement" => decode(value).map(Box::new).map(Self::Movement),
                "Shealth Vitality Nap Data" => decode(value).map(Box::new).map(Self::Nap),
                "vitality_score" | "Health Vitality Score" => {
                    decode(value).map(Box::new).map(Self::Vitality)
                }
                "weight" | "Weight" => decode(value).map(Box::new).map(Self::Weight),
                "floors_climbed" | "Floors Climbed" => {
                    decode(value).map(Box::new).map(Self::Floors)
                }
                "mood" | "Shealth Mood" => decode(value).map(Box::new).map(Self::Mood),
                "Activity Day Summary" => decode(value).map(Box::new).map(Self::ActivityDay),
                "Calories Burned" => decode(value).map(Box::new).map(Self::Calories),
                "Electrocardiogram 2" => decode(value).map(Box::new).map(Self::Ecg),
                _ => decode(value).map(Self::Unknown),
            },
            _ => decode(value).map(Self::Unknown),
        }
    }
    fn unknown_fields(&self) -> Vec<String> {
        let extra = match self {
            Self::SleepSession(row) => &row.unknown,
            Self::SleepNoise(row) => &row.unknown,
            Self::Fit(row) => &row.unknown,
            Self::Stress(row) => &row.unknown,
            Self::Steps(row) => &row.unknown,
            Self::Hrv(row) => &row.unknown,
            Self::HeartRate(row) => &row.unknown,
            Self::Oxygen(row) => &row.unknown,
            Self::Respiratory(row) => &row.unknown,
            Self::SkinTemperature(row) => &row.unknown,
            Self::Sleep(row) => &row.unknown,
            Self::SleepCombined(row) => &row.unknown,
            Self::SleepStage(row) => &row.unknown,
            Self::Snoring(row) => &row.unknown,
            Self::Movement(row) => &row.unknown,
            Self::Nap(row) => &row.unknown,
            Self::Vitality(row) => &row.unknown,
            Self::Weight(row) => &row.unknown,
            Self::Floors(row) => &row.unknown,
            Self::Mood(row) => &row.unknown,
            Self::ActivityDay(row) => &row.unknown,
            Self::Calories(row) => &row.unknown,
            Self::Ecg(row) => &row.unknown,
            Self::Unknown(_) => return vec![],
        };
        let mut fields: Vec<String> = extra
            .keys()
            .map(|key| format!("/values/{}", pointer(key)))
            .collect();
        let bins = match self {
            Self::Stress(row) => row.binning_data.as_ref().and_then(Option::as_ref),
            Self::Hrv(row) => row.binning_data.as_ref().and_then(Option::as_ref),
            Self::HeartRate(row) => row.binning_data.as_ref().and_then(Option::as_ref),
            Self::Respiratory(row) => row.binning_data.as_ref().and_then(Option::as_ref),
            Self::SkinTemperature(row) => row.binning_data.as_ref().and_then(Option::as_ref),
            Self::Movement(row) => row.binning_data.as_ref().and_then(Option::as_ref),
            _ => None,
        };
        if let Some(bins) = bins {
            fields.extend(bins.unknown_fields());
        }
        if let Self::Fit(row) = self {
            fields.extend(row.nested_unknown_fields());
        }
        fields
    }
}

/// In-session markers keep their source order and nullable numeric value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HealthEvent {
    pub kind: String,
    pub at: Timestamp,
    #[schemars(with = "Option<f64>")]
    pub value: Option<Number>,
    #[serde(flatten)]
    unknown: Map<String, Value>,
}

/// One health measurement: identified by provider, category and native ID,
/// timed by its start.
#[derive(Clone, Debug, PartialEq, crate::EventPayload)]
#[event(type = "health.record", key = ["provider", "category", "native_id"], replace = newer, time = start, quality = intrinsic)]
pub struct HealthRecord {
    provider: String,
    category: String,
    pub native_id: String,
    pub start: Option<Timestamp>,
    pub end: Option<Timestamp>,
    pub offset_minutes: Option<i32>,
    values: HealthValues,
    pub events: Vec<HealthEvent>,
}
impl HealthRecord {
    pub fn values(&self) -> &HealthValues {
        &self.values
    }
    pub fn provider(&self) -> &str {
        &self.provider
    }
    pub fn category(&self) -> &str {
        &self.category
    }
    /// Decode a source-normalized row, keeping category and field type together.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        provider: String,
        category: String,
        native_id: String,
        start: Option<Timestamp>,
        end: Option<Timestamp>,
        offset_minutes: Option<i32>,
        values: Map<String, Value>,
        events: Vec<Value>,
    ) -> Result<Self, serde_json::Error> {
        let values = HealthValues::decode(&provider, &category, Value::Object(values))?;
        let events = decode(Value::Array(events))?;
        Ok(Self {
            provider,
            category,
            native_id,
            start,
            end,
            offset_minutes,
            values,
            events,
        })
    }
}
#[derive(Deserialize)]
struct WireRecord {
    provider: String,
    category: String,
    native_id: String,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
    offset_minutes: Option<i32>,
    values: Map<String, Value>,
    #[serde(default)]
    events: Vec<Value>,
}
impl<'de> Deserialize<'de> for HealthRecord {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let row = WireRecord::deserialize(de)?;
        Self::new(
            row.provider,
            row.category,
            row.native_id,
            row.start,
            row.end,
            row.offset_minutes,
            row.values,
            row.events,
        )
        .map_err(serde::de::Error::custom)
    }
}
impl Serialize for HealthRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Row<'a> {
            provider: &'a str,
            category: &'a str,
            native_id: &'a str,
            start: Option<Timestamp>,
            end: Option<Timestamp>,
            offset_minutes: Option<i32>,
            values: &'a HealthValues,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            events: &'a Vec<HealthEvent>,
            #[serde(skip_serializing_if = "std::ops::Not::not")]
            untyped: bool,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            unknown_fields: Vec<String>,
        }
        let mut fields = self.values.unknown_fields();
        for (i, event) in self.events.iter().enumerate() {
            fields.extend(
                event
                    .unknown
                    .keys()
                    .map(|key| format!("/events/{i}/{}", pointer(key))),
            );
        }
        fields.sort();
        Row {
            provider: &self.provider,
            category: &self.category,
            native_id: &self.native_id,
            start: self.start,
            end: self.end,
            offset_minutes: self.offset_minutes,
            values: &self.values,
            events: &self.events,
            untyped: matches!(self.values, HealthValues::Unknown(_)),
            unknown_fields: fields,
        }
        .serialize(serializer)
    }
}
fn pointer(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}
#[cfg(test)]
mod tests;

impl JsonSchema for HealthRecord {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "HealthRecord".into()
    }
    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        #[derive(JsonSchema)]
        #[allow(dead_code)]
        struct HealthRecordSchema {
            provider: String,
            category: String,
            native_id: String,
            start: Option<Timestamp>,
            end: Option<Timestamp>,
            offset_minutes: Option<i32>,
            values: HealthValues,
            #[serde(default)]
            events: Vec<HealthEvent>,
            #[serde(default)]
            untyped: bool,
            #[serde(default)]
            unknown_fields: Vec<String>,
        }
        HealthRecordSchema::json_schema(generator)
    }
}
