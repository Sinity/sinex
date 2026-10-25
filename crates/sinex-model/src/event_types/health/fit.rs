use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Number;

/// One local day of a Google Fit data source's points of one data type. The
/// points themselves stay in the stored material, where the record's byte
/// range finds them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FitDay {
    /// The day the points start on, `YYYY-MM-DD` in `zone`; null for points
    /// without a start time.
    pub day: Option<String>,
    /// The IANA zone the day is reckoned in.
    pub zone: String,
    /// How many points the day has.
    pub points: u64,
    /// The numbers at each position of the points' `fitValue`, in order.
    pub fields: Vec<FitField>,
}

/// The numeric values (`intVal` or `fpVal`) at one `fitValue` position.
/// A position holding only text or maps has a count of zero and no
/// minimum, maximum or mean.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FitField {
    pub count: u64,
    pub sum: f64,
    #[schemars(with = "Option<f64>")]
    pub min: Option<Number>,
    #[schemars(with = "Option<f64>")]
    pub max: Option<Number>,
    pub mean: Option<f64>,
}

impl FitField {
    /// Add one value. `min` and `max` keep the value as exported.
    pub fn add(&mut self, value: &Number) {
        let number = |n: &Number| n.as_f64().unwrap_or(f64::NAN);
        let x = number(value);
        self.count += 1;
        self.sum += x;
        if self.min.as_ref().is_none_or(|m| x < number(m)) {
            self.min = Some(value.clone());
        }
        if self.max.as_ref().is_none_or(|m| x > number(m)) {
            self.max = Some(value.clone());
        }
        self.mean = Some(self.sum / self.count as f64);
    }
}
