use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Number;

/// One run of a Google Fit data source's consecutive points of one data type
/// on one local day. A day whose points the file interleaves with other days
/// has several runs; its figures are the runs' combined: counts and sums
/// add, the minimum and maximum are the runs' extremes, and a mean is the sum
/// over the count. The points stay in the stored material, where the
/// record's byte range finds them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FitRun {
    /// The day the points start on, `YYYY-MM-DD` in `zone`; null for points
    /// without a start time.
    pub day: Option<String>,
    /// The IANA zone the day is reckoned in.
    pub zone: String,
    /// How many runs of the same data type and day came earlier in the file:
    /// 0 for the first, so a day that arrived split has a run 1.
    pub run: u32,
    /// How many points the run has.
    pub points: u64,
    /// The numbers at each position of the points' `fitValue`, in order.
    pub fields: Vec<FitField>,
}

/// The numeric values (`intVal` or `fpVal`) at one `fitValue` position.
/// A position holding only text or maps has a count of zero and no
/// minimum or maximum.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FitField {
    pub count: u64,
    pub sum: f64,
    #[schemars(with = "Option<f64>")]
    pub min: Option<Number>,
    #[schemars(with = "Option<f64>")]
    pub max: Option<Number>,
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
    }
}
