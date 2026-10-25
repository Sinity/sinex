//! Registered methods: numerical procedures with the question they estimate and
//! their assumptions stated in code. A method refuses before computing (too few
//! units, a constant series, gaps where it needs a dense series) and every
//! result counts what it excluded and why.
pub mod baseline;
pub mod changepoints;
pub mod correlation;
pub mod fdr;
pub mod stats;
pub mod trend;

use crate::cell::{Cell, Eligibility, Exclusion};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// One unit of a series: its label and cell, in unit order. Series given to a
/// method list every calendar unit of their range, missing ones included.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub unit: String,
    pub cell: Cell<f64>,
}

/// What a method's result estimates.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Estimand {
    pub unit: &'static str,
    /// The eligible units, as a description over the panel.
    pub population: String,
    pub question: String,
    pub dependence: Dependence,
    /// True only when the design identifies a cause; no registered method's does.
    pub causal: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Dependence {
    Independent,
    Autocorrelated,
}

/// The series a method reads, by metric id, and which cells it may use.
#[derive(Clone, Debug, Default)]
pub struct MethodInput {
    pub series: BTreeMap<String, Vec<Point>>,
    pub eligibility: Eligibility,
}

impl MethodInput {
    pub fn get(&self, metric: &str) -> Result<&[Point], NotEstimable> {
        self.series
            .get(metric)
            .map(Vec::as_slice)
            .ok_or_else(|| NotEstimable::new(format!("no series for {metric}")))
    }

    /// The eligible values of a series with their positions, and the
    /// exclusion counts.
    pub fn eligible(&self, points: &[Point]) -> (Vec<(usize, f64)>, Excluded) {
        let mut ok = Vec::new();
        let mut excluded = Excluded::default();
        for (i, p) in points.iter().enumerate() {
            match p.cell.eligible(self.eligibility) {
                Ok(v) => ok.push((i, v)),
                Err(e) => excluded.add(e),
            }
        }
        (ok, excluded)
    }
}

/// Counts of excluded units per reason.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Excluded(pub BTreeMap<Exclusion, u64>);

impl Excluded {
    pub fn add(&mut self, e: Exclusion) {
        *self.0.entry(e).or_default() += 1;
    }

    pub fn total(&self) -> u64 {
        self.0.values().sum()
    }
}

/// A completed estimate.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MethodResult {
    pub method: &'static str,
    pub version: u32,
    pub estimand: Estimand,
    pub output: Value,
    /// Units used.
    pub n: u64,
    pub n_excluded: Excluded,
    pub diagnostics: Value,
}

/// A legitimate answer: the method refused, and why ("2 paired days, 10 needed").
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct NotEstimable {
    pub reason: String,
    pub n: u64,
    pub n_excluded: Excluded,
}

impl NotEstimable {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            n: 0,
            n_excluded: Excluded::default(),
        }
    }

    pub fn with_counts(mut self, n: u64, excluded: Excluded) -> Self {
        self.n = n;
        self.n_excluded = excluded;
        self
    }
}

/// A registered method.
pub trait Method {
    const ID: &'static str;
    const VERSION: u32;
    const SUMMARY: &'static str;
    type Params: Serialize + DeserializeOwned;

    fn estimand(p: &Self::Params) -> Estimand;
    fn run(p: &Self::Params, input: &MethodInput) -> Result<MethodResult, NotEstimable>;
}

/// The registered methods: id, version and what each estimates.
pub fn catalog() -> Vec<(&'static str, u32, &'static str)> {
    vec![
        entry::<correlation::LaggedCorrelation>(),
        entry::<correlation::RankCorrelation>(),
        entry::<trend::Trend>(),
        entry::<changepoints::ChangePoints>(),
        entry::<baseline::RobustBaseline>(),
        entry::<baseline::Anomaly>(),
    ]
}

fn entry<M: Method>() -> (&'static str, u32, &'static str) {
    (M::ID, M::VERSION, M::SUMMARY)
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum MethodError {
    #[error("no registered method {0}")]
    Unknown(String),
    #[error("parameters for {method}: {error}")]
    Params { method: String, error: String },
}

/// Run a registered method by id, with JSON parameters.
pub fn run(
    id: &str,
    params: &Value,
    input: &MethodInput,
) -> Result<Result<MethodResult, NotEstimable>, MethodError> {
    match id {
        correlation::LaggedCorrelation::ID => {
            dispatch::<correlation::LaggedCorrelation>(params, input)
        }
        correlation::RankCorrelation::ID => dispatch::<correlation::RankCorrelation>(params, input),
        trend::Trend::ID => dispatch::<trend::Trend>(params, input),
        changepoints::ChangePoints::ID => dispatch::<changepoints::ChangePoints>(params, input),
        baseline::RobustBaseline::ID => dispatch::<baseline::RobustBaseline>(params, input),
        baseline::Anomaly::ID => dispatch::<baseline::Anomaly>(params, input),
        other => Err(MethodError::Unknown(other.to_owned())),
    }
}

fn dispatch<M: Method>(
    params: &Value,
    input: &MethodInput,
) -> Result<Result<MethodResult, NotEstimable>, MethodError> {
    let p: M::Params = serde_json::from_value(params.clone()).map_err(|e| MethodError::Params {
        method: M::ID.to_owned(),
        error: e.to_string(),
    })?;
    Ok(M::run(&p, input))
}

/// A dense series: every unit eligible. Methods that need one refuse a series
/// with holes, listing them, and never interpolate.
pub fn dense(input: &MethodInput, points: &[Point]) -> Result<Vec<f64>, NotEstimable> {
    let mut values = Vec::with_capacity(points.len());
    let mut holes = Vec::new();
    let mut excluded = Excluded::default();
    for p in points {
        match p.cell.eligible(input.eligibility) {
            Ok(v) => values.push(v),
            Err(e) => {
                excluded.add(e);
                holes.push(p.unit.clone());
            }
        }
    }
    if holes.is_empty() {
        Ok(values)
    } else {
        let shown: Vec<_> = holes.iter().take(10).cloned().collect();
        Err(NotEstimable::new(format!(
            "needs a dense series; {} unit(s) not eligible: {}{}",
            holes.len(),
            shown.join(", "),
            if holes.len() > shown.len() {
                ", …"
            } else {
                ""
            }
        ))
        .with_counts(values.len() as u64, excluded))
    }
}

#[cfg(test)]
pub(crate) fn observed(units: &[f64]) -> Vec<Point> {
    units
        .iter()
        .enumerate()
        .map(|(i, &v)| Point {
            unit: format!("u{i}"),
            cell: Cell::Observed { value: v },
        })
        .collect()
}
