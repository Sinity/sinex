//! Binary forecast scoring over the caller's visible, resolved pairs.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Pair {
    pub forecast: String,
    pub probability: f64,
    pub outcome: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Bin {
    pub lower: f64,
    pub upper: f64,
    pub count: usize,
    pub mean_probability: Option<f64>,
    pub outcome_frequency: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Report {
    pub status: String,
    pub issued: usize,
    pub resolved: usize,
    pub unresolved: usize,
    pub brier: Option<f64>,
    /// Ten fixed equal-width bins, with 1.0 included in the final bin.
    pub calibration: Vec<Bin>,
    pub pairs: Vec<Pair>,
}

pub fn score(pairs: Vec<Pair>) -> Result<Report, String> {
    let mut ids = std::collections::BTreeSet::new();
    let mut counts = [0usize; 10];
    let mut probabilities = [0.0; 10];
    let mut outcomes = [0.0; 10];
    let mut squared_error = 0.0;
    let mut resolved = 0;
    for pair in &pairs {
        if !ids.insert(&pair.forecast) {
            return Err("duplicate forecast in scoring inputs".into());
        }
        let p = pair.probability;
        if !p.is_finite() || !(0.0..=1.0).contains(&p) {
            return Err("probability must be finite and between 0 and 1".into());
        }
        if let Some(outcome) = pair.outcome {
            let y = if outcome { 1.0 } else { 0.0 };
            let bin = ((p * 10.0).floor() as usize).min(9);
            counts[bin] += 1;
            probabilities[bin] += p;
            outcomes[bin] += y;
            squared_error += (p - y).powi(2);
            resolved += 1;
        }
    }
    Ok(Report {
        status: if resolved == 0 {
            "not_estimable"
        } else {
            "estimated"
        }
        .into(),
        issued: pairs.len(),
        resolved,
        unresolved: pairs.len() - resolved,
        brier: (resolved > 0).then(|| squared_error / resolved as f64),
        calibration: (0..10)
            .map(|i| Bin {
                lower: i as f64 / 10.0,
                upper: (i + 1) as f64 / 10.0,
                count: counts[i],
                mean_probability: (counts[i] > 0).then(|| probabilities[i] / counts[i] as f64),
                outcome_frequency: (counts[i] > 0).then(|| outcomes[i] / counts[i] as f64),
            })
            .collect(),
        pairs,
    })
}

#[cfg(test)]
mod tests;
