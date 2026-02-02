use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct RunStats {
    pub median_ms: f64,
    pub mean_ms: f64,
    pub stddev_ms: f64,
    pub ci95_lower: f64,
    pub ci95_upper: f64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub outliers: Vec<f64>,
    pub sample_count: usize,
}

impl RunStats {
    pub(super) fn from_samples(samples: &[f64]) -> Self {
        if samples.is_empty() {
            return Self::zero();
        }

        let mut sorted = samples.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let median = median(&sorted);
        let mean = mean(&sorted);
        let stddev = stddev(&sorted, mean);
        let (ci95_lower, ci95_upper) = ci95(mean, stddev, samples.len());
        let min = sorted[0];
        let max = sorted[sorted.len() - 1];
        let outliers = detect_outliers_iqr(&sorted);

        Self {
            median_ms: median,
            mean_ms: mean,
            stddev_ms: stddev,
            ci95_lower,
            ci95_upper,
            min_ms: min,
            max_ms: max,
            outliers,
            sample_count: samples.len(),
        }
    }

    fn zero() -> Self {
        Self {
            median_ms: 0.0,
            mean_ms: 0.0,
            stddev_ms: 0.0,
            ci95_lower: 0.0,
            ci95_upper: 0.0,
            min_ms: 0.0,
            max_ms: 0.0,
            outliers: vec![],
            sample_count: 0,
        }
    }

    pub(super) fn format_summary(&self) -> String {
        format!(
            "median={:.1}ms mean={:.1}ms σ={:.1}ms 95%CI=[{:.1}, {:.1}]",
            self.median_ms, self.mean_ms, self.stddev_ms, self.ci95_lower, self.ci95_upper
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) enum Regression {
    None,
    Detected {
        current_ms: f64,
        baseline_ms: f64,
        pct_change: f64,
        threshold_pct: f64,
    },
}

impl Regression {}

pub(super) fn compare_with_baseline(
    current: &RunStats,
    baseline: &RunStats,
    threshold_pct: f64,
) -> Regression {
    let current_ms = current.median_ms;
    let baseline_ms = baseline.median_ms;

    if baseline_ms == 0.0 {
        return Regression::None;
    }

    let pct_change = ((current_ms - baseline_ms) / baseline_ms) * 100.0;

    if pct_change > threshold_pct {
        Regression::Detected {
            current_ms,
            baseline_ms,
            pct_change,
            threshold_pct,
        }
    } else {
        Regression::None
    }
}

fn median(sorted: &[f64]) -> f64 {
    let len = sorted.len();
    if len == 0 {
        return 0.0;
    }
    if len.is_multiple_of(2) {
        f64::midpoint(sorted[len / 2 - 1], sorted[len / 2])
    } else {
        sorted[len / 2]
    }
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

fn stddev(values: &[f64], mean_val: f64) -> f64 {
    if values.len() <= 1 {
        return 0.0;
    }
    let variance =
        values.iter().map(|x| (x - mean_val).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    variance.sqrt()
}

fn ci95(mean_val: f64, stddev_val: f64, n: usize) -> (f64, f64) {
    if n == 0 {
        return (0.0, 0.0);
    }
    let t_value = t_critical(n);
    let margin = t_value * stddev_val / (n as f64).sqrt();
    (mean_val - margin, mean_val + margin)
}

fn t_critical(n: usize) -> f64 {
    // Approximate t-critical values for 95% CI
    match n {
        1 => 12.71,
        2 => 4.303,
        3 => 3.182,
        4 => 2.776,
        5 => 2.571,
        6..=10 => 2.262,
        11..=20 => 2.086,
        21..=30 => 2.042,
        _ => 1.96, // for large n, approaches z-value
    }
}

fn detect_outliers_iqr(sorted: &[f64]) -> Vec<f64> {
    if sorted.len() < 4 {
        return vec![];
    }

    let q1 = percentile(sorted, 25.0);
    let q3 = percentile(sorted, 75.0);
    let iqr = q3 - q1;
    let lower_bound = 1.5f64.mul_add(-iqr, q1);
    let upper_bound = 1.5f64.mul_add(iqr, q3);

    sorted
        .iter()
        .copied()
        .filter(|&x| x < lower_bound || x > upper_bound)
        .collect()
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_median() {
        assert_eq!(median(&[1.0, 2.0, 3.0]), 2.0);
        assert_eq!(median(&[1.0, 2.0, 3.0, 4.0]), 2.5);
        assert_eq!(median(&[5.0]), 5.0);
    }

    #[test]
    fn test_mean() {
        assert_eq!(mean(&[1.0, 2.0, 3.0, 4.0]), 2.5);
        assert_eq!(mean(&[10.0]), 10.0);
    }

    #[test]
    fn test_run_stats() {
        let samples = vec![100.0, 105.0, 95.0, 110.0, 90.0];
        let stats = RunStats::from_samples(&samples);
        assert!(stats.median_ms > 90.0 && stats.median_ms < 110.0);
        assert!(stats.mean_ms == 100.0);
    }
}
