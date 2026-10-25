//! The distribution functions the methods need: log-gamma, the regularized
//! incomplete beta function (Student's t), and the normal distribution.
//! Implemented here so the analysis crate stays dependency-free; each is
//! checked against published values in the tests.
use std::f64::consts::PI;

/// ln Γ(x) for x > 0 (Lanczos approximation, g = 7, n = 9).
fn ln_gamma(x: f64) -> f64 {
    const G: f64 = 7.0;
    const C: [f64; 9] = [
        0.999_999_999_999_809_9,
        676.520_368_121_885_1,
        -1_259.139_216_722_402_8,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];
    if x < 0.5 {
        // Reflection: Γ(x)Γ(1−x) = π / sin(πx).
        return (PI / (PI * x).sin()).ln() - ln_gamma(1.0 - x);
    }
    let x = x - 1.0;
    let mut a = C[0];
    let t = x + G + 0.5;
    for (i, c) in C.iter().enumerate().skip(1) {
        a += c / (x + i as f64);
    }
    0.5 * (2.0 * PI).ln() + (x + 0.5) * t.ln() - t + a.ln()
}

/// The regularized incomplete beta function I_x(a, b).
fn incomplete_beta(x: f64, a: f64, b: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    let ln_front = ln_gamma(a + b) - ln_gamma(a) - ln_gamma(b) + a * x.ln() + b * (1.0 - x).ln();
    // The continued fraction converges fast for x < (a+1)/(a+b+2); use the
    // symmetry I_x(a,b) = 1 − I_{1−x}(b,a) otherwise.
    if x < (a + 1.0) / (a + b + 2.0) {
        ln_front.exp() * beta_fraction(x, a, b) / a
    } else {
        1.0 - ln_front.exp() * beta_fraction(1.0 - x, b, a) / b
    }
}

/// Lentz's method for the incomplete beta continued fraction.
fn beta_fraction(x: f64, a: f64, b: f64) -> f64 {
    const TINY: f64 = 1e-300;
    const EPS: f64 = 1e-15;
    let mut c = 1.0;
    let mut d = 1.0 - (a + b) * x / (a + 1.0);
    if d.abs() < TINY {
        d = TINY;
    }
    d = 1.0 / d;
    let mut h = d;
    for m in 1..=300 {
        let m = f64::from(m);
        let m2 = 2.0 * m;
        let aa = m * (b - m) * x / ((a + m2 - 1.0) * (a + m2));
        d = 1.0 + aa * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = 1.0 + aa / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        h *= d * c;
        let aa = -(a + m) * (a + b + m) * x / ((a + m2) * (a + m2 + 1.0));
        d = 1.0 + aa * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = 1.0 + aa / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        let del = d * c;
        h *= del;
        if (del - 1.0).abs() < EPS {
            break;
        }
    }
    h
}

/// Two-sided p-value of Student's t with `df` degrees of freedom.
fn t_two_sided_p(t: f64, df: f64) -> f64 {
    if !t.is_finite() {
        return 0.0;
    }
    incomplete_beta(df / (df + t * t), df / 2.0, 0.5)
}

/// The two-sided p-value of a Pearson r over `n` pairs (t with n−2 df).
pub fn r_p_value(r: f64, n: f64) -> f64 {
    let df = n - 2.0;
    if df <= 0.0 {
        return 1.0;
    }
    if r.abs() >= 1.0 {
        return 0.0;
    }
    t_two_sided_p(r * (df / (1.0 - r * r)).sqrt(), df)
}

/// The standard normal CDF, through erfc.
fn normal_cdf(z: f64) -> f64 {
    0.5 * erfc(-z / std::f64::consts::SQRT_2)
}

/// erfc via the continued-fraction-free Chebyshev fit of Numerical Recipes
/// (fractional error below 1.2e-7 everywhere).
fn erfc(x: f64) -> f64 {
    let z = x.abs();
    let t = 1.0 / (1.0 + 0.5 * z);
    let poly = -z * z - 1.265_512_23
        + t * (1.000_023_68
            + t * (0.374_091_96
                + t * (0.096_784_18
                    + t * (-0.186_288_06
                        + t * (0.278_868_07
                            + t * (-1.135_203_98
                                + t * (1.488_515_87 + t * (-0.822_152_23 + t * 0.170_872_77))))))));
    let r = t * poly.exp();
    if x >= 0.0 { r } else { 2.0 - r }
}

/// Two-sided p-value of a standard normal z.
pub fn z_two_sided_p(z: f64) -> f64 {
    2.0 * (1.0 - normal_cdf(z.abs()))
}

pub const Z_975: f64 = 1.959_963_984_540_054;
const Z_80: f64 = 0.841_621_233_572_914_3;

/// The smallest |r| detectable at two-sided α = 0.05 with power 0.8 over
/// `n` effective pairs (Fisher z).
pub fn min_detectable_r(n: f64) -> Option<f64> {
    (n > 3.0).then(|| ((Z_975 + Z_80) / (n - 3.0).sqrt()).tanh())
}

/// A 95% interval for r over `n` effective pairs (Fisher z).
pub fn r_interval(r: f64, n: f64) -> Option<(f64, f64)> {
    if n <= 3.0 || r.abs() >= 1.0 {
        return None;
    }
    let z = r.atanh();
    let se = 1.0 / (n - 3.0).sqrt();
    Some(((z - Z_975 * se).tanh(), (z + Z_975 * se).tanh()))
}

pub fn mean(v: &[f64]) -> f64 {
    v.iter().sum::<f64>() / v.len() as f64
}

/// Pearson r; `None` when either side is constant or there are fewer than 2 pairs.
pub fn pearson(x: &[f64], y: &[f64]) -> Option<f64> {
    if x.len() != y.len() || x.len() < 2 {
        return None;
    }
    let (mx, my) = (mean(x), mean(y));
    let (mut sxy, mut sxx, mut syy) = (0.0, 0.0, 0.0);
    for (a, b) in x.iter().zip(y) {
        sxy += (a - mx) * (b - my);
        sxx += (a - mx) * (a - mx);
        syy += (b - my) * (b - my);
    }
    (sxx > 0.0 && syy > 0.0).then(|| (sxy / (sxx * syy).sqrt()).clamp(-1.0, 1.0))
}

/// Ranks with ties given their average rank (1-based).
pub fn ranks(v: &[f64]) -> Vec<f64> {
    let mut idx: Vec<usize> = (0..v.len()).collect();
    idx.sort_by(|&a, &b| v[a].total_cmp(&v[b]));
    let mut out = vec![0.0; v.len()];
    let mut i = 0;
    while i < idx.len() {
        let mut j = i;
        while j + 1 < idx.len() && v[idx[j + 1]] == v[idx[i]] {
            j += 1;
        }
        let rank = (i + j) as f64 / 2.0 + 1.0;
        for k in &idx[i..=j] {
            out[*k] = rank;
        }
        i = j + 1;
    }
    out
}

pub fn median(v: &[f64]) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    let mut s = v.to_vec();
    s.sort_by(f64::total_cmp);
    let n = s.len();
    Some(if n % 2 == 1 {
        s[n / 2]
    } else {
        (s[n / 2 - 1] + s[n / 2]) / 2.0
    })
}

/// Median absolute deviation, scaled by 1.4826 to estimate σ under normality.
pub fn scaled_mad(v: &[f64]) -> Option<f64> {
    let m = median(v)?;
    let dev: Vec<f64> = v.iter().map(|x| (x - m).abs()).collect();
    median(&dev).map(|d| d * 1.4826)
}

#[cfg(test)]
mod tests;
