use super::*;

fn close(a: f64, b: f64, tol: f64) -> bool {
    (a - b).abs() < tol
}

#[test]
fn ln_gamma_matches_known_values() {
    assert!(close(ln_gamma(0.5), PI.sqrt().ln(), 1e-12));
    assert!(close(ln_gamma(5.0), 24f64.ln(), 1e-12));
    assert!(close(ln_gamma(10.5), 13.940_625_219_403_763, 1e-10));
}

#[test]
fn student_t_matches_tables() {
    // Critical values: t(0.975, 10) = 2.228139, t(0.995, 30) = 2.749996.
    assert!(close(
        t_two_sided_p(2.228_138_851_986_272, 10.0),
        0.05,
        1e-7
    ));
    assert!(close(t_two_sided_p(2.749_995_653_567_08, 30.0), 0.01, 1e-7));
    assert!(close(
        t_two_sided_p(2.0, 10.0),
        0.073_388_034_770_165_38,
        1e-9
    ));
    assert!(close(t_two_sided_p(0.0, 5.0), 1.0, 1e-12));
}

#[test]
fn normal_matches_tables() {
    assert!(close(normal_cdf(Z_975), 0.975, 2e-7));
    assert!(close(normal_cdf(0.0), 0.5, 2e-7));
    assert!(close(z_two_sided_p(1.0), 0.317_310_507_862_914, 2e-7));
}

#[test]
fn pearson_and_spearman_against_direct_examples() {
    let x = [1.0, 2.0, 3.0, 4.0, 5.0];
    let y = [2.0, 4.0, 5.0, 4.0, 5.0];
    // Σ(x−3)(y−4) = 6, Σ(x−3)² = 10, Σ(y−4)² = 6 → r = 6/√60.
    assert!(close(pearson(&x, &y).unwrap(), 6.0 / 60f64.sqrt(), 1e-12));
    assert_eq!(pearson(&x, &[1.0; 5]), None);
    assert_eq!(ranks(&[10.0, 20.0, 20.0, 5.0]), vec![2.0, 3.5, 3.5, 1.0]);
}

#[test]
fn mad_is_scaled_to_sigma() {
    let v = [1.0, 2.0, 3.0, 4.0, 100.0];
    assert_eq!(median(&v), Some(3.0));
    assert!(close(scaled_mad(&v).unwrap(), 1.4826, 1e-12));
}

#[test]
fn fisher_bounds() {
    let (lo, hi) = r_interval(0.5, 28.0).unwrap();
    assert!(lo < 0.5 && hi > 0.5 && lo > 0.0);
    // About 0.5 at 29 pairs (Cohen's tables give n = 29 for r = .5).
    assert!(close(min_detectable_r(29.0).unwrap(), 0.5, 0.02));
}
