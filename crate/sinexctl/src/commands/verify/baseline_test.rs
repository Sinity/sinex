use super::*;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn score_is_100_when_all_pass() -> xtask::sandbox::TestResult<()> {
    let checks = vec![
        make_check("a", CheckStatus::Pass, CheckWeight::High),
        make_check("b", CheckStatus::Pass, CheckWeight::Medium),
    ];
    assert_eq!(compute_score(&checks), 100);
    Ok(())
}

#[sinex_test]
async fn score_is_0_when_all_fail() -> xtask::sandbox::TestResult<()> {
    let checks = vec![
        make_check("a", CheckStatus::Fail, CheckWeight::High),
        make_check("b", CheckStatus::Fail, CheckWeight::Low),
    ];
    assert_eq!(compute_score(&checks), 0);
    Ok(())
}

#[sinex_test]
async fn skipped_checks_are_excluded() -> xtask::sandbox::TestResult<()> {
    let checks = vec![
        make_check("a", CheckStatus::Pass, CheckWeight::High),
        make_check("b", CheckStatus::Skipped, CheckWeight::High),
        make_check("c", CheckStatus::Fail, CheckWeight::Medium),
    ];
    // Pass=3.0*1.0=3.0, Fail=2.0*0.0=0.0, total weight=5.0, score=60
    assert_eq!(compute_score(&checks), 60);
    Ok(())
}

#[sinex_test]
async fn degraded_is_half_weight() -> xtask::sandbox::TestResult<()> {
    let checks = vec![
        make_check("a", CheckStatus::Pass, CheckWeight::High),
        make_check("b", CheckStatus::Degraded, CheckWeight::High),
    ];
    // Pass=3.0, Degraded=3.0*0.5=1.5, total=4.5/6.0=75
    assert_eq!(compute_score(&checks), 75);
    Ok(())
}

#[sinex_test]
async fn all_skipped_is_100() -> xtask::sandbox::TestResult<()> {
    let checks = vec![make_check("a", CheckStatus::Skipped, CheckWeight::High)];
    assert_eq!(compute_score(&checks), 100);
    Ok(())
}

#[sinex_test]
async fn tally_counts_correctly() -> xtask::sandbox::TestResult<()> {
    let checks = vec![
        make_check("a", CheckStatus::Pass, CheckWeight::High),
        make_check("b", CheckStatus::Pass, CheckWeight::Medium),
        make_check("c", CheckStatus::Degraded, CheckWeight::Low),
        make_check("d", CheckStatus::Fail, CheckWeight::High),
        make_check("e", CheckStatus::Skipped, CheckWeight::Low),
    ];
    let (pass, degraded, fail, skipped) = tally(&checks);
    assert_eq!(pass, 2);
    assert_eq!(degraded, 1);
    assert_eq!(fail, 1);
    assert_eq!(skipped, 1);
    Ok(())
}

fn make_check(id: &'static str, status: CheckStatus, weight: CheckWeight) -> CheckResult {
    CheckResult {
        id,
        label: id,
        weight,
        status,
        detail: None,
        recommendation: None,
    }
}
