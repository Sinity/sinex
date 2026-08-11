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

#[sinex_test]
async fn xtask_stderr_summary_truncates_non_ascii_without_panicking() -> xtask::sandbox::TestResult<()>
{
    // Repeat a 3-byte Japanese character so the 500-byte cut point lands
    // mid-codepoint (500 is not a multiple of 3: byte 500 falls inside the
    // character spanning bytes 498..501). A naive `&s[..500]` slice panics;
    // this captures subprocess stderr, which can contain non-ASCII output.
    let stderr: String = "あ".repeat(200); // 600 bytes
    assert!(stderr.len() > 500, "fixture must exceed the truncation threshold in bytes");
    let result = XtaskResult {
        success: false,
        stderr,
    };

    let summary = result.stderr_summary();

    assert!(summary.ends_with('…'));
    assert!(std::str::from_utf8(summary.as_bytes()).is_ok());
    Ok(())
}

#[sinex_test]
async fn xtask_stderr_summary_passes_through_short_ascii() -> xtask::sandbox::TestResult<()> {
    let result = XtaskResult {
        success: true,
        stderr: "  short output  ".to_string(),
    };
    assert_eq!(result.stderr_summary(), "short output");
    Ok(())
}

/// sinex-recr: the closure-health discovery step used to shell out to
/// `gh issue list` (a substrate retired on 2026-07-10) and pass numeric
/// issue ids to a verifier that hard-requires bead string ids like
/// "sinex-e7e9" -- guaranteeing either a permanent Skipped or a guaranteed
/// false Fail. This exercises the real `bd list --json` output shape
/// against the actual production parser, proving it extracts bead string
/// ids (not numbers) and respects the 20-item cap.
#[sinex_test]
async fn recent_closures_parses_bead_string_ids_not_numeric_issue_ids()
-> xtask::sandbox::TestResult<()> {
    let bd_json = serde_json::to_vec(&serde_json::json!([
        {"id": "sinex-e7e9", "title": "retire verify-closure.yml", "issue_type": "task"},
        {"id": "sinex-x79t", "title": "derivation reconciler", "issue_type": "task"},
    ]))?;

    let ids = parse_recently_closed_bead_ids(&bd_json).map_err(|e| color_eyre::eyre::eyre!(e))?;

    assert_eq!(
        ids,
        vec!["sinex-e7e9".to_string(), "sinex-x79t".to_string()],
        "must extract bead string ids from bd's own JSON shape, not attempt \
         to parse a numeric GitHub issue `number` field that doesn't exist \
         in bd output"
    );
    Ok(())
}

#[sinex_test]
async fn recent_closures_caps_at_twenty_beads() -> xtask::sandbox::TestResult<()> {
    let many: Vec<serde_json::Value> = (0..35)
        .map(|i| serde_json::json!({"id": format!("sinex-fixture{i}"), "issue_type": "task"}))
        .collect();
    let bd_json = serde_json::to_vec(&many)?;

    let ids = parse_recently_closed_bead_ids(&bd_json).map_err(|e| color_eyre::eyre::eyre!(e))?;

    assert_eq!(
        ids.len(),
        20,
        "must cap to the most recently closed 20 beads, matching the \
         original issue-list window size, so the per-bead xtask verify \
         closure loop stays fast"
    );
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
