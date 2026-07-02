use super::{
    build_history_section, build_probe_issues_html, build_probe_issues_markdown,
    generate_results_table, html_escape,
};
use crate::bench::environment::Environment;
use crate::bench::history::{HistoryPoint, HistoryReport, ScenarioHistorySummary};
use crate::bench::runner::{RunResult, Scenario, ScenarioResult};
use crate::bench::stats::{Regression, RunStats};
use crate::sandbox::sinex_test;

fn sample_env() -> Environment {
    Environment {
        timestamp: "2026-03-27T00:00:00Z".to_string(),
        hostname: "host".to_string(),
        uname: "uname".to_string(),
        kernel: "kernel".to_string(),
        arch: "x86_64".to_string(),
        os: "NixOS".to_string(),
        cpu_model: "cpu".to_string(),
        cpu_cores: 1,
        cpu_threads: 1,
        memory_total_kb: 1024,
        memory_available_kb: 512,
        load_avg: "0.0".to_string(),
        pressure_cpu_some_avg10: Some(1.0),
        pressure_io_some_avg10: Some(2.0),
        pressure_io_full_avg10: Some(3.0),
        pressure_memory_some_avg10: Some(4.0),
        pressure_memory_full_avg10: Some(5.0),
        shm_used_mb: Some(6.0),
        shm_free_mb: Some(7.0),
        sinnix_observe_available: false,
        active_heavy_processes: vec!["pid 1: cargo test".to_string()],
        rustc_version: "rustc".to_string(),
        cargo_version: "cargo".to_string(),
        rustup_toolchain: "toolchain".to_string(),
        postgres_version: "psql".to_string(),
        database_url_masked: "postgres://***@db/sinex".to_string(),
        nats_url: "nats://127.0.0.1:4222".to_string(),
        git_sha: "abc".to_string(),
        git_sha_short: "abc".to_string(),
        git_branch: "master".to_string(),
        git_dirty: false,
        probe_issues: vec!["git_sha: boom <bad>".to_string()],
    }
}

#[sinex_test]
async fn markdown_probe_section_renders_issues() -> crate::sandbox::TestResult<()> {
    let markdown = build_probe_issues_markdown(&sample_env());
    assert!(markdown.contains("### Probe issues"));
    assert!(markdown.contains("git_sha: boom <bad>"));
    Ok(())
}

#[sinex_test]
async fn html_probe_section_escapes_issues() -> crate::sandbox::TestResult<()> {
    let html = build_probe_issues_html(&sample_env());
    assert!(html.contains("&lt;bad&gt;"));
    assert!(!html.contains("<bad>"));
    Ok(())
}

#[sinex_test]
async fn html_escape_covers_text_and_attribute_metacharacters() -> crate::sandbox::TestResult<()> {
    assert_eq!(
        html_escape("<tag attr=\"x&y\">it's</tag>"),
        "&lt;tag attr=&quot;x&amp;y&quot;&gt;it&#39;s&lt;/tag&gt;"
    );
    Ok(())
}

#[sinex_test]
async fn results_table_escapes_scenario_keys() -> crate::sandbox::TestResult<()> {
    let html = generate_results_table(&[ScenarioResult {
        scenario: Scenario {
            threads: 8,
            package: "pkg<script>alert(1)</script>".to_string(),
            db_pool_size: None,
        },
        runs: vec![RunResult {
            success: true,
            elapsed_ms: 12.0,
            stdout: String::new(),
            stderr: String::new(),
        }],
        stats: RunStats::from_samples(&[12.0]),
    }]);

    assert!(html.contains("pkg&lt;script&gt;alert(1)&lt;/script&gt;:t=8"));
    assert!(!html.contains("<script>alert(1)</script>"));
    Ok(())
}

#[sinex_test]
async fn history_section_escapes_dynamic_text() -> crate::sandbox::TestResult<()> {
    let html = build_history_section(&HistoryReport {
        run_id: 42,
        scenarios: vec![ScenarioHistorySummary {
            scenario_key: "scenario<img src=x>".to_string(),
            baseline: Some(RunStats::from_samples(&[10.0])),
            regression: Regression::None,
            trend: vec![HistoryPoint {
                median_ms: 10.0,
                p95_ms: 11.0,
                mean_ms: 10.5,
                throughput_runs_per_sec: 2.0,
                timestamp: "2026-05-02T00:00:00Z<script>".to_string(),
                git_sha: "abc<def>".to_string(),
            }],
        }],
    });

    assert!(html.contains("scenario&lt;img src=x&gt;"));
    assert!(html.contains("2026-05-02T00:00:00Z&lt;script&gt;"));
    assert!(html.contains("abc&lt;def&gt;"));
    assert!(!html.contains("<img src=x>"));
    assert!(!html.contains("<script>"));
    assert!(!html.contains("abc<def>"));
    Ok(())
}
