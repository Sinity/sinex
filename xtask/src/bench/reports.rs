use super::{
    config::BenchConfig, environment::Environment, history::HistoryReport, runner::ScenarioResult,
};
use color_eyre::eyre::{Result, WrapErr};
use std::path::Path;

pub(super) fn generate_markdown(
    config: &BenchConfig,
    env: &Environment,
    results: &[ScenarioResult],
    history: Option<&HistoryReport>,
    output_path: &Path,
) -> Result<()> {
    let mut md = String::new();
    let probe_issues = build_probe_issues_markdown(env);

    md.push_str("# Nextest Benchmark Report\n\n");
    md.push_str(&format!("**Generated:** {}\n\n", env.timestamp));

    md.push_str("## Configuration\n\n");
    md.push_str("| Setting | Value |\n");
    md.push_str("|---------|-------|\n");
    md.push_str(&format!("| Mode | {} |\n", config.mode));
    md.push_str(&format!("| Profile | {} |\n", config.profile));
    md.push_str(&format!("| Runs | {} |\n", config.runs));
    md.push_str(&format!(
        "| Threads | {} |\n",
        config
            .threads
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    ));
    md.push_str(&format!("| Git SHA | {} |\n", env.git_sha_short));
    md.push('\n');

    md.push_str("## Environment\n\n");
    md.push_str("```\n");
    md.push_str(&format!("CPU:      {}\n", env.cpu_model));
    md.push_str(&format!(
        "Cores:    {} cores / {} threads\n",
        env.cpu_cores, env.cpu_threads
    ));
    md.push_str(&format!(
        "Memory:   {} GB\n",
        env.memory_total_kb / 1024 / 1024
    ));
    md.push_str(&format!("Rust:     {}\n", env.rustc_version));
    md.push_str(&format!("OS:       {}\n", env.os));
    md.push_str("```\n\n");
    md.push_str(&probe_issues);

    md.push_str("## Results\n\n");

    if results.is_empty() {
        md.push_str("_No results available_\n");
    } else {
        md.push_str("| Scenario | Median (ms) | P95 (ms) | Mean (ms) | Stddev (ms) | Throughput (runs/s) | Min (ms) | Max (ms) | Samples |\n");
        md.push_str("|----------|-------------|----------|-----------|-------------|----------------------|----------|----------|---------|\n");

        for result in results {
            md.push_str(&format!(
                "| {} | {:.1} | {:.1} | {:.1} | {:.1} | {:.2} | {:.1} | {:.1} | {} |\n",
                result.scenario.key(),
                result.stats.median_ms,
                result.stats.p95_ms,
                result.stats.mean_ms,
                result.stats.stddev_ms,
                result.stats.throughput_runs_per_sec,
                result.stats.min_ms,
                result.stats.max_ms,
                result.stats.sample_count
            ));
        }
    }

    if let Some(history) = history {
        md.push_str("\n## Historical Context\n\n");
        md.push_str(&format!("Run ID: **{}**\n\n", history.run_id));
        for scenario in &history.scenarios {
            md.push_str(&format!("### {}\n", scenario.scenario_key));
            if let Some(baseline) = &scenario.baseline {
                md.push_str(&format!(
                    "- Baseline median/p95: {:.1}ms / {:.1}ms (throughput: {:.2} runs/s, samples: {})\n",
                    baseline.median_ms,
                    baseline.p95_ms,
                    baseline.throughput_runs_per_sec,
                    baseline.sample_count
                ));
            } else {
                md.push_str("- Baseline: _(none)_\n");
            }
            md.push_str(&format!(
                "- Regression: {}\n",
                scenario.regression_description()
            ));
            md.push_str("- Recent trend:\n");
            if scenario.trend.is_empty() {
                md.push_str("  - _(no historical data)_\n");
            } else {
                for point in &scenario.trend {
                    md.push_str(&format!(
                        "  - {} · median/p95 {:.1}ms / {:.1}ms · throughput {:.2} runs/s (git {})\n",
                        point.timestamp,
                        point.median_ms,
                        point.p95_ms,
                        point.throughput_runs_per_sec,
                        point.git_sha
                    ));
                }
            }
            md.push('\n');
        }
    }

    md.push('\n');

    std::fs::write(output_path, md).with_context(|| {
        format!(
            "Failed to write markdown report to {}",
            output_path.display()
        )
    })?;

    Ok(())
}

pub(super) fn generate_html(
    config: &BenchConfig,
    env: &Environment,
    results: &[ScenarioResult],
    history: Option<&HistoryReport>,
    output_path: &Path,
) -> Result<()> {
    let chart_data = generate_chart_data(results);
    let history_section = history.map(build_history_section).unwrap_or_default();
    let probe_issues_section = build_probe_issues_html(env);

    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nextest Benchmark Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }}
        h1, h2 {{
            color: #333;
        }}
        .card {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background: #f8f9fa;
            font-weight: 600;
        }}
        .meta {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }}
        .meta-item {{
            padding: 10px;
            background: #f8f9fa;
            border-radius: 4px;
        }}
        .meta-label {{
            font-size: 12px;
            color: #666;
            text-transform: uppercase;
        }}
        .meta-value {{
            font-size: 16px;
            font-weight: 600;
            color: #333;
        }}
    </style>
</head>
<body>
    <div class="card">
        <h1>Nextest Benchmark Report</h1>
        <p><strong>Generated:</strong> {}</p>
    </div>

    <div class="card">
        <h2>Configuration</h2>
        <div class="meta">
            <div class="meta-item">
                <div class="meta-label">Mode</div>
                <div class="meta-value">{}</div>
            </div>
            <div class="meta-item">
                <div class="meta-label">Profile</div>
                <div class="meta-value">{}</div>
            </div>
            <div class="meta-item">
                <div class="meta-label">Runs</div>
                <div class="meta-value">{}</div>
            </div>
            <div class="meta-item">
                <div class="meta-label">Git SHA</div>
                <div class="meta-value">{}</div>
            </div>
        </div>
    </div>

    <div class="card">
        <h2>Environment</h2>
        <div class="meta">
            <div class="meta-item">
                <div class="meta-label">CPU</div>
                <div class="meta-value">{}</div>
            </div>
            <div class="meta-item">
                <div class="meta-label">Cores / Threads</div>
                <div class="meta-value">{} / {}</div>
            </div>
            <div class="meta-item">
                <div class="meta-label">Memory</div>
                <div class="meta-value">{} GB</div>
            </div>
            <div class="meta-item">
                <div class="meta-label">Rust Version</div>
                <div class="meta-value">{}</div>
            </div>
        </div>
    </div>
    {}

    <div class="card">
        <h2>Results Chart</h2>
        <canvas id="resultsChart"></canvas>
    </div>

    <div class="card">
        <h2>Results Table</h2>
        {}
    </div>
    {}

    <script>
        const ctx = document.getElementById('resultsChart');
        const chartData = {};
        new Chart(ctx, {{
            type: 'bar',
            data: {{
                labels: chartData.labels,
                datasets: [{{
                    label: 'Median (ms)',
                    data: chartData.medians,
                    backgroundColor: 'rgba(54, 162, 235, 0.5)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1
                }}]
            }},
            options: {{
                responsive: true,
                scales: {{
                    y: {{
                        beginAtZero: true,
                        title: {{
                            display: true,
                            text: 'Time (ms)'
                        }}
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
"#,
        env.timestamp,
        config.mode,
        config.profile,
        config.runs,
        env.git_sha_short,
        env.cpu_model,
        env.cpu_cores,
        env.cpu_threads,
        env.memory_total_kb / 1024 / 1024,
        env.rustc_version,
        probe_issues_section,
        generate_results_table(results),
        history_section,
        chart_data
    );

    std::fs::write(output_path, html)
        .with_context(|| format!("Failed to write HTML report to {}", output_path.display()))?;

    Ok(())
}

fn generate_results_table(results: &[ScenarioResult]) -> String {
    if results.is_empty() {
        return "<p><em>No results available</em></p>".to_string();
    }

    let mut table = String::from("<table>\n");
    table.push_str("<thead>\n<tr>\n");
    table.push_str("<th>Scenario</th>\n");
    table.push_str("<th>Median (ms)</th>\n");
    table.push_str("<th>P95 (ms)</th>\n");
    table.push_str("<th>Mean (ms)</th>\n");
    table.push_str("<th>Stddev (ms)</th>\n");
    table.push_str("<th>Throughput (runs/s)</th>\n");
    table.push_str("<th>Min (ms)</th>\n");
    table.push_str("<th>Max (ms)</th>\n");
    table.push_str("<th>Samples</th>\n");
    table.push_str("</tr>\n</thead>\n<tbody>\n");

    for result in results {
        table.push_str("<tr>\n");
        table.push_str(&format!("<td>{}</td>\n", result.scenario.key()));
        table.push_str(&format!("<td>{:.1}</td>\n", result.stats.median_ms));
        table.push_str(&format!("<td>{:.1}</td>\n", result.stats.p95_ms));
        table.push_str(&format!("<td>{:.1}</td>\n", result.stats.mean_ms));
        table.push_str(&format!("<td>{:.1}</td>\n", result.stats.stddev_ms));
        table.push_str(&format!(
            "<td>{:.2}</td>\n",
            result.stats.throughput_runs_per_sec
        ));
        table.push_str(&format!("<td>{:.1}</td>\n", result.stats.min_ms));
        table.push_str(&format!("<td>{:.1}</td>\n", result.stats.max_ms));
        table.push_str(&format!("<td>{}</td>\n", result.stats.sample_count));
        table.push_str("</tr>\n");
    }

    table.push_str("</tbody>\n</table>\n");
    table
}

fn build_probe_issues_markdown(env: &Environment) -> String {
    if env.probe_issues.is_empty() {
        return String::new();
    }

    let mut md = String::from("### Probe issues\n\n");
    for issue in &env.probe_issues {
        md.push_str("- ");
        md.push_str(issue);
        md.push('\n');
    }
    md.push('\n');
    md
}

fn build_probe_issues_html(env: &Environment) -> String {
    if env.probe_issues.is_empty() {
        return String::new();
    }

    let mut items = String::new();
    for issue in &env.probe_issues {
        items.push_str("<li>");
        items.push_str(&html_escape(issue));
        items.push_str("</li>");
    }
    format!(
        r#"<div class="card">
        <h2>Probe issues</h2>
        <ul>{items}</ul>
    </div>"#
    )
}

fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn generate_chart_data(results: &[ScenarioResult]) -> String {
    let labels: Vec<String> = results.iter().map(|r| r.scenario.key()).collect();
    let medians: Vec<f64> = results.iter().map(|r| r.stats.median_ms).collect();

    serde_json::json!({
        "labels": labels,
        "medians": medians,
    })
    .to_string()
}

fn build_history_section(report: &HistoryReport) -> String {
    if report.scenarios.is_empty() {
        return "<div class=\"card\"><h2>Historical Context</h2><p><em>No history available.</em></p></div>".to_string();
    }

    let mut html = String::from("<div class=\"card\"><h2>Historical Context</h2>");
    html.push_str(&format!(
        "<p>Run ID: <strong>{}</strong></p>",
        report.run_id
    ));

    for scenario in &report.scenarios {
        html.push_str("<div class=\"meta\" style=\"margin-bottom: 12px;\">");
        html.push_str(&format!(
            "<div class=\"meta-item\"><div class=\"meta-label\">Scenario</div><div class=\"meta-value\">{}</div></div>",
            scenario.scenario_key
        ));
        let baseline = scenario
            .baseline
            .as_ref()
            .map(|b| format!("{:.1} / {:.1} ms", b.median_ms, b.p95_ms));
        html.push_str(&format!(
            "<div class=\"meta-item\"><div class=\"meta-label\">Baseline median / p95</div><div class=\"meta-value\">{}</div></div>",
            baseline.unwrap_or_else(|| "n/a".to_string())
        ));
        let baseline_tput = scenario
            .baseline
            .as_ref()
            .map(|b| format!("{:.2} runs/s", b.throughput_runs_per_sec));
        html.push_str(&format!(
            "<div class=\"meta-item\"><div class=\"meta-label\">Baseline throughput</div><div class=\"meta-value\">{}</div></div>",
            baseline_tput.unwrap_or_else(|| "n/a".to_string())
        ));
        html.push_str(&format!(
            "<div class=\"meta-item\"><div class=\"meta-label\">Regression</div><div class=\"meta-value\">{}</div></div>",
            scenario.regression_description()
        ));
        html.push_str("</div>");

        if scenario.trend.is_empty() {
            html.push_str("<p><em>No trend data available.</em></p>");
        } else {
            html.push_str("<table><thead><tr><th>Timestamp</th><th>Median (ms)</th><th>P95 (ms)</th><th>Mean (ms)</th><th>Throughput (runs/s)</th><th>Git SHA</th></tr></thead><tbody>");
            for point in &scenario.trend {
                html.push_str(&format!(
                    "<tr><td>{}</td><td>{:.1}</td><td>{:.1}</td><td>{:.1}</td><td>{:.2}</td><td>{}</td></tr>",
                    point.timestamp,
                    point.median_ms,
                    point.p95_ms,
                    point.mean_ms,
                    point.throughput_runs_per_sec,
                    point.git_sha
                ));
            }
            html.push_str("</tbody></table>");
        }
    }

    html.push_str("</div>");
    html
}

#[cfg(test)]
mod tests {
    use super::{build_probe_issues_html, build_probe_issues_markdown};
    use crate::bench::environment::Environment;
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
}
