use super::output::{ServiceRunStatus, SummaryData, SummaryOutput};
use super::{format_age, redact_runtime_target_url, runtime_target_kind_label};
use crate::history::{InvocationStatus, VelocityTrend};
use crate::runtime_metrics::IngestdStatus;
use console::style;

pub(super) fn render(output: &SummaryOutput, data: &SummaryData) {
    MotdRenderer::new(output, data).render();
}

/// Visual width of section labels including leading/trailing spaces.
/// All sections use this for consistent left-column alignment.
/// Layout: "  label" (2 + up to 6) + padding to 11 = "  build    " etc.
const LABEL_COL: usize = 11;

struct MotdRenderer<'a> {
    width: usize,
    output: &'a SummaryOutput,
    data: &'a SummaryData,
}

impl<'a> MotdRenderer<'a> {
    fn new(output: &'a SummaryOutput, data: &'a SummaryData) -> Self {
        let width = console::Term::stdout().size().1.max(80).min(120) as usize;
        Self {
            width,
            output,
            data,
        }
    }

    fn render(&self) {
        // Header (always)
        self.render_header();

        // Runtime target (always)
        self.render_target();

        // Infra + services (always)
        self.render_infra();

        // Build status (when any history exists)
        self.render_build();

        // Velocity trends (when meaningful data exists)
        self.render_loop_velocity();
        self.render_baseline_velocity();

        // Recommendations (when critical/warning exist)
        self.render_recommendations();

        // Runtime metrics (when services are active)
        self.render_runtime();

        // Active jobs (when >0)
        self.render_jobs();

        // Git working directory (when notable)
        self.render_git();
    }

    // ─── Header ─────────────────────────────────────────────────────────

    fn render_header(&self) {
        let w = self.width;
        let inner = w - 2; // inside the box (excluding │ on each side)

        // Top border
        println!("┌{}┐", "─".repeat(inner));

        // Content: "  sinex" left, "score   branch" right
        let left = format!("  {}", style("sinex").bold());
        let left_vis = console::measure_text_width(&left);

        let score_part = match self.data.history.health_report.as_ref() {
            Some(r) => {
                let s = format!("{}/100", r.score);
                if r.score >= 80 {
                    style(s).green().to_string()
                } else if r.score >= 60 {
                    style(s).yellow().to_string()
                } else {
                    style(s).red().to_string()
                }
            }
            None => style("--/100").dim().to_string(),
        };

        let branch_raw = self.data.git.branch.as_deref().unwrap_or("-");
        let max_branch = 20;
        let branch_name = if branch_raw.len() > max_branch {
            format!("{}…", &branch_raw[..max_branch - 1])
        } else {
            branch_raw.to_string()
        };
        let branch_part = if self.data.git.dirty {
            style(&branch_name).bold().to_string()
        } else {
            style(&branch_name).dim().to_string()
        };

        // Ahead/behind inline after branch
        let mut ab_part = String::new();
        if self.data.git.ahead > 0 {
            ab_part.push_str(&format!(
                " {}",
                style(format!("↑{}", self.data.git.ahead)).cyan()
            ));
        }
        if self.data.git.behind > 0 {
            ab_part.push_str(&format!(
                " {}",
                style(format!("↓{}", self.data.git.behind)).red()
            ));
        }

        let right = format!("{score_part}   {branch_part}{ab_part}  ");
        let right_vis = console::measure_text_width(&right);

        let padding = inner.saturating_sub(left_vis + right_vis);
        println!("│{}{}{}│", left, " ".repeat(padding), right);

        // Bottom border
        println!("└{}┘", "─".repeat(inner));
    }

    fn render_target(&self) {
        let label = style("  target").dim();
        let target = &self.output.runtime_target;
        let kind = runtime_target_kind_label(&target.kind);
        let source = target
            .source
            .as_deref()
            .map(|source| format!(" source {source}"))
            .unwrap_or_default();
        let db = target
            .database_url
            .as_deref()
            .map_or_else(|| "db unset".to_string(), redact_runtime_target_url);
        let gateway = target.gateway_url.as_deref().unwrap_or("gateway unset");

        println!(
            "{label}   {} {} {} {}",
            style(format!("{} ({kind})", target.name)).cyan(),
            style(source).dim(),
            style("·").dim(),
            style(format!("{db} · {gateway}")).dim()
        );
    }

    // ─── Infrastructure + Services ──────────────────────────────────────

    fn render_infra(&self) {
        let label = style("  infra").dim();

        let pg = if self.data.pg_probe.ready() {
            style("pg:ready").green().to_string()
        } else {
            style("pg:offline").red().bold().to_string()
        };

        let nats = if self.data.nats_probe.ready() {
            style("nats:reachable").green().to_string()
        } else {
            style("nats:offline").red().bold().to_string()
        };

        if self.data.services.is_empty() {
            println!("{label}    {pg}  {nats}");
        } else {
            let svc_parts: Vec<String> = self
                .data
                .services
                .iter()
                .map(|s| {
                    let short = s.name.strip_prefix("sinex-").unwrap_or(&s.name);
                    match s.status {
                        ServiceRunStatus::Running => {
                            style(format!("{short}:up")).green().to_string()
                        }
                        ServiceRunStatus::Stopped => {
                            style(format!("{short}:down")).red().to_string()
                        }
                        ServiceRunStatus::Skipped => {
                            style(format!("{short}:skip")).dim().to_string()
                        }
                        ServiceRunStatus::Unknown => {
                            style(format!("{short}:unknown")).yellow().to_string()
                        }
                    }
                })
                .collect();
            println!(
                "{label}    {pg}  {nats} {} {}",
                style("·").dim(),
                svc_parts.join("  ")
            );
        }
    }

    // ─── Build Status ───────────────────────────────────────────────────

    fn render_build(&self) {
        let cmds = &self.output.last_commands;
        let has_any = cmds.check.is_some() || cmds.test.is_some() || cmds.build.is_some();
        let show_history_note =
            self.output.history.synthetic || self.output.history.message.is_some();
        if !has_any && !show_history_note {
            return;
        }

        let label = style("  build").dim();
        let mut parts = Vec::new();

        for (name, cmd) in [
            ("check", &cmds.check),
            ("test", &cmds.test),
            ("build", &cmds.build),
        ] {
            if let Some(info) = cmd {
                let icon = if matches!(info.status, InvocationStatus::Success) {
                    style("✓").green().to_string()
                } else {
                    style("✗").red().to_string()
                };
                let age = format_age(info.age_mins);
                let dur = info
                    .duration_secs
                    .map_or_else(|| "?".to_string(), |duration| format!("{duration:.1}s"));
                parts.push(format!(
                    "{} {} {} {}",
                    name,
                    icon,
                    style(age).dim(),
                    style(dur).dim()
                ));
            }
        }

        if parts.is_empty() {
            println!(
                "{label}    {}",
                style("no recorded xtask invocations").dim()
            );
        } else {
            println!("{label}    {}", parts.join("   "));
        }

        // Diagnostics sub-line — show what's wrong and where
        let d = &self.output.diagnostics;
        if d.errors > 0 || d.warnings > 0 {
            let mut diag_parts = Vec::new();

            if d.errors > 0 {
                // Include package names for context
                let err_label = if self.data.history.error_packages.len() == 1 {
                    format!(
                        "{} error in {}",
                        d.errors, self.data.history.error_packages[0]
                    )
                } else if self.data.history.error_packages.len() <= 3
                    && !self.data.history.error_packages.is_empty()
                {
                    format!(
                        "{} error{} in {}",
                        d.errors,
                        if d.errors == 1 { "" } else { "s" },
                        self.data.history.error_packages.join(", ")
                    )
                } else {
                    format!("{} error{}", d.errors, if d.errors == 1 { "" } else { "s" })
                };
                diag_parts.push(style(err_label).red().bold().to_string());
            }

            if d.warnings > 0 {
                diag_parts.push(
                    style(format!(
                        "{} warning{}",
                        d.warnings,
                        if d.warnings == 1 { "" } else { "s" }
                    ))
                    .yellow()
                    .to_string(),
                );
            }
            if d.fixable > 0 {
                diag_parts.push(style(format!("{} fixable", d.fixable)).yellow().to_string());
            }
            if d.flaky_tests > 0 {
                diag_parts.push(
                    style(format!("{} flaky", d.flaky_tests))
                        .yellow()
                        .to_string(),
                );
            }

            // Action hint: most specific useful command
            let action = if d.fixable > 0 {
                format!(
                    " {} {}",
                    style("→").dim(),
                    style("xtask fix --smart").cyan()
                )
            } else if d.errors > 0 {
                format!(
                    " {} {}",
                    style("→").dim(),
                    style("xtask history diagnostics --level error").cyan()
                )
            } else {
                String::new()
            };

            let sep = format!(" {} ", style("·").dim());
            let indent = " ".repeat(LABEL_COL);
            println!("{indent}{}{action}", diag_parts.join(&sep));
        }

        if self.output.history.synthetic {
            let indent = " ".repeat(LABEL_COL);
            println!(
                "{indent}{}",
                style("history DB is synthetic; trends and diagnostics are seeded").yellow()
            );
        } else if let Some(message) = self.output.history.message.as_deref() {
            let indent = " ".repeat(LABEL_COL);
            println!("{indent}{}", style(message).yellow());
        }
    }

    // ─── Velocity ───────────────────────────────────────────────────────

    fn render_velocity_line(&self, label_text: &str, trends: &[VelocityTrend]) {
        let meaningful: Vec<_> = trends
            .iter()
            .filter(|v| v.sample_count >= 4 && v.recent_avg_secs.is_some())
            .collect();

        if meaningful.is_empty() {
            return;
        }

        let label = style(label_text).dim();
        let parts: Vec<String> = meaningful
            .iter()
            .map(|v| {
                let avg = format!("~{:.1}s", v.recent_avg_secs.unwrap_or(0.0));
                let delta = match v.delta_pct {
                    Some(d) if d < -5.0 => style(format!("↓{:.0}%", d.abs())).green().to_string(),
                    Some(d) if d > 5.0 => style(format!("↑{d:.0}%")).red().to_string(),
                    _ => style("→").dim().to_string(),
                };
                let label = match v.scope_label.as_deref() {
                    Some(scope) if !scope.is_empty() => format!("{} [{}]", v.command, scope),
                    _ => v.command.clone(),
                };
                format!("{label} {avg} {delta}")
            })
            .collect();

        println!("{label}    {}", parts.join("   "));
    }

    fn render_loop_velocity(&self) {
        self.render_velocity_line("  loop", &self.data.history.velocity);
    }

    fn render_baseline_velocity(&self) {
        self.render_velocity_line("  repo", &self.data.history.baseline_velocity);
    }

    // ─── Recommendations ────────────────────────────────────────────────

    fn render_recommendations(&self) {
        let actionable: Vec<_> = self
            .data
            .history
            .recommendations
            .iter()
            .filter(|r| r.severity != "info")
            .collect();

        if actionable.is_empty() {
            return;
        }

        let max_show = 3;
        for (i, rec) in actionable.iter().take(max_show).enumerate() {
            let label_text = "  action";
            let label = if i == 0 {
                style(label_text).dim().to_string()
            } else {
                " ".repeat(label_text.len())
            };

            let icon = if rec.severity == "critical" {
                style("✗").red().to_string()
            } else {
                style("⚠").yellow().to_string()
            };

            let action = style(&rec.action).cyan();
            println!(
                "{label}   {icon} {} {} {action}",
                rec.description,
                style("→").dim()
            );
        }

        if actionable.len() > max_show {
            let overflow = actionable.len() - max_show;
            let indent = " ".repeat(LABEL_COL);
            println!(
                "{indent}{} {}",
                style(format!("+{overflow} more")).dim(),
                style(format!("→ {}", "xtask analytics recommend")).cyan()
            );
        }
    }

    // ─── Runtime ────────────────────────────────────────────────────────

    fn render_runtime(&self) {
        let label = style("  runtime").dim();
        let Some(metrics) = &self.data.runtime_metrics else {
            println!(
                "{label}  {}",
                style("unavailable (runtime database target not configured)").dim()
            );
            return;
        };

        let assessment = metrics.assessment();
        let lag_high = assessment
            .warnings
            .iter()
            .any(|warning| warning.contains("consumer lag is high"));
        let batch_high = assessment
            .warnings
            .iter()
            .any(|warning| warning.contains("batch latency is high"));

        let status = match metrics.ingestd_status {
            IngestdStatus::Healthy => style("ingestd ok").green().to_string(),
            IngestdStatus::Stale => style("ingestd stale").yellow().to_string(),
            IngestdStatus::Down => style("ingestd down").red().to_string(),
            IngestdStatus::Unknown => style("ingestd unknown").dim().to_string(),
        };

        let lag = metrics
            .fresh_consumer_lag_pending()
            .map(|v| {
                let s = format!("{v:.0}");
                let colored = if lag_high {
                    style(s).red().to_string()
                } else if matches!(
                    assessment.status,
                    crate::runtime_metrics::RuntimeHealthStatus::Healthy
                ) {
                    style(s).green().to_string()
                } else {
                    style(s).yellow().to_string()
                };
                format!("lag {colored}")
            })
            .or_else(|| {
                metrics
                    .consumer_lag_age_secs
                    .filter(|_| metrics.consumer_lag_is_stale())
                    .map(|age| style(format!("lag stale ({age}s)")).yellow().to_string())
            })
            .unwrap_or_default();

        let batch = metrics
            .fresh_batch_latency_ms()
            .map(|v| {
                let summary = format!("batch {}ms", v as u64);
                if batch_high {
                    style(summary).red().to_string()
                } else if matches!(
                    assessment.status,
                    crate::runtime_metrics::RuntimeHealthStatus::Healthy
                ) {
                    style(summary).green().to_string()
                } else {
                    style(summary).yellow().to_string()
                }
            })
            .or_else(|| {
                metrics
                    .last_batch_latency_age_secs
                    .filter(|_| metrics.batch_latency_is_stale())
                    .map(|age| style(format!("batch stale ({age}s)")).yellow().to_string())
            })
            .unwrap_or_default();

        let heartbeat = metrics
            .last_heartbeat_age_secs
            .map(|secs| {
                let s = format!("heartbeat {secs}s ago");
                if matches!(metrics.ingestd_status, IngestdStatus::Healthy) {
                    style(s).green().to_string()
                } else if matches!(metrics.ingestd_status, IngestdStatus::Stale) {
                    style(s).yellow().to_string()
                } else if matches!(metrics.ingestd_status, IngestdStatus::Down) {
                    style(s).red().to_string()
                } else {
                    style(s).dim().to_string()
                }
            })
            .unwrap_or_default();

        let query = metrics
            .query_error
            .as_ref()
            .map(|error| style(format!("query error ({error})")).red().to_string())
            .unwrap_or_default();

        let sep = style("·").dim();
        let parts: Vec<&str> = [
            status.as_str(),
            lag.as_str(),
            batch.as_str(),
            heartbeat.as_str(),
            query.as_str(),
        ]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect();

        println!("{label}  {}", parts.join(&format!(" {sep} ")));
    }

    // ─── Active Jobs ────────────────────────────────────────────────────

    fn render_jobs(&self) {
        if self.data.active_job_details.is_empty() && self.data.job_issues.is_empty() {
            return;
        }

        let label = style("  jobs").dim();
        let count = self.data.active_job_details.len();
        let max_show = 3;

        let job_parts: Vec<String> = self
            .data
            .active_job_details
            .iter()
            .take(max_show)
            .map(|j| format!("{} ({}s)", j.command, j.elapsed_secs as u64))
            .collect();

        let sep = style("·").dim();
        let mut line = format!(
            "{label}     {} running: {}",
            count,
            job_parts.join(&format!(" {sep} "))
        );

        if count > max_show {
            line.push_str(&format!(
                " {} {}",
                style(format!("+{} more", count - max_show)).dim(),
                style("→ xtask jobs active").cyan()
            ));
        }

        println!("{line}");
        for issue in &self.data.job_issues {
            let indent = " ".repeat(LABEL_COL);
            println!("{indent}{}", style(issue).yellow());
        }
    }

    // ─── Git Working Directory ──────────────────────────────────────────

    fn render_git(&self) {
        let git = &self.data.git;

        // Show when there's something notable
        let has_commit = git.last_commit_hash.is_some();
        let notable = git.probe_message.is_some()
            || git.dirty
            || git.ahead > 0
            || git.behind > 0
            || git.stash_count.is_some_and(|count| count > 0)
            || git.uncommitted_count.is_some_and(|count| count > 0);

        if !has_commit && !notable {
            return;
        }

        let label = style("  git").dim();

        // First line: last commit (width-aware truncation)
        if let (Some(hash), Some(msg)) = (&git.last_commit_hash, &git.last_commit_message) {
            let age = git.last_commit_age_mins.map(format_age).unwrap_or_default();
            // Available space: width - label(LABEL_COL) - hash(7) - separators(6) - age
            let overhead = LABEL_COL + hash.len() + age.len() + 6;
            let max_msg = self.width.saturating_sub(overhead).max(10);
            let truncated_msg = if msg.len() > max_msg {
                format!("{}…", &msg[..max_msg - 1])
            } else {
                msg.clone()
            };
            println!(
                "{label}      {} {}   {}",
                style(hash).dim(),
                style(truncated_msg).dim(),
                style(age).dim()
            );
        }

        // Second line: stats (ahead/behind shown in header, not here)
        let mut stat_parts = Vec::new();
        if let Some(files) = &git.files_changed {
            stat_parts.push(files.clone());
        }
        // Only show uncommitted_count when files_changed is absent (untracked-only changes)
        if git.files_changed.is_none() && git.uncommitted_count.is_some_and(|count| count > 0) {
            stat_parts.push(format!(
                "{} uncommitted",
                git.uncommitted_count.unwrap_or_default()
            ));
        }
        if git.stash_count.is_some_and(|count| count > 0) {
            stat_parts.push(format!(
                "{} stash{}",
                git.stash_count.unwrap_or_default(),
                if git.stash_count == Some(1) { "" } else { "es" }
            ));
        }

        if !stat_parts.is_empty() {
            let indent = " ".repeat(LABEL_COL);
            let sep = style("·").dim();
            println!("{indent}{}", stat_parts.join(&format!(" {sep} ")));
        }

        if let Some(message) = &git.probe_message {
            let indent = " ".repeat(LABEL_COL);
            println!("{indent}{}", style(message).yellow());
        }
    }
}
