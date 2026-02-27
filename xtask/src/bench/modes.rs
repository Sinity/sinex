use super::{
    history::{BenchRunMetadata, HistoryDb, HistoryReport},
    reports,
    runner::{BenchContext, BenchRunner, ScenarioResult, generate_scenarios},
};
use color_eyre::eyre::{ContextCompat, Result, bail};
use console::style;
use std::time::{Duration, Instant};

pub(super) fn sweep_mode(ctx: &BenchContext) -> Result<()> {
    println!("{}", style("Running sweep mode").cyan().bold());
    println!();

    if !ctx.config.dry_run {
        ctx.compile()?;
    }

    let scenarios = generate_scenarios(&ctx.config);
    let runner = BenchRunner::new(ctx);

    println!();
    println!(
        "{} Testing {} scenarios with {} runs each",
        style("▶").cyan().bold(),
        scenarios.len(),
        ctx.config.runs
    );

    let mut results = Vec::new();

    for scenario in &scenarios {
        let result = runner.run_scenario_multiple(scenario)?;
        results.push(result);
    }

    finalize_results(ctx, &results)?;

    Ok(())
}

pub(super) fn refine_mode(ctx: &BenchContext) -> Result<()> {
    println!("{}", style("Running refine mode").cyan().bold());
    println!("Two-phase optimization: quick sweep → find top performers → detailed analysis");
    println!();

    if !ctx.config.dry_run {
        ctx.compile()?;
    }

    let all_scenarios = generate_scenarios(&ctx.config);
    let runner = BenchRunner::new(ctx);

    // Phase 1: Quick sweep with refine_sweep_runs
    println!();
    println!("{}", style("━━━━ Phase 1: Quick Sweep ━━━━").cyan().bold());
    println!(
        "{} Testing {} scenarios with {} run(s) each",
        style("▶").cyan().bold(),
        all_scenarios.len(),
        ctx.config.refine_sweep_runs
    );

    let mut quick_results = Vec::new();

    for scenario in &all_scenarios {
        let runs_count = ctx.config.refine_sweep_runs as usize;
        let mut runs = Vec::new();

        for i in 0..runs_count {
            let result = runner.run_scenario(scenario, i, runs_count)?;
            runs.push(result);
        }

        let samples: Vec<f64> = runs.iter().map(|r| r.elapsed_ms).collect();
        let stats = super::stats::RunStats::from_samples(&samples);

        quick_results.push(super::runner::ScenarioResult {
            scenario: scenario.clone(),
            runs,
            stats: stats.clone(),
        });

        println!(
            "  {} {} - {:.1}ms",
            style("✓").green(),
            scenario.key(),
            stats.median_ms
        );
    }

    // Find best result
    let best_median = quick_results
        .iter()
        .map(|r| r.stats.median_ms)
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(0.0);

    println!();
    println!(
        "{} Best quick result: {:.1}ms",
        style("🏆").cyan(),
        best_median
    );

    // Phase 2: Identify top performers
    println!();
    println!(
        "{}",
        style("━━━━ Phase 2: Identify Top Performers ━━━━")
            .cyan()
            .bold()
    );

    let threshold = best_median * (1.0 + ctx.config.refine_threshold_pct / 100.0);
    println!(
        "Threshold: {:.1}ms (within {}% of best)",
        threshold, ctx.config.refine_threshold_pct
    );

    // Find top thread counts
    let mut thread_scores: std::collections::HashMap<u32, f64> = std::collections::HashMap::new();
    for result in &quick_results {
        let entry = thread_scores
            .entry(result.scenario.threads)
            .or_insert(f64::MAX);
        *entry = entry.min(result.stats.median_ms);
    }

    let mut top_threads: Vec<_> = thread_scores.into_iter().collect();
    top_threads.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    top_threads.truncate(ctx.config.refine_top_threads);

    let top_thread_values: std::collections::HashSet<_> =
        top_threads.iter().map(|(t, _)| *t).collect();

    println!();
    println!("Top {} thread counts:", ctx.config.refine_top_threads);
    for (threads, score) in &top_threads {
        println!("  {threads} threads: {score:.1}ms");
    }

    // Filter scenarios: must be in top threads and within threshold
    let refined_scenarios: Vec<_> = quick_results
        .iter()
        .filter(|r| {
            top_thread_values.contains(&r.scenario.threads) && r.stats.median_ms <= threshold
        })
        .map(|r| r.scenario.clone())
        .collect();

    println!();
    println!(
        "{} Selected {} scenarios for detailed analysis (from {} total)",
        style("✓").green().bold(),
        refined_scenarios.len(),
        all_scenarios.len()
    );

    if refined_scenarios.is_empty() {
        println!(
            "{} No scenarios passed refinement criteria",
            style("⚠").yellow()
        );
        return Ok(());
    }

    // Phase 3: Detailed analysis
    println!();
    println!(
        "{}",
        style("━━━━ Phase 3: Detailed Analysis ━━━━").cyan().bold()
    );
    println!(
        "{} Testing {} scenarios with {} runs each",
        style("▶").cyan().bold(),
        refined_scenarios.len(),
        ctx.config.runs
    );

    let mut final_results = Vec::new();

    for scenario in &refined_scenarios {
        let result = runner.run_scenario_multiple(scenario)?;
        final_results.push(result);
    }

    // Find overall best
    if let Some(best) = final_results.iter().min_by(|a, b| {
        a.stats
            .median_ms
            .partial_cmp(&b.stats.median_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
    }) {
        println!();
        println!("{}", style("━━━━ Best Configuration ━━━━").cyan().bold());
        println!(
            "  Scenario: {}",
            style(&best.scenario.key()).yellow().bold()
        );
        println!("  Median:   {:.1}ms", best.stats.median_ms);
        println!("  Mean:     {:.1}ms", best.stats.mean_ms);
        println!("  Stddev:   {:.1}ms", best.stats.stddev_ms);
        println!(
            "  95% CI:   [{:.1}, {:.1}]ms",
            best.stats.ci95_lower, best.stats.ci95_upper
        );
    }

    finalize_results(ctx, &final_results)?;

    Ok(())
}

pub(super) fn bisect_mode(ctx: &BenchContext) -> Result<()> {
    println!("{}", style("Running bisect mode").cyan().bold());

    let good = ctx
        .config
        .bisect_good
        .as_ref()
        .context("--bisect-good required for bisect mode")?;
    let bad = ctx
        .config
        .bisect_bad
        .as_ref()
        .context("--bisect-bad required for bisect mode")?;

    println!("Good commit: {}", style(good).green());
    println!("Bad commit:  {}", style(bad).red());
    println!();

    bail!("Bisect mode not yet fully implemented - requires git integration")
}

pub(super) fn stress_mode(ctx: &BenchContext) -> Result<()> {
    println!("{}", style("Running stress mode").cyan().bold());
    println!(
        "Will run until failure (max {} iterations)",
        ctx.config.stress_limit
    );
    println!();

    if !ctx.config.dry_run {
        ctx.compile()?;
    }

    let scenarios = generate_scenarios(&ctx.config);
    if scenarios.is_empty() {
        bail!("No scenarios to test");
    }

    let scenario = &scenarios[0];
    let runner = BenchRunner::new(ctx);

    println!();
    println!(
        "{} Stress testing scenario: {}",
        style("▶").cyan().bold(),
        style(&scenario.key()).yellow()
    );

    let mut iteration = 0;
    let start = Instant::now();

    while iteration < ctx.config.stress_limit {
        iteration += 1;

        println!();
        println!("{} Iteration {}", style("▶").cyan(), iteration);

        let result = runner.run_scenario(scenario, 0, 1)?;

        if !result.success {
            let elapsed = start.elapsed();
            println!();
            println!(
                "{} {} after {} iterations ({:.1}s)",
                style("✗").red().bold(),
                style("Failed").red(),
                iteration,
                elapsed.as_secs_f64()
            );
            return Ok(());
        }
    }

    let elapsed = start.elapsed();
    println!();
    println!(
        "{} Completed {} iterations without failure ({:.1}s)",
        style("✓").green().bold(),
        iteration,
        elapsed.as_secs_f64()
    );

    Ok(())
}

pub(super) fn soak_mode(ctx: &BenchContext) -> Result<()> {
    println!("{}", style("Running soak mode").cyan().bold());
    println!(
        "Will run for {} seconds ({:.1} minutes)",
        ctx.config.soak_duration,
        ctx.config.soak_duration as f64 / 60.0
    );
    println!();

    if !ctx.config.dry_run {
        ctx.compile()?;
    }

    let scenarios = generate_scenarios(&ctx.config);
    if scenarios.is_empty() {
        bail!("No scenarios to test");
    }

    let scenario = &scenarios[0];
    let runner = BenchRunner::new(ctx);

    println!();
    println!(
        "{} Soak testing scenario: {}",
        style("▶").cyan().bold(),
        style(&scenario.key()).yellow()
    );

    let start = Instant::now();
    let target_duration = Duration::from_secs(ctx.config.soak_duration);
    let mut iteration = 0;
    let mut failures = 0;

    while start.elapsed() < target_duration {
        iteration += 1;

        let remaining = target_duration
            .checked_sub(start.elapsed())
            .unwrap_or(Duration::ZERO);

        println!();
        println!(
            "{} Iteration {} (remaining: {:.1}m)",
            style("▶").cyan(),
            iteration,
            remaining.as_secs_f64() / 60.0
        );

        let result = runner.run_scenario(scenario, 0, 1)?;

        if !result.success {
            failures += 1;
            println!(
                "{} Failure #{} at iteration {}",
                style("✗").red(),
                failures,
                iteration
            );
        }
    }

    let elapsed = start.elapsed();
    println!();
    println!(
        "{} Soak test complete",
        if failures == 0 {
            style("✓").green().bold()
        } else {
            style("✗").red().bold()
        }
    );
    println!("  Iterations: {}", style(iteration.to_string()).cyan());
    println!(
        "  Failures:   {}",
        if failures == 0 {
            style(failures.to_string()).green()
        } else {
            style(failures.to_string()).red()
        }
    );
    println!("  Duration:   {:.1}s", elapsed.as_secs_f64());

    Ok(())
}

fn finalize_results(ctx: &BenchContext, results: &[ScenarioResult]) -> Result<()> {
    println!();
    println!("{}", style("━━━━ Finalization ━━━━").cyan().bold());

    let history_report = if ctx.config.history_db.is_some() {
        Some(save_to_history(ctx, results)?)
    } else {
        None
    };

    if ctx.config.report_md {
        generate_markdown_report(ctx, results, history_report.as_ref())?;
    }

    if ctx.config.report_html {
        generate_html_report(ctx, results, history_report.as_ref())?;
    }

    println!();
    println!("{} Benchmark complete", style("✓").green().bold());
    println!(
        "  Output directory: {}",
        style(ctx.output_dir.display()).cyan()
    );

    Ok(())
}

fn save_to_history(ctx: &BenchContext, results: &[ScenarioResult]) -> Result<HistoryReport> {
    let db_path = ctx
        .config
        .history_db
        .as_ref()
        .expect("history_db must be configured for save_to_history");

    println!();
    println!(
        "{} Saving to history database: {}",
        style("💾").cyan(),
        style(db_path.display()).dim()
    );

    let db = HistoryDb::open(db_path)?;

    let metadata = BenchRunMetadata {
        mode: ctx.config.mode.to_string(),
        profile: ctx.config.profile.clone(),
        git_sha: ctx.environment.git_sha.clone(),
        git_branch: ctx.environment.git_branch.clone(),
        git_dirty: ctx.environment.git_dirty,
        rustc_version: ctx.environment.rustc_version.clone(),
    };

    let run_id = db.save_run(&metadata, results)?;

    let history_report = HistoryReport {
        run_id,
        scenarios: db.summarize_scenarios(
            results,
            Some(run_id),
            ctx.config.regression_threshold_pct,
            ctx.config.history_trend_limit,
        )?,
    };

    println!(
        "  {} Saved as run #{}",
        style("✓").green(),
        style(run_id).cyan()
    );

    Ok(history_report)
}

fn generate_markdown_report(
    ctx: &BenchContext,
    results: &[ScenarioResult],
    history: Option<&HistoryReport>,
) -> Result<()> {
    let report_path = ctx.output_dir.join("report.md");

    println!();
    println!(
        "{} Generating markdown report: {}",
        style("📄").cyan(),
        style(report_path.display()).dim()
    );

    reports::generate_markdown(
        &ctx.config,
        &ctx.environment,
        results,
        history,
        &report_path,
    )?;

    println!("  {} Report generated", style("✓").green());

    Ok(())
}

fn generate_html_report(
    ctx: &BenchContext,
    results: &[ScenarioResult],
    history: Option<&HistoryReport>,
) -> Result<()> {
    let report_path = ctx.output_dir.join("report.html");

    println!();
    println!(
        "{} Generating HTML report: {}",
        style("📊").cyan(),
        style(report_path.display()).dim()
    );

    reports::generate_html(
        &ctx.config,
        &ctx.environment,
        results,
        history,
        &report_path,
    )?;

    println!("  {} Report generated", style("✓").green());

    Ok(())
}
