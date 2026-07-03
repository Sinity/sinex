use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use std::fs;
use std::path::{Path, PathBuf};

use crate::command::{
    CommandContext, CommandMetadata, CommandResult, HistoryAccessMode, XtaskCommand,
};
use crate::process::ProcessBuilder;

#[derive(Debug, Clone, clap::Args)]
pub struct ImpactCommand {
    #[command(subcommand)]
    pub subcommand: ImpactSubcommand,
}

#[derive(Debug, Clone, clap::Subcommand)]
pub enum ImpactSubcommand {
    /// Explain the default `xtask test` impact plan for the current diff.
    Explain {
        /// Planner mode to explain.
        #[arg(long, value_enum, default_value_t = crate::impact::ImpactMode::Balanced)]
        mode: crate::impact::ImpactMode,
    },

    /// Run tests in evidence-seeding mode.
    Seed {
        /// Package to seed.
        #[arg(short, long)]
        package: Option<String>,

        /// Optional nextest filter.
        #[arg(short = 'E', long)]
        filter: Option<String>,
    },

    /// Run one nextest filter under LLVM coverage and import covered line regions.
    SeedCoverage {
        /// Package to seed.
        #[arg(short, long)]
        package: Option<String>,

        /// Exact nextest filter for one test, for example `test(my_test)`.
        #[arg(short = 'E', long)]
        filter: String,

        /// Test name to record when it cannot be inferred from `-E test(name)`.
        #[arg(long)]
        test_name: Option<String>,
    },

    /// Sample skipped tests by forcing a broader local run.
    Audit {
        /// Number of skipped proof decisions to sample.
        #[arg(long = "sample-skips", default_value_t = 10)]
        sample_skips: usize,

        /// Planner mode to audit.
        #[arg(long, value_enum, default_value_t = crate::impact::ImpactMode::Balanced)]
        mode: crate::impact::ImpactMode,
    },
}

impl XtaskCommand for ImpactCommand {
    fn name(&self) -> &'static str {
        "impact"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            ImpactSubcommand::Explain { mode } => explain(ctx, *mode),
            ImpactSubcommand::Seed { package, filter } => {
                seed(ctx, package.as_deref(), filter.as_deref())
            }
            ImpactSubcommand::SeedCoverage {
                package,
                filter,
                test_name,
            } => seed_coverage(ctx, package.as_deref(), filter, test_name.as_deref()),
            ImpactSubcommand::Audit { sample_skips, mode } => audit(ctx, *sample_skips, *mode),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::analysis()
            .with_history_tracking(true)
            .with_history_access(HistoryAccessMode::ReadWrite)
    }
}

fn explain(ctx: &CommandContext, mode: crate::impact::ImpactMode) -> Result<CommandResult> {
    let plan = match ctx.try_with_history_db_query(|db| {
        crate::impact::plan_default_test_impact_with_history_and_mode(Some(db), mode)
    }) {
        Some(result) => result?,
        None => crate::impact::plan_default_test_impact_with_history_and_mode(None, mode)?,
    };
    if ctx.is_human() {
        print_plan(&plan);
    }
    Ok(CommandResult::success()
        .with_message("impact plan resolved")
        .with_duration(ctx.elapsed())
        .with_data(serde_json::to_value(&plan)?))
}

fn seed(
    ctx: &CommandContext,
    package: Option<&str>,
    filter: Option<&str>,
) -> Result<CommandResult> {
    let xtask = current_xtask()?;
    let mut args = vec!["test".to_string(), "--impact-mode=off".to_string()];
    if let Some(package) = package {
        args.push("-p".to_string());
        args.push(package.to_string());
    }
    if let Some(filter) = filter {
        args.push("-E".to_string());
        args.push(filter.to_string());
    }
    if ctx.is_human() {
        println!("Impact seed: {}", shell_words(&xtask, &args));
    }
    let output = ProcessBuilder::new(&xtask)
        .args(&args)
        .inherit_output()
        .without_timeout()
        .run();
    match output {
        Ok(_) => Ok(CommandResult::success()
            .with_message("impact seed completed")
            .with_duration(ctx.elapsed())
            .with_data(serde_json::json!({ "command": command_json(&xtask, &args) }))),
        Err(error) => Err(error).wrap_err("impact seed test run failed"),
    }
}

fn seed_coverage(
    ctx: &CommandContext,
    package: Option<&str>,
    filter: &str,
    test_name: Option<&str>,
) -> Result<CommandResult> {
    let recorded_test_name = test_name
        .map(str::to_string)
        .or_else(|| exact_test_name_from_filter(filter))
        .ok_or_else(|| {
            eyre!(
                "coverage seeding needs an exact test identity; pass -E 'test(name)' or --test-name"
            )
        })?;

    let mut args = vec![
        "llvm-cov".to_string(),
        "nextest".to_string(),
        "--json".to_string(),
    ];
    if let Some(package) = package {
        args.push("--package".to_string());
        args.push(package.to_string());
    } else {
        args.push("--workspace".to_string());
    }
    args.push("-E".to_string());
    args.push(filter.to_string());

    if ctx.is_human() {
        println!("Impact coverage seed: {}", shell_words("cargo", &args));
    }

    let output = ProcessBuilder::cargo()
        .args(&args)
        .with_description("cargo llvm-cov nextest --json")
        .without_timeout()
        .run_capture()?;
    if !output.success() {
        bail!(
            "impact coverage seed failed with exit code {}:\n{}",
            output.exit_code,
            output.combined()
        );
    }

    let coverage_json = extract_json_object(&output.stdout)
        .ok_or_else(|| eyre!("cargo llvm-cov did not emit JSON on stdout"))?;
    let regions = coverage_regions_from_llvm_json(
        coverage_json,
        &recorded_test_name,
        package,
        &crate::config::workspace_root(),
    )?;
    if regions.is_empty() {
        bail!("LLVM coverage JSON contained no covered regions for {recorded_test_name}");
    }

    let invocation_component = ctx
        .invocation_id()
        .map_or_else(|| "manual".to_string(), |id| id.to_string());
    let artifact_dir = crate::config::workspace_root()
        .join(".sinex")
        .join("test-artifacts")
        .join("impact")
        .join(format!("coverage-{invocation_component}"));
    fs::create_dir_all(&artifact_dir).wrap_err_with(|| {
        format!(
            "failed to create impact coverage artifact dir {}",
            artifact_dir.display()
        )
    })?;
    let artifact_path = artifact_dir.join(format!(
        "{}.coverage.json",
        sanitize_artifact_component(&recorded_test_name)
    ));
    let envelope = serde_json::json!({
        "artifact_kind": "coverage_regions",
        "regions": regions,
    });
    fs::write(&artifact_path, serde_json::to_vec_pretty(&envelope)?).wrap_err_with(|| {
        format!(
            "failed to write impact coverage artifact {}",
            artifact_path.display()
        )
    })?;

    let imported = if let Some(invocation_id) = ctx.invocation_id() {
        ctx.try_with_history_db(|db| {
            db.import_test_dependency_artifacts(invocation_id, &artifact_dir)
        })
        .transpose()?
        .unwrap_or(0)
    } else {
        0
    };

    Ok(CommandResult::success()
        .with_message("impact coverage seed completed")
        .with_duration(ctx.elapsed())
        .with_data(serde_json::json!({
            "test_name": recorded_test_name,
            "package": package,
            "filter": filter,
            "regions": envelope["regions"].as_array().map_or(0, Vec::len),
            "imported": imported,
            "artifact": artifact_path.display().to_string(),
            "command": command_json("cargo", &args),
        })))
}

fn audit(
    ctx: &CommandContext,
    sample_skips: usize,
    mode: crate::impact::ImpactMode,
) -> Result<CommandResult> {
    let plan = match ctx.try_with_history_db_query(|db| {
        crate::impact::plan_default_test_impact_with_history_and_mode(Some(db), mode)
    }) {
        Some(result) => result?,
        None => crate::impact::plan_default_test_impact_with_history_and_mode(None, mode)?,
    };
    let impact_run_id = ctx
        .try_with_history_db(|db| db.record_impact_plan(ctx.invocation_id(), "audit", &plan))
        .transpose()?;
    let sampled_skips = audit_sample_decisions(&plan, sample_skips);
    let audit_command = audit_command_for_sample(&plan, &sampled_skips);
    if ctx.is_human() {
        println!("Impact audit");
        println!("  sampled skipped decisions: {}", sampled_skips.len());
        if let Some((program, args)) = &audit_command {
            println!("  running: {}", shell_words(program, args));
        }
    }
    let (status, output_json, false_negative_count) = if let Some((program, args)) = &audit_command
    {
        match ProcessBuilder::new(program)
            .args(args)
            .inherit_output()
            .without_timeout()
            .run()
        {
            Ok(_) => ("success".to_string(), None, 0usize),
            Err(error) => {
                let rendered = format!("{error:#}");
                (
                    "failed".to_string(),
                    Some(serde_json::json!({ "error": rendered }).to_string()),
                    1usize,
                )
            }
        }
    } else {
        ("no_sample".to_string(), None, 0usize)
    };
    let sampled_json = serde_json::to_string(&sampled_skips)?;
    let recorded_command_json = serde_json::to_string(
        &audit_command
            .as_ref()
            .map(|(program, args)| command_json(program, args)),
    )?;
    let audit_run_id = ctx
        .try_with_history_db(|db| {
            db.record_impact_audit_run(
                ctx.invocation_id(),
                impact_run_id,
                sample_skips,
                &sampled_json,
                &recorded_command_json,
                &status,
                false_negative_count,
                output_json.as_deref(),
            )
        })
        .transpose()?;
    let mut result = if false_negative_count == 0 {
        CommandResult::success()
    } else {
        CommandResult::failure(crate::output::StructuredError {
            code: "IMPACT_AUDIT_FAILED".to_string(),
            message: "impact audit broad sample failed".to_string(),
            location: Some("impact::audit".to_string()),
            suggestion: Some(
                "Treat the implicated impact evidence as stale; seed evidence or run package/workspace scope"
                    .to_string(),
            ),
        })
    };
    result = result
        .with_message("impact audit executed")
        .with_duration(ctx.elapsed())
        .with_data(serde_json::json!({
            "sample_skips": sample_skips,
            "sampled_skips": sampled_skips,
            "audit_run_id": audit_run_id,
            "status": status,
            "false_negative_count": false_negative_count,
            "command": audit_command.map(|(program, args)| command_json(&program, &args)),
            "plan": plan,
        }));
    Ok(result)
}

fn exact_test_name_from_filter(filter: &str) -> Option<String> {
    let trimmed = filter.trim();
    let inner = trimmed.strip_prefix("test(")?.strip_suffix(')')?.trim();
    if inner.is_empty()
        || inner
            .chars()
            .any(|ch| matches!(ch, ' ' | '&' | '|' | '(' | ')'))
    {
        return None;
    }
    Some(inner.trim_matches('"').trim_matches('\'').to_string())
}

fn extract_json_object(output: &str) -> Option<&str> {
    let start = output.find('{')?;
    let end = output.rfind('}')?;
    (start <= end).then_some(&output[start..=end])
}

fn coverage_regions_from_llvm_json(
    rendered: &str,
    test_name: &str,
    package: Option<&str>,
    workspace_root: &Path,
) -> Result<Vec<serde_json::Value>> {
    let value: serde_json::Value =
        serde_json::from_str(rendered).wrap_err("failed to parse LLVM coverage JSON")?;
    let mut regions = Vec::new();
    let Some(data) = value.get("data").and_then(serde_json::Value::as_array) else {
        return Ok(regions);
    };
    for export in data {
        let Some(files) = export.get("files").and_then(serde_json::Value::as_array) else {
            continue;
        };
        for file in files {
            let Some(filename) = file.get("filename").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let file_path = workspace_relative_path(filename, workspace_root);
            let Some(segments) = file.get("segments").and_then(serde_json::Value::as_array) else {
                continue;
            };
            let mut points = Vec::new();
            for segment in segments {
                let Some(segment) = segment.as_array() else {
                    continue;
                };
                let Some(line) = segment.first().and_then(serde_json::Value::as_u64) else {
                    continue;
                };
                let count = segment
                    .get(2)
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or_default();
                points.push((u32::try_from(line).unwrap_or(u32::MAX), count > 0));
            }
            points.sort_unstable_by_key(|(line, _)| *line);
            points.dedup_by_key(|(line, _)| *line);
            for (idx, (line, covered)) in points.iter().copied().enumerate() {
                if !covered {
                    continue;
                }
                let next_line = points
                    .get(idx + 1)
                    .map_or(line, |(next_line, _)| next_line.saturating_sub(1));
                let line_end = next_line.max(line);
                let region_hash = format!("{file_path}:{line}-{line_end}:{test_name}");
                let content_hash = crate::impact::hash_file_if_exists(&file_path);
                regions.push(serde_json::json!({
                    "test_name": test_name,
                    "package": package,
                    "file_path": file_path,
                    "function_name": null,
                    "line_start": line,
                    "line_end": line_end,
                    "region_hash": region_hash,
                    "content_hash": content_hash,
                }));
            }
        }
    }
    Ok(regions)
}

fn workspace_relative_path(filename: &str, workspace_root: &Path) -> String {
    let path = PathBuf::from(filename);
    if let Ok(relative) = path.strip_prefix(workspace_root) {
        return relative.to_string_lossy().into_owned();
    }
    if let Ok(canonical_root) = workspace_root.canonicalize()
        && let Ok(canonical_path) = path.canonicalize()
        && let Ok(relative) = canonical_path.strip_prefix(canonical_root)
    {
        return relative.to_string_lossy().into_owned();
    }
    filename.trim_start_matches("./").to_string()
}

fn print_plan(plan: &crate::impact::ImpactPlan) {
    println!("Impact plan");
    println!("  changed files: {}", plan.changed.len());
    if !plan.affected_packages.is_empty() {
        println!("  packages: {}", plan.affected_packages.join(", "));
    } else if !plan.impacted_tests.is_empty() {
        println!("  impacted tests: {}", plan.impacted_tests.len());
        if let Some(filter) = &plan.impact_filter {
            println!("  filter: {filter}");
        }
    } else if plan.is_workspace() {
        println!("  scope: workspace");
    } else if plan.can_reuse_exact_proof() {
        println!("  scope: exact proof reuse candidate");
    }
    for decision in &plan.decisions {
        let subject = decision.subject.as_deref().unwrap_or("workspace");
        println!("  {:?}: {subject} ({})", decision.action, decision.reason);
    }
    for risk in &plan.accepted_risks {
        println!("  accepted risk: {risk}");
    }
    for gap in &plan.evidence_gaps {
        println!("  evidence gap: {gap}");
    }
    for stale in &plan.stale_evidence {
        println!("  stale evidence: {stale}");
    }
}

fn audit_command_for_plan(plan: &crate::impact::ImpactPlan) -> Option<(String, Vec<String>)> {
    let xtask = current_xtask().ok()?;
    let mut args = vec!["test".to_string(), "--impact-mode=off".to_string()];
    let packages = crate::impact::packages_for_plan(plan);
    if let Some(packages) = packages {
        for package in packages {
            args.push("-p".to_string());
            args.push(package);
        }
    } else if plan.is_workspace() || plan.can_reuse_exact_proof() || plan.impact_filter.is_some() {
        args.push("--all".to_string());
    } else {
        return None;
    }
    Some((xtask, args))
}

fn audit_sample_decisions(
    plan: &crate::impact::ImpactPlan,
    sample_skips: usize,
) -> Vec<crate::impact::ImpactDecision> {
    if sample_skips == 0 {
        return Vec::new();
    }
    let sampled_skips = plan
        .decisions
        .iter()
        .filter(|decision| {
            matches!(
                decision.action,
                crate::impact::ImpactAction::ReuseExactProof
                    | crate::impact::ImpactAction::AuditSkippedTests
            )
        })
        .take(sample_skips)
        .cloned()
        .collect::<Vec<_>>();
    if !sampled_skips.is_empty() || plan.impact_filter.is_none() {
        return sampled_skips;
    }
    plan.decisions
        .iter()
        .filter(|decision| decision.action == crate::impact::ImpactAction::RunImpactedTests)
        .take(sample_skips)
        .cloned()
        .collect()
}

fn audit_command_for_sample(
    plan: &crate::impact::ImpactPlan,
    sampled_skips: &[crate::impact::ImpactDecision],
) -> Option<(String, Vec<String>)> {
    if sampled_skips.is_empty() {
        return None;
    }
    audit_command_for_plan(plan)
}

fn current_xtask() -> Result<String> {
    std::env::current_exe()
        .map(|path| path.to_string_lossy().into_owned())
        .context("failed to resolve current xtask executable")
}

fn command_json(program: &str, args: &[String]) -> serde_json::Value {
    serde_json::json!({
        "program": program,
        "args": args,
    })
}

fn shell_words(program: &str, args: &[String]) -> String {
    std::iter::once(program.to_string())
        .chain(args.iter().cloned())
        .collect::<Vec<_>>()
        .join(" ")
}

fn sanitize_artifact_component(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
#[path = "impact_test.rs"]
mod tests;
