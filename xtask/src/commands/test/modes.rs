use color_eyre::eyre::Result;

use super::{BenchArgs, CoverageArgs, FuzzArgs, MutantsArgs, VmArgs};
use crate::command::{CommandContext, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;

pub(super) fn execute_bench(bench: &BenchArgs, ctx: &CommandContext) -> Result<CommandResult> {
    // Handle --report (read and print existing report)
    if let Some(ref report_path) = bench.report {
        return crate::commands::verify::execute_report(Some(report_path.clone()), ctx);
    }

    // Handle --compare (diff two reports)
    if let Some(ref paths) = bench.compare {
        return crate::commands::verify::execute_compare(&paths[0], &paths[1], ctx);
    }

    // Guard: bench invokes `cargo nextest run` which needs target/ lock
    if std::env::var("NEXTEST_RUN_ID").is_ok() {
        return Err(color_eyre::eyre::eyre!(
            "Cannot run `xtask test bench` inside an active nextest run — \
             cargo target/ lock would deadlock.\n\
             Use `xtask test --bg bench` instead."
        ));
    }

    if bench.contracts {
        // Contract enforcement mode for stored perf budgets.
        return crate::commands::verify::execute_perf(
            crate::commands::verify::PerfArgs {
                profile: bench.profile.clone(),
                runs: bench.runs,
                threads: bench.threads.clone(),
                target: bench.target.clone(),
                contracts: bench.contracts_file.clone(),
                output_dir: bench.output.clone(),
                history_db: bench.history_db.clone(),
            },
            ctx,
        );
    }

    // Standard bench mode
    use crate::bench::{self, BenchConfig};

    let config = BenchConfig {
        mode: bench.mode,
        profile: bench.profile.clone(),
        runs: bench.runs,
        threads: bench.threads.clone(),
        baseline: None,
        regression_threshold_pct: 10.0,
        history_db: bench.history_db.clone(),
        history_trend_limit: 5,
        report_md: false,
        report_html: false,
        git_tag: false,
        dry_run: bench.dry_run,
        gha: false,
        bisect_good: None,
        bisect_bad: None,
        stress_limit: 100,
        soak_duration: 3600,
        output: bench.output.clone(),
        verbose: bench.verbose,
        refine_top_threads: 3,
        refine_threshold_pct: 10.0,
        refine_sweep_runs: 1,
        target: bench.target.clone(),
        continue_on_fail: false,
        fail_fast: false,
    };
    bench::run(config).map(|()| CommandResult::success())
}

pub(super) async fn execute_fuzz(fuzz: &FuzzArgs, ctx: &CommandContext) -> Result<CommandResult> {
    // List mode
    if fuzz.list || fuzz.target.is_none() {
        let list_result = crate::commands::fuzz::FuzzCommand {
            subcommand: crate::commands::fuzz::FuzzSubcommand::List,
        }
        .execute(ctx)
        .await?;
        let target_count = parse_fuzz_target_count(&list_result)?;

        if fuzz.list || target_count == 0 {
            if target_count == 0 {
                return Ok(CommandResult::failure(crate::output::StructuredError {
                    code: "FUZZ_NO_TARGETS".to_string(),
                    message: "No fuzz targets found".to_string(),
                    location: Some("test fuzz".to_string()),
                    suggestion: Some(
                        "Add fuzz targets under crate/*/fuzz/ and rerun `xtask test fuzz`."
                            .to_string(),
                    ),
                })
                .with_duration(ctx.elapsed()));
            }
            return Ok(list_result);
        }
    }

    // Run specific target
    if let Some(ref target) = fuzz.target {
        return crate::commands::fuzz::FuzzCommand {
            subcommand: crate::commands::fuzz::FuzzSubcommand::Run {
                target: target.clone(),
                max_time: fuzz.max_time,
                jobs: fuzz.jobs,
            },
        }
        .execute(ctx)
        .await;
    }

    Ok(CommandResult::success()
        .with_message("No fuzz target specified")
        .with_duration(ctx.elapsed()))
}

fn parse_fuzz_target_count(result: &CommandResult) -> Result<u64> {
    let data = result
        .data
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("fuzz list result is missing structured data"))?;
    let target_count = data
        .get("target_count")
        .ok_or_else(|| color_eyre::eyre::eyre!("fuzz list result is missing target_count"))?;
    target_count
        .as_u64()
        .ok_or_else(|| color_eyre::eyre::eyre!("fuzz list result has invalid target_count"))
}

pub(super) async fn execute_coverage(
    cov: &CoverageArgs,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    // Guard: coverage invokes `cargo llvm-cov` which needs target/ lock
    if std::env::var("NEXTEST_RUN_ID").is_ok() {
        return Err(color_eyre::eyre::eyre!(
            "Cannot run `xtask test coverage` inside an active nextest run — \
             cargo target/ lock would deadlock.\n\
             Use `xtask test --bg coverage` instead."
        ));
    }

    let subcommand = if let Some(threshold) = cov.enforce {
        crate::commands::coverage::CoverageSubcommand::Enforce {
            threshold,
            package: cov.package.clone(),
            html: cov.html,
            output: cov.output.clone(),
        }
    } else {
        crate::commands::coverage::CoverageSubcommand::Html {
            output: cov.output.clone(),
            open: cov.open,
            package: cov.package.clone(),
        }
    };

    crate::commands::coverage::CoverageCommand { subcommand }
        .execute(ctx)
        .await
}

pub(super) fn execute_mutants(m: &MutantsArgs, _ctx: &CommandContext) -> Result<CommandResult> {
    use color_eyre::eyre::eyre;

    // Guard: mutants invokes cargo-mutants which needs target/ lock
    if std::env::var("NEXTEST_RUN_ID").is_ok() {
        return Err(eyre!(
            "Cannot run `xtask test mutants` inside an active nextest run — \
             cargo target/ lock would deadlock.\n\
             Use `xtask test --bg mutants` instead."
        ));
    }

    if !ProcessBuilder::new("cargo-mutants")
        .arg("--version")
        .run_success()?
    {
        return Err(eyre!(
            "cargo-mutants not found in PATH. Add it to this repo's devshell/flake."
        ));
    }

    let mut builder =
        ProcessBuilder::new("cargo-mutants").with_timeout(std::time::Duration::from_hours(4));
    builder = builder
        .arg("--timeout")
        .arg(format!("{}", m.timeout))
        .arg("--jobs")
        .arg(format!("{}", m.jobs));

    if let Some(pkg) = &m.package {
        builder = builder.arg("--package").arg(pkg);
    }
    if let Some(f) = &m.file {
        builder = builder.arg("--file").arg(f);
    }

    let description = match (&m.package, &m.file) {
        (Some(pkg), _) => format!("cargo-mutants --package {pkg}"),
        (None, Some(f)) => format!("cargo-mutants --file {f}"),
        (None, None) => "cargo-mutants (full workspace)".to_string(),
    };

    builder
        .with_description(&description)
        .inherit_output()
        .run()?;

    Ok(CommandResult::success()
        .with_message("Mutation testing completed successfully")
        .with_detail(format!("Timeout per mutant: {}s", m.timeout))
        .with_detail(format!("Parallel jobs: {}", m.jobs)))
}

pub(super) async fn execute_vm(vm: &VmArgs, ctx: &CommandContext) -> Result<CommandResult> {
    let vm_cmd = crate::commands::vm::VmCommand {
        subcommand: crate::commands::vm::VmSubcommand::Test {
            category: vm.category.clone(),
            timeout: vm.timeout,
            keep_failed: vm.keep_failed,
            list: vm.list,
            validate: vm.validate,
            tests: vm.args.clone(),
        },
    };
    vm_cmd.execute(ctx).await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum DiskSpaceStatus {
    Sufficient { available_gb: u64, min_gb: u64 },
    Low { available_gb: u64, min_gb: u64 },
    Unknown { issue: String },
}

fn classify_disk_space_probe_result(
    available_gb: std::result::Result<u64, String>,
    min_gb: u64,
) -> DiskSpaceStatus {
    match available_gb {
        Ok(available_gb) if available_gb >= min_gb => DiskSpaceStatus::Sufficient {
            available_gb,
            min_gb,
        },
        Ok(available_gb) => DiskSpaceStatus::Low {
            available_gb,
            min_gb,
        },
        Err(issue) => DiskSpaceStatus::Unknown { issue },
    }
}

/// Check if sufficient disk space is available on current directory's filesystem.
/// Probe failures remain explicit instead of being treated as healthy.
pub(super) fn check_disk_space_gb(min_gb: u64) -> DiskSpaceStatus {
    #[cfg(unix)]
    {
        use nix::sys::statvfs::statvfs;
        classify_disk_space_probe_result(
            statvfs(".")
                .map(|stat| {
                    let available_bytes = stat.blocks_available() * stat.fragment_size();
                    available_bytes / (1024 * 1024 * 1024)
                })
                .map_err(|error| error.to_string()),
            min_gb,
        )
    }
    #[cfg(not(unix))]
    {
        classify_disk_space_probe_result(
            Err("disk-space probing is unavailable on this platform".to_string()),
            min_gb,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_parse_fuzz_target_count_accepts_valid_count() -> ::xtask::sandbox::TestResult<()>
    {
        let result = CommandResult::success().with_data(serde_json::json!({
            "target_count": 3u64
        }));

        assert_eq!(super::parse_fuzz_target_count(&result)?, 3);
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_fuzz_target_count_rejects_missing_count() -> ::xtask::sandbox::TestResult<()>
    {
        let result = CommandResult::success().with_data(serde_json::json!({
            "items": []
        }));

        let error =
            super::parse_fuzz_target_count(&result).expect_err("missing target count must surface");
        assert!(format!("{error:#}").contains("missing target_count"));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_fuzz_target_count_rejects_non_numeric_count()
    -> ::xtask::sandbox::TestResult<()> {
        let result = CommandResult::success().with_data(serde_json::json!({
            "target_count": "three"
        }));

        let error = super::parse_fuzz_target_count(&result)
            .expect_err("non-numeric target count must surface");
        assert!(format!("{error:#}").contains("invalid target_count"));
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_disk_space_probe_reports_low_space() -> ::xtask::sandbox::TestResult<()>
    {
        let status = super::classify_disk_space_probe_result(Ok(1), 2);
        assert!(matches!(
            status,
            DiskSpaceStatus::Low {
                available_gb: 1,
                min_gb: 2
            }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_disk_space_probe_reports_sufficient_space()
    -> ::xtask::sandbox::TestResult<()> {
        let status = super::classify_disk_space_probe_result(Ok(4), 2);
        assert!(matches!(
            status,
            DiskSpaceStatus::Sufficient {
                available_gb: 4,
                min_gb: 2
            }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn test_classify_disk_space_probe_surfaces_probe_failures()
    -> ::xtask::sandbox::TestResult<()> {
        let status = super::classify_disk_space_probe_result(Err("statvfs failed".to_string()), 2);
        let DiskSpaceStatus::Unknown { issue } = status else {
            panic!("expected unknown disk-space status");
        };
        assert!(issue.contains("statvfs failed"));
        Ok(())
    }
}
