//! Release-readiness gate for shipping claims and executable evidence.

use color_eyre::eyre::Result;
use serde::Serialize;

use crate::command::{
    CommandContext, CommandMetadata, CommandResult, HistoryAccessMode, XtaskCommand,
};
use crate::output::StructuredError;
use crate::process::ProcessBuilder;

/// Emit the release-readiness claim matrix and optionally execute required gates.
#[derive(Debug, Clone, clap::Args)]
pub struct ReleaseReadinessCommand {
    /// Release target/name to report in the readiness payload.
    #[arg(long, default_value = "local")]
    pub target: String,

    /// Base ref for changed-surface checks.
    #[arg(long, default_value = "origin/master")]
    pub base_ref: String,

    /// Execute the required command bundle. Without this flag the command emits
    /// the release contract and required commands without running expensive gates.
    #[arg(long)]
    pub run_required_checks: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ReleaseReadinessReport {
    target: String,
    git_revision: String,
    status: ReleaseReadinessStatus,
    shipped_claims: Vec<ReleaseClaim>,
    non_claims: Vec<ReleaseNonClaim>,
    caveats: Vec<ReleaseCaveat>,
    required_checks: Vec<ReleaseCheck>,
    generated_artifacts: Vec<GeneratedArtifact>,
    check_results: Vec<ReleaseCheckResult>,
    summary: ReleaseReadinessSummary,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ReleaseReadinessStatus {
    ContractOnly,
    Ready,
    Blocked,
}

#[derive(Debug, Clone, Serialize)]
struct ReleaseClaim {
    area: &'static str,
    claim: &'static str,
    evidence: Vec<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
struct ReleaseNonClaim {
    area: &'static str,
    non_claim: &'static str,
    owner: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct ReleaseCaveat {
    area: &'static str,
    caveat: &'static str,
    owner: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct ReleaseCheck {
    id: &'static str,
    family: &'static str,
    command: String,
    proves: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct GeneratedArtifact {
    path: &'static str,
    validation_command: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct ReleaseCheckResult {
    id: &'static str,
    command: String,
    status: CheckStatus,
    detail: String,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CheckStatus {
    NotRun,
    Passed,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
struct ReleaseReadinessSummary {
    required_check_count: usize,
    passed_check_count: usize,
    failed_check_count: usize,
    not_run_check_count: usize,
    ready_for_release: bool,
}

impl XtaskCommand for ReleaseReadinessCommand {
    fn name(&self) -> &'static str {
        "release-readiness"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let report = build_release_readiness_report(
            &self.target,
            &self.base_ref,
            self.run_required_checks,
            run_release_check,
        );
        let failed = report.summary.failed_check_count > 0;
        let data = serde_json::to_value(&report)?;

        if failed {
            let first_failure = report
                .check_results
                .iter()
                .find(|result| result.status == CheckStatus::Failed)
                .expect("failure count implies failed result");
            return Ok(CommandResult::failure(
                StructuredError::new(
                    "RELEASE_READINESS_BLOCKED",
                    format!(
                        "release-readiness check `{}` failed: {}",
                        first_failure.id, first_failure.command
                    ),
                )
                .with_suggestion(first_failure.detail.clone()),
            )
            .with_data(data)
            .with_duration(ctx.elapsed()));
        }

        let message = match report.status {
            ReleaseReadinessStatus::ContractOnly => {
                "Release-readiness contract emitted; rerun with --run-required-checks to gate release"
            }
            ReleaseReadinessStatus::Ready => "Release-readiness required checks passed",
            ReleaseReadinessStatus::Blocked => unreachable!("handled above"),
        };

        Ok(CommandResult::success()
            .with_message(message)
            .with_data(data)
            .with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::check()
            .with_history_access(HistoryAccessMode::None)
            .with_history_tracking(false)
    }
}

fn build_release_readiness_report(
    target: &str,
    base_ref: &str,
    run_required_checks: bool,
    runner: impl Fn(&ReleaseCheck) -> ReleaseCheckResult,
) -> ReleaseReadinessReport {
    let required_checks = release_checks(base_ref);
    let check_results: Vec<ReleaseCheckResult> = if run_required_checks {
        required_checks.iter().map(runner).collect()
    } else {
        required_checks
            .iter()
            .map(|check| ReleaseCheckResult {
                id: check.id,
                command: check.command.clone(),
                status: CheckStatus::NotRun,
                detail: "not run; pass --run-required-checks to execute this gate".to_string(),
            })
            .collect()
    };
    let summary = summarize_results(&check_results);
    let status = if summary.failed_check_count > 0 {
        ReleaseReadinessStatus::Blocked
    } else if run_required_checks {
        ReleaseReadinessStatus::Ready
    } else {
        ReleaseReadinessStatus::ContractOnly
    };

    ReleaseReadinessReport {
        target: target.to_string(),
        git_revision: git_revision(),
        status,
        shipped_claims: shipped_claims(),
        non_claims: non_claims(),
        caveats: caveats(),
        required_checks,
        generated_artifacts: generated_artifacts(),
        check_results,
        summary,
    }
}

fn release_checks(base_ref: &str) -> Vec<ReleaseCheck> {
    vec![
        ReleaseCheck {
            id: "nix-eval",
            family: "format-lint-static",
            command: "xtask check --nix --allow-contended-host".to_string(),
            proves: "flake outputs and generated devshell wiring evaluate through the repo-native check gate",
        },
        ReleaseCheck {
            id: "changed-strict",
            family: "focused-rust",
            command: format!("xtask check --changed-strict {base_ref} --allow-contended-host"),
            proves: "changed Rust/API surfaces compile through the affected-package gate",
        },
        ReleaseCheck {
            id: "schema-strict-diff",
            family: "schema",
            command: "xtask schema strict-diff".to_string(),
            proves: "declared schema and inspected database have no strict drift",
        },
        ReleaseCheck {
            id: "command-reference",
            family: "operator-surfaces",
            command: "xtask docs command-reference --check".to_string(),
            proves: "checked-in xtask command reference matches the live clap tree",
        },
        ReleaseCheck {
            id: "schema-bundle",
            family: "generated-artifacts",
            command: "xtask docs schema-bundle --check".to_string(),
            proves: "checked-in payload schema bundle matches the Rust registry",
        },
        ReleaseCheck {
            id: "source-catalog-drift",
            family: "generated-artifacts",
            command: "xtask test -p sinexd -E 'test(source_catalog_artifact_matches_inventory)' --allow-contended-host"
                .to_string(),
            proves: "checked-in NixOS source catalog artifact matches the linked Rust source inventory",
        },
        ReleaseCheck {
            id: "privacy-catalog",
            family: "privacy",
            command: "xtask privacy catalog --format json".to_string(),
            proves: "privacy catalog can be loaded by the repo-native privacy engine",
        },
    ]
}

fn shipped_claims() -> Vec<ReleaseClaim> {
    vec![
        ReleaseClaim {
            area: "runtime",
            claim: "event admission, transport routing, stream pressure, DLQ pressure, and system health are represented by typed runtime surfaces",
            evidence: vec!["#1732", "#1739", "OperationView/runtime health DTOs"],
        },
        ReleaseClaim {
            area: "source packages",
            claim: "registered source contracts, runtime bindings, catalog export, and privacy coverage are available for accepted packages",
            evidence: vec![
                "SourceDefinition/SourceMeta inventory",
                "source catalog and privacy coverage tests",
            ],
        },
        ReleaseClaim {
            area: "event admission",
            claim: "producers publish EventIntent-shaped admission payloads rather than raw canonical events",
            evidence: vec!["EventIntent DTO", "event-engine integration tests"],
        },
        ReleaseClaim {
            area: "operator surfaces",
            claim: "CLI/API/TUI-facing read paths use finite DTOs or ViewEnvelope-shaped payloads for release-visible status",
            evidence: vec!["sinexctl ViewEnvelope tests", "gateway fixture validation"],
        },
        ReleaseClaim {
            area: "schema",
            claim: "schema convergence and strict-diff are repo-native gates",
            evidence: vec!["xtask schema strict-diff", "schema contract tests"],
        },
    ]
}

fn non_claims() -> Vec<ReleaseNonClaim> {
    vec![
        ReleaseNonClaim {
            area: "capture packages",
            non_claim: "media audio/OCR and email sync packages are not claimed complete until their package-specific issues close",
            owner: "#1043/#1469",
        },
        ReleaseNonClaim {
            area: "package completeness",
            non_claim: "accepted package modes are not claimed fully complete where the package-completeness gate still reports blocking requirements",
            owner: "#1963",
        },
        ReleaseNonClaim {
            area: "runtime pressure",
            non_claim: "resource-pressure behavior is claimed only for paths with code-owned budget, pressure, coverage, or operation evidence",
            owner: "#1963/#1043",
        },
        ReleaseNonClaim {
            area: "privacy enforcement",
            non_claim: "package-level disclosure metadata is not a claim that every destination surface has package-specific evidence",
            owner: "#1963/#1043",
        },
    ]
}

fn caveats() -> Vec<ReleaseCaveat> {
    vec![
        ReleaseCaveat {
            area: "deployment",
            caveat: "Sinnix owns live host paths, systemd units, service resource policy, and rollout state",
            owner: "#1738",
        },
        ReleaseCaveat {
            area: "verification",
            caveat: "GitHub Actions capacity/billing failures are tool failures; local equivalent gates must be recorded when they occur",
            owner: "#1698",
        },
    ]
}

fn generated_artifacts() -> Vec<GeneratedArtifact> {
    vec![
        GeneratedArtifact {
            path: "xtask/docs/command-reference.md",
            validation_command: "xtask docs command-reference --check",
        },
        GeneratedArtifact {
            path: "schemas/payloads",
            validation_command: "xtask docs schema-bundle --check",
        },
        GeneratedArtifact {
            path: "nixos/modules/source-catalog.generated.json",
            validation_command: "xtask test -p sinexd -E 'test(source_catalog_artifact_matches_inventory)' --allow-contended-host",
        },
    ]
}

fn summarize_results(results: &[ReleaseCheckResult]) -> ReleaseReadinessSummary {
    let passed_check_count = results
        .iter()
        .filter(|result| result.status == CheckStatus::Passed)
        .count();
    let failed_check_count = results
        .iter()
        .filter(|result| result.status == CheckStatus::Failed)
        .count();
    let not_run_check_count = results
        .iter()
        .filter(|result| result.status == CheckStatus::NotRun)
        .count();

    ReleaseReadinessSummary {
        required_check_count: results.len(),
        passed_check_count,
        failed_check_count,
        not_run_check_count,
        ready_for_release: failed_check_count == 0 && not_run_check_count == 0,
    }
}

fn run_release_check(check: &ReleaseCheck) -> ReleaseCheckResult {
    match ProcessBuilder::new("sh")
        .args(["-lc", check.command.as_str()])
        .with_description(format!("release-readiness check {}", check.id))
        .run()
    {
        Ok(_) => ReleaseCheckResult {
            id: check.id,
            command: check.command.clone(),
            status: CheckStatus::Passed,
            detail: "passed".to_string(),
        },
        Err(error) => ReleaseCheckResult {
            id: check.id,
            command: check.command.clone(),
            status: CheckStatus::Failed,
            detail: format!("{error:#}"),
        },
    }
}

fn git_revision() -> String {
    ProcessBuilder::git()
        .args(["rev-parse", "HEAD"])
        .run()
        .map(|output| output.stdout.trim().to_string())
        .unwrap_or_else(|error| format!("unknown: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_only_report_separates_claims_non_claims_and_checks() {
        let report = build_release_readiness_report("rc", "origin/master", false, |_| {
            unreachable!("contract-only mode must not run checks")
        });

        assert_eq!(report.target, "rc");
        assert_eq!(report.status, ReleaseReadinessStatus::ContractOnly);
        assert!(!report.shipped_claims.is_empty());
        assert!(!report.non_claims.is_empty());
        assert!(!report.caveats.is_empty());
        assert!(!report.generated_artifacts.is_empty());
        assert!(
            report
                .required_checks
                .iter()
                .any(|check| check.id == "changed-strict")
        );
        assert!(report.required_checks.iter().any(|check| {
            check.id == "source-catalog-drift"
                && check
                    .command
                    .contains("source_catalog_artifact_matches_inventory")
        }));
        assert_eq!(
            report.summary.not_run_check_count,
            report.required_checks.len()
        );
        assert!(!report.summary.ready_for_release);
    }

    #[test]
    fn generated_source_catalog_artifact_points_at_behavior_owner_test() {
        let source_catalog = generated_artifacts()
            .into_iter()
            .find(|artifact| artifact.path == "nixos/modules/source-catalog.generated.json")
            .expect("source catalog generated artifact must be listed");

        assert_eq!(
            source_catalog.validation_command,
            "xtask test -p sinexd -E 'test(source_catalog_artifact_matches_inventory)' --allow-contended-host"
        );
    }

    #[test]
    fn failing_required_check_blocks_release_readiness() {
        let report = build_release_readiness_report("rc", "origin/master", true, |check| {
            ReleaseCheckResult {
                id: check.id,
                command: check.command.clone(),
                status: if check.id == "schema-strict-diff" {
                    CheckStatus::Failed
                } else {
                    CheckStatus::Passed
                },
                detail: "synthetic check result".to_string(),
            }
        });

        assert_eq!(report.status, ReleaseReadinessStatus::Blocked);
        assert_eq!(report.summary.failed_check_count, 1);
        assert!(!report.summary.ready_for_release);
        assert!(report.check_results.iter().any(
            |result| result.id == "schema-strict-diff" && result.status == CheckStatus::Failed
        ));
    }

    #[test]
    fn all_required_checks_pass_makes_release_ready() {
        let report = build_release_readiness_report("rc", "origin/master", true, |check| {
            ReleaseCheckResult {
                id: check.id,
                command: check.command.clone(),
                status: CheckStatus::Passed,
                detail: "synthetic pass".to_string(),
            }
        });

        assert_eq!(report.status, ReleaseReadinessStatus::Ready);
        assert_eq!(report.summary.failed_check_count, 0);
        assert_eq!(report.summary.not_run_check_count, 0);
        assert!(report.summary.ready_for_release);
    }
}
