//! Codebase snapshot command - promoted from analyze snapshot

use color_eyre::eyre::{Result, WrapErr, bail};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;

/// Generate a codebase snapshot for AI context (via repomix)
#[derive(Debug, Clone, clap::Args)]
pub struct SnapshotCommand {
    /// Output file path
    #[arg(short, long)]
    pub output: Option<PathBuf>,
    /// Include patterns (glob)
    #[arg(long)]
    pub include: Vec<String>,
    /// Exclude patterns (glob)
    #[arg(long)]
    pub exclude: Vec<String>,
    /// Use Tree-sitter to extract essential code structure (smaller output)
    #[arg(long)]
    pub compress: bool,
    /// Remove code comments from output
    #[arg(long)]
    pub remove_comments: bool,

    /// U1: Auto-include files mentioned in the most recent build_diagnostics run
    #[arg(long)]
    pub diagnostics: bool,

    /// U2: Include files changed since HEAD (staged + unstaged)
    #[arg(long)]
    pub changed: bool,

    /// U3: Inject structured xtask state (recent checks, diagnostics, jobs) into the snapshot
    #[arg(long)]
    pub context: bool,

    /// U4: Include CLAUDE.md and .claude/includes/ (project memory) in the snapshot
    #[arg(long)]
    pub project_memory: bool,

    /// U5: Scope to a crate or directory group (e.g., sinex-db, core, nodes, tests)
    #[arg(long)]
    pub scope: Option<String>,
}

/// Snapshot metadata
#[derive(Debug, Serialize)]
struct SnapshotResult {
    output_file: String,
    file_count: usize,
    total_bytes: usize,
    compressed: bool,
    context_injected: bool,
}

impl XtaskCommand for SnapshotCommand {
    fn name(&self) -> &'static str {
        "snapshot"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Check if repomix is available
        let repomix_check = Command::new("which").arg("repomix").output();
        if repomix_check.is_err() || !repomix_check.unwrap().status.success() {
            return Ok(CommandResult::failure(crate::output::StructuredError {
                code: "TOOL_NOT_FOUND".to_string(),
                message: "repomix not found. Install with: npm install -g repomix".to_string(),
                location: Some("snapshot".to_string()),
                suggestion: Some("Install: npm install -g repomix".to_string()),
            }));
        }

        let output_path = self.output.as_ref().map_or_else(
            || "context.xml".to_string(),
            |p| p.to_string_lossy().to_string(),
        );

        let mut args = vec!["--output".to_string(), output_path.clone()];

        // Tree-sitter semantic compression (extracts code structure)
        if self.compress {
            args.push("--compress".to_string());
        }

        // Remove comments
        if self.remove_comments {
            args.push("--remove-comments".to_string());
        }

        // --- Collect dynamic includes ---
        let mut dynamic_includes: Vec<String> = self.include.clone();

        // U1: Include files from most recent build_diagnostics invocation
        if self.diagnostics {
            let diag_files = collect_diagnostic_files(ctx);
            if ctx.is_human() && !diag_files.is_empty() {
                println!(
                    "  Diagnostics: including {} files from recent check run",
                    diag_files.len()
                );
            }
            dynamic_includes.extend(diag_files);
        }

        // U2: Include files changed since HEAD
        if self.changed {
            let changed_files = collect_changed_files()?;
            if ctx.is_human() && !changed_files.is_empty() {
                println!(
                    "  Changed: including {} files from git diff",
                    changed_files.len()
                );
            }
            dynamic_includes.extend(changed_files);
        }

        // U4: Include project memory (CLAUDE.md + .claude/includes/)
        if self.project_memory {
            dynamic_includes.push("CLAUDE.md".to_string());
            dynamic_includes.push(".claude/**".to_string());
            if ctx.is_human() {
                println!("  Project memory: including CLAUDE.md and .claude/");
            }
        }

        // U5: Scope to crate/directory group
        if let Some(scope) = &self.scope {
            let scope_includes = collect_scope_includes(scope)?;
            if ctx.is_human() {
                println!(
                    "  Scope '{}': {} include pattern(s)",
                    scope,
                    scope_includes.len()
                );
            }
            dynamic_includes.extend(scope_includes);
        }

        // Add all includes
        for inc in &dynamic_includes {
            args.push("--include".to_string());
            args.push(inc.clone());
        }

        // Add excludes (with sensible defaults for sinex)
        let default_excludes = [
            "target/",
            "node_modules/",
            ".git/",
            "*.lock",
            "*.log",
            "test-results/",
        ];

        for exc in default_excludes
            .iter()
            .map(std::string::ToString::to_string)
            .chain(self.exclude.iter().cloned())
        {
            args.push("--ignore".to_string());
            args.push(exc);
        }

        if ctx.is_human() {
            println!("Generating codebase snapshot...");
            if self.compress {
                println!("  Mode: Tree-sitter structure extraction");
            }
            println!("  Output: {output_path}");
        }

        let stage = ctx.start_stage("repomix");
        let result = Command::new("repomix")
            .args(&args)
            .output()
            .context("Failed to run repomix")?;
        ctx.finish_stage(stage, result.status.success());

        if !result.status.success() {
            let stderr = String::from_utf8_lossy(&result.stderr);
            return Ok(CommandResult::failure(crate::output::StructuredError {
                code: "REPOMIX_FAILED".to_string(),
                message: format!("repomix failed: {stderr}"),
                location: Some("snapshot".to_string()),
                suggestion: Some("Reinstall repomix: npm install -g repomix".to_string()),
            }));
        }

        // U3: Inject xtask context block into the output file
        let context_injected = if self.context {
            let context_block = build_context_block(ctx);
            if let Err(e) = append_context_block(&output_path, &context_block) {
                if ctx.is_human() {
                    eprintln!("  Warning: could not inject xtask context: {e}");
                }
                false
            } else {
                if ctx.is_human() {
                    println!("  Context: xtask state injected");
                }
                true
            }
        } else {
            false
        };

        // Single read for both size and file count (avoid separate metadata + read_to_string)
        let snapshot_output = read_snapshot_output(Path::new(&output_path))?;
        let file_size = snapshot_output.len();
        let file_count = snapshot_output.matches("<file ").count();

        let snapshot_result = SnapshotResult {
            output_file: output_path,
            file_count,
            total_bytes: file_size,
            compressed: self.compress,
            context_injected,
        };

        if ctx.is_human() {
            println!("\nSnapshot created:");
            println!("  File: {}", snapshot_result.output_file);
            println!("  Files included: {}", snapshot_result.file_count);
            println!(
                "  Size: {} bytes{}",
                snapshot_result.total_bytes,
                if self.compress {
                    " (structure-only)"
                } else {
                    ""
                }
            );

            Ok(CommandResult::success()
                .with_message("Snapshot created")
                .with_duration(ctx.elapsed()))
        } else {
            Ok(CommandResult::success()
                .with_data(serde_json::to_value(&snapshot_result)?)
                .with_duration(ctx.elapsed()))
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// U1: Diagnostic file collection
// ─────────────────────────────────────────────────────────────────────────────

/// Return distinct file paths from the most recent build_diagnostics invocation.
fn collect_diagnostic_files(ctx: &CommandContext) -> Vec<String> {
    ctx.with_history_db(|db| {
        // Get current (package-scoped) diagnostics filtered to check command.
        let diags = db.get_current_diagnostics(None, None, None, Some("check"), false)?;
        let mut paths: Vec<String> = diags
            .into_iter()
            .filter_map(|d| d.file_path)
            .filter(|p| !p.is_empty())
            .collect();
        paths.sort();
        paths.dedup();
        Ok(paths)
    })
    .unwrap_or_default()
}

// ─────────────────────────────────────────────────────────────────────────────
// U2: Changed files collection
// ─────────────────────────────────────────────────────────────────────────────

/// Return files changed since HEAD (staged + unstaged via git diff --name-only HEAD).
fn collect_changed_files() -> Result<Vec<String>> {
    let mut files = git_name_only(
        &["diff", "--name-only", "HEAD"],
        "git diff --name-only HEAD",
    )?;
    files.extend(git_name_only(
        &["diff", "--name-only", "--cached"],
        "git diff --name-only --cached",
    )?);
    files.sort();
    files.dedup();
    Ok(files)
}

fn git_name_only(args: &[&str], description: &str) -> Result<Vec<String>> {
    let output = ProcessBuilder::git()
        .args(args.iter().copied())
        .with_description(description)
        .run()?;

    Ok(output
        .stdout
        .lines()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

// ─────────────────────────────────────────────────────────────────────────────
// U3: Context block injection
// ─────────────────────────────────────────────────────────────────────────────

/// Build the [xtask-context] block as a string.
fn build_context_block(ctx: &CommandContext) -> String {
    let mut lines: Vec<String> = vec!["[xtask-context]".to_string()];

    // Recent check/test invocations
    lines.push(format!("recent_runs: {}", format_recent_runs(ctx)));

    // Active diagnostics
    lines.push(format!(
        "active_diagnostics: {}",
        format_active_diagnostics(ctx)
    ));

    // Coordinator state
    lines.push(format!("coordinator_state: {}", format_coordinator_state()));

    // Active background jobs
    lines.push(format!("active_jobs: {}", format_active_jobs()));

    lines.join("\n")
}

fn format_recent_runs(ctx: &CommandContext) -> String {
    ctx.with_history_db(|db| {
        // get_recent(limit, command_filter)
        let invocations = db.get_recent(5, Some("check"))?;

        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let items: Vec<String> = invocations
            .iter()
            .map(|inv| {
                let age_secs = now - inv.started_at.unix_timestamp();
                let when = format_age(age_secs);
                let status = inv.status.as_str();
                format!("{{id:{}, status:{}, when:\"{}\"}}", inv.id, status, when)
            })
            .collect();
        Ok(format!("[{}]", items.join(", ")))
    })
    .unwrap_or_else(|| "[]".to_string())
}

fn format_active_diagnostics(ctx: &CommandContext) -> String {
    ctx.with_history_db(|db| {
        // get_current_diagnostics(level, file_pattern, package, command, fixable_only)
        let diags = db.get_current_diagnostics(Some("error"), None, None, None, false)?;

        let items: Vec<String> = diags
            .iter()
            .take(10)
            .map(|d| {
                let file = d.file_path.as_deref().unwrap_or("?");
                let line = d
                    .line
                    .map(|l| l.to_string())
                    .unwrap_or_else(|| "?".to_string());
                let msg = d.message.chars().take(60).collect::<String>();
                format!("{{file:\"{file}\", line:{line}, msg:\"{msg}\"}}")
            })
            .collect();
        Ok(format!("[{}]", items.join(", ")))
    })
    .unwrap_or_else(|| "[]".to_string())
}

fn format_coordinator_state() -> String {
    use crate::coordinator::JobCoordinator;

    let coord = match JobCoordinator::new() {
        Ok(c) => c,
        Err(_) => return "{}".to_string(),
    };

    let mut parts: Vec<String> = vec![];
    for cmd in &["check", "test", "build"] {
        if let Ok(Some(state)) = coord.state(cmd) {
            parts.push(format!(
                "{cmd}: {{job_id:{}, scope:\"{}\", fingerprint:\"{}\"}}",
                state.job_id,
                state.scope_key,
                &state.tree_fingerprint[..state.tree_fingerprint.len().min(12)]
            ));
        }
    }
    if parts.is_empty() {
        "{}".to_string()
    } else {
        format!("{{{}}}", parts.join(", "))
    }
}

fn format_active_jobs() -> String {
    use crate::config::config;
    use crate::jobs::JobManager;

    let jobs_dir = config().jobs_dir();
    let mgr = match JobManager::new(jobs_dir) {
        Ok(m) => m,
        Err(_) => return "[]".to_string(),
    };

    let active = match mgr.list_active() {
        Ok(jobs) => jobs,
        Err(_) => return "[]".to_string(),
    };

    let items: Vec<String> = active
        .iter()
        .map(|j| {
            format!(
                "{{id:{}, command:\"{}\", status:\"{}\"}}",
                j.id,
                j.command,
                j.job_status.as_str()
            )
        })
        .collect();
    format!("[{}]", items.join(", "))
}

/// Append the context block as an XML comment to the repomix output file.
fn append_context_block(output_path: &str, context: &str) -> Result<()> {
    use std::io::Write;

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(output_path)
        .with_context(|| format!("open {output_path} for append"))?;

    writeln!(file, "\n<!--")?;
    writeln!(file, "{context}")?;
    writeln!(file, "-->")?;

    Ok(())
}

fn format_age(secs: i64) -> String {
    if secs < 60 {
        format!("{secs}s ago")
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else if secs < 86_400 {
        format!("{}h ago", secs / 3600)
    } else {
        format!("{}d ago", secs / 86_400)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// U5: Scope-based include resolution
// ─────────────────────────────────────────────────────────────────────────────

/// Resolve a scope string into repomix --include patterns.
///
/// Predefined aliases:
/// - `core`  → `crate/core/**`
/// - `nodes` → `crate/nodes/**`
/// - `tests` → `tests/**, xtask/tests/**`
/// - `cli`   → `crate/cli/**`
///
/// Any other string is treated as a crate name. The function resolves the crate's
/// directory from `cargo metadata` and collects transitive workspace dependencies.
fn collect_scope_includes(scope: &str) -> Result<Vec<String>> {
    Ok(match scope {
        "core" => vec!["crate/core/**".to_string()],
        "nodes" => vec!["crate/nodes/**".to_string()],
        "tests" => vec![
            "tests/**".to_string(),
            "xtask/tests/**".to_string(),
            "xtask/src/**".to_string(),
        ],
        "cli" => vec!["crate/cli/**".to_string()],
        "all" | "workspace" => vec!["crate/**".to_string()],
        crate_name => collect_crate_scope(crate_name)?,
    })
}

/// Use `cargo metadata` to find a crate and its transitive workspace dependencies,
/// returning their directory paths as include globs.
fn collect_crate_scope(crate_name: &str) -> Result<Vec<String>> {
    let metadata = cargo_metadata(
        ["metadata", "--format-version=1", "--no-deps", "--quiet"],
        "workspace package metadata",
    )?;

    let workspace_root = crate::config::workspace_root();

    // Build name → relative_dir map for all workspace members
    let mut name_to_dir: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    if let Some(packages) = metadata["packages"].as_array() {
        for pkg in packages {
            let name = pkg["name"].as_str().unwrap_or("").to_string();
            let manifest = pkg["manifest_path"].as_str().unwrap_or("");
            if !manifest.is_empty() {
                let manifest_path = std::path::Path::new(manifest);
                if let Some(crate_dir) = manifest_path.parent() {
                    if let Ok(rel) = crate_dir.strip_prefix(&workspace_root) {
                        name_to_dir.insert(name, rel.to_string_lossy().to_string());
                    }
                }
            }
        }
    }

    if name_to_dir.is_empty() {
        bail!("workspace package metadata returned no packages");
    }
    if !name_to_dir.contains_key(crate_name) {
        bail!("scope '{crate_name}' did not match a workspace package or predefined alias");
    }

    let full_meta = cargo_metadata(
        [
            "metadata",
            "--format-version=1",
            "--quiet",
            "--filter-platform",
            std::env::consts::ARCH, // avoids cross-compilation noise
        ],
        "workspace dependency metadata",
    )?;

    // Collect: crate itself + transitive workspace deps
    let mut included_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    included_names.insert(crate_name.to_string());

    collect_transitive_workspace_deps(crate_name, &full_meta, &mut included_names);

    // Map crate names to directory globs
    let mut patterns: Vec<String> = included_names
        .iter()
        .filter_map(|name| name_to_dir.get(name))
        .map(|dir| format!("{dir}/**"))
        .collect();
    patterns.sort();
    patterns.dedup();

    if patterns.is_empty() {
        bail!("scope '{crate_name}' resolved no workspace include patterns");
    }

    Ok(patterns)
}

/// Walk the cargo metadata dependency graph to find transitive workspace deps.
fn collect_transitive_workspace_deps(
    root_name: &str,
    metadata: &serde_json::Value,
    result: &mut std::collections::HashSet<String>,
) {
    // Build id → (name, deps_names) index
    let mut id_to_name: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
    let mut id_to_deps: std::collections::HashMap<&str, Vec<&str>> =
        std::collections::HashMap::new();
    let workspace_members: std::collections::HashSet<&str> = metadata["workspace_members"]
        .as_array()
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    if let Some(packages) = metadata["packages"].as_array() {
        for pkg in packages {
            let id = pkg["id"].as_str().unwrap_or("");
            let name = pkg["name"].as_str().unwrap_or("");
            id_to_name.insert(id, name);
            let dep_names: Vec<&str> = pkg["dependencies"]
                .as_array()
                .map(|deps| deps.iter().filter_map(|d| d["name"].as_str()).collect())
                .unwrap_or_default();
            id_to_deps.insert(id, dep_names);
        }
    }

    // Find root package id
    let root_id = id_to_name
        .iter()
        .find(|&(_, &name)| name == root_name)
        .map(|(&id, _)| id);

    let Some(root_id) = root_id else { return };

    // BFS over dep graph — only follow workspace members
    let mut queue: std::collections::VecDeque<&str> = std::collections::VecDeque::new();
    queue.push_back(root_id);

    while let Some(pkg_id) = queue.pop_front() {
        if let Some(&name) = id_to_name.get(pkg_id) {
            // Check if this is a workspace member
            if workspace_members.iter().any(|m| m.contains(name)) {
                result.insert(name.to_string());
                // Follow its dependencies
                if let Some(dep_names) = id_to_deps.get(pkg_id) {
                    for dep_name in dep_names {
                        if let Some((&dep_id, _)) =
                            id_to_name.iter().find(|&(_, &n)| n == *dep_name)
                        {
                            if workspace_members.iter().any(|m| m.contains(dep_name))
                                && !result.contains(*dep_name)
                            {
                                queue.push_back(dep_id);
                            }
                        }
                    }
                }
            }
        }
    }
}

fn cargo_metadata<I, S>(args: I, description: &str) -> Result<serde_json::Value>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let output = ProcessBuilder::cargo()
        .args(args)
        .with_description(description)
        .run()?;

    serde_json::from_str(&output.stdout)
        .with_context(|| format!("failed to parse {description} output as cargo metadata JSON"))
}

fn read_snapshot_output(output_path: &Path) -> Result<String> {
    std::fs::read_to_string(output_path)
        .with_context(|| format!("failed to read snapshot output {}", output_path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use ::xtask::sandbox::EnvGuard;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    fn write_executable_script(
        path: &std::path::Path,
        body: &str,
    ) -> ::xtask::sandbox::TestResult<()> {
        fs::write(path, body)?;
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions)?;
        Ok(())
    }

    #[sinex_test]
    async fn test_collect_changed_files_reports_git_failures() -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let bin_dir = temp.path().join("bin");
        fs::create_dir_all(&bin_dir)?;
        write_executable_script(
            &bin_dir.join("git"),
            r#"#!/bin/sh
printf 'fatal: synthetic git failure\n' >&2
exit 128
"#,
        )?;

        let mut env = EnvGuard::new();
        env.set("PATH", bin_dir.display().to_string());

        let error = collect_changed_files().expect_err("git failure should surface");
        assert!(error.to_string().contains("git diff --name-only HEAD"));
        assert!(error.to_string().contains("synthetic git failure"));
        Ok(())
    }

    #[sinex_test]
    async fn test_collect_changed_files_deduplicates_head_and_cached()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let bin_dir = temp.path().join("bin");
        fs::create_dir_all(&bin_dir)?;
        write_executable_script(
            &bin_dir.join("git"),
            r#"#!/bin/sh
if [ "$1" = "diff" ] && [ "$2" = "--name-only" ] && [ "$3" = "HEAD" ]; then
  printf 'a.rs\nshared.rs\n'
  exit 0
fi
if [ "$1" = "diff" ] && [ "$2" = "--name-only" ] && [ "$3" = "--cached" ]; then
  printf 'b.rs\nshared.rs\n'
  exit 0
fi
printf 'unexpected git invocation: %s\n' "$*" >&2
exit 1
"#,
        )?;

        let mut env = EnvGuard::new();
        env.set("PATH", bin_dir.display().to_string());

        assert_eq!(
            collect_changed_files()?,
            vec![
                "a.rs".to_string(),
                "b.rs".to_string(),
                "shared.rs".to_string()
            ]
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_collect_crate_scope_reports_metadata_failures()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let bin_dir = temp.path().join("bin");
        fs::create_dir_all(&bin_dir)?;
        write_executable_script(
            &bin_dir.join("cargo"),
            r#"#!/bin/sh
printf 'cargo metadata exploded\n' >&2
exit 101
"#,
        )?;

        let mut env = EnvGuard::new();
        env.set("PATH", bin_dir.display().to_string());

        let error = collect_crate_scope("sinex-db").expect_err("metadata failure should surface");
        assert!(error.to_string().contains("workspace package metadata"));
        assert!(error.to_string().contains("cargo metadata exploded"));
        Ok(())
    }

    #[sinex_test]
    async fn test_collect_crate_scope_reports_unknown_workspace_package()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let bin_dir = temp.path().join("bin");
        fs::create_dir_all(&bin_dir)?;
        write_executable_script(
            &bin_dir.join("cargo"),
            r#"#!/bin/sh
printf '%s\n' '{"packages":[{"name":"sinex-db","manifest_path":"/realm/project/sinex/crate/lib/sinex-db/Cargo.toml"}]}'
"#,
        )?;

        let mut env = EnvGuard::new();
        env.set("PATH", bin_dir.display().to_string());

        let error = collect_crate_scope("missing-crate")
            .expect_err("unknown workspace package should surface");
        assert!(
            error.to_string().contains("missing-crate"),
            "unexpected error: {error:?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_read_snapshot_output_reports_missing_file() -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let missing = temp.path().join("missing.xml");
        let error = read_snapshot_output(&missing).expect_err("missing snapshot should error");
        assert!(error.to_string().contains("failed to read snapshot output"));
        assert!(error.to_string().contains("missing.xml"));
        Ok(())
    }
}
