//! Codebase snapshot command - promoted from analyze snapshot

use color_eyre::eyre::{Result, WrapErr};
use serde::Serialize;
use std::path::PathBuf;
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

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
            let diag_files = collect_diagnostic_files();
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
            let changed_files = collect_changed_files();
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
            let scope_includes = collect_scope_includes(scope);
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
            let context_block = build_context_block();
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
        let content = std::fs::read_to_string(&output_path).unwrap_or_default();
        let file_size = content.len();
        let file_count = content.matches("<file ").count();

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
fn collect_diagnostic_files() -> Vec<String> {
    use crate::config::config;
    use crate::history::HistoryDb;

    let db = match HistoryDb::open(&config().history_db_path()) {
        Ok(db) => db,
        Err(_) => return vec![],
    };

    // Get current (package-scoped) diagnostics filtered to check command.
    match db.get_current_diagnostics(None, None, None, Some("check"), false) {
        Ok(diags) => {
            let mut paths: Vec<String> = diags
                .into_iter()
                .filter_map(|d| d.file_path)
                .filter(|p| !p.is_empty())
                .collect();
            paths.sort();
            paths.dedup();
            paths
        }
        Err(_) => vec![],
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// U2: Changed files collection
// ─────────────────────────────────────────────────────────────────────────────

/// Return files changed since HEAD (staged + unstaged via git diff --name-only HEAD).
fn collect_changed_files() -> Vec<String> {
    // Unstaged + staged relative to HEAD
    let head_diff = Command::new("git")
        .args(["diff", "--name-only", "HEAD"])
        .output()
        .ok();

    // Untracked new files (staged but not yet committed — git diff HEAD misses new files)
    let cached_diff = Command::new("git")
        .args(["diff", "--name-only", "--cached"])
        .output()
        .ok();

    let mut files: Vec<String> = vec![];
    for output in [head_diff, cached_diff].into_iter().flatten() {
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout);
            for line in text.lines() {
                let path = line.trim().to_string();
                if !path.is_empty() {
                    files.push(path);
                }
            }
        }
    }
    files.sort();
    files.dedup();
    files
}

// ─────────────────────────────────────────────────────────────────────────────
// U3: Context block injection
// ─────────────────────────────────────────────────────────────────────────────

/// Build the [xtask-context] block as a string.
fn build_context_block() -> String {
    let mut lines: Vec<String> = vec!["[xtask-context]".to_string()];

    // Recent check/test invocations
    lines.push(format!("recent_runs: {}", format_recent_runs()));

    // Active diagnostics
    lines.push(format!(
        "active_diagnostics: {}",
        format_active_diagnostics()
    ));

    // Coordinator state
    lines.push(format!("coordinator_state: {}", format_coordinator_state()));

    // Active background jobs
    lines.push(format!("active_jobs: {}", format_active_jobs()));

    lines.join("\n")
}

fn format_recent_runs() -> String {
    use crate::config::config;
    use crate::history::HistoryDb;

    let db = match HistoryDb::open(&config().history_db_path()) {
        Ok(db) => db,
        Err(_) => return "[]".to_string(),
    };

    // get_recent(limit, command_filter)
    let invocations = match db.get_recent(5, Some("check")) {
        Ok(v) => v,
        Err(_) => return "[]".to_string(),
    };

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
    format!("[{}]", items.join(", "))
}

fn format_active_diagnostics() -> String {
    use crate::config::config;
    use crate::history::HistoryDb;

    let db = match HistoryDb::open(&config().history_db_path()) {
        Ok(db) => db,
        Err(_) => return "[]".to_string(),
    };

    // get_current_diagnostics(level, file_pattern, package, command, fixable_only)
    let diags = match db.get_current_diagnostics(Some("error"), None, None, None, false) {
        Ok(d) => d,
        Err(_) => return "[]".to_string(),
    };

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
    format!("[{}]", items.join(", "))
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
fn collect_scope_includes(scope: &str) -> Vec<String> {
    match scope {
        "core" => vec!["crate/core/**".to_string()],
        "nodes" => vec!["crate/nodes/**".to_string()],
        "tests" => vec![
            "tests/**".to_string(),
            "xtask/tests/**".to_string(),
            "xtask/src/**".to_string(),
        ],
        "cli" => vec!["crate/cli/**".to_string()],
        "all" | "workspace" => vec!["crate/**".to_string()],
        crate_name => collect_crate_scope(crate_name),
    }
}

/// Use `cargo metadata` to find a crate and its transitive workspace dependencies,
/// returning their directory paths as include globs.
fn collect_crate_scope(crate_name: &str) -> Vec<String> {
    // Run cargo metadata to get workspace package list
    let output = Command::new("cargo")
        .args(["metadata", "--format-version=1", "--no-deps", "--quiet"])
        .output();

    let output = match output {
        Ok(o) if o.status.success() => o,
        _ => {
            // Fallback: best-effort glob based on crate name
            return vec![format!("crate/**/{crate_name}/**")];
        }
    };

    let metadata: serde_json::Value = match serde_json::from_slice(&output.stdout) {
        Ok(v) => v,
        Err(_) => return vec![format!("crate/**/{crate_name}/**")],
    };

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
        return vec![format!("crate/**/{crate_name}/**")];
    }

    // Now run cargo metadata WITH deps to get transitive dep graph for this crate
    let output_with_deps = Command::new("cargo")
        .args([
            "metadata",
            "--format-version=1",
            "--quiet",
            "--filter-platform",
            std::env::consts::ARCH, // avoids cross-compilation noise
        ])
        .output();

    // Collect: crate itself + transitive workspace deps
    let mut included_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    included_names.insert(crate_name.to_string());

    if let Ok(out) = output_with_deps {
        if out.status.success() {
            if let Ok(full_meta) = serde_json::from_slice::<serde_json::Value>(&out.stdout) {
                collect_transitive_workspace_deps(crate_name, &full_meta, &mut included_names);
            }
        }
    }

    // Map crate names to directory globs
    let mut patterns: Vec<String> = included_names
        .iter()
        .filter_map(|name| name_to_dir.get(name))
        .map(|dir| format!("{dir}/**"))
        .collect();
    patterns.sort();
    patterns.dedup();

    if patterns.is_empty() {
        vec![format!("crate/**/{crate_name}/**")]
    } else {
        patterns
    }
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
