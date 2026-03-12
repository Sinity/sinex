//! Documentation generation command

use crate::process::ProcessBuilder;
use color_eyre::eyre::Result;
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::commands::snapshot::SnapshotCommand;

/// Documentation subcommand variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum DocsSubcommand {
    /// Build documentation
    Build {
        /// Build for specific package(s)
        #[arg(short, long)]
        package: Vec<String>,

        /// Open in browser after build
        #[arg(long)]
        open: bool,

        /// Include private items
        #[arg(long)]
        private: bool,

        /// Build all-features documentation
        #[arg(long)]
        all_features: bool,
    },

    /// Serve documentation locally (requires `simple-http-server` or `python3`)
    Serve {
        /// Port to serve on
        #[arg(short, long, default_value = "8080")]
        port: u16,

        /// Build docs before serving
        #[arg(long)]
        build: bool,
    },

    /// Generate AGENTS.md by resolving the CLAUDE.md transclusion tree.
    ///
    /// Reads CLAUDE.md from the workspace root and recursively expands all
    /// `@path` import lines into their file contents, writing the result to
    /// AGENTS.md at the workspace root.  This gives agent frameworks that read
    /// AGENTS.md (e.g. Codex) the same context that Claude Code agents receive
    /// via native `@path` transclusion.
    Agents {
        /// Output file (default: AGENTS.md in workspace root)
        #[arg(long)]
        output: Option<std::path::PathBuf>,

        /// Print to stdout instead of writing a file
        #[arg(long)]
        stdout: bool,
    },

    /// Generate a codebase snapshot for AI context (via repomix)
    Snapshot(SnapshotCommand),
}

/// Documentation generation command
#[derive(Debug, Clone, clap::Args)]
pub struct DocsCommand {
    #[command(subcommand)]
    pub subcommand: DocsSubcommand,
}

impl XtaskCommand for DocsCommand {
    fn name(&self) -> &'static str {
        "docs"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            DocsSubcommand::Build {
                package,
                open,
                private,
                all_features,
            } => execute_build(package, *open, *private, *all_features, ctx),
            DocsSubcommand::Serve { port, build } => execute_serve(*port, *build, ctx),
            DocsSubcommand::Agents { output, stdout } => {
                execute_agents(output.as_deref(), *stdout, ctx)
            }
            DocsSubcommand::Snapshot(cmd) => cmd.execute(ctx).await,
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}

fn execute_build(
    packages: &[String],
    open: bool,
    private: bool,
    all_features: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let mut args = vec!["doc".to_string()];

    if packages.is_empty() {
        args.push("--workspace".to_string());
    } else {
        for pkg in packages {
            args.push("-p".to_string());
            args.push(pkg.clone());
        }
    }

    if open {
        args.push("--open".to_string());
    }

    if private {
        args.push("--document-private-items".to_string());
    }

    if all_features {
        args.push("--all-features".to_string());
    }

    // Exclude test-utils which can cause build issues
    args.push("--exclude".to_string());
    args.push("sinex-test-utils".to_string());

    if ctx.is_human() {
        println!("Building documentation...");
        if packages.is_empty() {
            println!("  Scope: workspace");
        } else {
            println!("  Package(s): {}", packages.join(", "));
        }
        if private {
            println!("  Including private items");
        }
        if all_features {
            println!("  All features enabled");
        }
        println!();
    }

    let stage = ctx.start_stage("doc_build");
    let doc_result = ProcessBuilder::cargo()
        .args(&args)
        .with_description("cargo doc")
        .inherit_output()
        .run_success();
    let doc_ok = doc_result.unwrap_or(false);
    ctx.finish_stage(stage, doc_ok);

    if !doc_ok {
        return Ok(CommandResult::failure(crate::output::StructuredError {
            code: "DOC_BUILD_FAILED".to_string(),
            message: "cargo doc failed".to_string(),
            location: Some("docs::build".to_string()),
            suggestion: Some("Fix doc comment syntax errors (/// or //)".to_string()),
        }));
    }

    let doc_path = if let Some(pkg) = packages.first() {
        // Convert package name: sinex-core -> sinex_core
        let crate_name = pkg.replace('-', "_");
        format!("target/doc/{crate_name}/index.html")
    } else {
        "target/doc/index.html".to_string()
    };

    if ctx.is_human() {
        println!("\nDocumentation built successfully!");
        println!("  Location: {doc_path}");
        if !open {
            println!("  Use --open to view in browser");
        }
    }

    Ok(CommandResult::success()
        .with_message("Documentation built")
        .with_data(serde_json::json!({
            "packages": packages,
            "path": doc_path,
            "private": private,
            "all_features": all_features,
        }))
        .with_duration(ctx.elapsed()))
}

fn execute_serve(port: u16, build_first: bool, ctx: &CommandContext) -> Result<CommandResult> {
    if build_first {
        execute_build(&[], false, false, false, ctx)?;
    }

    let doc_dir = "target/doc";

    // Check if docs exist
    if !std::path::Path::new(doc_dir).exists() {
        return Ok(CommandResult::failure(crate::output::StructuredError {
            code: "DOCS_NOT_FOUND".to_string(),
            message: "Documentation not built yet".to_string(),
            location: Some("docs::serve".to_string()),
            suggestion: Some("Build docs first: xtask docs build".to_string()),
        }));
    }

    if ctx.is_human() {
        println!("Serving documentation at http://localhost:{port}/");
        println!("Press Ctrl+C to stop.\n");
    }

    // Try simple-http-server first
    let http_server_result = Command::new("simple-http-server")
        .args(["-p", &port.to_string(), "-i", doc_dir])
        .status();

    if http_server_result.is_ok_and(|s| s.success()) {
        return Ok(CommandResult::success()
            .with_message("Documentation server stopped")
            .with_duration(ctx.elapsed()));
    }

    // Fall back to Python
    let python_result = Command::new("python3")
        .args(["-m", "http.server", &port.to_string()])
        .current_dir(doc_dir)
        .status();

    if python_result.is_ok_and(|s| s.success()) {
        return Ok(CommandResult::success()
            .with_message("Documentation server stopped")
            .with_duration(ctx.elapsed()));
    }

    // Neither worked
    Ok(CommandResult::failure(crate::output::StructuredError {
        code: "SERVER_NOT_FOUND".to_string(),
        message: "No HTTP server found".to_string(),
        location: Some("docs::serve".to_string()),
        suggestion: Some(
            "Install simple-http-server: cargo install simple-http-server".to_string(),
        ),
    }))
}

/// Recursively resolve `@path` transclusion lines in `content`, reading relative paths
/// from `base_dir`.  Returns the fully expanded text.
///
/// Rules (matching CLAUDE.md transclusion behavior):
/// - Lines that are exactly `@<path>` (with optional trailing whitespace) are replaced by
///   the content of the referenced file, itself recursively expanded.
/// - Relative paths are resolved relative to `base_dir` (the directory of the including file).
/// - Absolute paths starting with `~` expand `$HOME`.
/// - Lines inside code blocks (``` fences) are NOT expanded (the spec says `@` inside
///   code blocks is not evaluated).
/// - Files that cannot be read are replaced by a `<!-- could not read: <path> -->` comment.
/// - Circular references (same file visited twice in a call stack) are skipped.
fn resolve_transclusions(
    content: &str,
    base_dir: &std::path::Path,
    visited: &mut std::collections::HashSet<std::path::PathBuf>,
    depth: usize,
) -> String {
    const MAX_DEPTH: usize = 10;
    if depth > MAX_DEPTH {
        return content.to_string();
    }

    let mut out = String::with_capacity(content.len());
    let mut in_code_block = false;

    for line in content.lines() {
        let trimmed = line.trim();

        // Toggle code-block state on ``` fences
        if trimmed.starts_with("```") {
            in_code_block = !in_code_block;
            out.push_str(line);
            out.push('\n');
            continue;
        }

        // Only expand @-lines outside code blocks
        if !in_code_block && trimmed.starts_with('@') && !trimmed.contains(' ') {
            let raw_path = &trimmed[1..]; // strip leading @

            // Expand ~ to $HOME
            let expanded = if raw_path.starts_with('~') {
                let home = std::env::var("HOME").unwrap_or_else(|_| "~".to_string());
                format!("{home}{}", &raw_path[1..])
            } else {
                raw_path.to_string()
            };

            let path = if std::path::Path::new(&expanded).is_absolute() {
                std::path::PathBuf::from(&expanded)
            } else {
                base_dir.join(&expanded)
            };

            let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());

            if visited.contains(&canonical) {
                out.push_str(&format!("<!-- circular transclusion skipped: {expanded} -->\n"));
                continue;
            }

            match std::fs::read_to_string(&path) {
                Ok(child_content) => {
                    visited.insert(canonical.clone());
                    let child_base = canonical.parent().unwrap_or(&canonical).to_path_buf();
                    let expanded_child =
                        resolve_transclusions(&child_content, &child_base, visited, depth + 1);
                    visited.remove(&canonical);
                    // Append child content (ensure it ends with a newline)
                    out.push_str(&expanded_child);
                    if !expanded_child.ends_with('\n') {
                        out.push('\n');
                    }
                }
                Err(_) => {
                    out.push_str(&format!("<!-- could not read: {expanded} -->\n"));
                }
            }
            continue;
        }

        out.push_str(line);
        out.push('\n');
    }

    out
}

fn execute_agents(
    output: Option<&std::path::Path>,
    to_stdout: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    use color_eyre::eyre::{Context, OptionExt};

    // Locate workspace root (walk up from cwd looking for Cargo.toml with [workspace])
    let workspace = {
        let mut current = std::env::current_dir()?;
        loop {
            let toml = current.join("Cargo.toml");
            if toml.exists() {
                let content = std::fs::read_to_string(&toml).unwrap_or_default();
                if content.contains("[workspace]") {
                    break current;
                }
            }
            if !current.pop() {
                color_eyre::eyre::bail!(
                    "Could not find workspace root (Cargo.toml with [workspace])"
                );
            }
        }
    };

    let claude_md = workspace.join("CLAUDE.md");
    if !claude_md.exists() {
        color_eyre::eyre::bail!("CLAUDE.md not found at {}", claude_md.display());
    }

    let source =
        std::fs::read_to_string(&claude_md).wrap_err("Failed to read CLAUDE.md")?;

    let base_dir = workspace.clone();
    let mut visited = std::collections::HashSet::new();
    visited.insert(claude_md.canonicalize().unwrap_or_else(|_| claude_md.clone()));

    let resolved = resolve_transclusions(&source, &base_dir, &mut visited, 0);

    // Prepend a generation header
    let header = format!(
        "<!-- This file is auto-generated by `xtask docs agents`.\n\
         Generated from CLAUDE.md transclusion tree.\n\
         Do not edit manually — run `xtask docs agents` to regenerate. -->\n\n"
    );
    let output_content = format!("{header}{resolved}");

    if to_stdout {
        print!("{output_content}");
        return Ok(CommandResult::success()
            .with_message("AGENTS.md printed to stdout")
            .with_duration(ctx.elapsed()));
    }

    let dest = output
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| workspace.join("AGENTS.md"));

    std::fs::write(&dest, &output_content)
        .wrap_err_with(|| format!("Failed to write {}", dest.display()))?;

    let byte_count = output_content.len();
    let line_count = output_content.lines().count();

    if ctx.is_human() {
        println!(
            "Generated {} ({line_count} lines, {byte_count} bytes)",
            dest.display()
        );
    }

    Ok(CommandResult::success()
        .with_message("AGENTS.md generated")
        .with_data(serde_json::json!({
            "path": dest.to_string_lossy(),
            "lines": line_count,
            "bytes": byte_count,
        }))
        .with_duration(ctx.elapsed()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_docs_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = DocsCommand {
            subcommand: DocsSubcommand::Build {
                package: vec![],
                open: false,
                private: false,
                all_features: false,
            },
        };

        let metadata = cmd.metadata();
        assert!(metadata.timeout.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn test_docs_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = DocsCommand {
            subcommand: DocsSubcommand::Serve {
                port: 8080,
                build: false,
            },
        };

        assert_eq!(cmd.name(), "docs");
        Ok(())
    }
}
