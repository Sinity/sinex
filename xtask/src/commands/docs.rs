//! Documentation generation command

use crate::command_catalog::collect_command_catalog;
use crate::command_docs::{render_command_guide, render_command_reference};
use crate::config::{ast_grep_catalog_path, ast_grep_rules_dir};
use crate::process::ProcessBuilder;
use crate::proof_catalog::{build_proof_catalog, render_proof_catalog_json};
use color_eyre::eyre::{Context, Result};
use serde::Deserialize;
use sinex_primitives::events::schema_registry::{
    SchemaBundle as PayloadSchemaBundle, SchemaBundleEntry as PayloadSchemaBundleEntry,
    generate_schema_bundle,
};
use std::collections::{BTreeMap, BTreeSet};
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

        /// Exit non-zero if the generated output would change
        #[arg(long, conflicts_with = "stdout")]
        check: bool,
    },

    /// Generate the concise xtask command guide used for agent memory and humans.
    CommandGuide {
        /// Output file (default: xtask/docs/command-guide.md)
        #[arg(long)]
        output: Option<std::path::PathBuf>,

        /// Print to stdout instead of writing a file
        #[arg(long)]
        stdout: bool,

        /// Exit non-zero if the generated output would change
        #[arg(long, conflicts_with = "stdout")]
        check: bool,
    },

    /// Generate the exhaustive xtask public command reference from the live clap tree.
    CommandReference {
        /// Output file (default: xtask/docs/command-reference.md)
        #[arg(long)]
        output: Option<std::path::PathBuf>,

        /// Print to stdout instead of writing a file
        #[arg(long)]
        stdout: bool,

        /// Exit non-zero if the generated output would change
        #[arg(long, conflicts_with = "stdout")]
        check: bool,
    },

    /// Generate the checked-in JSON schema bundle from the Rust EventPayload registry.
    SchemaBundle {
        /// Output directory root (default: schemas/ in workspace root)
        #[arg(long)]
        output_dir: Option<std::path::PathBuf>,

        /// Exit non-zero if the generated bundle would change
        #[arg(long)]
        check: bool,
    },

    /// Generate the proof catalog from Rust descriptors, payloads, commands, and scenarios.
    ProofCatalog {
        /// Output file (default: docs/proof-catalog.json)
        #[arg(long)]
        output: Option<std::path::PathBuf>,

        /// Print to stdout instead of writing a file
        #[arg(long)]
        stdout: bool,

        /// Exit non-zero if the generated output would change
        #[arg(long, conflicts_with = "stdout")]
        check: bool,
    },

    /// Generate the live ast-grep rule catalog from `.config/ast-grep/rules/`.
    AstGrepCatalog {
        /// Output file (default: .config/ast-grep/README.md)
        #[arg(long)]
        output: Option<std::path::PathBuf>,

        /// Print to stdout instead of writing a file
        #[arg(long)]
        stdout: bool,

        /// Exit non-zero if the generated output would change
        #[arg(long, conflicts_with = "stdout")]
        check: bool,
    },

    /// Refresh all generated repo surfaces tracked in the repo.
    Sync,

    /// Verify that all generated repo surfaces are up to date.
    Check,

    /// Generate a codebase snapshot for AI context (via repomix)
    Snapshot(SnapshotCommand),

}

/// Generate and verify repo documentation surfaces.
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
            DocsSubcommand::Agents {
                output,
                stdout,
                check,
            } => execute_agents(output.as_deref(), *stdout, *check, ctx),
            DocsSubcommand::CommandGuide {
                output,
                stdout,
                check,
            } => execute_command_guide(output.as_deref(), *stdout, *check, ctx),
            DocsSubcommand::CommandReference {
                output,
                stdout,
                check,
            } => execute_command_reference(output.as_deref(), *stdout, *check, ctx),
            DocsSubcommand::SchemaBundle { output_dir, check } => {
                execute_schema_bundle(output_dir.as_deref(), *check, ctx)
            }
            DocsSubcommand::ProofCatalog {
                output,
                stdout,
                check,
            } => execute_proof_catalog(output.as_deref(), *stdout, *check, ctx),
            DocsSubcommand::AstGrepCatalog {
                output,
                stdout,
                check,
            } => execute_ast_grep_catalog(output.as_deref(), *stdout, *check, ctx),
            DocsSubcommand::Sync => execute_sync(ctx),
            DocsSubcommand::Check => execute_check(ctx),
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
    let doc_ok = ProcessBuilder::cargo()
        .args(&args)
        .with_description("cargo doc")
        .inherit_output()
        .run_success()
        .context("failed to execute cargo doc")?;
    ctx.finish_stage(stage, doc_ok);

    if !doc_ok {
        return Ok(CommandResult::failure(crate::output::StructuredError {
            code: "DOC_BUILD_FAILED".to_string(),
            message: "cargo doc failed".to_string(),
            location: Some("docs::build".to_string()),
            suggestion: Some("Fix doc comment syntax errors (/// or //)".to_string()),
        }));
    }

    let doc_root = crate::config::workspace_target_dir().join("doc");
    let doc_path = if let Some(pkg) = packages.first() {
        let crate_name = pkg.replace('-', "_");
        doc_root.join(crate_name).join("index.html")
    } else {
        doc_root.join("index.html")
    };

    if ctx.is_human() {
        println!("\nDocumentation built successfully!");
        println!("  Location: {}", doc_path.display());
        if !open {
            println!("  Use --open to view in browser");
        }
    }

    Ok(CommandResult::success()
        .with_message("Documentation built")
        .with_data(serde_json::json!({
            "packages": packages,
            "path": doc_path.display().to_string(),
            "private": private,
            "all_features": all_features,
        }))
        .with_duration(ctx.elapsed()))
}

fn execute_serve(port: u16, build_first: bool, ctx: &CommandContext) -> Result<CommandResult> {
    if build_first {
        execute_build(&[], false, false, false, ctx)?;
    }

    let doc_dir = crate::config::workspace_target_dir().join("doc");

    // Check if docs exist
    if !doc_dir.exists() {
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

    let doc_dir_str = doc_dir.to_string_lossy().into_owned();

    // Try simple-http-server first
    let mut http_server = Command::new("simple-http-server");
    http_server.args(["-p", &port.to_string(), "-i", &doc_dir_str]);
    match run_foreground_docs_server(&mut http_server, "docs serve (simple-http-server)") {
        Ok(status) if crate::process::status_indicates_clean_interactive_shutdown(&status) => {
            return Ok(CommandResult::success()
                .with_message("Documentation server stopped")
                .with_duration(ctx.elapsed()));
        }
        Ok(status) => {
            return Err(color_eyre::eyre::eyre!(
                "simple-http-server exited with status {status}"
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).wrap_err("failed to launch simple-http-server");
        }
    }

    // Fall back to Python
    let mut python = Command::new("python3");
    python
        .args(["-m", "http.server", &port.to_string()])
        .current_dir(&doc_dir);
    match run_foreground_docs_server(&mut python, "docs serve (python)") {
        Ok(status) if crate::process::status_indicates_clean_interactive_shutdown(&status) => {
            return Ok(CommandResult::success()
                .with_message("Documentation server stopped")
                .with_duration(ctx.elapsed()));
        }
        Ok(status) => {
            return Err(color_eyre::eyre::eyre!(
                "python3 -m http.server exited with status {status}"
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).wrap_err("failed to launch python3 http.server");
        }
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

fn run_foreground_docs_server(
    command: &mut Command,
    label: &str,
) -> std::io::Result<std::process::ExitStatus> {
    crate::process::run_managed_foreground_std_command(command, label)
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
            let expanded = if let Some(stripped) = raw_path.strip_prefix('~') {
                let home = std::env::var("HOME").unwrap_or_else(|_| "~".to_string());
                format!("{home}{stripped}")
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
                out.push_str(&format!(
                    "<!-- circular transclusion skipped: {expanded} -->\n"
                ));
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
    check_only: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let workspace = find_workspace_root(std::env::current_dir()?)?;
    let surface = generated_agents_surface(&workspace)?;
    let dest = output.map_or(surface.path, std::path::Path::to_path_buf);
    write_generated_output(
        &dest,
        &surface.content,
        to_stdout,
        check_only,
        surface.label,
        surface.regenerate_command,
        ctx,
    )
}

fn execute_command_guide(
    output: Option<&std::path::Path>,
    to_stdout: bool,
    check_only: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let workspace = find_workspace_root(std::env::current_dir()?)?;
    let surface = generated_command_guide_surface(&workspace);
    let dest = output.map_or(surface.path, std::path::Path::to_path_buf);

    write_generated_output(
        &dest,
        &surface.content,
        to_stdout,
        check_only,
        surface.label,
        surface.regenerate_command,
        ctx,
    )
}

fn execute_command_reference(
    output: Option<&std::path::Path>,
    to_stdout: bool,
    check_only: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let workspace = find_workspace_root(std::env::current_dir()?)?;
    let surface = generated_command_reference_surface(&workspace);
    let dest = output.map_or(surface.path, std::path::Path::to_path_buf);

    write_generated_output(
        &dest,
        &surface.content,
        to_stdout,
        check_only,
        surface.label,
        surface.regenerate_command,
        ctx,
    )
}

fn execute_sync(ctx: &CommandContext) -> Result<CommandResult> {
    let workspace = find_workspace_root(std::env::current_dir()?)?;
    let surfaces = generated_surfaces(&workspace)?;
    let outcomes = sync_generated_surfaces(&surfaces, false, ctx)?;
    let schema_bundle_result = generated_schema_bundle(&workspace, &workspace.join("schemas"))?;
    let schema_bundle = sync_schema_bundle(&schema_bundle_result, false, ctx)?;

    Ok(CommandResult::success()
        .with_message("Generated repo surfaces synchronized")
        .with_data(serde_json::json!({
            "surfaces": outcomes,
            "schema_bundle": schema_bundle,
        }))
        .with_duration(ctx.elapsed()))
}

fn execute_check(ctx: &CommandContext) -> Result<CommandResult> {
    let workspace = find_workspace_root(std::env::current_dir()?)?;
    let surfaces = generated_surfaces(&workspace)?;
    let outcomes = sync_generated_surfaces(&surfaces, true, ctx)?;
    let schema_bundle_result = generated_schema_bundle(&workspace, &workspace.join("schemas"))?;
    let schema_bundle = sync_schema_bundle(&schema_bundle_result, true, ctx)?;
    let changed = outcomes.iter().any(|outcome| outcome.changed) || schema_bundle.changed;

    let result = if changed {
        CommandResult::failure(crate::output::StructuredError {
            code: "GENERATED_SURFACES_STALE".to_string(),
            message: "One or more generated repo surfaces are stale".to_string(),
            location: Some("xtask/docs".to_string()),
            suggestion: Some(
                "Run `xtask docs sync` to regenerate generated repo surfaces".to_string(),
            ),
        })
        .with_message("Generated repo surfaces are stale")
    } else {
        CommandResult::success().with_message("Generated repo surfaces already up to date")
    };

    Ok(result
        .with_data(serde_json::json!({
            "surfaces": outcomes,
            "schema_bundle": schema_bundle,
        }))
        .with_duration(ctx.elapsed()))
}

struct GeneratedSurface {
    label: &'static str,
    path: std::path::PathBuf,
    content: String,
    regenerate_command: &'static str,
}

#[derive(serde::Serialize)]
struct GeneratedSurfaceOutcome {
    label: String,
    path: String,
    lines: usize,
    bytes: usize,
    changed: bool,
}

#[derive(serde::Serialize)]
struct SchemaBundleOutcome {
    root: String,
    files: usize,
    stale_or_changed_files: usize,
    removed_files: usize,
    changed: bool,
    stale_paths: Vec<String>,
}

struct SchemaBundle {
    root: std::path::PathBuf,
    files: BTreeMap<std::path::PathBuf, String>,
}

#[derive(serde::Serialize)]
struct SchemaBundleRegistry {
    version: String,
    entries: Vec<SchemaBundleRegistryEntry>,
}

#[derive(serde::Serialize)]
struct SchemaBundleRegistryEntry {
    source: String,
    event_type: String,
    version: String,
    path: String,
    content_hash: String,
}

fn generated_surfaces(workspace: &std::path::Path) -> Result<Vec<GeneratedSurface>> {
    Ok(vec![
        generated_ast_grep_catalog_surface(workspace)?,
        generated_proof_catalog_surface(workspace)?,
        generated_command_guide_surface(workspace),
        generated_command_reference_surface(workspace),
        generated_agents_surface(workspace)?,
    ])
}

fn generated_proof_catalog_surface(workspace: &std::path::Path) -> Result<GeneratedSurface> {
    let catalog = build_proof_catalog(workspace)?;
    Ok(GeneratedSurface {
        label: "proof catalog",
        path: workspace.join("docs/proof-catalog.json"),
        content: render_proof_catalog_json(&catalog)?,
        regenerate_command: "xtask docs proof-catalog",
    })
}

fn generated_ast_grep_catalog_surface(_workspace: &std::path::Path) -> Result<GeneratedSurface> {
    let rules_dir = ast_grep_rules_dir();
    let rules = load_ast_grep_rules(&rules_dir)?;
    Ok(GeneratedSurface {
        label: "ast-grep rule catalog",
        path: ast_grep_catalog_path(),
        content: render_ast_grep_catalog(&rules),
        regenerate_command: "xtask docs ast-grep-catalog",
    })
}

fn generated_agents_surface(workspace: &std::path::Path) -> Result<GeneratedSurface> {
    let claude_md = workspace.join("CLAUDE.md");
    if !claude_md.exists() {
        color_eyre::eyre::bail!("CLAUDE.md not found at {}", claude_md.display());
    }

    let source = std::fs::read_to_string(&claude_md).wrap_err("Failed to read CLAUDE.md")?;

    let mut visited = std::collections::HashSet::new();
    visited.insert(
        claude_md
            .canonicalize()
            .unwrap_or_else(|_| claude_md.clone()),
    );

    let resolved = resolve_transclusions(&source, workspace, &mut visited, 0);
    let header = "<!-- This file is auto-generated by `xtask docs agents`.\nGenerated from CLAUDE.md transclusion tree.\nDo not edit manually — run `xtask docs agents` to regenerate. -->\n\n";

    Ok(GeneratedSurface {
        label: "AGENTS.md",
        path: workspace.join("AGENTS.md"),
        content: format!("{header}{resolved}"),
        regenerate_command: "xtask docs agents",
    })
}

fn generated_command_guide_surface(workspace: &std::path::Path) -> GeneratedSurface {
    let commands = collect_command_catalog();
    GeneratedSurface {
        label: "xtask command guide",
        path: workspace.join("xtask/docs/command-guide.md"),
        content: render_command_guide(&commands),
        regenerate_command: "xtask docs command-guide",
    }
}

fn generated_command_reference_surface(workspace: &std::path::Path) -> GeneratedSurface {
    let commands = collect_command_catalog();
    GeneratedSurface {
        label: "xtask command reference",
        path: workspace.join("xtask/docs/command-reference.md"),
        content: render_command_reference(&commands),
        regenerate_command: "xtask docs command-reference",
    }
}

fn execute_ast_grep_catalog(
    output: Option<&std::path::Path>,
    to_stdout: bool,
    check_only: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let workspace = find_workspace_root(std::env::current_dir()?)?;
    let surface = generated_ast_grep_catalog_surface(&workspace)?;
    let dest = output.map_or(surface.path, std::path::Path::to_path_buf);

    write_generated_output(
        &dest,
        &surface.content,
        to_stdout,
        check_only,
        surface.label,
        surface.regenerate_command,
        ctx,
    )
}

fn execute_proof_catalog(
    output: Option<&std::path::Path>,
    to_stdout: bool,
    check_only: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let workspace = find_workspace_root(std::env::current_dir()?)?;
    let surface = generated_proof_catalog_surface(&workspace)?;
    let dest = output.map_or(surface.path, std::path::Path::to_path_buf);

    write_generated_output(
        &dest,
        &surface.content,
        to_stdout,
        check_only,
        surface.label,
        surface.regenerate_command,
        ctx,
    )
}

fn execute_schema_bundle(
    output_dir: Option<&std::path::Path>,
    check_only: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let workspace = find_workspace_root(std::env::current_dir()?)?;
    let root = output_dir.map_or_else(|| workspace.join("schemas"), std::path::Path::to_path_buf);
    let schema_bundle_result = generated_schema_bundle(&workspace, &root)?;
    let outcome = sync_schema_bundle(&schema_bundle_result, check_only, ctx)?;

    let result = if check_only && outcome.changed {
        CommandResult::failure(crate::output::StructuredError {
            code: "SCHEMA_BUNDLE_STALE".to_string(),
            message: "Schema bundle is stale or missing".to_string(),
            location: Some(root.display().to_string()),
            suggestion: Some("Run `xtask docs schema-bundle` to regenerate the bundle".to_string()),
        })
    } else {
        CommandResult::success()
    };

    let message = if check_only {
        if outcome.changed {
            "Schema bundle is stale".to_string()
        } else {
            "Schema bundle already up to date".to_string()
        }
    } else if outcome.changed {
        "Schema bundle synchronized".to_string()
    } else {
        "Schema bundle already up to date".to_string()
    };

    Ok(result
        .with_message(message)
        .with_data(serde_json::json!(outcome))
        .with_duration(ctx.elapsed()))
}

fn generated_schema_bundle(
    _workspace: &std::path::Path,
    root: &std::path::Path,
) -> Result<SchemaBundle> {
    let mut files = BTreeMap::new();
    let mut seen_paths = BTreeMap::<(u64, String, String), String>::new();
    let mut registries = BTreeMap::<u64, Vec<SchemaBundleRegistryEntry>>::new();

    let payload_bundle = generate_schema_bundle()
        .context("failed to generate shared event payload schema bundle")?;
    populate_schema_bundle_files(
        &payload_bundle,
        root,
        &mut files,
        &mut registries,
        &mut seen_paths,
    )?;

    for (major, entries) in registries {
        let registry = SchemaBundleRegistry {
            version: format!("v{major}"),
            entries,
        };
        let registry_content = serde_json::to_string_pretty(&registry)
            .context("failed to render schema bundle registry")?
            + "\n";
        files.insert(
            root.join(format!("v{major}/registry.json")),
            registry_content,
        );
    }

    Ok(SchemaBundle {
        root: root.to_path_buf(),
        files,
    })
}

fn populate_schema_bundle_files(
    payload_bundle: &PayloadSchemaBundle,
    root: &std::path::Path,
    files: &mut BTreeMap<std::path::PathBuf, String>,
    registries: &mut BTreeMap<u64, Vec<SchemaBundleRegistryEntry>>,
    seen_paths: &mut BTreeMap<(u64, String, String), String>,
) -> Result<()> {
    for entry in payload_bundle.entries() {
        populate_schema_bundle_entry(files, registries, seen_paths, root, entry)?;
    }
    Ok(())
}

fn populate_schema_bundle_entry(
    files: &mut BTreeMap<std::path::PathBuf, String>,
    registries: &mut BTreeMap<u64, Vec<SchemaBundleRegistryEntry>>,
    seen_paths: &mut BTreeMap<(u64, String, String), String>,
    root: &std::path::Path,
    entry: &PayloadSchemaBundleEntry,
) -> Result<()> {
    let major = entry
        .major_version()
        .with_context(|| format!("invalid schema version for {}/{}", entry.source, entry.event_type))?;
    let path_key = (major, entry.source.clone(), entry.event_type.clone());
    if let Some(existing_version) = seen_paths.insert(path_key, entry.version.clone())
        && existing_version != entry.version
    {
        color_eyre::eyre::bail!(
            "schema bundle path collision for {}/{} in v{}: {} and {}",
            entry.source,
            entry.event_type,
            major,
            existing_version,
            entry.version
        );
    }

    let schema_content = serde_json::to_string_pretty(&entry.schema_content)
        .context("failed to render schema bundle JSON")?
        + "\n";
    files.insert(root.join(entry.bundle_relative_path()?), schema_content);
    registries
        .entry(major)
        .or_default()
        .push(SchemaBundleRegistryEntry {
            source: entry.source.clone(),
            event_type: entry.event_type.clone(),
            version: entry.version.clone(),
            path: entry.registry_path(),
            content_hash: entry.content_hash.clone(),
        });

    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
struct AstGrepRuleCatalogEntry {
    id: String,
    message: String,
    severity: String,
    language: Option<String>,
    note: Option<String>,
    ignores: Option<Vec<String>>,
}

fn load_ast_grep_rules(rules_dir: &std::path::Path) -> Result<Vec<AstGrepRuleCatalogEntry>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(rules_dir)
        .wrap_err_with(|| format!("Failed to read {}", rules_dir.display()))?
    {
        let entry =
            entry.wrap_err_with(|| format!("Failed to enumerate {}", rules_dir.display()))?;
        let path = entry.path();
        let is_yaml = matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("yml" | "yaml")
        );
        if is_yaml && path.is_file() {
            paths.push(path);
        }
    }
    paths.sort();

    let mut rules = Vec::with_capacity(paths.len());
    for path in paths {
        let content = std::fs::read_to_string(&path)
            .wrap_err_with(|| format!("Failed to read {}", path.display()))?;
        let mut rule: AstGrepRuleCatalogEntry = serde_yml::from_str(&content)
            .wrap_err_with(|| format!("Failed to parse {}", path.display()))?;
        rule.ignores.get_or_insert_with(Vec::new).sort();
        rules.push(rule);
    }

    rules.sort_by(|left, right| {
        severity_rank(&left.severity)
            .cmp(&severity_rank(&right.severity))
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(rules)
}

fn severity_rank(severity: &str) -> u8 {
    match severity {
        "error" => 0,
        "warning" => 1,
        "hint" => 2,
        _ => 3,
    }
}

fn render_ast_grep_catalog(rules: &[AstGrepRuleCatalogEntry]) -> String {
    let mut output = String::new();
    output.push_str("# ast-grep Rule Catalog\n\n");
    output.push_str("Generated from `.config/ast-grep/rules/*.yml`.\n\n");
    output.push_str("Config file: `.config/ast-grep/sgconfig.yml`\n");
    output.push_str("Manual scan: `ast-grep scan --config .config/ast-grep/sgconfig.yml .`\n\n");
    output.push_str("Use `xtask check --forbidden` for the public local enforcement surface.\n");
    output.push_str(
        "Within xtask automation, `error` severity is blocking; `warning` and `hint` remain advisory.\n\n",
    );
    output.push_str("## Rules\n\n");
    output.push_str("| ID | Severity | Language | Message |\n");
    output.push_str("| --- | --- | --- | --- |\n");
    for rule in rules {
        output.push_str(&format!(
            "| `{}` | `{}` | `{}` | {} |\n",
            rule.id,
            rule.severity,
            rule.language.as_deref().unwrap_or(""),
            rule.message
        ));
    }
    output.push('\n');

    for rule in rules {
        output.push_str(&format!("## `{}`\n\n", rule.id));
        output.push_str(&format!("- Severity: `{}`\n", rule.severity));
        if let Some(language) = &rule.language {
            output.push_str(&format!("- Language: `{language}`\n"));
        }
        output.push_str(&format!("- Message: {}\n", rule.message));
        let ignores = rule.ignores.as_deref().unwrap_or(&[]);
        if !ignores.is_empty() {
            output.push_str("- Ignore globs:\n");
            for ignore in ignores {
                output.push_str(&format!("  - `{ignore}`\n"));
            }
        }
        if let Some(note) = &rule.note {
            output.push_str("- Intent:\n");
            for line in note.trim().lines() {
                output.push_str(&format!("  {}\n", line.trim_end()));
            }
        }
        output.push('\n');
    }

    output
}

fn sync_generated_surfaces(
    surfaces: &[GeneratedSurface],
    check_only: bool,
    ctx: &CommandContext,
) -> Result<Vec<GeneratedSurfaceOutcome>> {
    let mut outcomes = Vec::with_capacity(surfaces.len());
    for surface in surfaces {
        let status = sync_generated_surface(
            &surface.path,
            &surface.content,
            check_only,
            surface.label,
            ctx,
        )?;
        outcomes.push(status);
    }
    Ok(outcomes)
}

fn sync_schema_bundle(
    bundle: &SchemaBundle,
    check_only: bool,
    ctx: &CommandContext,
) -> Result<SchemaBundleOutcome> {
    let existing = discover_existing_schema_bundle_files(&bundle.root)?;
    let desired: BTreeSet<_> = bundle.files.keys().cloned().collect();
    let stale_paths: Vec<_> = existing.difference(&desired).cloned().collect();

    let mut stale_or_changed = stale_paths.len();
    let mut changed = !stale_paths.is_empty();

    for (path, content) in &bundle.files {
        let current = std::fs::read_to_string(path).ok();
        if current.as_deref() != Some(content.as_str()) {
            stale_or_changed += 1;
            changed = true;
            if !check_only {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)
                        .wrap_err_with(|| format!("Failed to create {}", parent.display()))?;
                }
                std::fs::write(path, content)
                    .wrap_err_with(|| format!("Failed to write {}", path.display()))?;
            }
        }
    }

    if !check_only {
        for path in &stale_paths {
            if path.exists() {
                std::fs::remove_file(path)
                    .wrap_err_with(|| format!("Failed to remove stale {}", path.display()))?;
                prune_empty_schema_dirs(&bundle.root, path.parent());
            }
        }
    }

    if ctx.is_human() {
        if check_only {
            if changed {
                eprintln!(
                    "Schema bundle under {} is stale or missing ({} affected files)",
                    bundle.root.display(),
                    stale_or_changed
                );
            } else {
                println!(
                    "Schema bundle under {} already up to date ({} files)",
                    bundle.root.display(),
                    bundle.files.len()
                );
            }
        } else if changed {
            println!(
                "Synchronized schema bundle under {} ({} files, {} stale/changed, {} removed)",
                bundle.root.display(),
                bundle.files.len(),
                stale_or_changed,
                stale_paths.len()
            );
        } else {
            println!(
                "Schema bundle under {} already up to date ({} files)",
                bundle.root.display(),
                bundle.files.len()
            );
        }
    }

    Ok(SchemaBundleOutcome {
        root: bundle.root.display().to_string(),
        files: bundle.files.len(),
        stale_or_changed_files: stale_or_changed,
        removed_files: stale_paths.len(),
        changed,
        stale_paths: stale_paths
            .into_iter()
            .map(|path| path.display().to_string())
            .collect(),
    })
}

fn discover_existing_schema_bundle_files(
    root: &std::path::Path,
) -> Result<BTreeSet<std::path::PathBuf>> {
    let mut files = BTreeSet::new();
    if !root.exists() {
        return Ok(files);
    }

    for entry in walkdir::WalkDir::new(root) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.into_path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        if name == "README.md" {
            continue;
        }
        let Ok(relative) = path.strip_prefix(root) else {
            continue;
        };
        let mut components = relative.components();
        let Some(first) = components.next() else {
            continue;
        };
        if first.as_os_str().to_string_lossy().starts_with('v') {
            files.insert(path);
        }
    }

    Ok(files)
}

fn prune_empty_schema_dirs(root: &std::path::Path, start: Option<&std::path::Path>) {
    let mut current = start.map(std::path::Path::to_path_buf);
    while let Some(dir) = current {
        if dir == root {
            break;
        }
        let is_empty = std::fs::read_dir(&dir)
            .ok()
            .and_then(|mut entries| entries.next().transpose().ok())
            .flatten()
            .is_none();
        if !is_empty {
            break;
        }
        let parent = dir.parent().map(std::path::Path::to_path_buf);
        let _ = std::fs::remove_dir(&dir);
        current = parent;
    }
}

fn write_generated_output(
    dest: &std::path::Path,
    content: &str,
    to_stdout: bool,
    check_only: bool,
    label: &str,
    regenerate_command: &str,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    if to_stdout {
        print!("{content}");
        return Ok(CommandResult::success()
            .with_message(format!("{label} printed to stdout"))
            .with_duration(ctx.elapsed()));
    }

    let outcome = sync_generated_surface(dest, content, check_only, label, ctx)?;

    let message = if check_only {
        if outcome.changed {
            format!("{label} is stale")
        } else {
            format!("{label} already up to date")
        }
    } else if outcome.changed {
        format!("{label} generated")
    } else {
        format!("{label} already up to date")
    };

    let result = if check_only && outcome.changed {
        CommandResult::failure(crate::output::StructuredError {
            code: "GENERATED_DOCS_STALE".to_string(),
            message: format!("{label} is stale or missing"),
            location: Some(dest.display().to_string()),
            suggestion: Some(format!("Run `{regenerate_command}` to regenerate")),
        })
    } else {
        CommandResult::success()
    };

    Ok(result
        .with_message(message)
        .with_data(serde_json::json!({
            "path": outcome.path,
            "lines": outcome.lines,
            "bytes": outcome.bytes,
            "changed": outcome.changed,
        }))
        .with_duration(ctx.elapsed()))
}

fn sync_generated_surface(
    dest: &std::path::Path,
    content: &str,
    check_only: bool,
    label: &str,
    ctx: &CommandContext,
) -> Result<GeneratedSurfaceOutcome> {
    let existing = std::fs::read_to_string(dest).ok();
    let changed = existing.as_deref() != Some(content);
    let byte_count = content.len();
    let line_count = content.lines().count();

    if check_only {
        if ctx.is_human() {
            if changed {
                eprintln!(
                    "{} is stale or missing ({line_count} lines, {byte_count} bytes expected)",
                    dest.display()
                );
            } else {
                println!(
                    "{} already up to date ({line_count} lines, {byte_count} bytes)",
                    dest.display()
                );
            }
        }

        return Ok(GeneratedSurfaceOutcome {
            label: label.to_string(),
            path: dest.to_string_lossy().into_owned(),
            lines: line_count,
            bytes: byte_count,
            changed,
        });
    }

    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .wrap_err_with(|| format!("Failed to create {}", parent.display()))?;
    }
    if changed {
        std::fs::write(dest, content)
            .wrap_err_with(|| format!("Failed to write {}", dest.display()))?;
    }

    if ctx.is_human() {
        if changed {
            println!(
                "Generated {} ({line_count} lines, {byte_count} bytes)",
                dest.display()
            );
        } else {
            println!(
                "{} already up to date ({line_count} lines, {byte_count} bytes)",
                dest.display()
            );
        }
    }

    Ok(GeneratedSurfaceOutcome {
        label: label.to_string(),
        path: dest.to_string_lossy().into_owned(),
        lines: line_count,
        bytes: byte_count,
        changed,
    })
}

fn find_workspace_root(mut current: std::path::PathBuf) -> Result<std::path::PathBuf> {
    loop {
        let toml = current.join("Cargo.toml");
        if toml.exists() {
            let content = std::fs::read_to_string(&toml).wrap_err_with(|| {
                format!("Failed to read workspace manifest {}", toml.display())
            })?;
            if content.contains("[workspace]") {
                return Ok(current);
            }
        }
        if !current.pop() {
            color_eyre::eyre::bail!("Could not find workspace root (Cargo.toml with [workspace])");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_catalog::{ArgInfo, CommandInfo};
    use crate::command_docs::{render_command_guide, render_command_reference};
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

    #[sinex_test]
    async fn test_find_workspace_root_reports_manifest_read_failures()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let manifest = temp.path().join("Cargo.toml");
        std::fs::create_dir(&manifest)?;

        let error = find_workspace_root(temp.path().to_path_buf()).unwrap_err();
        assert!(format!("{error:#}").contains("Failed to read workspace manifest"));
        Ok(())
    }

    #[sinex_test]
    async fn test_find_workspace_root_finds_workspace_manifest() -> ::xtask::sandbox::TestResult<()>
    {
        let temp = tempfile::tempdir()?;
        std::fs::write(
            temp.path().join("Cargo.toml"),
            "[workspace]\nmembers = []\n",
        )?;
        let nested = temp.path().join("nested/child");
        std::fs::create_dir_all(&nested)?;

        let workspace = find_workspace_root(nested)?;
        assert_eq!(workspace, temp.path());
        Ok(())
    }

    #[test]
    fn test_render_command_reference_renders_nested_sections() {
        let rendered = render_command_reference(&[CommandInfo {
            name: "check".to_string(),
            about: Some("Compile verification".to_string()),
            args: vec![ArgInfo {
                name: "package".to_string(),
                short: Some('p'),
                long: Some("package".to_string()),
                help: Some("Check specific package(s) only".to_string()),
                required: false,
                global: false,
                possible_values: vec![],
                takes_value: true,
            }],
            subcommands: vec![CommandInfo {
                name: "deep".to_string(),
                about: Some("Nested sample".to_string()),
                args: vec![],
                subcommands: vec![],
            }],
        }]);

        assert!(rendered.contains("# xtask Command Reference"));
        assert!(rendered.contains("## `xtask check`"));
        assert!(
            rendered.contains("| `-p, --package` | yes | no | Check specific package(s) only |")
        );
        assert!(rendered.contains("### `xtask check deep`"));
    }

    #[test]
    fn test_render_command_guide_renders_curated_sections() {
        let rendered = render_command_guide(&crate::command_catalog::collect_command_catalog());

        assert!(rendered.contains("# xtask Command Guide"));
        assert!(rendered.contains("## Agent Defaults"));
        assert!(rendered.contains("`xtask check`"));
    }

    #[test]
    fn test_schema_bundle_major_version_parses_semver() {
        assert_eq!(
            sinex_primitives::events::schema_registry::schema_bundle_major_version("1.0.0")
                .unwrap(),
            1
        );
        assert_eq!(
            sinex_primitives::events::schema_registry::schema_bundle_major_version("1").unwrap(),
            1
        );
        assert!(
            sinex_primitives::events::schema_registry::schema_bundle_major_version("").is_err()
        );
        assert!(
            sinex_primitives::events::schema_registry::schema_bundle_major_version("x.0.0")
                .is_err()
        );
    }

    #[test]
    fn test_schema_bundle_content_hash_matches_registry_contract() {
        let schema = serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "properties": {
                "created_at": {
                    "format": "date-time",
                    "type": "string"
                },
                "path": {
                    "type": "string"
                },
                "permissions": {
                    "format": "uint32",
                    "minimum": 0.0,
                    "type": ["integer", "null"]
                },
                "size": {
                    "format": "uint64",
                    "minimum": 0.0,
                    "type": "integer"
                }
            },
            "required": ["created_at", "path", "size"],
            "title": "FileCreatedPayload",
            "type": "object"
        });
        assert_eq!(
            sinex_primitives::events::schema_registry::calculate_schema_content_hash(
                "fs-watcher",
                "file.created",
                "1.0.0",
                &schema
            )
            .unwrap(),
            "dfed8161f597e83e0efaff7ed7efb56ea960fc51c00bb401bc06c154220dcaed"
        );
    }

    #[test]
    fn test_render_ast_grep_catalog_renders_rule_details() {
        let rendered = render_ast_grep_catalog(&[
            AstGrepRuleCatalogEntry {
                id: "cargo-command-outside-process".to_string(),
                message: "Keep cargo spawning centralized".to_string(),
                severity: "error".to_string(),
                language: Some("rust".to_string()),
                note: Some("Use xtask::process helpers.".to_string()),
                ignores: Some(vec!["xtask/src/process.rs".to_string()]),
            },
            AstGrepRuleCatalogEntry {
                id: "string-from-literal".to_string(),
                message: "Prefer .to_string() or .into()".to_string(),
                severity: "hint".to_string(),
                language: Some("rust".to_string()),
                note: None,
                ignores: None,
            },
        ]);

        assert!(rendered.contains("# ast-grep Rule Catalog"));
        assert!(rendered.contains("`cargo-command-outside-process`"));
        assert!(rendered.contains("Within xtask automation, `error` severity is blocking"));
        assert!(rendered.contains("`xtask/src/process.rs`"));
    }
}
