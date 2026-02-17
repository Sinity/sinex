//! Documentation generation command

use anyhow::{Context, Result};
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Documentation subcommand variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum DocsSubcommand {
    /// Build documentation
    Build {
        /// Build for specific package
        #[arg(short, long)]
        package: Option<String>,

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
}

/// Documentation generation command
#[derive(Debug, Clone, clap::Args)]
pub struct DocsCommand {
    #[command(subcommand)]
    pub subcommand: DocsSubcommand,
}

#[async_trait::async_trait]
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
            } => execute_build(package.as_deref(), *open, *private, *all_features, ctx),
            DocsSubcommand::Serve { port, build } => execute_serve(*port, *build, ctx),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}

fn execute_build(
    package: Option<&str>,
    open: bool,
    private: bool,
    all_features: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let mut args = vec!["doc".to_string()];

    if let Some(pkg) = package {
        args.push("-p".to_string());
        args.push(pkg.to_string());
    } else {
        args.push("--workspace".to_string());
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
        if let Some(pkg) = &package {
            println!("  Package: {pkg}");
        } else {
            println!("  Scope: workspace");
        }
        if private {
            println!("  Including private items");
        }
        if all_features {
            println!("  All features enabled");
        }
        println!();
    }

    let status = Command::new("cargo")
        .args(&args)
        .status()
        .context("Failed to run cargo doc")?;

    if !status.success() {
        return Ok(CommandResult::failure(crate::output::StructuredError {
            code: "DOC_BUILD_FAILED".to_string(),
            message: "cargo doc failed".to_string(),
            location: Some("docs::build".to_string()),
            suggestion: Some("Fix doc comment syntax errors (/// or //)".to_string()),
        }));
    }

    let doc_path = if let Some(pkg) = &package {
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
            "package": package,
            "path": doc_path,
            "private": private,
            "all_features": all_features,
        }))
        .with_duration(ctx.elapsed()))
}

fn execute_serve(port: u16, build_first: bool, ctx: &CommandContext) -> Result<CommandResult> {
    if build_first {
        execute_build(None, false, false, false, ctx)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_docs_command_metadata() {
        let cmd = DocsCommand {
            subcommand: DocsSubcommand::Build {
                package: None,
                open: false,
                private: false,
                all_features: false,
            },
        };

        let metadata = cmd.metadata();
        assert!(metadata.timeout.is_some());
    }

    #[test]
    fn test_docs_command_name() {
        let cmd = DocsCommand {
            subcommand: DocsSubcommand::Serve {
                port: 8080,
                build: false,
            },
        };

        assert_eq!(cmd.name(), "docs");
    }
}
