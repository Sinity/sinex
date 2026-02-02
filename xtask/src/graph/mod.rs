//! Dependency graph visualization and analysis

use anyhow::Context;
use anyhow::Result;
use clap::Subcommand;

pub mod impact;
pub mod render;
pub mod workspace;

pub use render::Renderer;
pub use workspace::WorkspaceGraph;

#[derive(Debug, Clone, Subcommand)]
pub enum GraphCommand {
    /// Visualize dependency graph
    Deps {
        /// Output format (dot, json, ascii)
        #[arg(long, default_value = "ascii")]
        render_format: String,

        /// Focus on specific package
        #[arg(long)]
        focus: Option<String>,

        /// Show reverse dependencies
        #[arg(long)]
        reverse: bool,

        /// Maximum depth
        #[arg(long, default_value = "10")]
        depth: usize,

        /// Output file (if not specified, writes to stdout)
        #[arg(short, long)]
        output: Option<String>,
    },
}

impl GraphCommand {
    /// Execute the graph command
    pub fn run(
        &self,
        ctx: &crate::command::CommandContext,
    ) -> Result<crate::command::CommandResult> {
        use crate::command::CommandResult;
        use crate::graph::render::Renderer;

        match self {
            Self::Deps {
                render_format,
                focus,
                reverse,
                depth,
                output,
            } => {
                // Load the workspace graph
                let graph = WorkspaceGraph::new()?;

                // Render in the requested format
                let rendered = match render_format.as_str() {
                    "dot" => {
                        let mut renderer = render::DotRenderer::new(graph);
                        if let Some(focus_pkg) = focus {
                            renderer = renderer.with_focus(focus_pkg.clone(), *reverse);
                        }
                        renderer.render()?
                    }
                    "json" => {
                        let renderer = render::JsonRenderer::new(graph);
                        renderer.render()?
                    }
                    _ => {
                        let renderer = render::AsciiRenderer::new(&graph, focus.clone(), *depth);
                        renderer.render()?
                    }
                };

                // Output to file or return data
                if let Some(output_path) = output {
                    std::fs::write(output_path, &rendered)
                        .with_context(|| format!("Failed to write to {output_path}"))?;

                    Ok(CommandResult::success()
                        .with_message(format!("Graph written to {output_path}"))
                        .with_duration(ctx.elapsed()))
                } else if render_format == "json" {
                    // For JSON, we want the raw data if it's JSON
                    let json_data: serde_json::Value = serde_json::from_str(&rendered)?;
                    Ok(CommandResult::success()
                        .with_data(json_data)
                        .with_silent()
                        .with_duration(ctx.elapsed()))
                } else {
                    Ok(CommandResult::success()
                        .with_data(serde_json::Value::String(rendered))
                        .with_silent()
                        .with_duration(ctx.elapsed()))
                }
            }
        }
    }
}
