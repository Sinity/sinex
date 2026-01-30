use crate::affected;
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;
use anyhow::Result;

#[derive(Debug, Clone, clap::Args)]
pub struct BuildCommand {
    /// Packages to build (default: all)
    #[arg(short, long)]
    pub package: Vec<String>,
    /// Build in release mode
    #[arg(short, long)]
    pub release: bool,
    /// Only build affected packages
    #[arg(short, long)]
    pub affected: bool,
}

impl XtaskCommand for BuildCommand {
    fn name(&self) -> &str {
        "build"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let mut cargo = ProcessBuilder::cargo().arg("build");

        if self.release {
            cargo = cargo.arg("--release");
        }

        let mut packages = self.package.clone();

        if self.affected {
            let affected = affected::affected_packages()?;
            if affected.is_empty() {
                if ctx.is_human() {
                    println!("No packages affected by current changes.");
                }
                return Ok(CommandResult::success());
            }
            packages.extend(affected);
        }

        if packages.is_empty() {
            cargo = cargo.arg("--workspace");
        } else {
            for p in &packages {
                cargo = cargo.arg("-p").arg(p);
            }
        }

        if ctx.is_human() {
            println!("Building packages...");
        }

        cargo.inherit_output().run_ok()?;

        Ok(CommandResult::success())
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}
