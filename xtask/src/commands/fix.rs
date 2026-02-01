use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;
use anyhow::Result;

#[derive(Debug, Clone, clap::Args)]
pub struct FixCommand {
    /// Packages to fix (default: all)
    #[arg(short, long)]
    pub package: Vec<String>,
    /// Only fix affected packages
    #[arg(short, long)]
    pub affected: bool,
}

impl XtaskCommand for FixCommand {
    fn name(&self) -> &str {
        "fix"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Handle background execution
        if ctx.is_background() {
            let mut args = Vec::new();
            for p in &self.package {
                args.push("-p".to_string());
                args.push(p.clone());
            }
            if self.affected {
                args.push("--affected".to_string());
            }
            return ctx.spawn_background("fix", &args);
        }

        if ctx.is_human() {
            println!("Applying automatic fixes...");
        }

        // 1. cargo fmt
        println!("Running cargo fmt...");
        let mut fmt = ProcessBuilder::cargo().arg("fmt");
        for p in &self.package {
            fmt = fmt.arg("-p").arg(p);
        }
        fmt.run_ok()?;

        // 2. cargo fix --allow-dirty
        println!("Running cargo fix...");
        let mut fix = ProcessBuilder::cargo();
        fix = fix.arg("fix").arg("--allow-dirty").arg("--allow-staged");
        for p in &self.package {
            fix = fix.arg("-p").arg(p);
        }
        fix.run_ok()?;

        // 3. cargo clippy --fix --allow-dirty
        println!("Running clippy --fix...");
        let mut clippy = ProcessBuilder::cargo();
        clippy = clippy
            .arg("clippy")
            .arg("--fix")
            .arg("--allow-dirty")
            .arg("--allow-staged");
        for p in &self.package {
            clippy = clippy.arg("-p").arg(p);
        }
        clippy.run_ok()?;

        Ok(CommandResult::success().with_detail("fixes applied"))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build() // Category build for now
    }
}
