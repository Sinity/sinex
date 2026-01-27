//! Schema management commands - generate, deploy, compatibility checks

use anyhow::{bail, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::NamedTempFile;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;

/// Schema command variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum SchemaSubcommand {
    Generate {
        #[arg(short, long, default_value = "schemas/v1")]
        output: String,
        #[arg(short, long)]
        sync: bool,
    },
    Deploy {
        #[arg(short, long, default_value = "schemas/v1")]
        input: String,
        #[arg(long)]
        database_url: String,
    },
    Compat {
        #[arg(long)]
        base: Option<String>,
        #[arg(long, default_value = "schemas/v1")]
        glob: String,
    },
    CheckReady {
        #[arg(long)]
        database: Option<String>,
        #[arg(long)]
        superuser: Option<String>,
    },
}

/// Schema management command
pub struct SchemaCommand {
    pub subcommand: SchemaSubcommand,
}

impl XtaskCommand for SchemaCommand {
    fn name(&self) -> &str {
        "schema"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            SchemaSubcommand::Generate { output, sync } => execute_generate(output, *sync, ctx),
            SchemaSubcommand::Deploy {
                input,
                database_url,
            } => execute_deploy(input, database_url, ctx),
            SchemaSubcommand::Compat { base, glob } => execute_compat(base.clone(), glob, ctx),
            SchemaSubcommand::CheckReady {
                database,
                superuser,
            } => execute_check_ready(database.clone(), superuser.clone(), ctx),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::database()
    }
}

fn execute_generate(output: &str, sync: bool, ctx: &CommandContext) -> Result<CommandResult> {
    let mut args = vec!["generate", "--output", output];
    if sync {
        args.push("--sync");
    }

    let mut cmd = sinex_schema_cmd();
    cmd.args(&args);

    if ctx.is_human() {
        println!("========== schema generate ==========");
    }

    let status = cmd
        .status()
        .with_context(|| "failed to spawn schema generate")?;

    if !status.success() {
        bail!("schema generate failed with status {}", status);
    }

    Ok(CommandResult::success()
        .with_message(format!("Schemas generated in {}", output))
        .with_duration(ctx.elapsed()))
}

fn execute_deploy(input: &str, database_url: &str, ctx: &CommandContext) -> Result<CommandResult> {
    let db_url = database_url.trim();
    if db_url.is_empty() {
        bail!("DATABASE_URL is required for schema deploy (use --database-url or env)");
    }

    ensure_psql()?;
    ensure_db_connection(db_url)?;

    // Check for required extensions
    let required_exts = ["pg_jsonschema", "pgx_ulid", "timescaledb", "vector"];
    let mut missing = Vec::new();
    for ext in required_exts {
        if !psql_query_bool(
            db_url,
            &format!("SELECT 1 FROM pg_extension WHERE extname='{ext}'"),
        )? {
            missing.push(ext);
        }
    }
    if !missing.is_empty() {
        bail!(
            "Missing extensions in target database: {}",
            missing.join(", ")
        );
    }

    let mut cmd = sinex_schema_cmd();
    cmd.arg("sync").arg("--input").arg(input);

    if ctx.is_human() {
        println!("========== schema deploy ==========");
    }

    let status = cmd
        .status()
        .with_context(|| "failed to spawn schema deploy")?;

    if !status.success() {
        bail!("schema deploy failed with status {}", status);
    }

    Ok(CommandResult::success()
        .with_message(format!("Schemas deployed from {}", input))
        .with_duration(ctx.elapsed()))
}

fn execute_compat(base: Option<String>, glob: &str, ctx: &CommandContext) -> Result<CommandResult> {
    // CI sometimes passes an empty base ref on branch pushes; treat that as "unspecified"
    let base_branch = base
        .or_else(|| std::env::var("CI_BASE_BRANCH").ok())
        .filter(|s| !s.trim().is_empty());

    let base = match base_branch {
        Some(b) => b,
        None => resolve_default_base_branch()?,
    };

    let diff_output = ProcessBuilder::git()
        .args(&["diff", "--name-only", &format!("{base}...HEAD"), "--", glob])
        .with_description("git diff for schema compat")
        .run()?;

    // git diff can return 0 or 1 (for changes found)
    if diff_output.exit_code != 0 && diff_output.exit_code != 1 {
        bail!("git diff failed with status {}", diff_output.exit_code);
    }

    let changed = diff_output.stdout.trim();
    if changed.is_empty() {
        if ctx.is_human() {
            println!("✅ No schema edits detected");
        }
        return Ok(CommandResult::success()
            .with_message("No schema changes detected")
            .with_duration(ctx.elapsed()));
    }

    if ctx.is_human() {
        println!("🔍 Checking compatibility for updated schemas against {base}:");
        println!("{changed}");
    }

    let mut errors = 0;
    let mut checked = Vec::new();
    let mut skipped = Vec::new();

    for file in changed.lines().filter(|l| !l.trim().is_empty()) {
        let path = Path::new(file);
        if !path.exists() {
            if ctx.is_human() {
                println!("⚠️  Skipping deleted schema {file}");
            }
            skipped.push(format!("{} (deleted)", file));
            continue;
        }

        let git_obj = format!("{base}:{file}");
        let cat_file = Command::new("git")
            .arg("cat-file")
            .arg("-e")
            .arg(&git_obj)
            .status()
            .unwrap_or_else(|_| Command::new("false").status().unwrap());
        if !cat_file.success() {
            if ctx.is_human() {
                println!("➕ New schema {file} (no backward check required)");
            }
            skipped.push(format!("{} (new)", file));
            continue;
        }

        let tmp = NamedTempFile::new()?;
        let old_contents = ProcessBuilder::git()
            .args(&["show", &git_obj])
            .with_description(&format!("reading {}", git_obj))
            .run()?;

        fs::write(tmp.path(), old_contents.stdout.as_bytes())?;

        if ctx.is_human() {
            println!("Comparing {file} against {base}...");
        }

        let mut cmd = sinex_schema_cmd();
        cmd.arg("validate").arg(tmp.path()).arg(path.as_os_str());
        let status = cmd
            .status()
            .with_context(|| format!("failed to spawn schema validate for {file}"))?;

        if !status.success() {
            errors += 1;
            if ctx.is_human() {
                eprintln!("❌ Compatibility regression detected in {file}");
            }
        } else {
            if ctx.is_human() {
                println!("✅ {file} remains backward compatible");
            }
            checked.push(file.to_string());
        }
    }

    if errors > 0 {
        bail!("Schema compatibility check failed ({errors} issue(s))");
    }

    if ctx.is_human() {
        println!("✅ Schema compatibility check passed");
    }

    Ok(CommandResult::success()
        .with_message("Schema compatibility check passed")
        .with_details(checked)
        .with_duration(ctx.elapsed()))
}

fn execute_check_ready(
    database: Option<String>,
    superuser: Option<String>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ensure_psql()?;

    let db = database
        .or_else(|| std::env::var("DATABASE_NAME").ok())
        .or_else(|| std::env::var("PGDATABASE").ok())
        .unwrap_or_else(|| "sinex_dev".to_string());

    let superuser = superuser
        .or_else(|| std::env::var("SUPERUSER").ok())
        .unwrap_or_else(|| "postgres".to_string());

    // Check core.events
    let mut cmd = pg_command("psql");
    cmd.arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-d")
        .arg(&db)
        .arg("-c")
        .arg("SELECT to_regclass('core.events') AS reg")
        .env("PGUSER", &superuser);

    let status = cmd
        .status()
        .with_context(|| "psql core.events check failed")?;

    if !status.success() {
        bail!("core.events missing in database {db}");
    }

    // Check sinex_schemas.event_payload_schemas
    let mut cmd2 = pg_command("psql");
    cmd2.arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-d")
        .arg(&db)
        .arg("-c")
        .arg("SELECT to_regclass('sinex_schemas.event_payload_schemas') AS reg")
        .env("PGUSER", &superuser);

    let status2 = cmd2
        .status()
        .with_context(|| "psql schema registry check failed")?;

    if !status2.success() {
        bail!("sinex_schemas.event_payload_schemas missing in database {db}");
    }

    if ctx.is_human() {
        println!("✅ core.events and sinex_schemas.event_payload_schemas are present");
    }

    Ok(CommandResult::success()
        .with_message("Schema tables verified")
        .with_duration(ctx.elapsed()))
}

// Helper functions

fn sinex_schema_cmd() -> Command {
    let mut cmd = Command::new("cargo");
    cmd.arg("run")
        .arg("--quiet")
        .arg("--package")
        .arg("sinex-core")
        .arg("--bin")
        .arg("sinex-schema")
        .arg("--features")
        .arg("schema-manager")
        .arg("--");
    cmd
}

fn resolve_default_base_branch() -> Result<String> {
    let output = ProcessBuilder::git()
        .args(&["symbolic-ref", "refs/remotes/origin/HEAD"])
        .with_description("resolving origin/HEAD")
        .run()?;

    if output.success() {
        let text = output.stdout.trim();
        let branch = text.strip_prefix("refs/remotes/origin/").unwrap_or(text);
        if !branch.is_empty() {
            return Ok(branch.to_string());
        }
    }

    Ok("master".to_string())
}

fn ensure_psql() -> Result<()> {
    let status = pg_command("psql")
        .arg("--version")
        .status()
        .with_context(|| "failed to spawn psql")?;

    if !status.success() {
        bail!("psql not available on PATH");
    }
    Ok(())
}

fn ensure_db_connection(db_url: &str) -> Result<()> {
    let status = pg_command("psql")
        .arg(db_url)
        .arg("-c")
        .arg("SELECT 1")
        .status()
        .with_context(|| format!("failed to connect to {db_url}"))?;

    if !status.success() {
        bail!("Unable to connect to {db_url}");
    }
    Ok(())
}

fn psql_query_bool(db_url: &str, query: &str) -> Result<bool> {
    let output = pg_command("psql")
        .arg(db_url)
        .args(["-Atqc", query])
        .output()
        .with_context(|| format!("failed to run psql query: {query}"))?;

    if !output.status.success() {
        bail!("psql exited with status {}", output.status);
    }

    Ok(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
}

fn pg_command(binary: &str) -> Command {
    if let Ok(prefix) = std::env::var("SINEX_PG_BIN") {
        let mut path = PathBuf::from(prefix);
        path.push(binary);
        Command::new(path)
    } else {
        Command::new(binary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::OutputWriter;

    #[test]
    fn test_schema_command_metadata() {
        let cmd = SchemaCommand {
            subcommand: SchemaSubcommand::Generate {
                output: "schemas/v1".to_string(),
                sync: false,
            },
        };

        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("database".to_string()));
        assert!(metadata.timeout.is_some());
    }

    #[test]
    fn test_schema_command_name() {
        let cmd = SchemaCommand {
            subcommand: SchemaSubcommand::CheckReady {
                database: None,
                superuser: None,
            },
        };

        assert_eq!(cmd.name(), "schema");
    }

    #[test]
    fn test_deploy_requires_database_url() {
        let cmd = SchemaCommand {
            subcommand: SchemaSubcommand::Deploy {
                input: "schemas/v1".to_string(),
                database_url: "".to_string(),
            },
        };

        let ctx = CommandContext::new(OutputWriter::new(crate::output::OutputFormat::Silent));
        let result = cmd.execute(&ctx);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("DATABASE_URL is required"));
    }
}
