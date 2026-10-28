//! Development commands: `check`, `test`, `db`, `boundaries`, plus devkit's
//! `status`, `history` and `changed`.
mod bench;
mod boundaries;
mod database;

use clap::Parser;
use devkit::{Nextest, Run, pg::Schema};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

type Result<T = ()> = std::result::Result<T, String>;

#[derive(Parser)]
#[command(name = "xtask", about = "Sinex development commands")]
enum Cli {
    /// fmt, dependency boundaries and clippy against the schema database.
    Check,
    /// Compile selected packages or targets against the schema database.
    Compile {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, required = true)]
        args: Vec<String>,
    },
    /// Rust tests under nextest; each database test clones the schema template.
    Test {
        /// Further `cargo nextest run` arguments: packages, filters, names.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// The development database: `ensure` prints `DATABASE_URL`, `exec` runs a
    /// command with it set.
    #[command(subcommand)]
    Db(devkit::cli::db::Db),
    /// Check the workspace dependency graph.
    Boundaries { dir: Option<PathBuf> },
    /// Backfill and derivation on a fixed sample of the real inputs.
    #[command(subcommand)]
    Bench(bench::Bench),
    #[command(flatten)]
    Shared(devkit::cli::Command),
}

fn main() -> ExitCode {
    match Cli::parse() {
        Cli::Check => recorded("check", &[], check),
        Cli::Compile { args } => recorded("compile", &args, |run| compile(run, &args)),
        Cli::Test { args } => recorded("test", &args, |run| test(run, &args)),
        Cli::Db(command) => {
            let (hash, apply) = match database::schema(Path::new(".")) {
                Ok(schema) => schema,
                Err(error) => {
                    eprintln!("{error}");
                    return ExitCode::FAILURE;
                }
            };
            let schema = Schema {
                project: "sinex",
                hash,
                bootstrap: &apply,
            };
            devkit::cli::db::execute(command, &database::cluster_options(), &schema)
        }
        Cli::Boundaries { dir } => {
            match boundaries::check(dir.as_deref().unwrap_or(Path::new("."))) {
                Ok(()) => {
                    println!("dependency boundaries passed");
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("xtask boundaries: {error}");
                    ExitCode::FAILURE
                }
            }
        }
        Cli::Bench(command) => bench::execute(command),
        Cli::Shared(command) => devkit::cli::execute("sinex", command),
    }
}

fn recorded(command: &str, args: &[String], steps: impl FnOnce(&mut Run) -> Result) -> ExitCode {
    let mut run = Run::start("sinex", command, args);
    if let Err(error) = steps(&mut run) {
        run.failure.get_or_insert(error);
    }
    run.finish()
}

/// Format, boundaries and clippy. The store's queries are checked at compile
/// time, so clippy runs against the schema database.
fn check(run: &mut Run) -> Result {
    run.command(
        "fmt",
        devkit::cargo_command().args(["fmt", "--all", "--", "--check"]),
    )?;
    run.step("boundaries", |run| Ok(boundaries::check(&run.root)?))?;
    let mut prepared = None;
    run.step("database", |run| {
        prepared = Some(database::prepare(&run.root)?);
        Ok(())
    })?;
    // The server (stopped on drop when this run started it) lives until clippy ends.
    let (server, databases) = prepared.expect("prepared");
    run.cargo_with(
        "clippy",
        &["clippy", "--locked", "--workspace", "--all-targets"],
        &database::env(&server, &databases),
    )?;
    run.cargo_with(
        "clippy-capture",
        &[
            "clippy",
            "--locked",
            "-p",
            "sinex-session",
            "-p",
            "sinex-capture",
            "--all-targets",
        ],
        &[],
    )?;
    run.cargo_with(
        "clippy-source-core",
        &[
            "clippy",
            "--locked",
            "-p",
            "sinex-source-core",
            "--no-default-features",
            "--all-targets",
        ],
        &[],
    )?;
    Ok(())
}

fn compile(run: &mut Run, args: &[String]) -> Result {
    let selected: Vec<&str> = ["check", "--locked"]
        .into_iter()
        .chain(args.iter().map(String::as_str))
        .collect();
    if boundaries::database_free(&run.root, args)? {
        run.cargo_with("compile", &selected, &[])?;
    } else {
        let mut prepared = None;
        run.step("database", |run| {
            prepared = Some(database::prepare(&run.root)?);
            Ok(())
        })?;
        let (server, databases) = prepared.expect("prepared");
        run.cargo_with("compile", &selected, &database::env(&server, &databases))?;
    }
    Ok(())
}

fn test(run: &mut Run, args: &[String]) -> Result {
    let mut prepared = None;
    run.step("database", |run| {
        prepared = Some(database::prepare(&run.root)?);
        Ok(())
    })?;
    let (server, databases) = prepared.expect("prepared");
    // One NATS server for the run; each test takes its own subject prefix
    // (`devkit::nats::test_nats`). It stops when the run ends.
    let mut nats = None;
    run.step("nats", |_| {
        nats = Some(devkit::nats::Server::start()?);
        Ok(())
    })?;
    let mut env = database::env(&server, &databases);
    env.extend(nats.as_ref().expect("started").test_env());
    run.nextest("test", &test_options(args, env))?;
    Ok(())
}

fn test_options(args: &[String], env: Vec<(String, String)>) -> Nextest {
    let explicit_scope = args
        .iter()
        .take_while(|arg| arg.as_str() != "--")
        .any(|arg| {
            arg == "-p"
                || (arg.starts_with("-p") && arg.len() > 2)
                || arg == "--package"
                || arg.starts_with("--package=")
                || arg == "--workspace"
                || arg == "--all"
        });
    let mut selected = vec!["--locked".to_owned()];
    if !explicit_scope {
        selected.push("--workspace".to_owned());
    }
    selected.extend_from_slice(args);
    Nextest {
        args: selected,
        env,
        // Preserve the original single name-filter invocation. New nextest
        // selectors keep devkit's default refusal when no tests match.
        allow_no_tests: args.len() == 1 && !args[0].starts_with('-'),
        tmpfs: true,
    }
}

fn target_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR").map_or_else(|| PathBuf::from("target"), PathBuf::from)
}

#[cfg(test)]
mod tests;
