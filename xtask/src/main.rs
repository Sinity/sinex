//! Development commands: `check`, `test`, `db`, `boundaries`, plus devkit's
//! `status`, `history` and `changed`.
mod boundaries;
mod database;

use clap::Parser;
use devkit::{Nextest, Run, pg::Schema};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

type Result<T = ()> = std::result::Result<T, String>;

#[derive(Parser)]
#[command(name = "xtask", about = "Sinex development commands")]
enum Cli {
    /// fmt, dependency boundaries and clippy against the schema database.
    Check,
    /// Rust tests under nextest; each database test clones the schema template.
    Test {
        /// Run only tests whose name contains this.
        filter: Option<String>,
    },
    /// The development database: `ensure` prints `DATABASE_URL`, `exec` runs a
    /// command with it set.
    #[command(subcommand)]
    Db(devkit::cli::db::Db),
    /// Check the workspace dependency graph.
    Boundaries { dir: Option<PathBuf> },
    #[command(flatten)]
    Shared(devkit::cli::Command),
}

fn main() -> ExitCode {
    match Cli::parse() {
        Cli::Check => recorded("check", &[], check),
        Cli::Test { filter } => {
            let args: Vec<String> = filter.iter().cloned().collect();
            recorded("test", &args, |run| test(run, filter.as_deref()))
        }
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
        Command::new("cargo").args(["fmt", "--all", "--", "--check"]),
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
    Ok(())
}

fn test(run: &mut Run, filter: Option<&str>) -> Result {
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
    let mut args = vec!["--locked".to_owned(), "--workspace".to_owned()];
    args.extend(filter.map(str::to_owned));
    run.nextest(
        "test",
        &Nextest {
            args,
            env,
            allow_no_tests: filter.is_some(),
            ..Nextest::default()
        },
    )?;
    Ok(())
}

fn target_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR").map_or_else(|| PathBuf::from("target"), PathBuf::from)
}
