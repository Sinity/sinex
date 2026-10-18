//! Development commands: `check`, `test`, `scenario`, `db`, `boundaries`,
//! plus devkit's `status`, `history` and `changed`.
mod boundaries;
mod postgres;

use clap::Parser;
use devkit::{Nextest, Run};
use postgres::Cluster;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

/// Ignored tests that `xtask test` does not run: they need rows admitted by a
/// running daemon first, so a scenario runs them.
const SCENARIO_ONLY_TESTS: &[&str] =
    &["shell::publication_tests::cancelled_publish_batch_rolls_back_and_republishes_with_same_ids"];

type Result<T = ()> = std::result::Result<T, String>;

#[derive(Parser)]
#[command(name = "xtask", about = "Sinex development commands")]
enum Cli {
    /// fmt, dependency boundaries and cargo check; no database.
    Check,
    /// Rust tests; database tests each get a fresh database in an owned PostgreSQL.
    Test {
        /// Run only tests whose name contains this.
        filter: Option<String>,
    },
    /// End-to-end scenarios against the development build, or the package in
    /// `--package DIR`; `all` runs all.
    Scenario {
        #[arg(required = true)]
        names: Vec<String>,
        #[arg(long)]
        package: Option<PathBuf>,
    },
    /// Run a command with an owned PostgreSQL (`DATABASE_URL` set).
    Db {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, required = true)]
        command: Vec<String>,
    },
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
        Cli::Scenario { names, package } => {
            recorded("scenario", &names, |run| scenario(run, &names, package))
        }
        Cli::Db { command } => recorded("db", &command, |run| db(run, &command)),
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

/// Everything that needs no database. The runtime's SQL is checked by `test`.
fn check(run: &mut Run) -> Result {
    run.command(
        "fmt",
        Command::new("cargo").args(["fmt", "--all", "--", "--check"]),
    )?;
    run.step("boundaries", |run| Ok(boundaries::check(&run.root)?))?;
    run.cargo(
        "check",
        &[
            "check",
            "--locked",
            "--workspace",
            "--exclude",
            "sinexd",
            "--all-targets",
        ],
    )?;
    run.cargo(
        "check sinexd admin",
        &[
            "check",
            "--locked",
            "-p",
            "sinexd",
            "--no-default-features",
            "--all-targets",
        ],
    )?;
    Ok(())
}

fn test(run: &mut Run, filter: Option<&str>) -> Result {
    let cluster = Cluster::start_with_schema(&admin_binary(run)?)?;
    let mut args = vec!["--locked".to_owned(), "--workspace".to_owned()];
    args.extend(filter.map(str::to_owned));
    let names = ignored_tests(&cluster, filter)?;
    run.nextest(
        "test",
        &Nextest {
            args,
            env: cluster.env("sinex"),
            // A name filter may match only database tests.
            allow_no_tests: filter.is_some(),
            ..Nextest::default()
        },
    )?;
    // Database tests are `#[ignore]`d; each runs alone in a fresh database.
    for name in names {
        if SCENARIO_ONLY_TESTS.contains(&name.as_str()) {
            continue;
        }
        let database = cluster.fresh_database()?;
        run.nextest(
            &format!("database test {name}"),
            &Nextest {
                args: ["--locked", "--workspace", "--run-ignored", "only", "-E"]
                    .map(String::from)
                    .into_iter()
                    .chain([format!("test(={name})")])
                    .collect(),
                env: cluster.env(&database),
                ..Nextest::default()
            },
        )?;
    }
    Ok(())
}

fn ignored_tests(cluster: &Cluster, filter: Option<&str>) -> Result<Vec<String>> {
    let output = Command::new("cargo")
        .args([
            "test",
            "--locked",
            "--workspace",
            "--quiet",
            "--",
            "--ignored",
            "--list",
        ])
        .args(filter)
        .envs(cluster.env("sinex"))
        .output()
        .map_err(|e| format!("could not run cargo: {e}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    let mut names: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.strip_suffix(": test"))
        .map(str::to_owned)
        .collect();
    names.sort();
    names.dedup();
    Ok(names)
}

fn scenario(run: &mut Run, names: &[String], package: Option<PathBuf>) -> Result {
    let (cluster, bin, packaged) = match package {
        Some(dir) => {
            let bin = dir.join("bin");
            (Cluster::start_with_schema(&bin.join("sinexd"))?, bin, true)
        }
        None => {
            let cluster = Cluster::start_with_schema(&admin_binary(run)?)?;
            run.cargo_with(
                "build",
                &["build", "--locked", "-p", "sinexd", "-p", "sinexctl"],
                &cluster.env("sinex"),
            )?;
            // Copy the binaries, so a concurrent build cannot replace them mid-scenario.
            let bin = cluster.fast_dir().join("bin");
            std::fs::create_dir_all(&bin).map_err(|e| e.to_string())?;
            for name in ["sinexd", "sinexctl"] {
                copy_file(&target_dir().join("debug").join(name), &bin.join(name))?;
            }
            (cluster, bin, false)
        }
    };
    let mut command = Command::new("python3");
    command
        .arg("tests/run.py")
        .args(names)
        .envs(cluster.env("sinex"))
        .env("SINEX_BIN_DIR", &bin);
    if packaged {
        command.env("SINEX_PACKAGED", "1");
    }
    Ok(run.command("scenarios", &mut command)?)
}

fn db(run: &mut Run, command: &[String]) -> Result {
    let (program, rest) = command.split_first().ok_or("db needs a command")?;
    let cluster = Cluster::start_with_schema(&admin_binary(run)?)?;
    Ok(run.command(
        "command",
        Command::new(program).args(rest).envs(cluster.env("sinex")),
    )?)
}

/// Build the admin-only daemon: it applies the schema and compiles without a
/// database. The cluster keeps its own copy.
fn admin_binary(run: &mut Run) -> Result<PathBuf> {
    run.cargo(
        "build sinexd admin",
        &["build", "--locked", "-p", "sinexd", "--no-default-features"],
    )?;
    Ok(target_dir().join("debug/sinexd"))
}

fn copy_file(from: &Path, to: &Path) -> Result {
    std::fs::copy(from, to)
        .map(drop)
        .map_err(|e| format!("copy {}: {e}", from.display()))
}

fn target_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR").map_or_else(|| PathBuf::from("target"), PathBuf::from)
}
