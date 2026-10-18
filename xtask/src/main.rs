//! Development commands: `check`, `test`, `scenario`, `db`, `boundaries`,
//! plus devkit's `status`, `history` and `changed`.
mod boundaries;
mod database;

use clap::Parser;
use devkit::{Nextest, Run, pg::Schema};
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
    /// Rust tests; each database test gets its own copy of the schema database.
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
        Cli::Scenario { names, package } => {
            recorded("scenario", &names, |run| scenario(run, &names, package))
        }
        Cli::Db(command) => {
            let hash = match database::schema_hash(Path::new(".")) {
                Ok(hash) => hash,
                Err(error) => {
                    eprintln!("{error}");
                    return ExitCode::FAILURE;
                }
            };
            let apply =
                |server: &devkit::pg::Server, name: &str| database::bootstrap(server, name, None);
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

/// A per-run directory for test stores and copied binaries, removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Result<Self> {
        let base = if Path::new("/dev/shm").is_dir() {
            PathBuf::from("/dev/shm")
        } else {
            std::env::temp_dir()
        };
        let path = base.join(format!("sinex-xtask-{}", std::process::id()));
        std::fs::create_dir_all(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(Self(path))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn test(run: &mut Run, filter: Option<&str>) -> Result {
    let scratch = Scratch::new()?;
    let mut prepared = None;
    run.step("database", |run| {
        prepared = Some(database::prepare(&run.root, None)?);
        Ok(())
    })?;
    let (server, databases) = prepared.expect("prepared");

    // Tests that do not need a database still read the test URLs, so they
    // share one copy of the schema database.
    let shared = format!("t_{}_shared", std::process::id());
    server.clone_database(&databases.template, &shared)?;
    let env = database::env(&server, &databases.schema, &shared, &scratch.0);
    let mut names = Vec::new();
    run.step("list database tests", |_| {
        names = ignored_tests(&env, filter)?;
        Ok(())
    })?;
    let mut args = vec!["--locked".to_owned(), "--workspace".to_owned()];
    args.extend(filter.map(str::to_owned));
    let result = run.nextest(
        "test",
        &Nextest {
            args,
            env,
            // A name filter may match only database tests.
            allow_no_tests: filter.is_some(),
            ..Nextest::default()
        },
    );
    server.drop_database(&shared)?;
    result?;

    // Database tests are `#[ignore]`d; each runs alone in its own copy.
    for (index, name) in names.iter().enumerate() {
        if SCENARIO_ONLY_TESTS.contains(&name.as_str()) {
            continue;
        }
        let copy = format!("t_{}_db{index}", std::process::id());
        server.clone_database(&databases.template, &copy)?;
        let result = run.nextest(
            &format!("database test {name}"),
            &Nextest {
                args: ["--locked", "--workspace", "--run-ignored", "only", "-E"]
                    .map(String::from)
                    .into_iter()
                    .chain([format!("test(={name})")])
                    .collect(),
                env: database::env(&server, &databases.schema, &copy, &scratch.0),
                ..Nextest::default()
            },
        );
        match result {
            Ok(()) => server.drop_database(&copy)?,
            Err(error) => {
                eprintln!("kept database {copy} of the failed test {name}");
                return Err(error.into());
            }
        }
    }
    Ok(())
}

fn ignored_tests(env: &[(String, String)], filter: Option<&str>) -> Result<Vec<String>> {
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
        .envs(env.iter().cloned())
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
    let scratch = Scratch::new()?;
    let admin = package.as_ref().map(|dir| dir.join("bin/sinexd"));
    let mut prepared = None;
    run.step("database", |run| {
        prepared = Some(database::prepare(&run.root, admin.as_deref())?);
        Ok(())
    })?;
    let (server, databases) = prepared.expect("prepared");
    let bin = match &package {
        Some(dir) => dir.join("bin"),
        None => {
            run.cargo_with(
                "build",
                &["build", "--locked", "-p", "sinexd", "-p", "sinexctl"],
                &[("DATABASE_URL".into(), server.url(&databases.schema, None))],
            )?;
            // Copy the binaries, so a concurrent build cannot replace them mid-scenario.
            let bin = scratch.0.join("bin");
            std::fs::create_dir_all(&bin).map_err(|e| e.to_string())?;
            for name in ["sinexd", "sinexctl"] {
                copy_file(&target_dir().join("debug").join(name), &bin.join(name))?;
            }
            bin
        }
    };
    let shared = format!("t_{}_scenario", std::process::id());
    server.clone_database(&databases.template, &shared)?;
    let mut command = Command::new("python3");
    command
        .arg("tests/run.py")
        .args(names)
        .envs(database::env(
            &server,
            &databases.schema,
            &shared,
            &scratch.0,
        ))
        .env("SINEX_BIN_DIR", &bin);
    if package.is_some() {
        command.env("SINEX_PACKAGED", "1");
    }
    let result = run.command("scenarios", &mut command);
    server.drop_database(&shared)?;
    Ok(result?)
}

fn copy_file(from: &Path, to: &Path) -> Result {
    std::fs::copy(from, to)
        .map(drop)
        .map_err(|e| format!("copy {}: {e}", from.display()))
}

fn target_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR").map_or_else(|| PathBuf::from("target"), PathBuf::from)
}
