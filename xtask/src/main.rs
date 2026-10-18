//! Development commands: `check`, `test`, `scenario`, `db` and `boundaries`.
mod boundaries;
mod postgres;

use postgres::Cluster;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

const USAGE: &str = "\
xtask check                  fmt, dependency boundaries, cargo check (no database)
xtask test [FILTER]          Rust tests; database tests each get a fresh database
xtask scenario NAME... [--package DIR]
                             end-to-end scenarios (tests/run.py) against the
                             development build, or the package in DIR; `all` runs all
xtask db -- COMMAND...       run COMMAND with an owned PostgreSQL (DATABASE_URL set)
xtask boundaries [DIR]       check the workspace dependency graph";

/// Ignored tests that `xtask test` does not run: they need rows admitted by a
/// running daemon first, so a scenario runs them.
const SCENARIO_ONLY_TESTS: &[&str] =
    &["shell::publication_tests::cancelled_publish_batch_rolls_back_and_republishes_with_same_ids"];

type Result<T = ()> = std::result::Result<T, String>;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (cmd, rest) = match args.split_first() {
        Some((cmd, rest)) => (cmd.as_str(), rest),
        None => ("help", &[][..]),
    };
    let result = match cmd {
        "check" => check(),
        "test" => test(rest),
        "scenario" => scenario(rest),
        "db" => db(rest),
        "boundaries" => boundaries::check(Path::new(rest.first().map_or(".", String::as_str)))
            .map(|()| println!("dependency boundaries passed")),
        "help" | "--help" | "-h" => {
            println!("{USAGE}");
            Ok(())
        }
        other => {
            eprintln!("unknown xtask command: {other}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xtask {cmd}: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Everything that needs no database. The runtime's SQL is checked by `test`.
fn check() -> Result {
    cargo(&["fmt", "--all", "--", "--check"])?;
    boundaries::check(Path::new("."))?;
    cargo(&[
        "check",
        "--locked",
        "--workspace",
        "--exclude",
        "sinexd",
        "--all-targets",
    ])?;
    cargo(&[
        "check",
        "--locked",
        "-p",
        "sinexd",
        "--no-default-features",
        "--all-targets",
    ])
}

fn test(args: &[String]) -> Result {
    let filter: Vec<&str> = args.iter().map(String::as_str).collect();
    let cluster = Cluster::start_with_schema(&admin_binary()?)?;
    let mut all = vec!["test", "--locked", "--workspace", "--"];
    all.extend(&filter);
    cargo_with(&all, &cluster.env("sinex"))?;
    // Database tests are `#[ignore]`d; each runs alone in a fresh database.
    for name in ignored_tests(&cluster, &filter)? {
        if SCENARIO_ONLY_TESTS.contains(&name.as_str()) {
            continue;
        }
        let database = cluster.fresh_database()?;
        cargo_with(
            &[
                "test",
                "--locked",
                "--workspace",
                "--",
                "--ignored",
                "--exact",
                &name,
            ],
            &cluster.env(&database),
        )?;
    }
    Ok(())
}

fn ignored_tests(cluster: &Cluster, filter: &[&str]) -> Result<Vec<String>> {
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

fn scenario(args: &[String]) -> Result {
    let mut names = Vec::new();
    let mut package = None;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        if arg == "--package" {
            package = Some(PathBuf::from(
                args.next().ok_or("--package needs a directory")?,
            ));
        } else {
            names.push(arg.as_str());
        }
    }
    if names.is_empty() {
        return Err("name at least one scenario, or `all`".into());
    }
    let (cluster, bin, packaged) = match package {
        Some(dir) => {
            let bin = dir.join("bin");
            (Cluster::start_with_schema(&bin.join("sinexd"))?, bin, true)
        }
        None => {
            let cluster = Cluster::start_with_schema(&admin_binary()?)?;
            cargo_with(
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
        .args(&names)
        .envs(cluster.env("sinex"))
        .env("SINEX_BIN_DIR", &bin);
    if packaged {
        command.env("SINEX_PACKAGED", "1");
    }
    status(&mut command)
}

fn db(args: &[String]) -> Result {
    let command = match args.split_first() {
        Some((first, rest)) if first == "--" => rest,
        _ => args,
    };
    let (program, rest) = command.split_first().ok_or("db needs a command")?;
    let cluster = Cluster::start_with_schema(&admin_binary()?)?;
    status(Command::new(program).args(rest).envs(cluster.env("sinex")))
}

/// Build the admin-only daemon: it applies the schema and compiles without a
/// database. The cluster keeps its own copy.
fn admin_binary() -> Result<PathBuf> {
    cargo(&["build", "--locked", "-p", "sinexd", "--no-default-features"])?;
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

fn cargo(args: &[&str]) -> Result {
    cargo_with(args, &[])
}

fn cargo_with(args: &[&str], env: &[(String, String)]) -> Result {
    status(Command::new("cargo").args(args).envs(env.iter().cloned()))
}

fn status(command: &mut Command) -> Result {
    let shown = format!("{command:?}");
    let status = command
        .status()
        .map_err(|e| format!("could not run {shown}: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{shown} failed: {status}"))
    }
}
