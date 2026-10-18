//! Development commands: `check`, `test`, `scenario` and `boundaries`.
mod boundaries;
mod postgres;

use postgres::Cluster;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

const USAGE: &str = "\
xtask check                  fmt, dependency boundaries, cargo check (no database)
xtask test [FILTER]          all tests (cargo nextest) with an owned PostgreSQL
xtask scenario NAME... [--package DIR]
                             end-to-end scenarios (tests/run.py) against the
                             development build, or the package in DIR; `all` runs all
xtask boundaries [DIR]       check the workspace dependency graph";

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

/// Everything that compiles without a database. The runtime's SQL is checked by `test`.
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

/// One build and one nextest run. Checked queries compile against a schema
/// applied by the admin-only daemon; database tests clone their own databases.
fn test(filter: &[String]) -> Result {
    let cluster = Cluster::start()?;
    let env = development_database(&cluster)?;
    let mut args = vec!["nextest", "run", "--locked", "--workspace"];
    args.extend(filter.iter().map(String::as_str));
    cargo_with(&args, &env)
}

/// Create the `sinex` database with the schema, applied by the admin-only
/// daemon (which compiles without a database), and return the environment
/// for compiling checked queries against it and for reaching the cluster.
fn development_database(cluster: &Cluster) -> Result<Vec<(String, String)>> {
    cargo(&["build", "--locked", "-p", "sinexd", "--no-default-features"])?;
    let admin = cluster.fast_dir().join("admin-sinexd");
    copy_file(&target_dir().join("debug/sinexd"), &admin)?;
    let url = cluster.create_database("sinex")?;
    let file = cluster.fast_dir().join("admin-url");
    std::fs::write(&file, &url).map_err(|e| e.to_string())?;
    status(
        Command::new(admin)
            .args(["admin", "schema", "--connection-file"])
            .arg(&file),
    )?;
    let mut env = cluster.env();
    env.push(("DATABASE_URL".into(), url));
    Ok(env)
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
    let cluster = Cluster::start()?;
    let (bin, packaged, env) = match package {
        Some(dir) => (dir.join("bin"), true, cluster.env()),
        None => {
            // DATABASE_URL stays set: shell_publication compiles a database test.
            let env = development_database(&cluster)?;
            cargo_with(
                &["build", "--locked", "-p", "sinexd", "-p", "sinexctl"],
                &env,
            )?;
            // Copy the binaries, so a concurrent build cannot replace them mid-scenario.
            let bin = cluster.fast_dir().join("bin");
            std::fs::create_dir_all(&bin).map_err(|e| e.to_string())?;
            for name in ["sinexd", "sinexctl"] {
                copy_file(&target_dir().join("debug").join(name), &bin.join(name))?;
            }
            (bin, false, env)
        }
    };
    let mut command = Command::new("python3");
    command
        .arg("tests/run.py")
        .args(&names)
        .envs(env)
        .env("SINEX_BIN_DIR", &bin);
    if packaged {
        command.env("SINEX_PACKAGED", "1");
    }
    status(&mut command)
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
