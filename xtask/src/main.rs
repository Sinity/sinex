//! Sinex development commands and shared devkit runners.
mod arguments;
mod bench;
mod boundaries;
mod common_link;
mod database;
mod development;

use arguments::{Cli, Compilation, TestRun};
use devkit::{Nextest, Run, pg::Schema};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

type Result<T = ()> = std::result::Result<T, String>;

fn main() -> ExitCode {
    match arguments::parse(std::env::args().collect()).unwrap_or_else(|error| error.exit()) {
        Cli::Vm(options) => recorded("vm", &invocation(), |run| {
            devkit::vm::execute(
                run,
                &options,
                &["module", "vm-stack", "vm-topology", "vm-restore"],
            )
        }),
        Cli::Run(options) => recorded("run", &invocation(), |run| {
            development::execute(run, &options)
        }),
        Cli::Coverage(options) => recorded("coverage", &invocation(), |run| {
            with_services(run, |run, env| {
                devkit::coverage::execute(run, &options, env)
            })
        }),
        Cli::Check => recorded("check", &[], check),
        Cli::Warm => recorded("warm", &[], |run| {
            // A lint failure on master must not leave the test builds cold.
            let tests = build_tests(run);
            clippy(run).and(tests)
        }),
        Cli::Compile(compilation) => {
            recorded("compile", &invocation(), |run| compile(run, &compilation))
        }
        Cli::Test { no_run: true, .. } => recorded("test", &["--no-run".into()], build_tests),
        Cli::Test { run: selection, .. } => {
            let inherited = std::env::var("NEXTEST_TEST_THREADS").ok();
            let arguments = selection.recorded(&invocation(), inherited.as_deref());
            recorded("test", &arguments, |run| test(run, &selection))
        }
        Cli::Db(devkit::cli::db::Db::Exec { command }) => {
            exit_code(database::exec(Path::new("."), &command))
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
        Cli::Bench(command) => bench::execute(command),
        Cli::LinkCommon { checkout } => exit_code(common_link::link(checkout)),
        Cli::UnlinkCommon => exit_code(common_link::unlink()),
        Cli::Shared(command) => devkit::cli::execute("sinex", command),
    }
}

fn exit_code(result: Result) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
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
    clippy(run)
}

/// Clippy of the workspace, fuzz harness included, against the schema
/// database.
fn clippy(run: &mut Run) -> Result {
    let mut prepared = None;
    run.step("database", |run| {
        prepared = Some(database::prepare(&run.root)?);
        Ok(())
    })?;
    // The server (stopped on drop when this run started it) lives until clippy ends.
    let (server, databases) = prepared.expect("prepared");
    let result = run.cargo_with(
        "clippy",
        &["clippy", "--locked", "--workspace", "--all-targets"],
        &database::env(&server, &databases),
    );
    run.step("cleanup", |_| {
        drop(server);
        Ok(())
    })?;
    result?;
    Ok(())
}

fn compile(run: &mut Run, compilation: &Compilation) -> Result {
    let arguments = compilation.arguments();
    let selected: Vec<&str> = ["check", "--locked"]
        .into_iter()
        .chain(arguments.iter().map(String::as_str))
        .collect();
    if boundaries::compiles_without_database(&run.root, compilation)? {
        run.cargo_with("compile", &selected, &[])?;
    } else {
        let mut prepared = None;
        run.step("database", |run| {
            prepared = Some(database::prepare(&run.root)?);
            Ok(())
        })?;
        let (server, databases) = prepared.expect("prepared");
        let result = run.cargo_with("compile", &selected, &database::env(&server, &databases));
        run.step("cleanup", |_| {
            drop(server);
            Ok(())
        })?;
        result?;
    }
    Ok(())
}

fn services(
    run: &mut Run,
) -> Result<(
    devkit::pg::Server,
    devkit::pg::SchemaDatabases,
    devkit::nats::Server,
)> {
    let mut prepared = None;
    run.step("database", |run| {
        prepared = Some(database::prepare(&run.root)?);
        Ok(())
    })?;
    let (server, databases) = prepared.ok_or("database not prepared")?;
    let mut nats = None;
    run.step("nats", |_| {
        nats = Some(devkit::nats::Server::start()?);
        Ok(())
    })?;
    Ok((server, databases, nats.ok_or("NATS not started")?))
}

fn with_services(
    run: &mut Run,
    command: impl FnOnce(&mut Run, &[(String, String)]) -> Result,
) -> Result {
    let (server, databases, nats) = services(run)?;
    let mut env = database::env(&server, &databases);
    env.extend(nats.test_env());
    let result = command(run, &env);
    run.step("cleanup", |_| {
        drop((nats, server));
        Ok(())
    })?;
    result
}

/// Cargo's package and target selection bounds the build; nextest filters
/// bound execution inside those targets. With no package selection, test the
/// workspace.
fn test(run: &mut Run, selection: &TestRun) -> Result {
    with_services(run, |run, env| {
        run.nextest("test", &test_options(selection, env.to_vec()))?;
        Ok(())
    })
}

fn test_options(selection: &TestRun, env: Vec<(String, String)>) -> Nextest {
    Nextest {
        args: selection.nextest_arguments(),
        env,
        // A single name that matches no test stays a pass; every other
        // selector keeps devkit's refusal when no tests match.
        allow_no_tests: selection.names_one_test(),
        tmpfs: true,
        junit: None,
    }
}

/// Every test binary, built against the schema database and not run.
fn build_tests(run: &mut Run) -> Result {
    let mut prepared = None;
    run.step("database", |run| {
        prepared = Some(database::prepare(&run.root)?);
        Ok(())
    })?;
    let (server, databases) = prepared.expect("prepared");
    let env = database::env(&server, &databases);
    run.cargo_with(
        "test build",
        &["test", "--no-run", "--locked", "--workspace"],
        &env,
    )?;
    Ok(())
}

fn target_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR").map_or_else(|| PathBuf::from("target"), PathBuf::from)
}

fn invocation() -> Vec<String> {
    std::env::args().skip(2).collect()
}

#[cfg(test)]
mod tests;
