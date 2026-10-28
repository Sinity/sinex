//! Sinex development commands and shared devkit runners.
mod bench;
mod boundaries;
mod common_link;
mod database;
mod development;

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
    /// Run exported NixOS checks and record scenario results.
    Vm(devkit::vm::Vm),
    /// Run isolated development services and daemon, optionally watching source edits.
    Run(development::Development),
    /// LLVM line coverage with explicit output and optional threshold.
    Coverage(devkit::coverage::Coverage),
    /// Compile selected packages or targets against the schema database.
    Compile {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, required = true)]
        args: Vec<String>,
    },
    /// Rust tests under nextest; each database test clones the schema template.
    Test {
        /// Build every test binary against the schema database without
        /// running any (a cloud environment's warm build).
        #[arg(long, conflicts_with = "args")]
        no_run: bool,
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
    /// Build against a local checkout of polylogue-sinex-common instead of
    /// the commit Cargo.lock pins.
    LinkCommon {
        /// The checkout; by default `$POLYLOGUE_SINEX_COMMON`, else
        /// `../polylogue-sinex-common`.
        checkout: Option<PathBuf>,
    },
    /// Return to the commit of polylogue-sinex-common that Cargo.lock pinned.
    UnlinkCommon,
    #[command(flatten)]
    Shared(devkit::cli::Command),
}

fn main() -> ExitCode {
    match Cli::parse() {
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
        Cli::Compile { args } => recorded("compile", &args, |run| compile(run, &args)),
        Cli::Test { no_run: true, .. } => recorded("test", &["--no-run".into()], build_tests),
        Cli::Test { args, .. } => {
            let threads = std::env::var("NEXTEST_TEST_THREADS").ok();
            let arguments = test_arguments(&args, threads.as_deref());
            recorded("test", &arguments, |run| {
                test(run, &args, threads.as_deref())
            })
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
    run.command(
        "fuzz-fmt",
        devkit::cargo_command().args([
            "fmt",
            "--manifest-path",
            "fuzz/Cargo.toml",
            "--",
            "--check",
        ]),
    )?;
    run.step("boundaries", |run| Ok(boundaries::check(&run.root)?))?;
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
    let result = result.and_then(|_| {
        run.cargo_with(
            "fuzz-clippy",
            &[
                "clippy",
                "--locked",
                "--manifest-path",
                "fuzz/Cargo.toml",
                "--all-targets",
            ],
            &database::env(&server, &databases),
        )
    });
    run.step("cleanup", |_| {
        drop(server);
        Ok(())
    })?;
    result?;
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

fn test(run: &mut Run, args: &[String], threads: Option<&str>) -> Result {
    let arguments = test_arguments(args, threads);
    with_services(run, |run, env| {
        if full_test(&arguments) {
            run.cargo_with(
                "fuzz-tests",
                &[
                    "test",
                    "--locked",
                    "--manifest-path",
                    "fuzz/Cargo.toml",
                    "--lib",
                ],
                env,
            )?;
        }
        let mut options = test_options(args, env.to_vec());
        options.args = test_arguments(&options.args, threads);
        run.nextest("test", &options)?;
        Ok(())
    })
}

fn full_test(args: &[String]) -> bool {
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--workspace" | "--all" | "--locked" => {}
            "--test-threads" | "--jobs" | "-j" => {
                if args.next().is_none() {
                    return false;
                }
            }
            _ if thread_option(arg) => {}
            _ => return false,
        }
    }
    true
}

fn thread_option(arg: &str) -> bool {
    matches!(arg, "--test-threads" | "--jobs" | "-j")
        || arg.starts_with("--test-threads=")
        || arg.starts_with("--jobs=")
        || (arg.starts_with("-j") && arg.len() > 2)
}

// Record environment concurrency as arguments so history keeps different test setups apart.
fn test_arguments(args: &[String], threads: Option<&str>) -> Vec<String> {
    let Some(threads) = threads else {
        return args.to_vec();
    };
    let explicit_threads = args
        .iter()
        .take_while(|arg| arg.as_str() != "--")
        .any(|arg| thread_option(arg));
    if explicit_threads {
        return args.to_vec();
    }
    let mut arguments = vec!["--test-threads".into(), threads.into()];
    arguments.extend_from_slice(args);
    arguments
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
        "fuzz test build",
        &[
            "test",
            "--no-run",
            "--locked",
            "--manifest-path",
            "fuzz/Cargo.toml",
            "--lib",
        ],
        &env,
    )?;
    run.cargo_with(
        "test build",
        &["test", "--no-run", "--locked", "--workspace"],
        &env,
    )?;
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

fn invocation() -> Vec<String> {
    std::env::args().skip(2).collect()
}

#[cfg(test)]
mod tests;
