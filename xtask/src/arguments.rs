//! The command lines of `test` and `compile`.
//!
//! xtask declares as clap fields only what it decides from: which packages
//! and targets a compilation selects (whether the schema database is needed)
//! and the test thread count (which a run records). Every other argument,
//! with the values of its options, reaches Cargo or nextest unchanged and in
//! order, so a flag xtask does not know cannot turn its value into a test
//! name. An argument xtask does not declare starts the pass-through; xtask's
//! own options come before it.

use clap::{Args, Parser};

/// Cargo's target flags, which bound what compiles.
#[derive(Args, Debug, Default, Clone)]
pub(crate) struct Targets {
    /// Library unit tests.
    #[arg(long)]
    lib: bool,
    /// Every binary's unit tests.
    #[arg(long)]
    bins: bool,
    /// The named binary.
    #[arg(long, value_name = "NAME")]
    bin: Vec<String>,
    /// Every target marked `test = true`.
    #[arg(long)]
    tests: bool,
    /// The named integration test target.
    #[arg(long, value_name = "NAME")]
    test: Vec<String>,
    #[arg(long)]
    examples: bool,
    #[arg(long, value_name = "NAME")]
    example: Vec<String>,
    #[arg(long)]
    benches: bool,
    #[arg(long, value_name = "NAME")]
    bench: Vec<String>,
    #[arg(long)]
    all_targets: bool,
}

impl Targets {
    /// Whether test or benchmark targets compile, which brings in
    /// development dependencies.
    fn includes_development_targets(&self) -> bool {
        self.tests
            || self.benches
            || self.all_targets
            || !self.test.is_empty()
            || !self.bench.is_empty()
    }

    fn arguments(&self) -> Vec<String> {
        let mut arguments = Vec::new();
        for (set, flag) in [
            (self.lib, "--lib"),
            (self.bins, "--bins"),
            (self.tests, "--tests"),
            (self.examples, "--examples"),
            (self.benches, "--benches"),
            (self.all_targets, "--all-targets"),
        ] {
            if set {
                arguments.push(flag.to_owned());
            }
        }
        for (names, flag) in [
            (&self.bin, "--bin"),
            (&self.test, "--test"),
            (&self.example, "--example"),
            (&self.bench, "--bench"),
        ] {
            for name in names {
                arguments.extend([flag.to_owned(), name.clone()]);
            }
        }
        arguments
    }
}

/// The compilation `compile` runs: `cargo check` of the selected packages
/// and targets.
#[derive(Args, Debug, Default, Clone)]
pub(crate) struct Compilation {
    /// Check this package.
    #[arg(short = 'p', long = "package", value_name = "SPEC")]
    packages: Vec<String>,
    /// Check every package.
    #[arg(long, visible_alias = "all")]
    workspace: bool,
    /// Leave this package out of a workspace check.
    #[arg(long, value_name = "SPEC")]
    exclude: Vec<String>,
    #[command(flatten)]
    targets: Targets,
    /// Features to enable; they decide which dependencies compile.
    #[arg(short = 'F', long, value_name = "FEATURES")]
    features: Vec<String>,
    #[arg(long)]
    all_features: bool,
    #[arg(long)]
    no_default_features: bool,
    /// Further `cargo check` arguments.
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "CARGO"
    )]
    rest: Vec<String>,
}

impl Compilation {
    /// The packages named, or none for the workspace.
    pub(crate) fn packages(&self) -> &[String] {
        &self.packages
    }

    /// Whether the whole workspace is selected, whatever `-p` names.
    pub(crate) fn workspace(&self) -> bool {
        self.workspace || !self.exclude.is_empty()
    }

    pub(crate) fn includes_development_targets(&self) -> bool {
        self.targets.includes_development_targets()
    }

    /// The feature flags, as `cargo metadata` and `cargo check` take them.
    pub(crate) fn feature_arguments(&self) -> Vec<String> {
        let mut arguments = Vec::new();
        for features in &self.features {
            arguments.extend(["--features".to_owned(), features.clone()]);
        }
        if self.all_features {
            arguments.push("--all-features".into());
        }
        if self.no_default_features {
            arguments.push("--no-default-features".into());
        }
        arguments
    }

    /// The arguments of `cargo check`, after `--locked`.
    pub(crate) fn arguments(&self) -> Vec<String> {
        let mut arguments = Vec::new();
        for package in &self.packages {
            arguments.extend(["-p".to_owned(), package.clone()]);
        }
        if self.workspace {
            arguments.push("--workspace".into());
        }
        for spec in &self.exclude {
            arguments.extend(["--exclude".to_owned(), spec.clone()]);
        }
        arguments.extend(self.targets.arguments());
        arguments.extend(self.feature_arguments());
        arguments.extend(self.rest.iter().cloned());
        arguments
    }
}

/// The tests `test` runs under nextest.
#[derive(Args, Debug, Default, Clone)]
pub(crate) struct TestRun {
    /// How many tests run at once (nextest's `--test-threads`, also
    /// `--jobs` and `-j`). Without it, `NEXTEST_TEST_THREADS` applies. The
    /// run records the count that applied.
    #[arg(
        short = 'j',
        long = "test-threads",
        visible_alias = "jobs",
        value_name = "N",
        allow_hyphen_values = true
    )]
    pub(crate) test_threads: Option<String>,
    /// Package and target selection, test names, filtersets and every other
    /// `cargo nextest run` option; `--` separates the test binaries'
    /// arguments.
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "NEXTEST"
    )]
    pub(crate) rest: Vec<String>,
}

impl TestRun {
    /// The `cargo nextest run` arguments. Nextest reads
    /// `NEXTEST_TEST_THREADS` itself when the option is absent.
    pub(crate) fn nextest_arguments(&self) -> Vec<String> {
        let mut arguments = vec!["--locked".to_owned()];
        if let Some(threads) = &self.test_threads {
            arguments.extend(["--test-threads".to_owned(), threads.clone()]);
        }
        arguments.extend(self.rest.iter().cloned());
        arguments
    }

    /// A single test name and nothing else: a name that matches no test is
    /// then not an error, as it never was for this invocation.
    pub(crate) fn names_one_test(&self) -> bool {
        self.test_threads.is_none()
            && matches!(self.rest.as_slice(), [name] if !name.starts_with('-'))
    }

    /// The arguments a run records: the command line, led by the thread
    /// count the environment set when the command line left it out, so that
    /// history keeps different concurrency apart.
    pub(crate) fn recorded(&self, invocation: &[String], inherited: Option<&str>) -> Vec<String> {
        match (&self.test_threads, inherited) {
            (None, Some(threads)) => ["--test-threads", threads]
                .into_iter()
                .map(str::to_owned)
                .chain(invocation.iter().cloned())
                .collect(),
            _ => invocation.to_vec(),
        }
    }
}

#[derive(Parser)]
#[command(name = "xtask", about = "Sinex development commands")]
pub(crate) enum Cli {
    /// fmt, dependency boundaries and clippy against the schema database.
    Check,
    /// Run exported NixOS checks and record scenario results.
    Vm(devkit::vm::Vm),
    /// Run isolated development services and daemon, optionally watching source edits.
    Run(crate::development::Development),
    /// LLVM line coverage with explicit output and optional threshold.
    Coverage(devkit::coverage::Coverage),
    /// Build every test binary and the clippy of every target without
    /// running any test: the warm target that new checkouts copy.
    Warm,
    /// Compile selected packages or targets against the schema database.
    #[command(arg_required_else_help = true)]
    Compile(Compilation),
    /// Rust tests under nextest; each database test clones the schema template.
    Test {
        /// Build every test binary against the schema database without
        /// running any (a cloud environment's warm build).
        #[arg(long, conflicts_with_all = ["test_threads", "rest"])]
        no_run: bool,
        #[command(flatten)]
        run: TestRun,
    },
    /// The development database: `ensure` prints `DATABASE_URL`, `exec` runs a
    /// command with it set.
    #[command(subcommand)]
    Db(devkit::cli::db::Db),
    /// Check the workspace dependency graph.
    Boundaries { dir: Option<std::path::PathBuf> },
    /// Backfill and derivation on a fixed sample of the real inputs.
    #[command(subcommand)]
    Bench(crate::bench::Bench),
    /// Build against a local checkout of polylogue-sinex-common instead of
    /// the commit Cargo.lock pins.
    LinkCommon {
        /// The checkout; by default `$POLYLOGUE_SINEX_COMMON`, else
        /// `../polylogue-sinex-common`.
        checkout: Option<std::path::PathBuf>,
    },
    /// Return to the commit of polylogue-sinex-common that Cargo.lock pinned.
    UnlinkCommon,
    #[command(flatten)]
    Shared(devkit::cli::Command),
}

/// Parses a command line. clap consumes the `--` that hands what follows to
/// the test binaries; that part is appended to the pass-through as written.
pub(crate) fn parse(mut args: Vec<String>) -> Result<Cli, clap::Error> {
    if !matches!(args.get(1).map(String::as_str), Some("test" | "compile")) {
        return Cli::try_parse_from(args);
    }
    let after = args
        .iter()
        .position(|arg| arg == "--")
        .map(|separator| args.split_off(separator));
    let attached: &[&str] = if args[1] == "test" {
        &["-j"]
    } else {
        &["-p", "-F"]
    };
    let mut cli = Cli::try_parse_from(separate_short_values(args, attached))?;
    if let Some(after) = after {
        match &mut cli {
            Cli::Test { run, .. } => run.rest.extend(after),
            Cli::Compile(compilation) => compilation.rest.extend(after),
            _ => {}
        }
    }
    Ok(cli)
}

/// Spells the short options `flags` with an attached value (`-pNAME`, `-jN`)
/// as two words. With pass-through
/// arguments allowed, clap reads a short option with an attached value as
/// the first argument it does not interpret.
fn separate_short_values(args: Vec<String>, flags: &[&str]) -> Vec<String> {
    let mut separated = Vec::new();
    for arg in args {
        match flags.iter().find_map(|flag| {
            Some((
                *flag,
                arg.strip_prefix(flag).filter(|value| !value.is_empty())?,
            ))
        }) {
            Some((flag, value)) => {
                separated.push(flag.to_owned());
                separated.push(value.strip_prefix('=').unwrap_or(value).to_owned());
            }
            None => separated.push(arg),
        }
    }
    separated
}
