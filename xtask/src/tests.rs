use super::*;
use crate::arguments::parse;

mod selection;

fn command(args: &[&str]) -> Cli {
    let args = ["xtask"].into_iter().chain(args.iter().copied());
    match parse(args.map(String::from).collect()) {
        Ok(cli) => cli,
        Err(error) => panic!("{error}"),
    }
}

fn with_command<'a>(name: &'a str, args: &[&'a str]) -> Vec<&'a str> {
    [name].into_iter().chain(args.iter().copied()).collect()
}

fn selection_of(args: &[&str]) -> TestRun {
    let Cli::Test { run, .. } = command(&with_command("test", args)) else {
        panic!("expected the test command")
    };
    run
}

fn options(args: &[&str]) -> Nextest {
    test_options(
        &selection_of(args),
        vec![("synthetic".into(), "kept".into())],
    )
}

fn compilation(args: &[&str]) -> Compilation {
    let Cli::Compile(compilation) = command(&with_command("compile", args)) else {
        panic!("expected the compile command")
    };
    compilation
}

fn owned(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| (*arg).to_owned()).collect()
}

#[devkit::test]
fn test_selection_defaults_to_the_full_workspace() {
    let got = options(&[]);
    // Cargo selects every member of this virtual workspace without a flag.
    assert_eq!(got.args, ["--locked"]);
    assert!(got.tmpfs);
    assert_eq!(got.env, [("synthetic".into(), "kept".into())]);
    assert!(!got.allow_no_tests);
}

#[devkit::test]
fn a_single_name_filter_may_match_no_test() {
    let got = options(&["session_socket"]);
    assert_eq!(got.args, ["--locked", "session_socket"]);
    assert!(got.allow_no_tests);
    for args in [
        vec!["session_socket", "other"],
        vec!["--test-threads", "2"],
        vec!["-p", "sinexd"],
        vec!["--nocapture"],
    ] {
        assert!(!options(&args).allow_no_tests, "{args:?}");
    }
}

#[devkit::test]
fn selection_and_runner_options_reach_nextest_unchanged_and_in_order() {
    for args in [
        vec!["-p", "sinex-sources", "-E", "test(session_socket)"],
        vec!["-psinex-sources", "-Etest(session_socket)", "--lib"],
        vec!["--package=sinex-sources", "--filterset=test(a)", "name"],
        vec!["--workspace", "--exclude", "sinex-fuzz", "--no-fail-fast"],
        vec![
            "--workspace",
            "-psinexd",
            "--lib",
            "--bin",
            "sinexd",
            "--features",
            "sinex-sources/full",
            "-F",
            "sinex-protocol/cli",
        ],
        vec!["-E", "package(sinex-protocol)", "--partition", "hash:1/2"],
        vec!["--base=HEAD^1"],
        vec!["--changed", "-E", "kind(test)"],
        vec!["-p", "sinexd", "--", "--nocapture"],
        vec!["--", "-p"],
    ] {
        let mut expected = vec!["--locked"];
        expected.extend(&args);
        assert_eq!(options(&args).args, expected, "{args:?}");
    }
}

// The nightly fuzz workflow's invocation: profile names are option values,
// not test names.
#[devkit::test]
fn options_with_values_keep_their_values() {
    for args in [
        vec![
            "--profile",
            "fuzz",
            "--cargo-profile",
            "release",
            "-p",
            "sinex-fuzz",
        ],
        vec!["--config", "profile.dev.debug=0", "-p", "sinexd"],
        vec!["--build-jobs", "1", "--run-ignored", "only", "-p", "sinexd"],
        // A value xtask has never heard of belongs to its flag.
        vec!["--some-new-option", "value", "--lib"],
    ] {
        let mut expected = vec!["--locked"];
        expected.extend(&args);
        let got = options(&args);
        assert_eq!(got.args, expected, "{args:?}");
        assert!(!got.allow_no_tests);
    }
}

#[devkit::test]
fn a_build_jobs_value_stays_with_its_flag_before_a_test_name() {
    let got = options(&["--build-jobs", "1", "session_socket"]);
    assert_eq!(
        got.args,
        ["--locked", "--build-jobs", "1", "session_socket"]
    );
    assert!(!got.allow_no_tests);
}

#[devkit::test]
fn test_thread_options_are_the_typed_thread_count() {
    for args in [
        vec!["--test-threads", "4"],
        vec!["--test-threads=4"],
        vec!["--jobs", "4"],
        vec!["--jobs=4"],
        vec!["-j", "4"],
        vec!["-j4"],
        vec!["-j=4"],
    ] {
        assert_eq!(
            options(&args).args,
            ["--locked", "--test-threads", "4"],
            "{args:?}"
        );
    }
    assert_eq!(
        options(&["--test-threads", "-2", "-p", "sinexd"]).args,
        ["--locked", "--test-threads", "-2", "-p", "sinexd"]
    );
}

#[devkit::test]
fn the_run_records_the_thread_count_that_applied() {
    let invocation = owned(&["session_socket"]);
    let selection = selection_of(&["session_socket"]);
    assert_eq!(
        selection.recorded(&invocation, Some("1")),
        ["--test-threads", "1", "session_socket"]
    );
    assert_eq!(selection.recorded(&invocation, None), invocation);
    // The environment's count does not reach nextest as an argument.
    assert_eq!(
        selection.nextest_arguments(),
        ["--locked", "session_socket"]
    );
    let explicit = owned(&["--test-threads", "4"]);
    assert_eq!(
        selection_of(&["--test-threads", "4"]).recorded(&explicit, Some("1")),
        explicit
    );
    // Arguments for the test binaries do not hide the environment's count.
    let after = owned(&["--", "--test-threads", "8"]);
    assert_eq!(
        selection_of(&["--", "--test-threads", "8"]).recorded(&after, Some("num-cpus")),
        ["--test-threads", "num-cpus", "--", "--test-threads", "8"]
    );
    assert_eq!(
        options(&["--", "--test-threads", "8"]).args,
        ["--locked", "--", "--test-threads", "8"]
    );
}

#[devkit::test]
fn no_run_takes_no_selection() {
    assert!(matches!(
        command(&["test", "--no-run"]),
        Cli::Test { no_run: true, .. }
    ));
    for args in [["--no-run", "name"], ["--no-run", "-j2"]] {
        assert!(
            parse(
                ["xtask", "test"]
                    .into_iter()
                    .chain(args)
                    .map(String::from)
                    .collect()
            )
            .is_err()
        );
    }
}

#[devkit::test]
fn compile_requires_a_selection_and_passes_the_rest_to_cargo() {
    assert!(parse(owned(&["xtask", "compile"])).is_err());
    let got = compilation(&["-p", "sinex-model", "--tests", "--release", "--", "-x"]);
    assert_eq!(got.packages(), ["sinex-model"]);
    assert!(got.includes_development_targets());
    assert_eq!(
        got.arguments(),
        ["-p", "sinex-model", "--tests", "--release", "--", "-x"]
    );
    assert_eq!(
        compilation(&["-psinex-model", "--profile", "dev", "--lib"]).arguments(),
        ["-p", "sinex-model", "--profile", "dev", "--lib"]
    );
}

#[devkit::test]
fn compile_decides_from_packages_targets_and_features() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let database_free =
        |args: &[&str]| boundaries::compiles_without_database(root, &compilation(args)).unwrap();
    for args in [
        vec!["-p", "sinexctl"],
        vec!["--package=sinex-session", "--tests"],
        vec!["-psinex-capture"],
        vec!["-p", "sinex-schema", "-p", "sinex-model"],
    ] {
        assert!(database_free(&args), "{args:?}");
    }
    for args in [
        vec!["--workspace"],
        vec!["--all"],
        vec!["-p", "sinex-store"],
        vec!["-p", "sinexctl", "--workspace"],
        vec!["-p", "sinexctl", "--exclude", "sinexd"],
        vec!["-p", "sinexctl", "-p", "sinexd"],
        vec!["--lib", "-p", "sinexd"],
        vec!["--release"],
    ] {
        assert!(!database_free(&args), "{args:?}");
    }
}
