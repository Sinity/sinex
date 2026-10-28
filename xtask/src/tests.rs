use super::*;

fn options(args: &[&str]) -> Nextest {
    let Cli::Test { args, .. } =
        Cli::try_parse_from(["xtask", "test"].into_iter().chain(args.iter().copied())).unwrap()
    else {
        panic!("expected test command")
    };
    test_options(&args, vec![("synthetic".into(), "kept".into())])
}

#[devkit::test]
fn test_selection_defaults_to_the_full_workspace() {
    let got = options(&[]);
    assert_eq!(got.args, ["--locked", "--workspace"]);
    assert!(got.tmpfs);
    assert_eq!(got.env, [("synthetic".into(), "kept".into())]);
    assert!(!got.allow_no_tests);
}

#[devkit::test]
fn test_selection_preserves_the_original_name_filter() {
    let got = options(&["session_socket"]);
    assert_eq!(got.args, ["--locked", "--workspace", "session_socket"]);
    assert!(got.allow_no_tests);
}

#[devkit::test]
fn test_selection_passes_package_and_filter_arguments_without_workspace() {
    for args in [
        vec!["-p", "sinex-sources", "session_socket"],
        vec!["-psinex-sources", "session_socket"],
        vec!["--package=sinex-sources", "-E", "test(session_socket)"],
        vec!["--package", "sinex-model", "-p", "sinex-protocol", "--lib"],
    ] {
        let got = options(&args);
        let expected: Vec<_> = ["--locked"].into_iter().chain(args).collect();
        assert_eq!(got.args, expected);
        assert!(!got.allow_no_tests);
    }
}

#[devkit::test]
fn test_selection_filtersets_keep_the_workspace_unless_package_is_selected() {
    let got = options(&["-E", "package(sinex-protocol)"]);
    assert_eq!(
        got.args,
        ["--locked", "--workspace", "-E", "package(sinex-protocol)"]
    );
    assert!(!got.allow_no_tests);
}

#[devkit::test]
fn test_selection_does_not_repeat_explicit_workspace_selection() {
    assert_eq!(options(&["--workspace"]).args, ["--locked", "--workspace"]);
}

#[devkit::test]
fn test_selection_does_not_treat_binary_arguments_as_package_selection() {
    let got = test_options(&["--".into(), "-p".into()], Vec::new());
    assert_eq!(got.args, ["--locked", "--workspace", "--", "-p"]);
}

#[devkit::test]
fn compile_requires_and_preserves_explicit_selection() {
    assert!(Cli::try_parse_from(["xtask", "compile"]).is_err());
    let Cli::Compile { args } =
        Cli::try_parse_from(["xtask", "compile", "-p", "sinex-model", "--tests"]).unwrap()
    else {
        panic!("expected compile")
    };
    assert_eq!(args, ["-p", "sinex-model", "--tests"]);
}

#[devkit::test]
fn compile_prepares_a_database_only_for_packages_with_checked_queries() {
    let database_free = |args: &[String]| {
        boundaries::database_free(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap(),
            args,
        )
        .unwrap()
    };
    for args in [
        vec!["-p", "sinexctl"],
        vec!["-psinex-capture"],
        vec!["-p", "sinex-schema", "-p", "sinex-model"],
    ] {
        assert!(database_free(
            &args.into_iter().map(String::from).collect::<Vec<_>>()
        ));
    }
    for args in [
        vec![],
        vec!["--workspace"],
        vec!["-p", "sinex-store"],
        vec!["--package=sinex-session", "--tests"],
        vec!["-p", "sinexctl", "--workspace"],
        vec!["-p", "sinexctl", "-p", "sinexd"],
        vec!["-p"],
    ] {
        assert!(!database_free(
            &args.into_iter().map(String::from).collect::<Vec<_>>()
        ));
    }
}

#[devkit::test]
fn environment_test_threads_are_recorded_without_changing_selection() {
    let args = vec!["session_socket".into()];
    let mut selected = test_options(&args, Vec::new());
    assert_eq!(
        test_arguments(&args, Some("1")),
        ["--test-threads", "1", "session_socket"]
    );
    selected.args = test_arguments(&selected.args, Some("1"));
    assert_eq!(
        selected.args,
        [
            "--test-threads",
            "1",
            "--locked",
            "--workspace",
            "session_socket"
        ]
    );
    assert!(selected.allow_no_tests);
    assert_eq!(test_arguments(&args, None), args);
}

#[devkit::test]
fn explicit_test_thread_options_take_precedence_over_the_environment() {
    for args in [
        vec!["--test-threads", "4"],
        vec!["--test-threads=4"],
        vec!["--jobs", "4"],
        vec!["--jobs=4"],
        vec!["-j", "4"],
        vec!["-j4"],
        vec!["-j=4"],
    ] {
        let args = args.into_iter().map(String::from).collect::<Vec<_>>();
        assert_eq!(test_arguments(&args, Some("1")), args);
    }
}

#[devkit::test]
fn binary_arguments_do_not_hide_inherited_test_threads() {
    let args = vec!["--".into(), "--test-threads".into(), "8".into()];
    assert_eq!(
        test_arguments(&args, Some("num-cpus")),
        ["--test-threads", "num-cpus", "--", "--test-threads", "8"]
    );
    assert_eq!(test_arguments(&[], Some("-2")), ["--test-threads", "-2"]);
}

#[devkit::test]
fn concurrency_options_keep_the_full_fuzz_regression_gate() {
    for args in [
        vec![],
        vec!["--test-threads", "1"],
        vec!["-j4"],
        vec!["--jobs=4"],
        vec!["--workspace", "-j", "1"],
    ] {
        let args = args.into_iter().map(String::from).collect::<Vec<_>>();
        assert!(full_test(&args));
    }
    assert!(full_test(&test_arguments(&[], Some("1"))));
    for args in [
        vec!["-p", "sinex-model"],
        vec!["--test-threads", "1", "session_socket"],
        vec!["--", "--test-threads", "1"],
        vec!["--test-threads"],
    ] {
        let args = args.into_iter().map(String::from).collect::<Vec<_>>();
        assert!(!full_test(&args));
    }
}
