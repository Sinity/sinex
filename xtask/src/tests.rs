use super::*;

fn options(args: &[&str]) -> Nextest {
    let Cli::Test { args } =
        Cli::try_parse_from(["xtask", "test"].into_iter().chain(args.iter().copied())).unwrap()
    else {
        panic!("expected test command")
    };
    test_options(&args, vec![("synthetic".into(), "retained".into())])
}

#[test]
fn test_selection_defaults_to_the_full_workspace() {
    let got = options(&[]);
    assert_eq!(got.args, ["--locked", "--workspace"]);
    assert!(got.tmpfs);
    assert_eq!(got.env, [("synthetic".into(), "retained".into())]);
    assert!(!got.allow_no_tests);
}

#[test]
fn test_selection_preserves_the_original_name_filter() {
    let got = options(&["session_socket"]);
    assert_eq!(got.args, ["--locked", "--workspace", "session_socket"]);
    assert!(got.allow_no_tests);
}

#[test]
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

#[test]
fn test_selection_filtersets_keep_the_workspace_unless_package_is_selected() {
    let got = options(&["-E", "package(sinex-protocol)"]);
    assert_eq!(
        got.args,
        ["--locked", "--workspace", "-E", "package(sinex-protocol)"]
    );
    assert!(!got.allow_no_tests);
}

#[test]
fn test_selection_does_not_repeat_explicit_workspace_selection() {
    assert_eq!(options(&["--workspace"]).args, ["--locked", "--workspace"]);
}

#[test]
fn test_selection_does_not_treat_binary_arguments_as_package_selection() {
    let got = test_options(&["--".into(), "-p".into()], Vec::new());
    assert_eq!(got.args, ["--locked", "--workspace", "--", "-p"]);
}
