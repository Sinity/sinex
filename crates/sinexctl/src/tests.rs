use super::*;

#[devkit::test]
fn the_cli_is_generated_from_the_table() {
    let app = cli();
    app.clone().debug_assert();
    for c in COMMANDS {
        assert!(app.find_subcommand(c.name).is_some(), "{}", c.name);
    }
    let m = cli()
        .try_get_matches_from([
            "sinexctl",
            "search",
            "--page",
            "5",
            "cargo test",
            "tide",
            "-la",
        ])
        .unwrap();
    let (name, sub) = m.subcommand().unwrap();
    let args = arguments(sinex_protocol::command(name).unwrap(), sub);
    // Options come first; everything after the text starts is text.
    assert_eq!(
        args["words"],
        Value::String("\"cargo test\" tide -la".into())
    );
    assert_eq!(args["page"], Value::from(5u64));
    let m = cli()
        .try_get_matches_from([
            "sinexctl",
            "query",
            "commands",
            "where",
            "exit_code",
            "!=",
            "0",
        ])
        .unwrap();
    let (name, sub) = m.subcommand().unwrap();
    let request =
        Request::from_args(name, arguments(sinex_protocol::command(name).unwrap(), sub)).unwrap();
    assert_eq!(
        request,
        Request::Query {
            query: "commands where exit_code != 0".into(),
            page: None,
            cursor: None
        }
    );
}

#[devkit::test]
fn cli_discovery_omits_unimplemented_operations_and_keeps_recall() {
    for name in ["stage", "same", "distinct"] {
        assert!(cli().find_subcommand(name).is_none(), "{name}");
        let error = cli().try_get_matches_from(["sinexctl", name]).unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }
    let matches = cli()
        .try_get_matches_from(["sinexctl", "recall", "yesterday"])
        .unwrap();
    let (name, args) = matches.subcommand().unwrap();
    let request = Request::from_args(
        name,
        arguments(sinex_protocol::command(name).unwrap(), args),
    )
    .unwrap();
    assert_eq!(
        request,
        Request::Recall {
            around: Some("yesterday".into()),
            cwd: None,
            page: None,
            cursor: None
        }
    );
}

#[devkit::test]
fn recall_cwd_can_follow_last_and_preserves_path_text() {
    let path = "/src/tide (notes) \" where cwds = /else";
    for words in [
        vec!["sinexctl", "recall", "last", "--cwd", path],
        vec!["sinexctl", "recall", "--cwd", path, "last"],
    ] {
        let matches = cli().try_get_matches_from(words).unwrap();
        let (name, args) = matches.subcommand().unwrap();
        let request = Request::from_args(
            name,
            arguments(sinex_protocol::command(name).unwrap(), args),
        )
        .unwrap();
        assert_eq!(
            request,
            Request::Recall {
                around: Some("last".into()),
                cwd: Some(path.into()),
                page: None,
                cursor: None
            }
        );
    }
    let matches = cli()
        .try_get_matches_from(["sinexctl", "recall", "2026-09-14", "08:00", "±90s"])
        .unwrap();
    let (name, args) = matches.subcommand().unwrap();
    let request = Request::from_args(
        name,
        arguments(sinex_protocol::command(name).unwrap(), args),
    )
    .unwrap();
    assert!(
        matches!(request, Request::Recall {around: Some(ref s), cwd: None, ..} if s == "2026-09-14 08:00 ±90s")
    );
}

#[devkit::test]
fn schema_field_discovery_uses_the_shared_request_table() {
    let matches = cli()
        .try_get_matches_from([
            "sinexctl",
            "fields",
            "--kind",
            "command.executed",
            "--page",
            "3",
        ])
        .unwrap();
    let (name, args) = matches.subcommand().unwrap();
    let request = Request::from_args(
        name,
        arguments(sinex_protocol::command(name).unwrap(), args),
    )
    .unwrap();
    assert_eq!(
        request,
        Request::Fields {
            kind: Some("command.executed".into()),
            page: Some(3),
            cursor: None
        }
    );
    assert!(sinex_protocol::command("fields").unwrap().mcp);
    assert!(sinex_protocol::command("kinds").unwrap().mcp);
}
