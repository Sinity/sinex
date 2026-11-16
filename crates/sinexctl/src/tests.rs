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
            "--page-size",
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
    assert_eq!(args["page-size"], Value::from(5u64));
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
            page_size: None,
            cursor: None
        }
    );
}

#[devkit::test]
fn stage_takes_several_paths_and_its_options() {
    let matches = cli()
        .try_get_matches_from([
            "sinexctl",
            "stage",
            "/synthetic/takeout-001.zip",
            "/synthetic/takeout-002.zip",
            "--parser",
            "takeout",
            "--account",
            "account@example.test",
            "--export-date",
            "2030-01-01",
            "--follow",
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
        Request::Stage {
            path: vec![
                "/synthetic/takeout-001.zip".into(),
                "/synthetic/takeout-002.zip".into()
            ],
            parser: vec!["takeout".into()],
            account: Some("account@example.test".into()),
            export_date: Some("2030-01-01".into()),
            follow: Some(true),
            batch_events: None,
        }
    );
    assert_eq!(request.access(), sinex_protocol::Access::Agent);
    let error = cli()
        .try_get_matches_from(["sinexctl", "stage"])
        .unwrap_err();
    assert_eq!(
        error.kind(),
        clap::error::ErrorKind::MissingRequiredArgument
    );
}

#[devkit::test]
fn cli_discovery_lists_stage_decisions_and_recall() {
    for name in ["stage", "same", "distinct", "withdraw-decision"] {
        assert!(cli().find_subcommand(name).is_some(), "{name}");
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
            page_size: None,
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
                page_size: None,
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
            "--page-size",
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
            page_size: Some(3),
            cursor: None
        }
    );
    assert!(sinex_protocol::command("fields").unwrap().mcp);
    assert!(sinex_protocol::command("kinds").unwrap().mcp);
}

#[devkit::test]
fn read_deadline_is_global_and_typed_before_the_query_text() {
    use command_surface::Deadline;
    for (args, expected) in [
        (
            vec!["sinexctl", "--deadline", "200", "query", "events"],
            Deadline::Milliseconds(200),
        ),
        (
            vec!["sinexctl", "query", "--deadline", "none", "events"],
            Deadline::None,
        ),
    ] {
        let matches = cli().try_get_matches_from(args).unwrap();
        assert_eq!(matches.get_one::<Deadline>("deadline"), Some(&expected));
        let (name, sub) = matches.subcommand().unwrap();
        let request =
            Request::from_args(name, arguments(sinex_protocol::command(name).unwrap(), sub))
                .unwrap();
        assert!(matches!(request, Request::Query {query,..} if query=="events"));
    }
    assert!(
        cli()
            .try_get_matches_from(["sinexctl", "--deadline", "-1", "query", "events"])
            .is_err()
    );
}

#[devkit::test]
fn help_starts_with_status_and_query_examples() {
    let rendered = cli().render_help().to_string();
    let first_commands: Vec<_> = rendered
        .lines()
        .skip_while(|line| *line != "Commands:")
        .skip(1)
        .take(5)
        .filter_map(|line| line.split_whitespace().next())
        .collect();
    assert_eq!(
        first_commands,
        ["status", "query", "recall", "brief", "search"]
    );
    for command in COMMANDS {
        let help = sinex_protocol::cli::subcommand(command)
            .render_long_help()
            .to_string();
        assert!(help.contains("Examples:"), "{}: {help}", command.name);
        for example in command.examples {
            assert!(help.contains(example), "{example}: {help}");
        }
    }
    let help = sinex_protocol::cli::subcommand(sinex_protocol::command("query").unwrap())
        .render_long_help()
        .to_string();
    for word in [
        "events",
        "commands",
        "shell-sessions",
        "materials",
        "runs",
        "sources",
        "archive",
        "failures",
        "docs/query.md",
    ] {
        assert!(help.contains(word), "{word}: {help}");
    }
}

#[devkit::test]
fn replay_and_whole_answer_options_stay_distinct() {
    let matches = cli()
        .try_get_matches_from(["sinexctl", "--all", "replay", "--every-change", "--dry-run"])
        .unwrap();
    assert!(matches.get_flag("all"));
    let (name, sub) = matches.subcommand().unwrap();
    let request =
        Request::from_args(name, arguments(sinex_protocol::command(name).unwrap(), sub)).unwrap();
    assert!(matches!(
        request,
        Request::Replay {
            every_change: Some(true),
            dry_run: Some(true),
            ..
        }
    ));
}

#[devkit::test]
fn list_page_hint_keeps_the_filters() {
    let request = Request::Comparisons {
        derivation: Some("shell.sessions".into()),
        page_size: Some(3),
        cursor: Some("previous".into()),
    };
    assert_eq!(
        next_page_command(sinex_protocol::command("comparisons").unwrap(), &request),
        "sinexctl comparisons 'shell.sessions' --page-size '3'"
    );
}
