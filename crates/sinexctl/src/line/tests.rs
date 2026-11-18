use super::*;

fn words(args: &[&str]) -> Vec<String> {
    args.iter().map(|word| (*word).into()).collect()
}
fn rows(args: &[&str]) -> Line {
    parse(&words(args)).unwrap().unwrap()
}

#[devkit::test]
fn existing_commands_keep_precedence_and_their_original_options() {
    for command in sinex_protocol::COMMANDS {
        for prefix in [
            vec![],
            vec!["--json"],
            vec!["--all"],
            vec!["--endpoint=http://127.0.0.1:7171"],
            vec!["--deadline", "none"],
        ] {
            let mut args = prefix;
            args.push(command.name);
            assert!(parse(&words(&args)).unwrap().is_none(), "{}", command.name);
        }
    }
    assert!(parse(&words(&["--help"])).unwrap().is_none());
    assert!(parse(&words(&["--json"])).unwrap().is_none());
}

#[devkit::test]
fn typed_steps_keep_argument_ownership_and_request_metadata() {
    let line = rows(&[
        "--page-size",
        "2",
        "--cursor",
        "population",
        "events",
        "read",
        "--view",
        "trace",
        "--cursor",
        "walk",
        "--page-size",
        "3",
        "save",
        "read",
        "--parameter",
        "source:text",
        "--deadline=none",
        "--request-id",
        "fixture",
        "--json",
    ]);
    assert_eq!(line.request.arguments["selection"], "events");
    assert_eq!(line.request.arguments["page_size"], 2);
    assert_eq!(line.request.cursor.as_deref(), Some("population"));
    assert_eq!(
        line.request.arguments["steps"],
        serde_json::json!([
            {"verb":"read","view":"trace","cursor":"walk","page_size":3},
            {"verb":"save","name":"read","parameter":["source:text"]}
        ])
    );
    assert_eq!(line.request.deadline, command_surface::Deadline::None);
    assert_eq!(line.request.request_id, "fixture");
    assert_eq!(line.request.format, Format::Json);
}

#[devkit::test]
fn literal_selection_and_step_words_do_not_take_stdin_or_command_meaning() {
    let line = rows(&["--", "status", "save", "--", "read"]);
    assert_eq!(line.request.arguments["selection"], "status");
    assert_eq!(line.request.arguments["steps"][0]["name"], "read");
    let line = rows(&["--", "-", "read"]);
    assert_eq!(line.stdin, Stdin::Unused);
    assert_eq!(line.request.arguments["selection"], "-");
    let line = rows(&["-", "read"]);
    assert_eq!(line.stdin, Stdin::References);
    assert_eq!(line.request.arguments["references"], serde_json::json!([]));
}

#[devkit::test]
fn bad_words_and_options_are_refused_before_io() {
    for args in [
        vec!["events", "where", "source", "=", "fixture"],
        vec!["events", "read", "--parameter", "x:text"],
        vec!["events", "save"],
        vec!["--page-size", "0", "events"],
        vec!["--deadline", "later", "events"],
        vec!["--mystery", "events"],
        vec!["--", "events", "save", "name", "--extra", "x"],
    ] {
        assert!(parse(&words(&args)).is_err(), "{args:?}");
    }
}

#[devkit::test]
fn explicit_requests_keep_their_original_identity_and_options() {
    let original = rows(&[
        "events",
        "--cursor",
        "page",
        "save",
        "fixture",
        "--request-id",
        "original",
        "--budget",
        "31",
        "--deadline",
        "none",
    ])
    .request;
    let json = serde_json::to_string(&original).unwrap();
    let line = rows(&["--endpoint", "http://127.0.0.1:7171", "--request", &json]);
    assert_eq!(line.request, original);
    assert_eq!(
        line.options.endpoint.as_deref(),
        Some("http://127.0.0.1:7171")
    );
    assert!(parse(&words(&["--deadline", "5", "--request", &json])).is_err());
    let other = rows(&["events", "save", "fixture"]).request;
    let another = rows(&["events", "save", "fixture"]).request;
    assert_ne!(
        other.request_id, another.request_id,
        "distinct CLI invocations are distinct intentions"
    );
}

#[devkit::test]
fn a_literal_json_word_does_not_change_failure_presentation() {
    let (_, json) = parse(&words(&["events", "save", "--", "--json", "--unknown"]))
        .err()
        .unwrap();
    assert!(!json);
    let (_, json) = parse(&words(&["--json", "events", "read", "--unknown"]))
        .err()
        .unwrap();
    assert!(json);
}
