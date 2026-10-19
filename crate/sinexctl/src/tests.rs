use super::*;

#[test]
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
