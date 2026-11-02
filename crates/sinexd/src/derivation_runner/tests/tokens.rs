use super::*;
use sinex_model::event_types::CommandReported;
use sinex_model::event_types::session::TerminalReport;

/// The token the shell integration mints for a command started at `secs`.
fn token(secs: i64, n: u32) -> String {
    let ms = u64::try_from(T + secs).unwrap() * 1000;
    format!(
        "{:08x}-{:04x}-7{:03x}-8000-{:012x}",
        ms >> 16,
        ms & 0xffff,
        n,
        u64::from(n)
    )
}

fn report(execution: &str, secs: i64, duration_secs: i64) -> Event {
    Event::build(
        &CommandReported {
            execution: execution.into(),
            shell: "zsh".into(),
            host: "box".into(),
            command: "make".into(),
            cwd: Some("/p".into()),
            started_at: at(secs),
            duration_ns: Some(duration_secs * 1_000_000_000),
            exit_code: Some(0),
            session: Some("4242".into()),
            atuin_id: None,
            git_identity: None,
            extra: Default::default(),
        },
        "session.integrations",
        location(0),
        ReadingOrder(0),
        *blake3::hash(format!("report{execution}").as_bytes()).as_bytes(),
        Class::Guarded,
    )
    .unwrap()
}

fn kitty_end(execution: &str, secs: i64) -> Event {
    let mut extra = serde_json::Map::new();
    extra.insert(
        "terminal_identity".into(),
        json!({"host": "box", "adapter": "kitty"}),
    );
    Event::build(
        &TerminalReport {
            received_at: at(secs),
            terminal: "kitty".into(),
            execution: Some(execution.into()),
            event: "cmd_end".into(),
            window_id: Some(7),
            cwd: Some("/p".into()),
            cmdline: Some("make".into()),
            exit_status: Some(0),
            title: None,
            output: None,
            extra,
        },
        "session.integrations",
        location(1),
        ReadingOrder(0),
        *blake3::hash(format!("end{execution}{secs}").as_bytes()).as_bytes(),
        Class::Guarded,
    )
    .unwrap()
}

/// law: fus.token-exact
#[devkit::test]
async fn a_token_read_reaches_the_commands_end_and_stops_at_its_lifetime(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    // A two-hour command: its end lies beyond the hour's reach but inside
    // the token's lifetime, so the read finds it and the two join.
    let long = token(10, 1);
    // A five-second command whose token reappears three days later: outside
    // the lifetime, so neither side's read reaches the other.
    let short = token(20, 2);
    w.admit(vec![
        report(&long, 10, 7_200),
        kitty_end(&long, 10 + 7_200),
        report(&short, 20, 5),
        kitty_end(&short, 20 + 3 * 86_400),
    ])
    .await;
    let mut d = w.runner().await;
    d.catch_up().await.unwrap();
    let fused: Vec<CommandFused> = w.live().await;
    assert_eq!(fused.len(), 1, "{fused:?}");
    assert_eq!(fused[0].execution.as_deref(), Some(long.as_str()));
    assert_eq!(fused[0].members.len(), 2);
}
