use sinex_primitives::SinexEnvironment;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn raw_event_subject_encoding_keeps_dot_and_underscore_distinct() -> TestResult<()> {
    let env = SinexEnvironment::new("dev")?;

    let dotted = env.nats_raw_event_subject_with_namespace(None, "shell.history", "line.added");
    let underscored =
        env.nats_raw_event_subject_with_namespace(None, "shell_history", "line_added");

    assert_ne!(dotted, underscored);
    assert_eq!(dotted, "dev.events.raw.shell_d_history.line_d_added");
    assert_eq!(underscored, "dev.events.raw.shell_u_history.line_u_added");
    Ok(())
}

#[sinex_test]
async fn raw_event_subject_encoding_keeps_namespace_shape() -> TestResult<()> {
    let env = SinexEnvironment::new("dev")?;

    let subject =
        env.nats_raw_event_subject_with_namespace(Some("suite-a"), "gateway.test", "inline.event");

    assert_eq!(subject, "dev.suite-a.events.raw.gateway_d_test.inline_d_event");
    Ok(())
}
