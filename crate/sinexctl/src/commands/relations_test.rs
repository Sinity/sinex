#![allow(clippy::expect_used)]

use super::*;
use clap::CommandFactory;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn within_command_builds_relation_evidence_request() -> xtask::sandbox::TestResult<()> {
    let command = RelationsCommand::parse_from([
        "relations",
        "within",
        "--within-secs",
        "300",
        "--seed-query-json",
        r#"{"event_types":["command.executed"],"limit":5}"#,
    ]);

    let request = command.subcommand.request()?;
    assert_eq!(request.seed_query.limit, 5);
    assert!(request.candidate_query.is_none());
    assert!(matches!(
        request.relation,
        EventRelationExpr::Within { within_secs: 300 }
    ));
    Ok(())
}

#[sinex_test]
async fn same_field_parser_supports_payload_fields() -> xtask::sandbox::TestResult<()> {
    assert_eq!(
        parse_same_field("payload:project").map_err(|error| color_eyre::eyre::eyre!(error))?,
        SameField::Payload("project".to_string())
    );
    assert!(parse_same_field("payload:").is_err());
    Ok(())
}

#[sinex_test]
async fn relations_help_includes_seed_query_json_flag() -> TestResult<()> {
    let help = RelationsCommand::command().render_long_help().to_string();
    assert!(
        help.contains("--seed-query-json"),
        "events relations command must expose the seed query input"
    );

    Ok(())
}
