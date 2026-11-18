//! Typed declarations for the selection-first surface. Runtime handlers bind
//! these declarations; transports do not reconstruct another schema.
mod steps;
use command_surface::{
    ArgumentForm, ArgumentSpec, Deadlines, FailureKind, LatencyClass, OperationDeclarations,
    OperationSpec, RequestOption, TextStyle, ToolFamily,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_protocol::rows::{Rows, RowsReply};
use std::sync::{Arc, LazyLock};
pub use steps::{required_access, steps};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Status {}

pub fn declarations() -> &'static Arc<OperationDeclarations> {
    static DECLARATIONS: LazyLock<Arc<OperationDeclarations>> = LazyLock::new(|| {
        let mut deadlines = Deadlines::default();
        for command in sinex_protocol::COMMANDS {
            deadlines.set(command.latency(), Some(command.default_deadline()));
        }
        let mut table = OperationDeclarations::builder("sinex", deadlines);
        table
            .declare_cli_global("endpoint", "daemon socket or HTTP endpoint")
            .expect("endpoint is a calling-process option");
        table
            .declare_cli_global("token_file", "file holding the API token")
            .expect("token file is a calling-process option");
        table.accept(RequestOption::Cursor);
        table.accept(RequestOption::BudgetTokens);
        table.accept(RequestOption::Format);
        for (code, kind) in [
            ("no_writer", FailureKind::NotConfigured),
            ("read_pool_busy", FailureKind::Timeout),
            ("statement_timeout", FailureKind::Timeout),
            ("database_unavailable", FailureKind::NotRunning),
            ("writer_stopped", FailureKind::NotRunning),
            ("storage_refused", FailureKind::Refused),
            ("request_conflict", FailureKind::Conflict),
            ("stale", FailureKind::Conflict),
        ] {
            table
                .register_failure_code(code, kind)
                .expect("request failure has one classification");
        }
        table
            .declare_tool(ToolFamily {
                name: "read".into(),
                description: concat!(
                    "Read stored evidence, its provenance, capture status and read-only SQL. Follow returned ready requests.\n",
                    "Examples:\n",
                    "{\"arguments\":{\"selection\":\"events\"}}\n",
                    "{\"arguments\":{\"selection\":\"events\",\"steps\":[{\"verb\":\"read\"}]}}\n",
                    "{\"operation\":\"status\",\"arguments\":{}}\n",
                    "{\"operation\":\"sql\",\"arguments\":{\"sql\":\"SELECT 1 AS number\"}}",
                ).into(),
                open_world: false,
                idempotent: true,
            })
            .expect("read tool is declared once");
        let mut rows = OperationSpec::request(
            "rows",
            &[],
            "read",
            "Read a selection and apply its typed steps",
            "A selection and its ordered steps are one question",
        );
        rows.failures = vec![
            FailureKind::InvalidQuery,
            FailureKind::UnknownField,
            FailureKind::BadValue,
            FailureKind::NotAQuery,
            FailureKind::Ambiguous,
            FailureKind::StaleCursor,
            FailureKind::NotFound,
            FailureKind::Refused,
            FailureKind::NotConfigured,
            FailureKind::NotRunning,
            FailureKind::Internal,
        ];
        rows.latency = LatencyClass::Search;
        rows.access = command_surface::Access::Permanent;
        rows.http_route = Some("/v1/rows".into());
        rows.arguments = vec![ArgumentSpec::new(
            "selection",
            ArgumentForm::Positional,
            "one query or source reference",
        )];
        table
            .declare::<Rows, RowsReply>(rows, render_page)
            .expect("rows request and reply match their declaration");
        table
            .declare_steps("rows", "steps", steps().clone())
            .expect("rows owns the declared step field");
        let mut status = OperationSpec::request(
            "status",
            &["status"],
            "read",
            "Show capture and consumer progress",
            "Health describes the system rather than selected evidence rows",
        );
        status.latency = LatencyClass::Status;
        status.access = command_surface::Access::Permanent;
        table
            .declare::<Status, serde_json::Value>(status, render_status)
            .expect("status request matches its declaration");
        let mut sql = OperationSpec::request(
            "sql",
            &["dev", "sql"],
            "read",
            "Run one read-only SQL statement",
            "The reader role, grants and statement remain bound across pages",
        );
        sql.failures = vec![
            FailureKind::BadValue,
            FailureKind::Refused,
            FailureKind::NotFound,
            FailureKind::NotRunning,
            FailureKind::StaleCursor,
            FailureKind::Internal,
        ];
        sql.latency = LatencyClass::Aggregate;
        sql.access = command_surface::Access::Permanent;
        sql.http_route = Some("/v1/sql".into());
        sql.arguments = vec![
            ArgumentSpec::new("sql", ArgumentForm::Rest, "one read-only SQL statement"),
            ArgumentSpec::new(
                "page_size",
                ArgumentForm::Named,
                "maximum rows in a transport page",
            ),
        ];
        table
            .declare::<sinex_protocol::sql::Sql, sinex_protocol::sql::SqlReply>(sql, |page, _| {
                serde_json::to_string_pretty(page).expect("SQL page serializes")
            })
            .expect("SQL request and page match their declaration");
        table
            .set_tool_default("read", "rows")
            .expect("rows belongs to the read tool");
        table.finish()
    });
    &DECLARATIONS
}

fn render_page(page: &RowsReply, _: TextStyle) -> String {
    serde_json::to_string_pretty(page).expect("query page serializes")
}

fn render_status(status: &serde_json::Value, _: TextStyle) -> String {
    serde_json::to_string_pretty(status).expect("status serializes")
}
