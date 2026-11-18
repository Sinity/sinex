//! Typed declarations for the selection-first surface. Runtime handlers bind
//! these declarations; transports do not reconstruct another schema.
mod steps;
use command_surface::{
    ArgumentForm, ArgumentSpec, Deadlines, LatencyClass, OperationDeclarations, OperationSpec,
    RequestOption, TextStyle, ToolFamily,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_protocol::rows::{Rows, RowsReply};
use std::sync::{Arc, LazyLock};
pub use steps::steps;

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
        table.accept(RequestOption::Cursor);
        table.accept(RequestOption::BudgetTokens);
        table.accept(RequestOption::Format);
        table
            .declare_tool(ToolFamily {
                name: "read".into(),
                description: concat!(
                    "Read stored evidence, its provenance and capture status. Follow returned ready requests.\n",
                    "Examples:\n",
                    "{\"arguments\":{\"selection\":\"events\"}}\n",
                    "{\"arguments\":{\"selection\":\"events\",\"steps\":[{\"verb\":\"read\"}]}}\n",
                    "{\"operation\":\"status\",\"arguments\":{}}",
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
        rows.latency = LatencyClass::Search;
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
        table
            .declare::<Status, serde_json::Value>(status, render_status)
            .expect("status request matches its declaration");
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
