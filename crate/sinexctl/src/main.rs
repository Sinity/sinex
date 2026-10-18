//! `sinexctl`: the CLI and the MCP server, generated from the command table in
//! `sinex-protocol`, calling the API. A small offline mode diagnoses a stopped
//! daemon without writing anything (architecture §7.2).
use clap::Parser;

#[derive(Parser)]
#[command(name = "sinexctl", about = "Query and operate Sinex")]
struct Cli {
    /// A command from the command table, such as `status` or `excise <event>`.
    command: Vec<String>,
}

fn main() {
    let _cli = Cli::parse();
    todo!(
        "architecture §7.2: commands generated from sinex_protocol::Request; API client; MCP over rmcp; offline mode via sinex-offline"
    )
}
