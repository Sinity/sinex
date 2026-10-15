use clap::{Parser, Subcommand};
use sinex_protocol::rpc::{ErrorKind, Method, RpcId, RpcRequest, RpcResponse};
use std::path::PathBuf;
#[derive(Parser)]
#[command(
    version,
    about = "Sinex foundation client; no installed-service fallback"
)]
struct Args {
    #[arg(long)]
    endpoint: String,
    #[arg(long)]
    token_file: PathBuf,
    #[arg(long, global = true)]
    json: bool,
    /// Query execution deadline; includes PostgreSQL pool acquisition and all page statements.
    #[arg(long, global = true, default_value_t = 60_000, value_parser = clap::value_parser!(u64).range(1..=3_600_000))]
    query_timeout_ms: u64,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Status,
    Version,
    ConfigShow,
    Catalog,
    Sources,
    Query {
        query: String,
        #[arg(long)]
        cursor: Option<String>,
    },
    Search {
        text: String,
    },
    Show {
        id: String,
    },
    Trace {
        id: String,
        #[arg(long)]
        bytes: bool,
    },
}
fn main() -> std::process::ExitCode {
    let args = Args::parse();
    let (method, mut params) = match args.command {
        Command::Status => (Method::Status, serde_json::json!({})),
        Command::Version => (Method::Version, serde_json::json!({})),
        Command::ConfigShow => (Method::ConfigShow, serde_json::json!({})),
        Command::Catalog => (Method::Catalog, serde_json::json!({})),
        Command::Sources => (Method::Sources, serde_json::json!({})),
        Command::Query { query, cursor } => (
            Method::Query,
            serde_json::json!({"query":query,"cursor":cursor}),
        ),
        Command::Search { text } => (
            Method::Query,
            serde_json::json!({"query":format!("events where payload.command ~ {}",serde_json::to_string(&text).expect("text encoding"))}),
        ),
        Command::Show { id } => (
            Method::Show,
            serde_json::json!({"ref":if id.starts_with("sinex:") {id} else {format!("sinex:ev/{id}")}}),
        ),
        Command::Trace { id, bytes } => (
            Method::Trace,
            serde_json::json!({"bytes":bytes,"ref":if id.starts_with("sinex:") {id} else {format!("sinex:ev/{id}")}}),
        ),
    };
    if method == Method::Query {
        params["timeout_ms"] = serde_json::json!(args.query_timeout_ms);
    }
    let token = match std::fs::read_to_string(&args.token_file) {
        Ok(s) if !s.trim().is_empty() => s.trim().to_string(),
        _ => {
            fail("internal", "cannot read configured token file");
            return std::process::ExitCode::FAILURE;
        }
    };
    let request = RpcRequest::new(RpcId::Number(1), method, params);
    let result = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_millis(
            if method == Method::Query {
                args.query_timeout_ms + 5_000
            } else {
                10_000
            },
        ))
        .build()
        .and_then(|c| {
            c.post(&args.endpoint)
                .bearer_auth(token)
                .json(&request)
                .send()
        })
        .and_then(|r| r.json::<RpcResponse>());
    match result {
        Ok(reply) => {
            if (reply.outcome().is_ok() || reply.id().is_some())
                && reply.id() != Some(&RpcId::Number(1))
            {
                fail(
                    "internal",
                    "daemon returned an incompatible response identity",
                );
                return std::process::ExitCode::FAILURE;
            }
            match reply.into_outcome() {
                Ok(value) => {
                    println!("{}", value);
                    if value.get("complete") == Some(&serde_json::Value::Bool(false)) {
                        std::process::ExitCode::from(ErrorKind::Partial.exit_code())
                    } else {
                        std::process::ExitCode::SUCCESS
                    }
                }
                Err(e) => {
                    eprintln!(
                        "{}",
                        serde_json::json!({"kind":e.kind,"message":e.message,"details":e.data})
                    );
                    std::process::ExitCode::from(e.kind.exit_code())
                }
            }
        }
        Err(error) if error.is_timeout() => {
            fail(
                "budget",
                "configured daemon transport deadline exceeded; no complete response received",
            );
            std::process::ExitCode::from(ErrorKind::Budget.exit_code())
        }
        Err(_) => {
            fail(
                "internal",
                "configured daemon unavailable or returned an invalid response",
            );
            std::process::ExitCode::from(ErrorKind::Internal.exit_code())
        }
    }
}
fn fail(kind: &str, message: &str) {
    eprintln!(
        "{}",
        serde_json::json!({"kind":kind,"message":message,"details":null})
    );
}
