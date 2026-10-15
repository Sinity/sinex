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
    #[arg(long, global = true, value_parser = clap::value_parser!(u64).range(1..=3_600_000))]
    query_timeout_ms: Option<u64>,
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
    /// Reparse one closed retained Atuin material. Repeat an explicit action to resume.
    Replay {
        query: String,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        action_id: Option<String>,
        #[arg(long)]
        basis: Option<String>,
        #[arg(long)]
        parser: Option<String>,
    },
    ReplayStatus {
        operation_id: String,
    },
    ReplayCancel {
        operation_id: String,
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
    let mut wait_replay = false;
    let (method, mut params) = match args.command {
        Command::Replay {
            query,
            dry_run,
            action_id,
            basis,
            parser,
        } => {
            if parser.is_some() || (dry_run && (action_id.is_some() || basis.is_some())) {
                fail(
                    "bad_query",
                    "--dry-run does not take an action/basis; parser overrides are unavailable",
                );
                return std::process::ExitCode::from(2);
            }
            if dry_run {
                (Method::ReplayPlan, serde_json::json!({"query":query}))
            } else {
                wait_replay = true;
                let action_id=action_id.unwrap_or_else(|| {
                    let id=sinex_protocol::fresh_action_id().to_string();
                    eprintln!("{}",serde_json::json!({"kind":"operation","action_id":id,"message":"Replay action identity; retain it to recover an interrupted reply."}));
                    id
                });
                (
                    Method::ReplayStart,
                    serde_json::json!({"query":query,"action_id":action_id,"expected_basis":basis}),
                )
            }
        }
        Command::ReplayStatus { operation_id } => (
            Method::ReplayStatus,
            serde_json::json!({"operation_id":operation_id}),
        ),
        Command::ReplayCancel { operation_id } => (
            Method::ReplayCancel,
            serde_json::json!({"operation_id":operation_id}),
        ),
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
    if method != Method::Query && args.query_timeout_ms.is_some() {
        fail("bad_query", "--query-timeout-ms requires query or search");
        return std::process::ExitCode::from(ErrorKind::BadQuery.exit_code());
    }
    let query_timeout_ms = args.query_timeout_ms.unwrap_or(60_000);
    if method == Method::Query {
        params["timeout_ms"] = serde_json::json!(query_timeout_ms);
    }
    let token = match std::fs::read_to_string(&args.token_file) {
        Ok(s) if !s.trim().is_empty() => s.trim().to_string(),
        _ => {
            fail("internal", "cannot read configured token file");
            return std::process::ExitCode::FAILURE;
        }
    };
    let mut request = RpcRequest::new(RpcId::Number(1), method, params);
    loop {
        let result = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_millis(
                if method == Method::Query {
                    query_timeout_ms + 5_000
                } else {
                    10_000
                },
            ))
            .build()
            .and_then(|c| {
                c.post(&args.endpoint)
                    .bearer_auth(&token)
                    .json(&request)
                    .send()
            })
            .and_then(|r| r.json::<RpcResponse>());
        return match result {
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
                        if wait_replay
                            && matches!(value["state"].as_str(), Some("recorded" | "running"))
                        {
                            request = RpcRequest::new(
                                RpcId::Number(1),
                                Method::ReplayStatus,
                                serde_json::json!({"operation_id":value["operation_id"]}),
                            );
                            std::thread::sleep(std::time::Duration::from_millis(25));
                            continue;
                        }
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
        };
    }
}
fn fail(kind: &str, message: &str) {
    eprintln!(
        "{}",
        serde_json::json!({"kind":kind,"message":message,"details":null})
    );
}
