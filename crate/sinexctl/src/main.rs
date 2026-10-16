mod mcp;
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
    /// Serve the implemented read tools over the official rmcp stdio transport.
    Mcp,
    Status,
    Version,
    ConfigShow,
    Catalog,
    Sources,
    /// Recover the latest retained shell policy session, optionally scoped to a configured project.
    Recall {
        #[arg(value_parser=["last"])]
        target: String,
        #[arg(long)]
        project: Option<String>,
    },
    /// Read raw events or run supported indexed queries.
    #[command(
        after_help = r#"Pass the query as one shell argument after your endpoint/token options.

Raw events (available without an indexed projection):
  query 'events'
  query 'events where source = terminal.atuin-history and kind = command.executed'
  query 'events where payload.command ~ "cargo test"'

Indexed retrieval (requires a configured, caught-up projection):
  query 'commands where cargo and exit_code != 0 sort when desc limit 20'

commands selects canonical commands. Raw events use fixed pages of up to 100 rows;
continue with --cursor from the response. Raw queries do not support limit or sort.
Pipelines such as 'events | limit 20' are unsupported. Indexed limit is a clause,
as shown above. Unsupported query shapes return bad_query; an unavailable indexed
projection returns partial (exit 5)."#
    )]
    Query {
        /// Supported query text; quote it as one shell argument.
        query: String,
        /// Opaque continuation cursor from a response to the same query.
        #[arg(long)]
        cursor: Option<String>,
        /// Indexed page size (1-100); raw events use fixed 100-row pages.
        #[arg(long)]
        page: Option<u32>,
    },
    /// Exclude one raw Atuin occurrence, or remove its exclusion without readmitting it.
    Delete {
        reference: String,
        #[arg(long)]
        undo: bool,
        #[arg(long)]
        action_id: Option<String>,
        /// Optional exact source occurrence key; mismatches refuse.
        #[arg(long)]
        expected_key: Option<String>,
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
        #[arg(required=true,num_args=1..)]
        words: Vec<String>,
        #[arg(long)]
        query: Option<String>,
        #[arg(long, default_value = "20")]
        page: u32,
        #[arg(long)]
        cursor: Option<String>,
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
    if matches!(&args.command, Command::Mcp) {
        if args.query_timeout_ms.is_some() {
            fail("bad_query", "MCP tools accept their own bounded timeout_ms");
            return std::process::ExitCode::from(2);
        }
        return match mcp::run(args.endpoint, args.token_file) {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(_) => {
                fail("internal", "MCP transport or configured daemon unavailable");
                std::process::ExitCode::FAILURE
            }
        };
    }
    let mut wait_replay = false;
    let (method, mut params) = match args.command {
        Command::Mcp => unreachable!(),
        Command::Delete {
            reference,
            undo,
            action_id,
            expected_key,
        } => {
            let action_id=action_id.unwrap_or_else(|| {
                let id=sinex_protocol::fresh_action_id().to_string();
                eprintln!("{}",serde_json::json!({"kind":"operation","action_id":id,"message":"Retain this action identity to recover a lost delete/undo reply."}));
                id
            });
            (
                if undo {
                    Method::DeleteUndo
                } else {
                    Method::Delete
                },
                serde_json::json!({"ref":reference,"action_id":action_id,"expected_key":expected_key}),
            )
        }
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
        Command::Recall { target: _, project } => (
            Method::Query,
            serde_json::json!({"query":project.map_or("recall last".into(),|p|format!("recall last where project = {}",serde_json::to_string(&p).unwrap())),"page":100}),
        ),
        Command::Status => (Method::Status, serde_json::json!({})),
        Command::Version => (Method::Version, serde_json::json!({})),
        Command::ConfigShow => (Method::ConfigShow, serde_json::json!({})),
        Command::Catalog => (Method::Catalog, serde_json::json!({})),
        Command::Sources => (Method::Sources, serde_json::json!({})),
        Command::Query {
            query,
            cursor,
            page,
        } => (
            Method::Query,
            serde_json::json!({"query":query,"cursor":cursor,"page":page}),
        ),
        Command::Search {
            words,
            query,
            page,
            cursor,
        } => {
            let words = words
                .iter()
                .map(|w| {
                    if w.chars().any(char::is_whitespace) {
                        serde_json::to_string(w).unwrap()
                    } else {
                        w.clone()
                    }
                })
                .collect::<Vec<_>>()
                .join(" ");
            (
                Method::Search,
                serde_json::json!({"query":format!("{words}{}",query.map_or(String::new(),|q|format!(" {q}"))),"page":page,"cursor":cursor}),
            )
        }
        Command::Show { id } => (
            Method::Show,
            serde_json::json!({"ref":if id.starts_with("sinex:") {id} else {format!("sinex:ev/{id}")}}),
        ),
        Command::Trace { id, bytes } => (
            Method::Trace,
            serde_json::json!({"bytes":bytes,"ref":if id.starts_with("sinex:") {id} else {format!("sinex:ev/{id}")}}),
        ),
    };
    if !matches!(method, Method::Query | Method::Search) && args.query_timeout_ms.is_some() {
        fail("bad_query", "--query-timeout-ms requires query or search");
        return std::process::ExitCode::from(ErrorKind::BadQuery.exit_code());
    }
    let query_timeout_ms = args.query_timeout_ms.unwrap_or(60_000);
    if matches!(method, Method::Query | Method::Search) {
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
                if matches!(method, Method::Query | Method::Search) {
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
                    if matches!(method, Method::Delete | Method::DeleteUndo) {
                        return exclusion_indeterminate(&request.params["action_id"]);
                    }
                    fail(
                        "internal",
                        "daemon returned an incompatible response identity",
                    );
                    return std::process::ExitCode::FAILURE;
                }
                match reply.into_outcome() {
                    Ok(value) => {
                        if matches!(method, Method::Delete | Method::DeleteUndo)
                            && !exclusion_reply_matches(method, &request.params, &value)
                        {
                            return exclusion_indeterminate(&request.params["action_id"]);
                        }
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
            Err(_) if matches!(method, Method::Delete | Method::DeleteUndo) => {
                exclusion_indeterminate(&request.params["action_id"])
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

fn exclusion_indeterminate(action_id: &serde_json::Value) -> std::process::ExitCode {
    eprintln!(
        "{}",
        serde_json::json!({"kind":"partial","message":"Mutation outcome indeterminate; retry the same action identity.","details":{"action_id":action_id}})
    );
    std::process::ExitCode::from(ErrorKind::Partial.exit_code())
}

fn exclusion_reply_matches(
    method: Method,
    params: &serde_json::Value,
    value: &serde_json::Value,
) -> bool {
    let Ok(request) = serde_json::from_value::<sinex_protocol::ExclusionRequest>(params.clone())
    else {
        return false;
    };
    let Ok(outcome) = serde_json::from_value::<sinex_protocol::ExclusionOutcome>(value.clone())
    else {
        return false;
    };
    outcome.action_id == request.action_id
        && outcome.method == method.as_str()
        && request.event_id().ok() == Some(outcome.target)
        && matches!(
            outcome.state,
            sinex_protocol::ExclusionState::Completed | sinex_protocol::ExclusionState::Noop
        )
        && request.expected_key.as_ref().is_none_or(|key| {
            outcome
                .occurrence
                .as_ref()
                .is_some_and(|occurrence| &occurrence.equivalence_key == key)
        })
}
