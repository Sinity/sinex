mod mcp;
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use sinex_protocol::rpc::{ErrorKind, Method, RpcId, RpcRequest, RpcResponse};
use std::path::PathBuf;
use std::process::ExitCode;

const DEFAULT_QUERY_TIMEOUT_MS: u64 = 60_000;

#[derive(Parser)]
#[command(version, about = "Command-line client for the Sinex daemon")]
struct Args {
    /// Daemon RPC URL.
    #[arg(long)]
    endpoint: String,
    /// File holding the bearer token.
    #[arg(long)]
    token_file: PathBuf,
    /// Output is always JSON; the flag is accepted and has no effect.
    #[arg(long, global = true)]
    json: bool,
    /// Deadline in milliseconds for query, search and recall, including the wait for a
    /// database connection. Default 60000.
    #[arg(long, global = true, value_parser = clap::value_parser!(u64).range(1..=3_600_000))]
    query_timeout_ms: Option<u64>,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Serve the read tools to an MCP client over stdio.
    Mcp,
    /// Show schema, writer and replay state and the supported sources and methods.
    Status,
    /// Show package and protocol versions.
    Version,
    /// Show the daemon configuration; secrets appear as file paths.
    ConfigShow,
    /// List the daemon's RPC methods.
    Catalog,
    /// Show each input's latest read, read history and record errors.
    Sources {
        /// Show read history for this input.
        #[arg(long)]
        input: Option<String>,
        /// Continue the input list after this input.
        #[arg(long)]
        after_input: Option<String>,
        /// With --input, continue the history before this commit sequence.
        #[arg(long)]
        before_seq: Option<i64>,
        /// With --input, list this observation's receipts.
        #[arg(long)]
        observation: Option<String>,
        /// With --observation, continue the receipts after this one.
        #[arg(long)]
        after_receipt: Option<String>,
    },
    /// Show the latest shell session, optionally only sessions in a configured project.
    Recall {
        #[arg(value_parser=["last"])]
        target: String,
        #[arg(long)]
        project: Option<String>,
    },
    /// Read raw events or run indexed queries.
    #[command(
        after_help = r#"Pass the query as one shell argument after --endpoint and --token-file.

Raw events (work without the search index):
  query 'events'
  query 'events where source = terminal.atuin-history and kind = command.executed'
  query 'events where payload.command ~ "cargo test"'

Indexed queries (need a configured search index that has caught up):
  query 'commands where cargo and exit_code != 0 sort when desc limit 20'

`commands` selects canonical commands. Raw events come in pages of up to 100 rows;
continue with --cursor from the response. Raw queries have no limit or sort, and
pipelines such as 'events | limit 20' are not supported. Unsupported queries
return bad_query (exit 2); an unavailable search index returns partial (exit 5)."#
    )]
    Query {
        /// Query text, quoted as one shell argument.
        query: String,
        /// Cursor from a previous response to the same query.
        #[arg(long)]
        cursor: Option<String>,
        /// Indexed page size (1-100); raw events always use 100-row pages.
        #[arg(long)]
        page: Option<u32>,
    },
    /// Exclude one raw Atuin occurrence from reads, or with --undo remove the exclusion.
    Delete {
        /// Event UUID or sinex:ev/UUID of a raw Atuin event.
        reference: String,
        #[arg(long)]
        undo: bool,
        /// Reuse this ID to retry an action; generated and printed when omitted.
        #[arg(long)]
        action_id: Option<String>,
        /// Refuse unless the occurrence has this key.
        #[arg(long)]
        expected_key: Option<String>,
    },
    /// Reparse stored Atuin material. Rerun with the same --action-id to resume.
    Replay {
        query: String,
        /// Show what would be replayed without starting.
        #[arg(long)]
        dry_run: bool,
        /// Reuse this ID to resume or retry; generated and printed when omitted.
        #[arg(long)]
        action_id: Option<String>,
        /// Refuse unless the selection still matches this value from --dry-run.
        #[arg(long)]
        basis: Option<String>,
        /// Not supported; replay always uses the current parser.
        #[arg(long)]
        parser: Option<String>,
    },
    /// Show a replay's progress.
    ReplayStatus { operation_id: String },
    /// Stop a replay between chunks.
    ReplayCancel { operation_id: String },
    /// Search shell commands by words; quote a phrase as one argument.
    Search {
        #[arg(required=true,num_args=1..)]
        words: Vec<String>,
        /// Extra query clauses, such as 'project = tide since 2026-01-01T00:00:00Z'.
        #[arg(long)]
        query: Option<String>,
        #[arg(long, default_value = "20")]
        page: u32,
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Show one event, command or session.
    Show { id: String },
    /// Follow an event back to its source records.
    Trace {
        id: String,
        /// Include the masked source bytes.
        #[arg(long)]
        bytes: bool,
    },
}

/// The query text for `recall last`, scoped to a project when given.
pub(crate) fn recall_query(project: Option<&str>) -> String {
    match project {
        Some(project) => format!("recall last where project = {}", json!(project)),
        None => "recall last".into(),
    }
}

/// Search words followed by optional extra clauses.
pub(crate) fn search_query(words: &str, clauses: Option<&str>) -> String {
    match clauses {
        Some(clauses) => format!("{words} {clauses}"),
        None => words.to_string(),
    }
}

fn event_ref(id: String) -> String {
    if id.starts_with("sinex:") {
        id
    } else {
        format!("sinex:ev/{id}")
    }
}

/// The given action ID, or a new one printed to stderr so the caller can retry with it.
fn action_id_or_new(action_id: Option<String>) -> String {
    action_id.unwrap_or_else(|| {
        let id = sinex_protocol::fresh_action_id().to_string();
        eprintln!(
            "{}",
            json!({"kind":"operation","action_id":id,"message":"Generated action ID; retry with --action-id to recover a lost reply."})
        );
        id
    })
}

/// The RPC method and params for a command. Errors are reported before returning.
fn build_request(command: Command) -> Result<(Method, Value), ExitCode> {
    Ok(match command {
        Command::Mcp => unreachable!("handled before building a request"),
        Command::Status => (Method::Status, json!({})),
        Command::Version => (Method::Version, json!({})),
        Command::ConfigShow => (Method::ConfigShow, json!({})),
        Command::Catalog => (Method::Catalog, json!({})),
        Command::Sources {
            input,
            after_input,
            before_seq,
            observation,
            after_receipt,
        } => (
            Method::Sources,
            json!({
                "input": input,
                "after_input": after_input,
                "before_seq": before_seq,
                "observation": observation,
                "after_receipt": after_receipt,
            }),
        ),
        Command::Recall { target: _, project } => (
            Method::Query,
            json!({"query": recall_query(project.as_deref()), "page": 100}),
        ),
        Command::Query {
            query,
            cursor,
            page,
        } => (
            Method::Query,
            json!({"query": query, "cursor": cursor, "page": page}),
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
                        json!(w).to_string()
                    } else {
                        w.clone()
                    }
                })
                .collect::<Vec<_>>()
                .join(" ");
            (
                Method::Search,
                json!({"query": search_query(&words, query.as_deref()), "page": page, "cursor": cursor}),
            )
        }
        Command::Show { id } => (Method::Show, json!({"ref": event_ref(id)})),
        Command::Trace { id, bytes } => {
            (Method::Trace, json!({"bytes": bytes, "ref": event_ref(id)}))
        }
        Command::Delete {
            reference,
            undo,
            action_id,
            expected_key,
        } => (
            if undo {
                Method::DeleteUndo
            } else {
                Method::Delete
            },
            json!({
                "ref": reference,
                "action_id": action_id_or_new(action_id),
                "expected_key": expected_key,
            }),
        ),
        Command::Replay {
            query,
            dry_run,
            action_id,
            basis,
            parser,
        } => {
            if parser.is_some() {
                return Err(fail(ErrorKind::BadQuery, "--parser is not supported"));
            }
            if dry_run && (action_id.is_some() || basis.is_some()) {
                return Err(fail(
                    ErrorKind::BadQuery,
                    "--dry-run does not take --action-id or --basis",
                ));
            }
            if dry_run {
                (Method::ReplayPlan, json!({"query": query}))
            } else {
                (
                    Method::ReplayStart,
                    json!({
                        "query": query,
                        "action_id": action_id_or_new(action_id),
                        "expected_basis": basis,
                    }),
                )
            }
        }
        Command::ReplayStatus { operation_id } => {
            (Method::ReplayStatus, json!({"operation_id": operation_id}))
        }
        Command::ReplayCancel { operation_id } => {
            (Method::ReplayCancel, json!({"operation_id": operation_id}))
        }
    })
}

fn main() -> ExitCode {
    let args = Args::parse();
    if matches!(args.command, Command::Mcp) {
        if args.query_timeout_ms.is_some() {
            return fail(
                ErrorKind::BadQuery,
                "--query-timeout-ms does not apply to mcp; MCP tools take timeout_ms",
            );
        }
        return match mcp::run(args.endpoint, args.token_file) {
            Ok(()) => ExitCode::SUCCESS,
            Err(_) => fail(
                ErrorKind::Internal,
                "MCP transport failed or the daemon is unavailable",
            ),
        };
    }
    let (method, mut params) = match build_request(args.command) {
        Ok(request) => request,
        Err(code) => return code,
    };
    let is_query = matches!(method, Method::Query | Method::Search);
    let is_exclusion = matches!(method, Method::Delete | Method::DeleteUndo);
    if !is_query && args.query_timeout_ms.is_some() {
        return fail(
            ErrorKind::BadQuery,
            "--query-timeout-ms applies only to query, search and recall",
        );
    }
    let query_timeout_ms = args.query_timeout_ms.unwrap_or(DEFAULT_QUERY_TIMEOUT_MS);
    if is_query {
        params["timeout_ms"] = json!(query_timeout_ms);
    }
    let token = match std::fs::read_to_string(&args.token_file) {
        Ok(s) if !s.trim().is_empty() => s.trim().to_string(),
        _ => return fail(ErrorKind::Internal, "cannot read the token file"),
    };
    // The HTTP deadline leaves the daemon time to report its own query timeout.
    let http_timeout_ms = if is_query {
        query_timeout_ms + 5_000
    } else {
        10_000
    };
    let client = match reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_millis(http_timeout_ms))
        .build()
    {
        Ok(client) => client,
        Err(_) => return fail(ErrorKind::Internal, "cannot start the HTTP client"),
    };
    let action_id = params["action_id"].clone();
    let mut request = RpcRequest::new(RpcId::Number(1), method, params);
    loop {
        let reply = client
            .post(&args.endpoint)
            .bearer_auth(&token)
            .json(&request)
            .send()
            .and_then(|r| r.json::<RpcResponse>());
        let reply = match reply {
            Ok(reply) => reply,
            Err(_) if is_exclusion => return exclusion_unknown(&action_id),
            Err(error) if error.is_timeout() => {
                return fail(
                    ErrorKind::Budget,
                    "no complete reply from the daemon before the deadline",
                );
            }
            Err(_) => {
                return fail(
                    ErrorKind::Internal,
                    "daemon unavailable or its reply is not valid JSON-RPC",
                );
            }
        };
        if (reply.outcome().is_ok() || reply.id().is_some())
            && reply.id() != Some(&RpcId::Number(1))
        {
            if is_exclusion {
                return exclusion_unknown(&action_id);
            }
            return fail(ErrorKind::Internal, "daemon reply has the wrong request ID");
        }
        let value = match reply.into_outcome() {
            Ok(value) => value,
            Err(e) => {
                eprintln!(
                    "{}",
                    json!({"kind":e.kind,"message":e.message,"details":e.data})
                );
                return ExitCode::from(e.kind.exit_code());
            }
        };
        if is_exclusion && !exclusion_reply_matches(method, &request.params, &value) {
            return exclusion_unknown(&action_id);
        }
        // `replay` waits for the operation to finish.
        if method == Method::ReplayStart
            && matches!(value["state"].as_str(), Some("recorded" | "running"))
        {
            request = RpcRequest::new(
                RpcId::Number(1),
                Method::ReplayStatus,
                json!({"operation_id": value["operation_id"]}),
            );
            std::thread::sleep(std::time::Duration::from_millis(25));
            continue;
        }
        println!("{value}");
        return if value.get("complete") == Some(&Value::Bool(false)) {
            ExitCode::from(ErrorKind::Partial.exit_code())
        } else {
            ExitCode::SUCCESS
        };
    }
}

/// Prints an error in the daemon's error shape and returns its exit code.
fn fail(kind: ErrorKind, message: &str) -> ExitCode {
    eprintln!("{}", json!({"kind":kind,"message":message,"details":null}));
    ExitCode::from(kind.exit_code())
}

fn exclusion_unknown(action_id: &Value) -> ExitCode {
    eprintln!(
        "{}",
        json!({"kind":"partial","message":"Outcome unknown; retry with the same action ID.","details":{"action_id":action_id}})
    );
    ExitCode::from(ErrorKind::Partial.exit_code())
}

/// Whether a delete or undo reply is for this request and reports success.
fn exclusion_reply_matches(method: Method, params: &Value, value: &Value) -> bool {
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
