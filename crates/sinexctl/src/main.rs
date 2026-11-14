//! `sinexctl`: the CLI and the MCP server, generated from the command table in
//! `sinex-protocol`, calling the daemon's API. A small offline mode diagnoses a
//! stopped daemon without writing anything.
//!
//! Exit codes come from the shared failure table in `command-surface`: 0
//! done, 1 a read that matched nothing, and for a failure its kind's code (2
//! a request to fix, 3 refused, 4 not found, a conflict or a broken part, 5
//! stopped early, 6 the daemon or a part it needs not running, 7 timed out).
//! With `--json` a failure prints `{"error": …}` on standard output and
//! nothing on standard error.
mod client;
mod mcp;
mod output;

mod diagnostics;

use clap::{Arg, ArgAction, ArgMatches, Command};
use client::{Client, Endpoint};
use command_surface::{Failure, FailureKind};
use serde_json::{Map, Value, json};
use sinex_protocol::{COMMANDS, Reply, Request};
use std::path::PathBuf;
use std::process::ExitCode;

/// The exit code of a read that succeeded and matched nothing.
const EMPTY: u8 = 1;

fn default_socket() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR")
        .map(|d| PathBuf::from(d).join("sinex/sinexd.sock"))
        .unwrap_or_else(|| PathBuf::from("/run/sinex/sinexd.sock"))
}

fn default_token_file() -> PathBuf {
    std::env::var_os("SINEX_TOKEN_FILE")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
                .map(|c| c.join("sinex/token"))
        })
        .unwrap_or_else(|| PathBuf::from("/etc/sinex/token"))
}

/// The CLI, generated from the command table, plus the local commands.
fn cli() -> Command {
    let mut app =
        Command::new("sinexctl")
            .about("Query and operate Sinex")
            .after_help(
                "Options go before a query or words: everything after the text starts is text, \
                 so `sinexctl search ls -la` searches for `ls -la`.",
            )
            .version(env!("CARGO_PKG_VERSION"))
            .arg(Arg::new("deadline").long("deadline").global(true)
                .value_name("MS|none").value_parser(sinex_protocol::deadline)
                .help("read execution budget in milliseconds; none waits without a deadline"))
            .arg(Arg::new("endpoint").long("endpoint").global(true).help(
                "unix:/path/to/socket or http://127.0.0.1:PORT (default: $SINEX_ENDPOINT, else the daemon's socket)",
            ))
            .arg(Arg::new("token-file").long("token-file").global(true).help(
                "file holding the API token (default: $SINEX_TOKEN_FILE, ~/.config/sinex/token)",
            ))
            .arg(
                Arg::new("json")
                    .long("json")
                    .global(true)
                    .action(ArgAction::SetTrue)
                    .help("print results as JSON"),
            )
            .arg(
                Arg::new("all")
                    .long("all")
                    .global(true)
                    .action(ArgAction::SetTrue)
                    .help("page through the whole answer"),
            );
    for c in COMMANDS {
        let mut sub = sinex_protocol::cli::subcommand(c);
        if c.name == "status" {
            sub =
                sub.arg(Arg::new("capture-state").long("capture-state").help(
                    "local capture state directory (default: $SINEX_CAPTURE_STATE or $XDG_STATE_HOME/sinex-session)",
                ))
                .arg(
                    Arg::new("offline")
                        .long("offline")
                        .action(ArgAction::SetTrue)
                        .help("read the database directly; works while the daemon is down"),
                )
                .arg(
                    Arg::new("database")
                        .long("database")
                        .help("database URL, for --offline"),
                );
        }
        app = app.subcommand(sub);
    }
    app.subcommand(Command::new("mcp").about("Serve the MCP tools on stdin and stdout"))
        .subcommand(
            Command::new("doctor")
                .about("Diagnose Sinex without the daemon: database, schema, content store, search index")
                .arg(Arg::new("database").long("database").required(true))
                .arg(Arg::new("store").long("store").required(true).value_parser(clap::value_parser!(PathBuf))),
        )
        .subcommand(
            Command::new("store").about("The content store").subcommand(
                Command::new("verify")
                    .about("Read every stored blob back and check it against its name (offline)")
                    .arg(Arg::new("offline").long("offline").action(ArgAction::SetTrue).required(true))
                    .arg(Arg::new("database").long("database").required(true))
                    .arg(Arg::new("store").long("store").required(true).value_parser(clap::value_parser!(PathBuf))),
            ),
        )
}

/// A command's arguments as the JSON object its request is built from.
fn arguments(spec: &sinex_protocol::Command, m: &ArgMatches) -> Map<String, Value> {
    sinex_protocol::cli::arguments(spec, m)
}

fn client(m: &ArgMatches) -> Result<Client, Failure> {
    let endpoint = match m
        .get_one::<String>("endpoint")
        .cloned()
        .or_else(|| std::env::var("SINEX_ENDPOINT").ok())
    {
        Some(e) => Endpoint::parse(&e)?,
        None => Endpoint::Unix(default_socket()),
    };
    let path = m
        .get_one::<String>("token-file")
        .map(PathBuf::from)
        .unwrap_or_else(default_token_file);
    let token = std::fs::read_to_string(&path)
        .map_err(|e| {
            Failure::new(
                FailureKind::NotConfigured,
                format!("token file {}: {e}", path.display()),
            )
            .with_hint(
                "sinexd writes read.token, agent.token and operator.token in its token directory; \
                 put one at this path or name it with --token-file",
            )
        })?
        .trim()
        .to_string();
    Ok(Client {
        endpoint,
        token,
        deadline: m
            .get_one::<command_surface::Deadline>("deadline")
            .copied()
            .unwrap_or_default(),
    })
}

/// A failure as the terminal shows it: the message, the input line with a
/// caret, then the hint, a runnable correction and the candidates.
fn describe(failure: &Failure) -> String {
    let mut text = failure.message.clone();
    if let Some(caret) = failure.caret() {
        text.push('\n');
        text.push_str(caret);
    }
    if let Some(hint) = &failure.hint {
        text.push_str(&format!("\nhint: {hint}"));
    }
    match failure.suggestion() {
        Some(command_surface::Suggestion::Query(query)) => {
            text.push_str(&format!("\ntry: {query}"));
        }
        Some(command_surface::Suggestion::Request(request)) => {
            text.push_str(&format!(
                "\ntry: {}",
                serde_json::to_string(request).expect("requests serialize")
            ));
        }
        None => {}
    }
    if !failure.candidates().is_empty() {
        text.push_str(&format!(
            "\ncandidates: {}",
            failure.candidates().join(", ")
        ));
    }
    text
}

/// Report `failure` and exit with its kind's code.
fn fail(failure: &Failure, json: bool) -> ExitCode {
    if json {
        print_json(&json!({ "error": failure }));
    } else {
        eprintln!("sinexctl: {}", describe(failure));
    }
    ExitCode::from(failure.kind.exit_code())
}

/// A local failure of the CLI itself: an argument or file it cannot use.
fn local(kind: FailureKind, message: impl Into<String>) -> Failure {
    Failure::new(kind, message)
}

/// `library-open`: ask the daemon where the item's bytes are, hand that file to
/// the player for its type, and record the view with how long the player ran.
async fn open_and_record(client: &Client, request: &Request, item: &str) -> ExitCode {
    let target = match client.call(request).await {
        Ok((_, Reply::Ok { result })) => result,
        Ok((_, Reply::Failed { failure } | Reply::StoppedEarly { failure, .. })) => {
            return fail(&failure, false);
        }
        Err(e) => return fail(&e.failure("library-open"), false),
    };
    let (Some(path), Some(player)) = (target["path"].as_str(), target["player"].as_str()) else {
        return fail(
            &client::protocol_mismatch("library-open", "the answer named no file or player"),
            false,
        );
    };
    eprintln!("opening {path} with {player}");
    let started = std::time::Instant::now();
    let status = tokio::process::Command::new(player)
        .arg(path)
        .stdin(std::process::Stdio::null())
        .status()
        .await;
    let seconds = started.elapsed().as_secs_f64();
    match status {
        Ok(s) if s.success() => {}
        Ok(s) => {
            return fail(
                &local(FailureKind::Io, format!("{player} exited with {s}")),
                false,
            );
        }
        Err(e) => {
            let kind = if e.kind() == std::io::ErrorKind::NotFound {
                FailureKind::NotConfigured
            } else {
                FailureKind::Io
            };
            return fail(&local(kind, format!("{player}: {e}")), false);
        }
    }
    // `xdg-open` returns at once; only a player that ran in the foreground
    // tells how long the item was open.
    let seconds = (player != "xdg-open").then_some(seconds);
    let viewed = Request::LibraryViewed {
        item: item.to_owned(),
        seconds: seconds.map(|s| format!("{s:.1}")),
        player: Some(player.to_owned()),
    };
    let mut failure = match client.call(&viewed).await {
        Ok((_, Reply::Ok { .. })) => return ExitCode::SUCCESS,
        Ok((_, Reply::Failed { failure } | Reply::StoppedEarly { failure, .. })) => failure,
        Err(e) => e.failure("library-viewed"),
    };
    failure.message = format!("opened, but the view was not recorded: {}", failure.message);
    fail(&failure, false)
}

fn print_json(v: &Value) {
    println!("{}", serde_json::to_string_pretty(v).expect("JSON prints"));
}

/// The text an answer was rendered as (`text`: the brief, a panel,
/// `analysis-why`), printed instead of the JSON document unless `--json` is
/// given.
fn rendered_text(result: &Value) -> Option<&str> {
    result["text"].as_str()
}

/// Status when the daemon cannot answer it: why, with the local capture
/// report, which stays readable without the daemon.
fn status_error(failure: &Failure, state: Option<&str>) -> ExitCode {
    let mut status = json!({"daemon": {"unavailable": failure.message, "error": failure}});
    capture_health::attach(&mut status, state);
    print_json(&status);
    ExitCode::from(failure.kind.exit_code())
}

/// A diagnostics report: the checks it found failing are a broken part of the
/// installation (`io`); a report that could not be made says why.
fn report_exit(report: &Value) -> ExitCode {
    if report["ok"] == Value::Bool(true) {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(FailureKind::Io.exit_code())
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let matches = cli().get_matches();
    let json = matches.get_flag("json");
    let (name, sub) = match matches.subcommand() {
        Some((name, sub)) => (name.to_string(), sub.clone()),
        // Bare `sinexctl`: the status view until the Today view is built.
        None => ("status".to_string(), matches.clone()),
    };
    let capture_state = sub
        .try_get_one::<String>("capture-state")
        .ok()
        .flatten()
        .map(String::as_str);
    let offline = sub
        .try_get_one::<bool>("offline")
        .ok()
        .flatten()
        .copied()
        .unwrap_or(false);
    match name.as_str() {
        "doctor" => {
            let database = sub.get_one::<String>("database").expect("required");
            let store = sub.get_one::<PathBuf>("store").expect("required");
            let report = match diagnostics::run("doctor", database, Some(store)).await {
                Ok(report) => report,
                Err(failure) => return fail(&failure, json),
            };
            if json {
                print_json(&report);
            } else {
                for c in report["checks"].as_array().into_iter().flatten() {
                    let mark = if c["ok"] == Value::Bool(true) {
                        "ok  "
                    } else {
                        "FAIL"
                    };
                    println!(
                        "{mark} {}: {}",
                        c["check"].as_str().unwrap_or("?"),
                        c["detail"].as_str().unwrap_or("")
                    );
                }
            }
            return report_exit(&report);
        }
        "store" => {
            let Some(("verify", v)) = sub.subcommand() else {
                return fail(
                    &local(
                        FailureKind::BadRequest,
                        "store takes a subcommand: verify --offline",
                    ),
                    json,
                );
            };
            let database = v.get_one::<String>("database").expect("required");
            let store = v.get_one::<PathBuf>("store").expect("required");
            return match diagnostics::run("verify-store", database, Some(store)).await {
                Ok(report) => {
                    print_json(&report);
                    report_exit(&report)
                }
                Err(failure) => fail(&failure, json),
            };
        }
        "status" if offline => {
            let Some(database) = sub.get_one::<String>("database") else {
                return fail(
                    &local(FailureKind::BadRequest, "status --offline needs --database"),
                    json,
                );
            };
            return match diagnostics::run("status", database, None).await {
                Ok(mut v) => {
                    capture_health::attach(&mut v, capture_state);
                    print_json(&v);
                    ExitCode::SUCCESS
                }
                Err(failure) => status_error(&failure, capture_state),
            };
        }
        _ => {}
    }
    let client = match client(&matches) {
        Ok(c) => c,
        Err(failure) if name == "status" => return status_error(&failure, capture_state),
        Err(failure) => return fail(&failure, json),
    };
    if name == "mcp" {
        return match mcp::serve(client).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => fail(&local(FailureKind::Io, e), false),
        };
    }
    let spec = sinex_protocol::command(&name).expect("subcommands come from the table");
    let mut args = arguments(spec, &sub);
    // `unlock` names the identity file; its key goes to the daemon over the
    // owner-only socket and is held in memory for the session only.
    if name == "unlock"
        && let Some(Value::String(path)) = args.get("identity").cloned()
    {
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                args.insert("identity".into(), Value::String(text));
            }
            Err(e) => {
                let kind = match e.kind() {
                    std::io::ErrorKind::NotFound => FailureKind::NotFound,
                    std::io::ErrorKind::PermissionDenied => FailureKind::Refused,
                    _ => FailureKind::Io,
                };
                return fail(&local(kind, format!("{path}: {e}")), json);
            }
        }
    }
    // The daemon reads staged paths as given, so they go as absolute paths.
    if name == "stage"
        && let Some(Value::Array(paths)) = args.get_mut("path")
    {
        for given in paths.iter_mut() {
            if let Value::String(text) = given {
                match std::path::absolute(&*text) {
                    Ok(path) => *text = path.display().to_string(),
                    Err(e) => {
                        return fail(&local(FailureKind::BadValue, format!("{text}: {e}")), json);
                    }
                }
            }
        }
    }
    if name == "follow" {
        let query = args
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let local = output::Local::new(None);
        let result = client
            .follow(&query, |event, data| {
                match event {
                    "notice" => eprintln!("# {data}"),
                    "error" => eprintln!("sinexctl: {data}"),
                    _ if json => println!("{data}"),
                    _ => {
                        if let Ok(row) = serde_json::from_str::<Value>(data) {
                            let columns: Vec<String> = row
                                .as_object()
                                .map(|o| o.keys().cloned().collect())
                                .unwrap_or_default();
                            let t = output::table(&columns, &[row], &local);
                            print!(
                                "{}",
                                t.lines()
                                    .nth(1)
                                    .map(|l| format!("{l}\n"))
                                    .unwrap_or_default()
                            );
                        }
                    }
                }
                true
            })
            .await;
        return match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => fail(&e.failure("follow"), json),
        };
    }
    let request = match Request::from_args(&name, args.clone()) {
        Ok(r) => r,
        Err(failure) => return fail(&failure, json),
    };
    if let Request::LibraryOpen { item, .. } = &request
        && !json
    {
        return open_and_record(&client, &request, item).await;
    }
    let all = matches.get_flag("all");
    let mut request = request;
    let mut collected: Vec<Value> = Vec::new();
    loop {
        let reply = match client.call(&request).await {
            Ok((_, reply)) => reply,
            Err(e) if name == "status" => return status_error(&e.failure(&name), capture_state),
            Err(e) => return fail(&e.failure(&name), json),
        };
        let mut result = match reply {
            Reply::Ok { result } => result,
            Reply::Failed { failure } if name == "status" => {
                return status_error(&failure, capture_state);
            }
            Reply::Failed { failure } => return fail(&failure, json),
            Reply::StoppedEarly { failure, result } => {
                if json {
                    print_json(&json!({ "error": failure, "result": result }));
                    return ExitCode::from(failure.kind.exit_code());
                }
                if name == "stage" {
                    let report = match serde_json::from_value(result) {
                        Ok(report) => report,
                        Err(e) => return fail(&client::protocol_mismatch(&name, e), json),
                    };
                    print!("{}", sinex_protocol::stage_report::render(&report));
                } else {
                    print_json(&result);
                }
                return fail(&failure, json);
            }
        };
        if name == "status" {
            capture_health::attach(&mut result, capture_state);
        }
        if let Request::Sql { sql, page, .. } = &request {
            // The daemon answers a statement a page at a time; the command
            // line reads every page and prints the whole result.
            collected.extend(result["rows"].as_array().cloned().unwrap_or_default());
            if let Some(cursor) = result["page"]["cursor"].as_str() {
                request = Request::Sql {
                    sql: sql.clone(),
                    page: *page,
                    cursor: Some(cursor.to_owned()),
                };
                continue;
            }
            if json {
                print_json(&serde_json::json!({ "rows": collected, "count": collected.len() }));
            } else {
                let columns: Vec<String> = collected
                    .first()
                    .and_then(Value::as_object)
                    .map(|o| o.keys().cloned().collect())
                    .unwrap_or_default();
                print!(
                    "{}",
                    output::table(&columns, &collected, &output::Local::new(None))
                );
                println!("{} rows", collected.len());
            }
            return answered(collected.is_empty());
        }
        let paged = result["page"].is_object() && result["rows"].is_array();
        if !paged {
            if json {
                print_json(&result);
            } else if matches!(name.as_str(), "stage" | "stage-show") {
                let report = match serde_json::from_value(result) {
                    Ok(report) => report,
                    Err(e) => return fail(&client::protocol_mismatch(&name, e), json),
                };
                print!("{}", sinex_protocol::stage_report::render(&report));
            } else if name == "stage-list" {
                let reports: sinex_protocol::stage_report::Reports =
                    match serde_json::from_value(result) {
                        Ok(reports) => reports,
                        Err(e) => return fail(&client::protocol_mismatch(&name, e), json),
                    };
                for report in &reports.stages {
                    print!("{}", sinex_protocol::stage_report::render(report));
                }
            } else if let Some(text) = rendered_text(&result) {
                print!("{text}");
                if !text.ends_with('\n') {
                    println!();
                }
            } else {
                print_json(&result);
            }
            return ExitCode::SUCCESS;
        }
        let cursor = result["page"]["cursor"].as_str().map(str::to_string);
        if all {
            collected.extend(result["rows"].as_array().cloned().unwrap_or_default());
            if let Some(c) = cursor {
                let Some(query) = result["query"].as_str() else {
                    return fail(
                        &client::protocol_mismatch(&name, "a paged answer has no query text"),
                        json,
                    );
                };
                request = Request::Query {
                    query: query.into(),
                    page: result["page"]["size"].as_u64(),
                    cursor: Some(c),
                };
                continue;
            }
            let empty = collected.is_empty();
            let mut whole = result.clone();
            whole["rows"] = Value::Array(collected);
            if json {
                print_json(&whole);
            } else {
                let local = output::Local::new(whole["resolved"]["zone"].as_str());
                print!("{}", output::answer(&whole, &local, None));
            }
            return answered(empty);
        }
        if json {
            print_json(&result);
        } else {
            let local = output::Local::new(result["resolved"]["zone"].as_str());
            let hint = format!(
                "sinexctl query '{}'",
                result["query"]
                    .as_str()
                    .unwrap_or("")
                    .replace('\'', "'\\''")
            );
            print!("{}", output::answer(&result, &local, Some(&hint)));
        }
        let empty = cursor.is_none() && result["rows"].as_array().is_some_and(Vec::is_empty);
        return answered(empty);
    }
}

/// A read's exit: 0 when it found rows, 1 when it succeeded and matched
/// nothing, so `sinexctl query … || handle` branches on an empty answer.
fn answered(empty: bool) -> ExitCode {
    if empty {
        ExitCode::from(EMPTY)
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests;

mod capture_health;
