//! `sinexctl`: the CLI and the MCP server, generated from the command table in
//! `sinex-protocol`, calling the daemon's API. A small offline mode diagnoses a
//! stopped daemon without writing anything.
//!
//! Exit codes: 0 done; 2 refused; 3 asks for something not built yet; 6 the
//! daemon is not reachable; 1 anything else.
mod client;
mod mcp;
mod output;

mod diagnostics;

use clap::{Arg, ArgAction, ArgMatches, Command};
use client::{Client, ClientError, Endpoint};
use serde_json::{Map, Value};
use sinex_protocol::{COMMANDS, Reply, Request};
use std::path::PathBuf;
use std::process::ExitCode;

const REFUSED: u8 = 2;
const NOT_BUILT: u8 = 3;
const UNREACHABLE: u8 = 6;

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
            sub = sub
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
            Command::new("private")
                .about("Private mode: while on, no source captures anything")
                .arg(
                    Arg::new("state")
                        .value_parser(["on", "off", "status"])
                        .default_value("status"),
                ),
        )
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

fn client(m: &ArgMatches) -> Result<Client, String> {
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
        .map_err(|e| format!("token file {}: {e}", path.display()))?
        .trim()
        .to_string();
    Ok(Client { endpoint, token })
}

fn fail(code: u8, message: impl std::fmt::Display) -> ExitCode {
    eprintln!("sinexctl: {message}");
    ExitCode::from(code)
}

/// `library-open`: ask the daemon where the item's bytes are, hand that file to
/// the player for its type, and record the view with how long the player ran.
async fn open_and_record(client: &Client, request: &Request, item: &str) -> ExitCode {
    let target = match client.call(request).await {
        Ok((_, Reply::Ok { result })) => result,
        Ok((_, Reply::Refused { reason, .. })) => return fail(REFUSED, reason),
        Err(ClientError::Unreachable(m)) => {
            return fail(UNREACHABLE, format!("the daemon is not reachable: {m}"));
        }
        Err(e) => return fail(1, e),
    };
    let (Some(path), Some(player)) = (target["path"].as_str(), target["player"].as_str()) else {
        return fail(1, "the daemon's answer named no file or player");
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
        Ok(s) => return fail(1, format!("{player} exited with {s}")),
        Err(e) => return fail(1, format!("{player}: {e}")),
    }
    // `xdg-open` returns at once; only a player that ran in the foreground
    // tells how long the item was open.
    let seconds = (player != "xdg-open").then_some(seconds);
    let viewed = Request::LibraryViewed {
        item: item.to_owned(),
        seconds: seconds.map(|s| format!("{s:.1}")),
        player: Some(player.to_owned()),
    };
    match client.call(&viewed).await {
        Ok((_, Reply::Ok { .. })) => ExitCode::SUCCESS,
        Ok((_, Reply::Refused { reason, .. })) => fail(
            REFUSED,
            format!("opened, but the view was not recorded: {reason}"),
        ),
        Err(e) => fail(1, format!("opened, but the view was not recorded: {e}")),
    }
}

fn print_json(v: &Value) {
    println!("{}", serde_json::to_string_pretty(v).expect("JSON prints"));
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
    match name.as_str() {
        "doctor" => {
            let database = sub.get_one::<String>("database").expect("required");
            let store = sub.get_one::<PathBuf>("store").expect("required");
            let report = match diagnostics::run("doctor", database, Some(store)).await {
                Ok(report) => report,
                Err(error) => return fail(1, error),
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
            return if report["ok"] == Value::Bool(true) {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            };
        }
        "store" => {
            let Some(("verify", v)) = sub.subcommand() else {
                return fail(1, "store takes a subcommand: verify --offline");
            };
            let database = v.get_one::<String>("database").expect("required");
            let store = v.get_one::<PathBuf>("store").expect("required");
            return match diagnostics::run("verify-store", database, Some(store)).await {
                Ok(report) => {
                    print_json(&report);
                    if report["ok"] == Value::Bool(true) {
                        ExitCode::SUCCESS
                    } else {
                        ExitCode::from(1)
                    }
                }
                Err(e) => fail(1, e),
            };
        }
        "private" => {
            use sinex_transport::private_mode::State;
            let mode = sinex_transport::private_mode::PrivateMode::default();
            let state = sub
                .get_one::<String>("state")
                .map_or("status", String::as_str);
            if state != "status"
                && let Err(e) = mode.set(state == "on")
            {
                return fail(1, format!("{}: {e}", mode.path().display()));
            }
            let switch = mode.state();
            if json {
                print_json(&serde_json::json!({
                    "private_mode": switch.name(),
                    "switch": mode.path(),
                    "reason": switch.reason(),
                }));
            } else {
                let said = match &switch {
                    State::Off => "off".to_string(),
                    State::On => "on: nothing is captured".to_string(),
                    State::Unchecked(reason) => {
                        format!(
                            "unknown: the switch cannot be checked ({reason}), so nothing is captured"
                        )
                    }
                };
                println!("private mode is {said} ({})", mode.path().display());
            }
            return ExitCode::SUCCESS;
        }
        "status" if sub.get_flag("offline") => {
            let Some(database) = sub.get_one::<String>("database") else {
                return fail(1, "status --offline needs --database");
            };
            return match diagnostics::run("status", database, None).await {
                Ok(v) => {
                    print_json(&v);
                    ExitCode::SUCCESS
                }
                Err(e) => fail(1, e),
            };
        }
        _ => {}
    }
    let client = match client(&matches) {
        Ok(c) => c,
        Err(e) => return fail(1, e),
    };
    if name == "mcp" {
        return match mcp::serve(client).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => fail(1, e),
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
            Err(e) => return fail(1, format!("{path}: {e}")),
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
            Err(ClientError::Unreachable(m)) => {
                fail(UNREACHABLE, format!("the daemon is not reachable: {m}"))
            }
            Err(e) => fail(REFUSED, e),
        };
    }
    let request = match Request::from_args(&name, args.clone()) {
        Ok(r) => r,
        Err(e) => return fail(1, e),
    };
    if let Request::LibraryOpen { item, .. } = &request
        && !json
    {
        return open_and_record(&client, &request, item).await;
    }
    let paged = matches!(
        request,
        Request::Query { .. }
            | Request::Search { .. }
            | Request::Recall { .. }
            | Request::Trace { .. }
    );
    let all = matches.get_flag("all") && paged;
    let mut request = request;
    let mut collected: Vec<Value> = Vec::new();
    loop {
        let reply = match client.call(&request).await {
            Ok((_, reply)) => reply,
            Err(ClientError::Unreachable(m)) => {
                return fail(UNREACHABLE, format!("the daemon is not reachable: {m}"));
            }
            Err(e) => return fail(1, e),
        };
        let result = match reply {
            Reply::Ok { result } => result,
            Reply::Refused {
                reason,
                not_built: true,
            } => return fail(NOT_BUILT, format!("not built yet: {reason}")),
            Reply::Refused { reason, .. } => return fail(REFUSED, reason),
        };
        if !paged {
            if json {
                print_json(&result);
            } else if name == "sql" {
                let rows = result["rows"].as_array().cloned().unwrap_or_default();
                let columns: Vec<String> = rows
                    .first()
                    .and_then(Value::as_object)
                    .map(|o| o.keys().cloned().collect())
                    .unwrap_or_default();
                print!(
                    "{}",
                    output::table(&columns, &rows, &output::Local::new(None))
                );
                println!("{} rows", rows.len());
            } else {
                print_json(&result);
            }
            return ExitCode::SUCCESS;
        }
        let cursor = result["page"]["cursor"].as_str().map(str::to_string);
        if all {
            collected.extend(result["rows"].as_array().cloned().unwrap_or_default());
            if let Some(c) = cursor {
                let mut next = args.clone();
                next.insert("cursor".into(), Value::String(c));
                request = Request::from_args(&name, next).expect("the same request with a cursor");
                continue;
            }
            let mut whole = result.clone();
            whole["rows"] = Value::Array(collected);
            if json {
                print_json(&whole);
            } else {
                let local = output::Local::new(whole["resolved"]["zone"].as_str());
                print!("{}", output::answer(&whole, &local, None));
            }
            return ExitCode::SUCCESS;
        }
        if json {
            print_json(&result);
        } else {
            let local = output::Local::new(result["resolved"]["zone"].as_str());
            let hint = format!("sinexctl {name} …");
            print!("{}", output::answer(&result, &local, Some(&hint)));
        }
        return ExitCode::SUCCESS;
    }
}

#[cfg(test)]
mod tests;
