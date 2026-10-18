//! `sinexctl`: the CLI and the MCP server, generated from the command table in
//! `sinex-protocol`, calling the daemon's API. A small offline mode diagnoses
//! a stopped daemon without writing anything (architecture §7.2).
//!
//! Exit codes: 0 done; 2 refused; 3 asks for something not built yet; 6 the
//! daemon is not reachable; 1 anything else.
mod client;
mod mcp;
mod output;

use clap::{Arg, ArgAction, ArgMatches, Command};
use client::{Client, ClientError, Endpoint};
use serde_json::{Map, Value};
use sinex_protocol::{ArgKind, COMMANDS, Reply, Request};
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
                "unix:/path/to/socket or http://127.0.0.1:PORT (default: the daemon's socket)",
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
        let mut sub = Command::new(c.name).about(c.summary);
        for a in c.args {
            let mut arg = Arg::new(a.name).help(a.help).required(a.required);
            arg = match (a.kind, a.positional) {
                (ArgKind::Rest, _) => arg
                    .num_args(1..)
                    .trailing_var_arg(true)
                    .allow_hyphen_values(true),
                (ArgKind::Bool, _) => arg.long(a.name).action(ArgAction::SetTrue).required(false),
                (ArgKind::Integer, false) => {
                    arg.long(a.name).value_parser(clap::value_parser!(u64))
                }
                (_, false) => arg.long(a.name),
                (_, true) => arg,
            };
            sub = sub.arg(arg);
        }
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
    let mut out = Map::new();
    for a in spec.args {
        let value = match a.kind {
            ArgKind::Rest => m.get_many::<String>(a.name).map(|words| {
                let words: Vec<String> = words
                    .map(|w| {
                        // For a search, one argument with spaces is a phrase.
                        if spec.name == "search" && w.contains(char::is_whitespace) {
                            serde_json::to_string(w).expect("strings serialize")
                        } else {
                            w.clone()
                        }
                    })
                    .collect();
                Value::String(words.join(" "))
            }),
            ArgKind::Bool => m.get_flag(a.name).then_some(Value::Bool(true)),
            ArgKind::Integer => m.get_one::<u64>(a.name).map(|n| Value::from(*n)),
            ArgKind::Text | ArgKind::Reference => m
                .get_one::<String>(a.name)
                .map(|s| Value::String(s.clone())),
        };
        if let Some(v) = value {
            out.insert(a.name.to_string(), v);
        }
    }
    out
}

fn client(m: &ArgMatches) -> Result<Client, String> {
    let endpoint = match m.get_one::<String>("endpoint") {
        Some(e) => Endpoint::parse(e)?,
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
            let report = sinex_offline::doctor(database, store).await;
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
            return match sinex_offline::verify_store(database, store).await {
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
        "status" if sub.get_flag("offline") => {
            let Some(database) = sub.get_one::<String>("database") else {
                return fail(1, "status --offline needs --database");
            };
            return match sinex_offline::status(database).await {
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
    let args = arguments(spec, &sub);
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
mod tests {
    use super::*;

    #[test]
    fn the_cli_is_generated_from_the_table() {
        let app = cli();
        app.clone().debug_assert();
        for c in COMMANDS {
            assert!(app.find_subcommand(c.name).is_some(), "{}", c.name);
        }
        let m = cli()
            .try_get_matches_from([
                "sinexctl",
                "search",
                "--page",
                "5",
                "cargo test",
                "tide",
                "-la",
            ])
            .unwrap();
        let (name, sub) = m.subcommand().unwrap();
        let args = arguments(sinex_protocol::command(name).unwrap(), sub);
        // Options come first; everything after the text starts is text.
        assert_eq!(
            args["words"],
            Value::String("\"cargo test\" tide -la".into())
        );
        assert_eq!(args["page"], Value::from(5u64));
        let m = cli()
            .try_get_matches_from([
                "sinexctl",
                "query",
                "commands",
                "where",
                "exit_code",
                "!=",
                "0",
            ])
            .unwrap();
        let (name, sub) = m.subcommand().unwrap();
        let request =
            Request::from_args(name, arguments(sinex_protocol::command(name).unwrap(), sub))
                .unwrap();
        assert_eq!(
            request,
            Request::Query {
                query: "commands where exit_code != 0".into(),
                page: None,
                cursor: None
            }
        );
    }
}
