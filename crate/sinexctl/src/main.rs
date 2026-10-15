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
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Status,
    Version,
    ConfigShow,
    Catalog,
}
fn main() -> std::process::ExitCode {
    let args = Args::parse();
    let method = match args.command {
        Command::Status => Method::Status,
        Command::Version => Method::Version,
        Command::ConfigShow => Method::ConfigShow,
        Command::Catalog => Method::Catalog,
    };
    let token = match std::fs::read_to_string(&args.token_file) {
        Ok(s) if !s.trim().is_empty() => s.trim().to_string(),
        _ => {
            fail("internal", "cannot read configured token file");
            return std::process::ExitCode::FAILURE;
        }
    };
    let request = RpcRequest::new(RpcId::Number(1), method, serde_json::json!({}));
    let result = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
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
                    std::process::ExitCode::SUCCESS
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
