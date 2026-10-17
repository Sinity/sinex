#[cfg(feature = "runtime")]
mod atuin;
#[cfg(any(feature = "runtime", test))]
#[path = "atuin/guard.rs"]
mod atuin_guard;
mod config;
#[cfg(feature = "runtime")]
mod output;
#[cfg(feature = "runtime")]
mod server;
#[cfg(feature = "runtime")]
mod zsh;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Sinex isolated foundation daemon")]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    #[cfg(feature = "runtime")]
    Serve {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, requires = "qualification_socket")]
        qualification_boundary: Option<String>,
        #[arg(long, requires = "qualification_boundary")]
        qualification_socket: Option<PathBuf>,
        /// Controlled synthetic parser-error fixture, never enabled in ordinary acquisition.
        #[arg(long)]
        qualification_historical_atuin: bool,
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    Catalog,
    Admin {
        #[command(subcommand)]
        command: AdminCommand,
    },
}
#[derive(Subcommand)]
enum ConfigCommand {
    Schema,
    Check {
        #[arg(long)]
        config: PathBuf,
    },
}
#[derive(Subcommand)]
enum AdminCommand {
    Schema {
        #[arg(long)]
        connection_file: PathBuf,
        #[arg(long, conflicts_with = "apply")]
        check: bool,
        #[arg(long, requires = "dump_file")]
        apply: bool,
        #[arg(long, requires = "apply")]
        dump_file: Option<PathBuf>,
    },
}
#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run(Args::parse()).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            eprintln!(
                "{}",
                serde_json::json!({"kind":"internal","message":message,"details":null})
            );
            std::process::ExitCode::FAILURE
        }
    }
}
async fn run(args: Args) -> Result<(), String> {
    match args.command {
        Command::Config {
            command: ConfigCommand::Schema,
        } => println!("{}", config::schema()),
        Command::Config {
            command: ConfigCommand::Check { config: path },
        } => {
            config::Config::load(&path)?;
            println!("{}", serde_json::json!({"valid":true}));
        }
        Command::Catalog => {
            #[cfg(feature = "runtime")]
            println!(
                "{}",
                serde_json::to_string(&sinex_protocol::rpc::catalog())
                    .map_err(|_| "cannot encode catalog")?
            );
            #[cfg(not(feature = "runtime"))]
            println!(
                "{}",
                serde_json::json!({"build":"query_free_admin","rpc_methods":[],"commands":["config.schema","config.check","admin.schema"]})
            );
        }
        Command::Admin {
            command:
                AdminCommand::Schema {
                    connection_file,
                    check,
                    apply,
                    dump_file,
                },
        } => {
            let url = config::secret(&connection_file)?;
            let status = if apply {
                sinex_store::apply_changes(&url, dump_file.as_deref().expect("required by clap"))
                    .await
            } else if check {
                sinex_store::check_schema(&url).await
            } else {
                sinex_store::bootstrap(&url).await
            }
            .map_err(public_store_error)?;
            println!(
                "{}",
                serde_json::to_string(&status).map_err(|_| "cannot encode schema status")?
            );
        }
        #[cfg(feature = "runtime")]
        Command::Serve {
            config: path,
            qualification_boundary,
            qualification_socket,
            qualification_historical_atuin,
        } => {
            let conf = config::Config::load(&path)?;
            let qualification = match (qualification_boundary, qualification_socket) {
                (Some(point), Some(socket)) => Some(
                    sinex_store::QualificationBoundary::new(point, socket)
                        .map_err(public_store_error)?,
                ),
                (None, None) => None,
                _ => {
                    return Err(
                        "qualification boundary and socket must be supplied together".into(),
                    );
                }
            };
            server::serve(conf, qualification, qualification_historical_atuin).await?;
        }
    }
    Ok(())
}

fn public_store_error(error: sinex_store::Error) -> String {
    match error {
        sinex_store::Error::Schema(reason) => format!("schema incompatible: {reason}"),
        sinex_store::Error::Invalid(reason) => format!("invalid store operation: {reason}"),
        sinex_store::Error::Database(error) => format!(
            "database operation failed (SQLSTATE {})",
            error
                .as_database_error()
                .and_then(|e| e.code())
                .as_deref()
                .unwrap_or("connection/unavailable")
        ),
        sinex_store::Error::WriterOwned => "database writer already owned".into(),
        _ => "store operation failed".into(),
    }
}
