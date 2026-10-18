#[cfg(feature = "runtime")]
mod atuin;
// `browser::snapshot` and `sqlite_file` need no database, so the admin build
// (`--no-default-features`) still compiles and tests them.
#[cfg(any(feature = "runtime", test))]
mod browser;
mod config;
#[cfg(feature = "runtime")]
mod output;
#[cfg(feature = "runtime")]
mod server;
#[cfg(any(feature = "runtime", test))]
mod sqlite_file;
#[cfg(feature = "runtime")]
mod zsh;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Sinex daemon")]
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
        /// Test only: pause the writer once at this point and wait for a controller.
        #[arg(long, requires = "test_pause_socket")]
        test_pause_at: Option<String>,
        /// Test only: Unix socket of the controller for --test-pause-at.
        #[arg(long, requires = "test_pause_at")]
        test_pause_socket: Option<PathBuf>,
        /// Test only: write a deliberately wrong Atuin interpretation for replay to fix.
        #[arg(long)]
        test_bad_atuin_parse: bool,
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
            test_pause_at,
            test_pause_socket,
            test_bad_atuin_parse,
        } => {
            let conf = config::Config::load(&path)?;
            // clap requires both flags together.
            let pause = test_pause_at
                .zip(test_pause_socket)
                .map(|(point, socket)| sinex_store::TestPause::new(point, socket))
                .transpose()
                .map_err(public_store_error)?;
            server::serve(conf, pause, test_bad_atuin_parse).await?;
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
