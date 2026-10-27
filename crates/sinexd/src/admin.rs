//! `sinexd admin`: administration that needs the database but not the running
//! daemon, and any command of the command table run locally.
use crate::{api, catalog, reader_context};
use clap::Subcommand;
use std::path::PathBuf;

#[derive(Subcommand)]
pub(crate) enum Admin {
    /// Read-only checks that work without a running daemon.
    Diagnostics {
        #[command(subcommand)]
        command: crate::diagnostics::Command,
    },
    /// Create the schema in an empty database, or confirm it is current.
    Schema {
        #[arg(long)]
        database: String,
        #[arg(long = "kind-owner")]
        kind_owners: Vec<String>,
    },
    /// As the database superuser: create the extensions and roles the schema
    /// needs, and give `owner` the roles. The owner then applies the schema.
    PrepareDatabase {
        #[arg(long)]
        database: String,
        #[arg(long)]
        owner: String,
    },
    /// Print every source and derivation as JSON, for the NixOS module's
    /// generated options (`nix/catalog.json`).
    Catalog,
    /// Print the NATS server `authorization` block: the daemon user with full
    /// access and one user per producer limited to its own subjects. Passwords
    /// are read from files, so they never pass through the Nix store, and the
    /// block holds only their bcrypt hashes.
    NatsAuthorization {
        #[arg(long, default_value = "sinex")]
        subject_root: String,
        #[arg(long, default_value = "sinexd")]
        daemon_user: String,
        #[arg(long)]
        daemon_password_file: PathBuf,
        /// `name=password-file`; repeatable.
        #[arg(long = "producer")]
        producers: Vec<String>,
    },
    /// Any command of the command table (`replay`, `hide`, `excise`,
    /// `recompute`, `library-add`, `crawl`, …), run locally against the
    /// database and content store without the daemon, with the operator's
    /// access: `sinexd admin <command> [arguments] --database URL --store DIR`.
    /// It is the same code the daemon's API runs.
    #[command(external_subcommand)]
    Table(Vec<String>),
}

pub(crate) async fn run(command: Admin) -> Result<(), String> {
    match command {
        Admin::Diagnostics { command } => crate::diagnostics::run(command).await,
        Admin::Table(args) => admin_table(args).await,
        Admin::Schema {
            database,
            kind_owners,
        } => {
            use sqlx::Connection;
            let mut owners = sinex_schema::kinds::Owners::default();
            for mapping in kind_owners {
                let (prefix, producer) = mapping
                    .split_once('=')
                    .ok_or("--kind-owner needs polylogue=PRODUCER")?;
                if prefix != "polylogue" || !sinex_protocol::nats::valid_producer(producer) {
                    return Err("invalid kind owner".into());
                }
                owners.prefixes.insert(prefix.into(), producer.into());
            }
            let mut connection = sqlx::PgConnection::connect(&database)
                .await
                .map_err(|e| e.to_string())?;
            sinex_schema::apply_with_owners(&mut connection, &owners)
                .await
                .map(|applied| println!("{applied}"))
                .map_err(|e| e.to_string())
        }
        Admin::PrepareDatabase { database, owner } => {
            use sqlx::Connection;
            let mut connection = sqlx::PgConnection::connect(&database)
                .await
                .map_err(|e| e.to_string())?;
            sinex_schema::prepare(&mut connection, &owner)
                .await
                .map_err(|e| e.to_string())
        }
        Admin::Catalog => {
            print!("{}", catalog::render());
            Ok(())
        }
        Admin::NatsAuthorization {
            subject_root,
            daemon_user,
            daemon_password_file,
            producers,
        } => {
            let hash_of = |path: &std::path::Path| {
                let password = std::fs::read_to_string(path)
                    .map_err(|e| format!("{}: {e}", path.display()))?;
                password_hash(password.trim_end_matches('\n'))
            };
            let daemon_hash = hash_of(&daemon_password_file)?;
            let mut users = Vec::new();
            for producer in producers {
                let (name, file) = producer
                    .split_once('=')
                    .ok_or_else(|| format!("--producer {producer}: expected name=password-file"))?;
                if !sinex_protocol::nats::valid_producer(name) {
                    return Err(format!("--producer {name}: not a valid producer name"));
                }
                users.push((name.to_owned(), hash_of(std::path::Path::new(file))?));
            }
            let users: Vec<(&str, &str)> = users
                .iter()
                .map(|(name, hash)| (name.as_str(), hash.as_str()))
                .collect();
            println!(
                "{}",
                sinex_protocol::Subjects::new(subject_root)
                    .authorization((&daemon_user, &daemon_hash), &users)
            );
            Ok(())
        }
    }
}

/// The bcrypt hash of a NATS password, in the `$2a$` form the server reads.
pub(crate) fn password_hash(password: &str) -> Result<String, String> {
    bcrypt::hash_with_result(password, bcrypt::DEFAULT_COST)
        .map(|hash| hash.format_for_version(bcrypt::Version::TwoA))
        .map_err(|e| format!("hashing a NATS password: {e}"))
}

/// Run one command-table command locally: the daemon's API, built over a
/// local writer, called with the operator's access.
async fn admin_table(args: Vec<String>) -> Result<(), String> {
    use clap::{Arg, value_parser};
    let name = args.first().ok_or("admin needs a command")?;
    let spec = sinex_protocol::command(name).ok_or_else(|| {
        let names: Vec<&str> = sinex_protocol::COMMANDS.iter().map(|c| c.name).collect();
        format!("no command `{name}`; commands: {}", names.join(", "))
    })?;
    let matches = sinex_protocol::cli::subcommand(spec)
        .arg(Arg::new("database").long("database").required(true))
        .arg(
            Arg::new("store")
                .long("store")
                .required(true)
                .value_parser(value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("identity-file")
                .long("identity-file")
                .value_parser(value_parser!(PathBuf)),
        )
        .arg(Arg::new("host").long("host").default_value("localhost"))
        .arg(Arg::new("zone").long("zone"))
        .arg(
            Arg::new("day-start")
                .long("day-start")
                .default_value("04:00"),
        )
        .try_get_matches_from(&args)
        .map_err(|e| e.to_string())?;
    let request =
        sinex_protocol::Request::from_args(name, sinex_protocol::cli::arguments(spec, &matches))?;
    let database: &String = matches.get_one("database").expect("required");
    let store: &PathBuf = matches.get_one("store").expect("required");
    let host: &String = matches.get_one("host").expect("defaulted");
    let zone = match matches.get_one::<String>("zone") {
        Some(name) => jiff::tz::TimeZone::get(name).map_err(|e| format!("zone {name}: {e}"))?,
        None => jiff::tz::TimeZone::system(),
    };
    let day_start: &String = matches.get_one("day-start").expect("defaulted");
    let day_start = sinex_model::query::time::clock(day_start)
        .ok_or_else(|| format!("day start `{day_start}` is not a clock time like 04:00"))?;
    let identity = if let Some(path) = matches.get_one::<PathBuf>("identity-file") {
        if !matches!(
            &request,
            sinex_protocol::Request::Replay {
                sealed: Some(true),
                ..
            }
        ) {
            return Err("--identity-file requires replay --sealed".into());
        }
        let identity =
            std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Some(identity)
    } else {
        None
    };
    let ctx = reader_context::ReaderContext::open(database, store).await?;
    let reads = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(database)
        .await
        .map_err(|e| e.to_string())?;
    let local = api::Api {
        reads,
        writer: Some(ctx.writer.clone()),
        blobs: ctx.blobs.clone(),
        zone,
        day_start,
        agent_grants: Vec::new(),
        unlock: Default::default(),
        host: host.clone(),
    };
    if let Some(identity) = identity {
        if let sinex_protocol::Reply::Refused { reason, .. } = local
            .call(
                sinex_protocol::Request::Unlock {
                    identity: identity.trim().to_owned(),
                    minutes: None,
                },
                sinex_protocol::Access::Operator,
            )
            .await
        {
            drop(local);
            ctx.close().await?;
            return Err(reason);
        }
    }
    let reply = local.call(request, sinex_protocol::Access::Operator).await;
    drop(local);
    ctx.close().await?;
    match reply {
        sinex_protocol::Reply::Ok { result } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&result).expect("JSON values print")
            );
            Ok(())
        }
        sinex_protocol::Reply::Refused { reason, .. } => Err(reason),
    }
}
