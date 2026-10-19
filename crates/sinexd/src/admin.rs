//! `sinexd admin`: administration that needs the database but not the running
//! daemon, and any command of the command table run locally.
use crate::{api, catalog, context};
use clap::Subcommand;
use std::path::PathBuf;

#[derive(Subcommand)]
pub enum Admin {
    /// Create the schema in an empty database, or confirm it is current.
    Schema {
        #[arg(long)]
        database: String,
    },
    /// Print every source and derivation as JSON, for the NixOS module's
    /// generated options (`nix/catalog.json`).
    Catalog,
    /// Sequence gaps in the Sinnix capture lake's lanes: numbers between the
    /// first and last seen with no record. Facts about Sinnix's capture, not
    /// errors here.
    SequenceGaps {
        #[arg(long)]
        database: String,
    },
    /// Print the NATS server `authorization` block: the daemon user with full
    /// access and one user per producer limited to its own subjects. Passwords
    /// are read from files, so they never pass through the Nix store.
    NatsAuthorization {
        #[arg(long, default_value = "sinex")]
        subjects: String,
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

pub async fn run(command: Admin) -> Result<(), String> {
    match command {
        Admin::Table(args) => admin_table(args).await,
        Admin::Schema { database } => sinex_schema::apply_url(&database)
            .await
            .map(|applied| println!("{applied}"))
            .map_err(|e| e.to_string()),
        Admin::Catalog => {
            print!("{}", catalog::render());
            Ok(())
        }
        Admin::SequenceGaps { database } => sequence_gaps(&database).await,
        Admin::NatsAuthorization {
            subjects,
            daemon_user,
            daemon_password_file,
            producers,
        } => {
            let secret = |path: &std::path::Path| {
                std::fs::read_to_string(path)
                    .map(|s| s.trim_end().to_owned())
                    .map_err(|e| format!("{}: {e}", path.display()))
            };
            let daemon_password = secret(&daemon_password_file)?;
            let mut users = Vec::new();
            for producer in producers {
                let (name, file) = producer
                    .split_once('=')
                    .ok_or_else(|| format!("--producer {producer}: expected name=password-file"))?;
                if !sinex_protocol::nats::valid_producer(name) {
                    return Err(format!("--producer {name}: not a valid producer name"));
                }
                users.push((name.to_owned(), secret(std::path::Path::new(file))?));
            }
            let users: Vec<(&str, &str)> = users
                .iter()
                .map(|(n, p)| (n.as_str(), p.as_str()))
                .collect();
            println!(
                "{}",
                sinex_protocol::Subjects::new(subjects)
                    .authorization((&daemon_user, &daemon_password), &users)
            );
            Ok(())
        }
    }
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
    let ctx = context::Context::open(database, store).await?;
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

async fn sequence_gaps(database: &str) -> Result<(), String> {
    use sqlx::Connection;
    let mut c = sqlx::PgConnection::connect(database)
        .await
        .map_err(|e| e.to_string())?;
    let gaps = sqlx::query!(
        r#"SELECT payload->>'lane' AS "lane!", payload->>'host' AS "host!",
                  count(DISTINCT (payload->>'seq')::bigint) AS "seen!",
                  min((payload->>'seq')::bigint) AS "first!",
                  max((payload->>'seq')::bigint) AS "last!"
           FROM core.events WHERE event_type = 'capture.observed'
           GROUP BY 1, 2 ORDER BY 1, 2"#
    )
    .fetch_all(&mut c)
    .await
    .map_err(|e| e.to_string())?;
    for g in gaps {
        println!(
            "{}@{}: {} seen in {}..={}, {} numbers without a record",
            g.lane,
            g.host,
            g.seen,
            g.first,
            g.last,
            (g.last - g.first + 1) - g.seen
        );
    }
    Ok(())
}
