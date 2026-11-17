//! `sinexd admin`: administration that needs the database but not the running
//! daemon, and any command of the command table run locally.
use crate::{Error, Result};
use crate::{api, catalog, reader_context};
use clap::Subcommand;
use std::path::PathBuf;

pub(crate) fn emit(text: &str) {
    use std::io::Write;
    let mut stdout = std::io::stdout().lock();
    if let Err(error) = stdout
        .write_all(text.as_bytes())
        .and_then(|()| stdout.flush())
    {
        if error.kind() == std::io::ErrorKind::BrokenPipe {
            std::process::exit(141);
        }
        eprintln!("sinexd: stdout: {error}");
        std::process::exit(i32::from(command_surface::FailureKind::Io.exit_code()));
    }
}

#[derive(Subcommand)]
pub(crate) enum Admin {
    /// Read-only checks that work without a running daemon.
    Diagnostics {
        #[command(subcommand)]
        command: crate::diagnostics::Command,
    },
    /// Startup's schema step: create the schema in an empty database or keep
    /// its declarations current. Refuses an older version and names
    /// `sinexd admin upgrade`; it never upgrades.
    Schema {
        #[arg(long)]
        database: String,
        #[arg(long = "kind-prefix")]
        kind_prefixes: Vec<String>,
    },
    /// Upgrade an older schema to this build's version in one transaction,
    /// with sinexd stopped; refuses while a writer holds the database.
    Upgrade {
        #[arg(long)]
        database: String,
        #[arg(long = "kind-prefix")]
        kind_prefixes: Vec<String>,
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
    /// Copy a backup under the copy job the daemon started, read as JSON from
    /// standard input; print the manifest. `sinexctl backup prepare` runs it
    /// while it holds the daemon's snapshot open.
    BackupCopy,
    /// Print the NATS server `authorization` block: the daemon user with full
    /// access and one user per producer limited to its own subjects. Passwords
    /// are read from files, so they never pass through the Nix store, and the
    /// block holds only their bcrypt hashes.
    NatsAuthorization {
        #[arg(long)]
        producer_grants: Option<PathBuf>,
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
    /// Any command of the command table (`replay`, `archive`, `tombstone`,
    /// `recompute`, `library-add`, `crawl`, …), run locally against the
    /// database and content store without the daemon, with the operator's
    /// access: `sinexd admin <command> [arguments] --database URL --store DIR`.
    /// It is the same code the daemon's API runs.
    #[command(external_subcommand)]
    Table(Vec<String>),
}

async fn schema(database: &str, mappings: Vec<String>, upgrade: bool) -> Result<()> {
    use sqlx::Connection;
    let mut kind_prefixes = sinex_schema::kinds::KindPrefixes::default();
    for mapping in mappings {
        let (prefix, producer) = mapping
            .split_once('=')
            .ok_or_else(|| Error::bad_request("--kind-prefix needs polylogue=PRODUCER"))?;
        if prefix != "polylogue" || !sinex_protocol::nats::valid_producer(producer) {
            return Err(Error::bad_request("invalid kind prefix"));
        }
        kind_prefixes
            .prefixes
            .insert(prefix.into(), producer.into());
    }
    let mut connection = sqlx::PgConnection::connect(database).await?;
    let applied = if upgrade {
        sinex_schema::apply_with_kind_prefixes(&mut connection, &kind_prefixes).await
    } else {
        sinex_schema::start(&mut connection, &kind_prefixes).await
    };
    applied
        .map(|applied| emit(&format!("{applied}\n")))
        .map_err(Error::from)
}

pub(crate) async fn run(
    command: Admin,
    class_recipient: Option<std::sync::Arc<sinex_sealing::Recipient>>,
    library_cache: std::path::PathBuf,
) -> Result<()> {
    match command {
        // `sinexctl` reads the report, or the failure, from standard output.
        Admin::Diagnostics { command } => match crate::diagnostics::run(command).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let failure = error.failure();
                emit(&format!("{}\n", serde_json::json!({ "error": failure })));
                Err(failure.into())
            }
        },
        Admin::Table(args) => admin_table(args, class_recipient, library_cache).await,
        Admin::Schema {
            database,
            kind_prefixes,
        } => schema(&database, kind_prefixes, false).await,
        Admin::Upgrade {
            database,
            kind_prefixes,
        } => schema(&database, kind_prefixes, true).await,
        Admin::PrepareDatabase { database, owner } => {
            use sqlx::Connection;
            let mut connection = sqlx::PgConnection::connect(&database).await?;
            sinex_schema::prepare(&mut connection, &owner)
                .await
                .map_err(Error::from)
        }
        Admin::Catalog => {
            emit(&format!("{}", catalog::render()));
            Ok(())
        }
        // `sinexctl` reads the manifest, or the failure, from standard output.
        Admin::BackupCopy => {
            let copied = async {
                let job: sinex_model::backup::CopyJob = serde_json::from_reader(std::io::stdin())?;
                crate::backup::copy(job).await
            }
            .await;
            match copied {
                Ok(manifest) => {
                    emit(&format!(
                        "{}\n",
                        serde_json::json!({ "manifest": manifest })
                    ));
                    Ok(())
                }
                Err(error) => {
                    let failure = error.failure();
                    emit(&format!("{}\n", serde_json::json!({ "error": failure })));
                    Err(failure.into())
                }
            }
        }
        Admin::NatsAuthorization {
            producer_grants,
            subject_root,
            daemon_user,
            daemon_password_file,
            producers,
        } => {
            let hash_of = |path: &std::path::Path| {
                let password = std::fs::read_to_string(path).map_err(|e| Error::file(path, e))?;
                password_hash(password.trim_end_matches('\n'))
            };
            let daemon_hash = hash_of(&daemon_password_file)?;
            let mut users = Vec::new();
            for producer in producers {
                let (name, file) = producer.split_once('=').ok_or_else(|| {
                    Error::bad_request(format!(
                        "--producer {producer}: expected name=password-file"
                    ))
                })?;
                if !sinex_protocol::nats::valid_producer(name) {
                    return Err(Error::bad_request(format!(
                        "--producer {name}: not a valid producer name"
                    )));
                }
                users.push((name.to_owned(), hash_of(std::path::Path::new(file))?));
            }
            let users: Vec<(&str, &str)> = users
                .iter()
                .map(|(name, hash)| (name.as_str(), hash.as_str()))
                .collect();
            let grants = crate::ingress::fetch::load(producer_grants.as_deref())?;
            let subjects = sinex_protocol::Subjects::new(subject_root);
            let consumers = grants
                .iter()
                .filter(|(_, grant)| !grant.kind_prefixes.is_empty() && !grant.classes.is_empty())
                .map(|(producer, grant)| {
                    (
                        producer.as_str(),
                        grant.consumer_scope(producer, &subjects).1,
                    )
                })
                .collect::<Vec<_>>();
            emit(&format!(
                "{}\n",
                subjects.authorization((&daemon_user, &daemon_hash), &users, &consumers)
            ));
            Ok(())
        }
    }
}

/// The bcrypt hash of a NATS password, in the `$2a$` form the server reads.
pub(crate) fn password_hash(password: &str) -> Result<String> {
    bcrypt::hash_with_result(password, bcrypt::DEFAULT_COST)
        .map(|hash| hash.format_for_version(bcrypt::Version::TwoA))
        .map_err(|e| Error::internal(format!("hashing a NATS password: {e}")))
}

/// Run one command-table command locally: the daemon's API, built over a
/// local writer, called with the operator's access.
async fn admin_table(
    args: Vec<String>,
    class_recipient: Option<std::sync::Arc<sinex_sealing::Recipient>>,
    library_cache: std::path::PathBuf,
) -> Result<()> {
    use clap::{Arg, value_parser};
    let name = args
        .first()
        .ok_or_else(|| Error::bad_request("admin needs a command"))?;
    let spec = sinex_protocol::command(name).ok_or_else(|| {
        let names: Vec<String> = sinex_protocol::COMMANDS
            .iter()
            .map(|c| c.name.to_owned())
            .collect();
        command_surface::Failure::new(
            command_surface::FailureKind::BadRequest,
            format!("no command `{name}`"),
        )
        .with_candidates(names)
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
        .arg(
            Arg::new("deadline")
                .long("deadline")
                .value_name("MS|none")
                .value_parser(sinex_protocol::deadline),
        )
        .arg(Arg::new("host").long("host").default_value("localhost"))
        .arg(Arg::new("zone").long("zone"))
        .arg(
            Arg::new("day-start")
                .long("day-start")
                .default_value("04:00"),
        )
        .try_get_matches_from(&args)
        .map_err(Error::bad_request)?;
    let mut request =
        sinex_protocol::Request::from_args(name, sinex_protocol::cli::arguments(spec, &matches))?;
    // The process ends with the command, so a local stage runs to its end;
    // its paths are recorded as absolute paths.
    if let sinex_protocol::Request::Stage { path, follow, .. } = &mut request {
        *follow = Some(true);
        for given in path.iter_mut() {
            *given = std::path::absolute(&*given)
                .map_err(|e| Error::bad_request(format!("{given}: {e}")))?
                .display()
                .to_string();
        }
    }
    let database: &String = matches.get_one("database").expect("required");
    let store: &PathBuf = matches.get_one("store").expect("required");
    let host: &String = matches.get_one("host").expect("defaulted");
    let zone = match matches.get_one::<String>("zone") {
        Some(name) => jiff::tz::TimeZone::get(name)
            .map_err(|e| Error::bad_request(format!("zone {name}: {e}")))?,
        None => jiff::tz::TimeZone::system(),
    };
    let day_start: &String = matches.get_one("day-start").expect("defaulted");
    let day_start = sinex_query::time::clock(day_start).ok_or_else(|| {
        Error::bad_request(format!(
            "day start `{day_start}` is not a clock time like 04:00"
        ))
    })?;
    let identity = if let Some(path) = matches.get_one::<PathBuf>("identity-file") {
        if !matches!(
            &request,
            sinex_protocol::Request::Replay {
                sealed: Some(true),
                ..
            } | sinex_protocol::Request::Stage { .. }
        ) {
            return Err(Error::bad_request(
                "--identity-file requires replay --sealed or stage",
            ));
        }
        let identity = std::fs::read_to_string(path).map_err(|e| Error::file(path, e))?;
        Some(identity)
    } else {
        None
    };
    let mut ctx = reader_context::ReaderContext::open(
        database,
        store,
        sinex_model::time_settings::TimeSettings::from_parts(zone.clone(), day_start),
    )
    .await?;
    ctx.writer.class_recipient = class_recipient;
    // A local command stops only when it ends.
    let (_running, stop) = tokio::sync::watch::channel(false);
    let reads = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            database
                .parse::<sqlx::postgres::PgConnectOptions>()?
                .application_name("sinex.admin.api"),
        )
        .await?;
    let local = api::Api {
        backup_producers: None,
        source_config: Default::default(),
        read_failures: Default::default(),
        running_stages: Default::default(),
        sql_cursors: Default::default(),
        stop,
        reads,
        writer: Some(ctx.writer.clone()),
        blobs: ctx.blobs.clone(),
        zone,
        day_start,
        derivations: crate::derivation_runner::Catalog::new(&Default::default())?,
        agent_grants: Vec::new(),
        unlock: Default::default(),
        host: host.clone(),
        library_roots: Vec::new(),
        library_cache,
    };
    if let Some(identity) = identity
        && let sinex_protocol::Reply::Failed { failure } = local
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
        return Err(failure.into());
    }
    let deadline = matches
        .get_one::<command_surface::Deadline>("deadline")
        .copied()
        .unwrap_or_default();
    let reply = local
        .call(
            sinex_protocol::Call { request, deadline },
            sinex_protocol::Access::Operator,
        )
        .await;
    drop(local);
    ctx.close().await?;
    match reply {
        sinex_protocol::Reply::Ok { result } => {
            emit(&format!(
                "{}\n",
                serde_json::to_string_pretty(&result).expect("JSON values print")
            ));
            Ok(())
        }
        sinex_protocol::Reply::Failed { failure } => Err(failure.into()),
        sinex_protocol::Reply::StoppedEarly { failure, result } => {
            emit(&format!(
                "{}\n",
                serde_json::to_string_pretty(&result).expect("JSON values print")
            ));
            Err(failure.into())
        }
    }
}
