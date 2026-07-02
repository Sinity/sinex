use super::*;
use crate::sandbox::sinex_test;
#[cfg(unix)]
use std::ffi::{OsStr, OsString};
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;

fn test_manager(root: &tempfile::TempDir) -> PostgresManager {
    PostgresManager::new(PostgresConfig {
        port: 55432,
        data_dir: root.path().join("data"),
        run_dir: root.path().join("run"),
        logs_dir: root.path().join("logs"),
        database: "sinex".to_string(),
        superuser: "postgres".to_string(),
        app_user: "sinex".to_string(),
        listen_addresses: String::new(),
        durability: PostgresDurabilityMode::Durable,
    })
}

#[sinex_test]
async fn test_postmaster_pid_state_reports_malformed_pid_file() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let manager = test_manager(&temp);
    fs::create_dir_all(&manager.config.data_dir)?;
    fs::write(
        manager.config.data_dir.join("postmaster.pid"),
        "not-a-pid\n",
    )?;

    let error = manager.postmaster_pid_state().unwrap_err();
    assert!(format!("{error:#}").contains("failed to parse postmaster pid"));
    Ok(())
}

#[sinex_test]
async fn test_force_cleanup_reports_socket_cleanup_failure() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let manager = test_manager(&temp);
    fs::create_dir_all(&manager.config.data_dir)?;
    fs::create_dir_all(&manager.config.run_dir)?;
    fs::write(manager.config.data_dir.join("postmaster.pid"), "999999\n")?;
    fs::create_dir(
        manager
            .config
            .run_dir
            .join(format!(".s.PGSQL.{}", manager.config.port)),
    )?;

    let error = manager.force_cleanup(false).unwrap_err();
    assert!(format!("{error:#}").contains("failed to remove postgres socket"));
    Ok(())
}

#[sinex_test]
async fn test_read_pid_returns_parsed_postmaster_pid() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let manager = test_manager(&temp);
    fs::create_dir_all(&manager.config.data_dir)?;
    fs::write(manager.config.data_dir.join("postmaster.pid"), "4321\n")?;

    assert_eq!(manager.read_pid(), Some(4321));
    Ok(())
}

#[sinex_test]
async fn test_read_pid_returns_none_for_missing_postmaster_pid() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let manager = test_manager(&temp);

    assert_eq!(manager.read_pid(), None);
    Ok(())
}

#[sinex_test]
async fn postgres_identifier_rendering_rejects_sql_fragments() -> TestResult<()> {
    assert_eq!(
        pg_identifier("sinex_test_db", "database")?,
        "\"sinex_test_db\""
    );
    assert!(pg_identifier("sinex-test-db", "database").is_err());
    assert!(pg_identifier("sinex;DROP_DATABASE", "database").is_err());
    Ok(())
}

#[sinex_test]
async fn postgres_literal_rendering_escapes_single_quotes() -> TestResult<()> {
    assert_eq!(pg_literal("sinex"), "'sinex'");
    assert_eq!(pg_literal("sin'ex"), "'sin''ex'");
    Ok(())
}

#[sinex_test]
async fn test_install_extensions_reports_create_failures() -> TestResult<()> {
    let error = PostgresManager::install_extensions_with("postgres", "sinex", |_, _, sql| {
        if sql.contains("SELECT 1 FROM pg_available_extensions") {
            Ok("1".to_string())
        } else {
            Err(color_eyre::eyre::eyre!("create extension failed"))
        }
    })
    .unwrap_err();

    assert!(format!("{error:#}").contains("failed to install postgres extension timescaledb"));
    Ok(())
}

#[sinex_test]
async fn test_install_extensions_skips_unavailable_extensions() -> TestResult<()> {
    let mut statements = Vec::new();

    PostgresManager::install_extensions_with("postgres", "sinex", |_, _, sql| {
        statements.push(sql.to_string());
        if sql.contains("timescaledb") || sql.contains("pg_trgm") {
            Ok("1".to_string())
        } else {
            Ok(String::new())
        }
    })?;

    assert!(
        statements
            .iter()
            .any(|sql| sql == "CREATE EXTENSION IF NOT EXISTS \"timescaledb\" CASCADE")
    );
    assert!(
        statements
            .iter()
            .any(|sql| sql == "CREATE EXTENSION IF NOT EXISTS \"pg_trgm\" CASCADE")
    );
    assert!(
        !statements
            .iter()
            .any(|sql| sql == "CREATE EXTENSION IF NOT EXISTS \"vector\" CASCADE")
    );
    assert!(
        !statements
            .iter()
            .any(|sql| sql == "CREATE EXTENSION IF NOT EXISTS \"pg_jsonschema\" CASCADE")
    );
    Ok(())
}

#[sinex_test]
async fn test_ensure_role_creates_nologin_role() -> TestResult<()> {
    let mut statements = Vec::new();

    PostgresManager::ensure_role_with(
        "sinex_api",
        false,
        false,
        "postgres",
        |actor, db, sql| {
            statements.push((actor.to_string(), db.to_string(), sql.to_string()));
            Ok(String::new())
        },
    )?;

    assert_eq!(
        statements,
        vec![
            (
                "postgres".to_string(),
                "postgres".to_string(),
                "SELECT 1 FROM pg_roles WHERE rolname = 'sinex_api'".to_string(),
            ),
            (
                "postgres".to_string(),
                "postgres".to_string(),
                "CREATE ROLE \"sinex_api\" NOLOGIN".to_string(),
            ),
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_ensure_role_skips_existing_role() -> TestResult<()> {
    let mut statements = Vec::new();

    PostgresManager::ensure_role_with(
        "sinex_readonly",
        false,
        false,
        "postgres",
        |actor, db, sql| {
            statements.push((actor.to_string(), db.to_string(), sql.to_string()));
            if sql.contains("SELECT 1 FROM pg_roles") {
                Ok("1".to_string())
            } else {
                Ok(String::new())
            }
        },
    )?;

    assert_eq!(
        statements,
        vec![(
            "postgres".to_string(),
            "postgres".to_string(),
            "SELECT 1 FROM pg_roles WHERE rolname = 'sinex_readonly'".to_string(),
        )]
    );
    Ok(())
}

#[sinex_test]
async fn test_ensure_role_rejects_invalid_role_identifiers() -> TestResult<()> {
    let error = PostgresManager::ensure_role_with(
        "sinex;drop role postgres",
        false,
        false,
        "postgres",
        |_, _, _| Ok(String::new()),
    )
    .unwrap_err();

    assert!(format!("{error:#}").contains("invalid PostgreSQL role identifier"));
    Ok(())
}

#[sinex_test]
async fn pg_isready_probe_reports_spawn_failures() -> TestResult<()> {
    let error =
        pg_isready_probe(Err(std::io::Error::other("pg_isready exploded"))).unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("failed to run pg_isready"));
    assert!(message.contains("pg_isready exploded"));
    Ok(())
}

#[sinex_test]
async fn pg_isready_probe_treats_exit_two_as_not_accepting() -> TestResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        let accepting = pg_isready_probe(Ok(std::process::Output {
            status: std::process::ExitStatus::from_raw(512),
            stdout: Vec::new(),
            stderr: Vec::new(),
        }))?;
        assert!(!accepting);
    }
    Ok(())
}

#[sinex_test]
async fn pg_isready_probe_reports_unexpected_exit_failures() -> TestResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        let error = pg_isready_probe(Ok(std::process::Output {
            status: std::process::ExitStatus::from_raw(768),
            stdout: Vec::new(),
            stderr: b"invalid option".to_vec(),
        }))
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("pg_isready exited unexpectedly"));
        assert!(message.contains("invalid option"));
    }
    Ok(())
}

#[sinex_test]
async fn test_ensure_runtime_config_rewrites_legacy_tail_block() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let manager = test_manager(&temp);
    fs::create_dir_all(&manager.config.data_dir)?;
    fs::write(
        manager.config.data_dir.join("postgresql.conf"),
        "shared_buffers = '128MB'\n\n# sinex-dev configuration\nport = 1111\n",
    )?;

    manager.ensure_runtime_config()?;

    let config = fs::read_to_string(manager.config.data_dir.join("postgresql.conf"))?;
    assert!(config.contains(MANAGED_CONFIG_BEGIN));
    assert!(config.contains(&format!("max_connections = {POSTGRES_MAX_CONNECTIONS}")));
    assert!(config.contains(&format!(
        "max_worker_processes = {POSTGRES_MAX_WORKER_PROCESSES}"
    )));
    assert!(config.contains(&format!("shared_buffers = '{POSTGRES_SHARED_BUFFERS}'")));
    assert!(config.contains(&format!(
        "timescaledb.max_background_workers = {TIMESCALEDB_MAX_BACKGROUND_WORKERS}"
    )));
    assert!(!config.contains("port = 1111"));
    Ok(())
}

#[sinex_test]
async fn test_ensure_runtime_config_replaces_existing_managed_block() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let manager = test_manager(&temp);
    fs::create_dir_all(&manager.config.data_dir)?;
    fs::write(
        manager.config.data_dir.join("postgresql.conf"),
        format!(
            "shared_buffers = '128MB'\n\n{MANAGED_CONFIG_BEGIN}\nport = 1111\n{MANAGED_CONFIG_END}\n"
        ),
    )?;

    manager.ensure_runtime_config()?;

    let config = fs::read_to_string(manager.config.data_dir.join("postgresql.conf"))?;
    assert_eq!(config.matches(MANAGED_CONFIG_BEGIN).count(), 1);
    assert!(config.contains("port = 55432"));
    let managed_block = config
        .split_once(MANAGED_CONFIG_BEGIN)
        .and_then(|(_, rest)| rest.split_once(MANAGED_CONFIG_END).map(|(block, _)| block))
        .ok_or_else(|| color_eyre::eyre::eyre!("managed postgres config block missing"))?;
    assert!(managed_block.contains(&format!("max_connections = {POSTGRES_MAX_CONNECTIONS}")));
    assert!(managed_block.contains(&format!("shared_buffers = '{POSTGRES_SHARED_BUFFERS}'")));
    assert!(!config.contains("port = 1111"));
    assert!(!managed_block.contains("max_connections = 256"));
    assert!(!managed_block.contains("shared_buffers = '128MB'"));
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn test_pg_commands_preserve_non_utf8_paths() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let data_dir = PathBuf::from(OsString::from_vec(b"/tmp/pg-data-\xff".to_vec()));
    let run_dir = PathBuf::from(OsString::from_vec(b"/tmp/pg-run-\xfe".to_vec()));
    let manager = PostgresManager::new(PostgresConfig {
        port: 55432,
        data_dir: data_dir.clone(),
        run_dir: run_dir.clone(),
        logs_dir: temp.path().join("logs"),
        database: "sinex".to_string(),
        superuser: "postgres".to_string(),
        app_user: "sinex".to_string(),
        listen_addresses: String::new(),
        durability: PostgresDurabilityMode::Durable,
    });

    let stop_args: Vec<OsString> = manager
        .pg_ctl_stop_command("fast")
        .get_args()
        .map(OsStr::to_os_string)
        .collect();
    assert!(stop_args.iter().any(|arg| arg == data_dir.as_os_str()));

    let ready_args: Vec<OsString> = manager
        .pg_isready_command()
        .get_args()
        .map(OsStr::to_os_string)
        .collect();
    assert!(ready_args.iter().any(|arg| arg == run_dir.as_os_str()));

    let psql_args: Vec<OsString> = manager
        .psql_command("postgres", "sinex", "SELECT 1")
        .get_args()
        .map(OsStr::to_os_string)
        .collect();
    assert!(psql_args.iter().any(|arg| arg == run_dir.as_os_str()));
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn test_pg_runtime_config_rejects_non_utf8_run_dir() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let manager = PostgresManager::new(PostgresConfig {
        port: 55432,
        data_dir: temp.path().join("data"),
        run_dir: PathBuf::from(OsString::from_vec(b"/tmp/pg-run-\xfe".to_vec())),
        logs_dir: temp.path().join("logs"),
        database: "sinex".to_string(),
        superuser: "postgres".to_string(),
        app_user: "sinex".to_string(),
        listen_addresses: String::new(),
        durability: PostgresDurabilityMode::Durable,
    });

    let error = manager.render_runtime_config().unwrap_err();
    assert!(format!("{error:#}").contains("postgres run dir must be valid UTF-8"));
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn test_pg_ctl_start_command_rejects_non_utf8_run_dir() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let manager = PostgresManager::new(PostgresConfig {
        port: 55432,
        data_dir: temp.path().join("data"),
        run_dir: PathBuf::from(OsString::from_vec(b"/tmp/pg-run-\xfe".to_vec())),
        logs_dir: temp.path().join("logs"),
        database: "sinex".to_string(),
        superuser: "postgres".to_string(),
        app_user: "sinex".to_string(),
        listen_addresses: String::new(),
        durability: PostgresDurabilityMode::Durable,
    });

    let error = manager
        .pg_ctl_start_command(&temp.path().join("postgres.log"))
        .unwrap_err();
    assert!(format!("{error:#}").contains("postgres run dir must be valid UTF-8"));
    Ok(())
}

#[sinex_test]
async fn test_ephemeral_fast_runtime_config_disables_crash_durability() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let mut manager = test_manager(&temp);
    manager.config.durability = PostgresDurabilityMode::EphemeralFast;

    let config = manager.render_runtime_config()?;

    assert!(config.contains("fsync = off"));
    assert!(config.contains("full_page_writes = off"));
    assert!(config.contains("synchronous_commit = off"));
    assert!(config.contains("autovacuum = off"));
    Ok(())
}
