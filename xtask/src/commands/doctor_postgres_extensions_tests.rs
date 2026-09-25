use super::*;

fn stub_psql_output(stdout: &str) -> std::io::Result<std::process::Output> {
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;

    Ok(std::process::Output {
        #[cfg(unix)]
        status: std::process::ExitStatus::from_raw(0),
        #[cfg(not(unix))]
        status: std::process::Command::new("true").status().unwrap(),
        stdout: stdout.as_bytes().to_vec(),
        stderr: Vec::new(),
    })
}

#[test]
fn extension_probe_reports_extensions_when_all_required_are_present() {
    let probe = probe_postgres_extensions(true, Ok(()), |_: &()| {
        stub_psql_output("plpgsql\nvector\ntimescaledb\n")
    });
    assert_eq!(
        probe.extensions,
        Some(vec![
            "plpgsql".to_string(),
            "vector".to_string(),
            "timescaledb".to_string()
        ])
    );
    assert!(probe.error.is_none());
}

#[test]
fn extension_probe_surfaces_an_error_when_a_required_extension_is_missing() {
    // Postgres is reachable and the query succeeds, but pgvector is absent.
    // The probe must still fail so doctor marks the environment unhealthy.
    let probe = probe_postgres_extensions(true, Ok(()), |_: &()| {
        stub_psql_output("plpgsql\ntimescaledb\n") // pgvector missing
    });
    assert!(
        probe.error.is_some(),
        "extensions={:?} error={:?} -- missing 'vector' (pgvector) from the required set was not \
         surfaced as an error, so doctor's all_ok never flips false for it",
        probe.extensions,
        probe.error,
    );
}

#[test]
fn extension_probe_surfaces_an_error_when_timescaledb_is_missing() {
    let probe =
        probe_postgres_extensions(true, Ok(()), |_: &()| stub_psql_output("plpgsql\nvector\n"));
    assert!(
        probe
            .error
            .as_deref()
            .is_some_and(|error| error.contains("timescaledb")),
        "extensions={:?} error={:?} -- missing 'timescaledb' was not reported",
        probe.extensions,
        probe.error,
    );
}
