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
#[ignore = "sinex-aunm open: probe_postgres_extensions (and the doctor report built from it) never \
            validates the returned extension list against sinex's actually-required extensions \
            (pgvector, timescaledb) -- it's a display-only listing, so a dev DB missing pgvector \
            still reports doctor as fully healthy"]
fn extension_probe_surfaces_an_error_when_a_required_extension_is_missing() {
    // Postgres is reachable and the query succeeds, but the extension the
    // whole embedding/semantic-lane plane depends on (pgvector) isn't
    // installed. Today's probe_postgres_extensions only distinguishes
    // "could I run the query" from "could I not run the query" -- it has no
    // concept of "the query succeeded but the answer is bad news". A missing
    // required extension must surface as `error: Some(..)` so doctor's
    // `all_ok` (which only flips false on `.error.is_some()`) actually goes
    // false, instead of silently reporting a healthy "Postgres Extensions:"
    // section that happens to be missing the one extension that matters.
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
