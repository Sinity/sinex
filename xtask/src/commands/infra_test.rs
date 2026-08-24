use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn lease_ports_require_the_declared_agentctl_ranges() -> crate::sandbox::TestResult<()> {
    assert_eq!(lease_port_value("45432", &LEASE_POSTGRES_PORT_RANGE)?, 45432);
    assert_eq!(lease_port_value("45559", &LEASE_POSTGRES_PORT_RANGE)?, 45559);
    assert!(lease_port_value("45431", &LEASE_POSTGRES_PORT_RANGE).is_err());
    assert_eq!(lease_port_value("44308", &LEASE_NATS_PORT_RANGE)?, 44308);
    assert_eq!(lease_port_value("44435", &LEASE_NATS_PORT_RANGE)?, 44435);
    assert!(lease_port_value("44436", &LEASE_NATS_PORT_RANGE).is_err());
    assert!(lease_port_value("not-a-port", &LEASE_NATS_PORT_RANGE).is_err());
    Ok(())
}

#[sinex_test]
async fn service_readiness_marker_is_atomic_and_job_bound() -> crate::sandbox::TestResult<()> {
    let directory = tempfile::tempdir()?;
    let marker = directory.path().join("ready");
    let job_id = "11111111-1111-4111-8111-111111111111";

    write_service_ready(&marker, job_id)?;

    assert_eq!(std::fs::read_to_string(&marker)?, format!("{job_id}\n"));
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 1);
    Ok(())
}
