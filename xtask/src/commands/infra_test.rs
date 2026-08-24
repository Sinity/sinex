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
