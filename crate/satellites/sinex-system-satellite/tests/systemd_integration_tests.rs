use sinex_system_satellite::systemd_integration::{SystemdMonitor, SystemdUnitType};
use sinex_test_utils::{sinex_test, TestResult};

#[sinex_test]
fn unit_type_detection_matches_suffix() -> TestResult<()> {
    assert_eq!(
        SystemdUnitType::from_name("sshd.service"),
        SystemdUnitType::Service
    );
    assert_eq!(
        SystemdUnitType::from_name("backup.timer"),
        SystemdUnitType::Timer
    );
    assert_eq!(
        SystemdUnitType::from_name("dbus.socket"),
        SystemdUnitType::Socket
    );
    assert_eq!(
        SystemdUnitType::from_name("multi-user.target"),
        SystemdUnitType::Target
    );
    Ok(())
}

#[sinex_test]
async fn systemd_monitor_creation_is_resilient() -> TestResult<()> {
    match SystemdMonitor::new() {
        Ok(monitor) => {
            let _ = monitor.list_service_units();
        }
        Err(e) => {
            eprintln!("SystemdMonitor not available in this environment: {}", e);
        }
    }
    Ok(())
}
