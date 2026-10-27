use super::*;

#[devkit::test]
fn nats_is_reaped_when_a_measurement_returns_an_error() {
    let mut child_id = None;
    let measured = (|| -> Result<(), String> {
        let child = Command::new("nats-server")
            .args(["-a", "127.0.0.1", "-p", &free_port()?.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| e.to_string())?;
        child_id = Some(child.id());
        let _nats = Process(child, None);
        Err("measurement failed".into())
    })();
    assert_eq!(measured.unwrap_err(), "measurement failed");
    assert!(!Path::new(&format!("/proc/{}", child_id.unwrap())).exists());
}
