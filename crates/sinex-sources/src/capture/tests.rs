use super::*;
use std::io::{BufRead, BufReader};
use std::process::Stdio;

fn shell(stop: &CaptureStop, script: &str) -> Child {
    stop.spawn(
        Command::new("sh")
            .args(["-c", script])
            .stdout(Stdio::piped()),
    )
    .unwrap()
    .unwrap()
}

fn pid(child: &mut Child) -> u32 {
    let mut text = String::new();
    BufReader::new(child.stdout().unwrap())
        .read_line(&mut text)
        .unwrap();
    text.trim().parse().unwrap()
}

fn absent(pid: u32) -> bool {
    !std::path::Path::new(&format!("/proc/{pid}")).exists()
}

#[devkit::test]
fn deadline_interrupts_and_reaps_an_owned_child() {
    let stop = CaptureStop::new(Some(Duration::from_millis(150)));
    let mut child = shell(&stop, "echo $$; exec sleep 30");
    let pid = pid(&mut child);
    child.wait().unwrap();
    assert!(stop.stopped());
    assert!(absent(pid));
    assert!(stop.spawn(&mut Command::new("true")).unwrap().is_none());
}

#[devkit::test]
fn child_drop_reaps_after_error_or_panic_without_waiting_for_a_deadline() {
    let stop = CaptureStop::new(None);
    let mut pid_seen = 0;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut child = shell(&stop, "echo $$; exec sleep 30");
        pid_seen = pid(&mut child);
        panic!("synthetic capture failure");
    }));
    assert!(result.is_err());
    assert!(absent(pid_seen));
}

#[devkit::test]
fn stop_escalates_when_the_capture_ignores_interrupts() {
    let stop = CaptureStop::new(None);
    let mut child = shell(&stop, "trap '' INT; echo $$; exec sleep 30");
    let pid = pid(&mut child);
    let start = Instant::now();
    stop.stop();
    child.wait().unwrap();
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(absent(pid));
}

#[devkit::test]
fn dropping_control_stops_a_still_owned_process_and_drops_the_timer() {
    let stop = CaptureStop::new(Some(Duration::from_secs(30)));
    let mut child = shell(&stop, "echo $$; exec sleep 30");
    let pid = pid(&mut child);
    let start = Instant::now();
    drop(stop);
    child.wait().unwrap();
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(absent(pid));
}

#[devkit::test]
fn a_blocked_socket_is_woken_and_keeps_queued_bytes() {
    use std::io::{Read, Write};
    let (reader, mut writer) = std::os::unix::net::UnixStream::pair().unwrap();
    let stop = CaptureStop::new(None);
    stop.shutdown_on_stop(&reader).unwrap();
    writer.write_all(b"queued exact bytes").unwrap();
    stop.stop();
    let mut bytes = Vec::new();
    (&reader).read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"queued exact bytes");
}

#[devkit::test]
fn a_leader_exit_reaps_descendants_and_stops_later_output_before_returning() {
    let dir = tempfile::tempdir().unwrap();
    let heartbeat = dir.path().join("heartbeat");
    let stop = CaptureStop::new(None);
    let mut command = Command::new("sh");
    command
        .args([
            "-c",
            "(while :; do printf x >> \"$1\"; sleep 0.01; done) >/dev/null & echo $!; exit 0",
            "fixture",
        ])
        .arg(&heartbeat)
        .stdout(Stdio::piped());
    let mut child = stop.spawn(&mut command).unwrap().unwrap();
    let descendant = pid(&mut child);
    assert!(child.wait().unwrap().success());
    assert!(
        absent(descendant),
        "the descendant must be reaped before wait returns"
    );
    let before = std::fs::read(&heartbeat).unwrap_or_default();
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(std::fs::read(&heartbeat).unwrap_or_default(), before);
}
