use super::*;
use std::process::Command;
use std::time::Duration;

/// sinex-zr8j: `try_reap_zombie_pid` signals a bare PID with no secondary
/// identity check (cmdline / start-time). On PID reuse, a stale invocation
/// row can cause the sweep to kill a completely unrelated live process.
///
/// This spawns a real, unrelated child process (standing in for "a live
/// process that happens to occupy the recorded PID") and calls the reaper
/// exactly as the stale-invocation sweep does: by bare PID, with nothing
/// establishing that this process is the one originally recorded. The
/// reaper kills it anyway — proving there is no identity verification.
#[test]
#[ignore = "sinex-zr8j open: try_reap_zombie_pid kills any live process at a bare PID with zero identity check, exactly reproducing the unrelated-process-killed-on-PID-reuse bug"]
fn try_reap_zombie_pid_kills_an_unrelated_process_with_no_identity_check() {
    let mut child = Command::new("sleep")
        .arg("30")
        .spawn()
        .expect("failed to spawn unrelated stand-in process");
    let pid = i64::from(child.id());

    // Give the process a moment to fully start.
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        history_process_is_alive(pid),
        "stand-in process should be alive before the reaper runs"
    );

    // The sweep calls this with only the recorded PID -- no cmdline or
    // start-time check against what was originally observed. This "unrelated"
    // process has no relationship whatsoever to whatever the stale row
    // originally referred to, yet it gets reaped anyway.
    try_reap_zombie_pid(pid);

    // Use try_wait() on the Child handle, not a kill(pid, 0)-style liveness
    // check: a killed-but-unreaped child is a zombie that STILL responds
    // "alive" to kill(0) until something calls wait() on it. try_wait() is
    // the only unambiguous signal of whether it was actually terminated.
    let exited = child
        .try_wait()
        .expect("try_wait should not error on our own child");
    let _ = child.kill();
    let _ = child.wait();

    assert!(
        exited.is_none(),
        "try_reap_zombie_pid killed a process with no identity verification beyond \
         the bare PID number -- this is the exact PID-reuse-kills-unrelated-process bug. \
         The fix must check /proc/<pid>/cmdline or start-time against what was recorded \
         at invocation start, and skip (not just warn) on mismatch."
    );
}
