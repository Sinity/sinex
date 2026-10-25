//! Process identity stays registered until its child has been reaped.
use rustix::process::{
    Pid, Signal, WaitId, WaitIdOptions, WaitOptions, kill_process_group, set_child_subreaper,
    waitid, waitpgid,
};
use std::io;
use std::os::unix::process::CommandExt;
use std::process::{ChildStdout, Command, ExitStatus};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Reaping releases a group ID. Keep capture spawns outside that final drain
// so another owned group cannot reuse it before cleanup finishes.
static SPAWN_REAP: Mutex<()> = Mutex::new(());

pub struct Child {
    child: std::process::Child,
    pub(super) registration: Arc<Registration>,
    status: Option<ExitStatus>,
}

pub(super) struct Registration(Mutex<Option<Pid>>);

impl Registration {
    pub fn interrupt(&self) {
        self.signal(Signal::INT);
    }

    pub fn kill(&self) {
        self.signal(Signal::KILL);
    }

    fn signal(&self, signal: Signal) {
        if let Some(pid) = *self.0.lock().unwrap_or_else(|e| e.into_inner()) {
            let _ = kill_process_group(pid, signal);
        }
    }

    pub fn running(&self) -> bool {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).is_some()
    }
}

impl Child {
    pub(super) fn spawn(command: &mut Command) -> io::Result<Self> {
        let _spawning = SPAWN_REAP.lock().unwrap_or_else(|e| e.into_inner());
        // A nonzero value enables Linux's process-wide child-subreaper flag.
        set_child_subreaper(Some(Pid::INIT))?;
        let child = command.process_group(0).spawn()?;
        let pid = Pid::from_raw(child.id() as i32).expect("a child has a positive PID");
        Ok(Self {
            child,
            registration: Arc::new(Registration(Mutex::new(Some(pid)))),
            status: None,
        })
    }

    pub fn stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout.take()
    }

    pub fn interrupt(&self) {
        self.registration.interrupt();
    }

    pub fn wait(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.status {
            return Ok(status);
        }
        loop {
            let mut registered = self
                .registration
                .0
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let pid = registered.expect("unreaped child stays registered");
            // Observe exit without reaping: the leader still holds its PID
            // while we stop descendants and remove the group registration.
            match waitid(
                WaitId::Pid(pid),
                WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
            ) {
                Ok(Some(_)) => {
                    let _reaping = SPAWN_REAP.lock().unwrap_or_else(|e| e.into_inner());
                    let _ = kill_process_group(pid, Signal::KILL);
                    let status = self.child.wait()?;
                    reap_group(pid)?;
                    *registered = None;
                    self.status = Some(status);
                    return Ok(status);
                }
                Ok(None) => {}
                Err(error) if error == rustix::io::Errno::INTR => continue,
                Err(error) => return Err(error.into()),
            }
            drop(registered);
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        let mut pid = self
            .registration
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(process) = *pid {
            let _reaping = SPAWN_REAP.lock().unwrap_or_else(|e| e.into_inner());
            let _ = kill_process_group(process, Signal::KILL);
            let _ = self.child.wait();
            let _ = reap_group(process);
            *pid = None;
        }
    }
}

fn reap_group(group: Pid) -> io::Result<()> {
    loop {
        match waitpgid(group, WaitOptions::empty()) {
            Ok(Some(_)) => {}
            Ok(None) => {}
            Err(error) if error == rustix::io::Errno::INTR => {}
            Err(error) if error == rustix::io::Errno::CHILD => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
}
