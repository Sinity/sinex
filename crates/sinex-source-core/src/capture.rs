//! One capture lifetime: interruptible waits and its owned child processes.
use std::io;
use std::process::Command;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

mod process;
pub use process::Child;
use process::Registration;

pub struct CaptureStop {
    state: Arc<CaptureStatus>,
    deadline: Option<JoinHandle<()>>,
}

struct CaptureStatus {
    inner: Mutex<Inner>,
    changed: Condvar,
}

#[derive(Default)]
struct Inner {
    stopped: bool,
    children: Vec<Weak<Registration>>,
    readers: Vec<std::os::unix::net::UnixStream>,
}

impl CaptureStop {
    pub fn new(duration: Option<Duration>) -> Self {
        let state = Arc::new(CaptureStatus {
            inner: Mutex::new(Inner::default()),
            changed: Condvar::new(),
        });
        let deadline = duration.map(|duration| {
            let state = state.clone();
            std::thread::spawn(move || {
                if !state.wait(duration) {
                    state.stop();
                }
            })
        });
        Self { state, deadline }
    }

    pub fn stopped(&self) -> bool {
        self.state
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stopped
    }

    /// Returns true when stopped, false when the interval elapses.
    pub fn wait(&self, duration: Duration) -> bool {
        self.state.wait(duration)
    }

    pub fn stop(&self) {
        self.state.stop();
    }

    /// Spawn in a separate process group. Stop interrupts the whole group;
    /// dropping the returned child kills and reaps it on errors and unwinding.
    /// Returns None once capture has stopped.
    pub fn spawn(&self, command: &mut Command) -> io::Result<Option<Child>> {
        let mut inner = self.state.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.stopped {
            return Ok(None);
        }
        let child = Child::spawn(command)?;
        inner.children.retain(|child| child.strong_count() > 0);
        inner.children.push(Arc::downgrade(&child.registration));
        Ok(Some(child))
    }

    pub fn status(&self, command: &mut Command) -> io::Result<Option<std::process::ExitStatus>> {
        match self.spawn(command)? {
            Some(mut child) => child.wait().map(Some),
            None => Ok(None),
        }
    }

    /// Read a short command's stdout while keeping it in the capture lifetime.
    pub fn output(&self, command: &mut Command) -> io::Result<Option<Vec<u8>>> {
        use std::io::Read;
        command
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        let Some(mut child) = self.spawn(command)? else {
            return Ok(None);
        };
        let mut bytes = Vec::new();
        child
            .stdout()
            .expect("piped stdout")
            .read_to_end(&mut bytes)?;
        Ok(child.wait()?.success().then_some(bytes))
    }

    /// Wake a blocked socket reader when capture stops, keeping queued bytes.
    pub fn shutdown_on_stop(&self, reader: &std::os::unix::net::UnixStream) -> io::Result<()> {
        let mut inner = self.state.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.stopped {
            reader.shutdown(std::net::Shutdown::Read)
        } else {
            inner.readers.push(reader.try_clone()?);
            Ok(())
        }
    }
}

impl Drop for CaptureStop {
    fn drop(&mut self) {
        self.stop();
        if let Some(deadline) = self.deadline.take() {
            let _ = deadline.join();
        }
    }
}

impl CaptureStatus {
    fn wait(&self, duration: Duration) -> bool {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let (inner, _) = self
            .changed
            .wait_timeout_while(inner, duration, |inner| !inner.stopped)
            .unwrap_or_else(|e| e.into_inner());
        inner.stopped
    }

    fn stop(&self) {
        let (children, readers) = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if inner.stopped {
                return;
            }
            inner.stopped = true;
            self.changed.notify_all();
            (
                inner
                    .children
                    .iter()
                    .filter_map(Weak::upgrade)
                    .collect::<Vec<_>>(),
                std::mem::take(&mut inner.readers),
            )
        };
        for reader in readers {
            let _ = reader.shutdown(std::net::Shutdown::Read);
        }
        for child in &children {
            child.interrupt();
        }
        let until = Instant::now() + Duration::from_millis(250);
        while children.iter().any(|child| child.running()) && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(10));
        }
        for child in children {
            child.kill();
        }
    }
}

/// Wait for the normal service-stop signals in either outside producer.
pub fn shutdown_signal() -> io::Result<impl std::future::Future<Output = ()> + Send> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => {},
            _ = terminate.recv() => {},
        }
    })
}

#[cfg(test)]
mod tests;
