//! Cross-checkout allocation for development NATS ports.

use color_eyre::eyre::{Result, WrapErr};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::net::TcpListener;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

const FIRST_NATS_PORT: u16 = 4_222;
pub const NATS_PORT_COUNT: u16 = 1_000;
const NATS_PORT_LOCK: &str = "sinex-dev-nats-start.lock";
const NATS_PORT_FILE: &str = "nats.port";

/// Hold this lock from port selection through NATS readiness. Every checkout
/// shares the per-user runtime directory, so concurrent stack startups cannot
/// select the same port while the first server is still binding it.
pub struct NatsStartupLock(File);

impl NatsStartupLock {
    pub fn acquire() -> Result<Self> {
        let runtime_dir = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "XDG_RUNTIME_DIR is required to coordinate Sinex NATS startup across checkouts"
                )
            })?;
        Self::acquire_at(&runtime_dir)
    }

    fn acquire_at(runtime_dir: &Path) -> Result<Self> {
        let path = runtime_dir.join(NATS_PORT_LOCK);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .wrap_err_with(|| format!("open Sinex NATS startup lock {}", path.display()))?;
        // This process holds the descriptor until NATS is protocol-ready.
        // Kernel flock ownership is released automatically on exit/cancellation.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if result != 0 {
            return Err(std::io::Error::last_os_error())
                .wrap_err_with(|| format!("acquire Sinex NATS startup lock {}", path.display()));
        }
        Ok(Self(file))
    }
}

impl Drop for NatsStartupLock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
    }
}

/// Pick and persist an available NATS port while the caller holds the global
/// startup lock. The previous checkout assignment is preferred for stability;
/// if it is now occupied, the search wraps through the shared Sinex port pool.
pub fn choose_and_persist(
    _startup_lock: &NatsStartupLock,
    state_dir: &Path,
    preferred: u16,
    is_available: impl FnMut(u16) -> bool,
) -> Result<u16> {
    let port = choose_available(preferred, is_available).ok_or_else(|| {
        color_eyre::eyre::eyre!(
            "no available Sinex NATS port in {FIRST_NATS_PORT}..{}",
            FIRST_NATS_PORT + NATS_PORT_COUNT - 1
        )
    })?;
    persist_port(state_dir, port)?;
    Ok(port)
}

pub fn persisted_port(state_dir: &Path) -> Option<u16> {
    fs::read_to_string(state_dir.join(NATS_PORT_FILE))
        .ok()?
        .trim()
        .parse()
        .ok()
        .filter(|port| *port != 0)
}

fn choose_available(mut preferred: u16, mut is_available: impl FnMut(u16) -> bool) -> Option<u16> {
    // Respect an explicit free endpoint, including ports outside the fallback
    // pool. Only collisions move a checkout into the shared search range.
    if preferred != 0 && is_available(preferred) {
        return Some(preferred);
    }
    if !(FIRST_NATS_PORT..FIRST_NATS_PORT + NATS_PORT_COUNT).contains(&preferred) {
        preferred = FIRST_NATS_PORT + (preferred % NATS_PORT_COUNT);
    }
    (0..NATS_PORT_COUNT)
        .map(|offset| FIRST_NATS_PORT + (preferred - FIRST_NATS_PORT + offset) % NATS_PORT_COUNT)
        .find(|port| is_available(*port))
}

fn persist_port(state_dir: &Path, port: u16) -> Result<()> {
    fs::create_dir_all(state_dir)?;
    let path = state_dir.join(NATS_PORT_FILE);
    let mut temp_file = tempfile::NamedTempFile::new_in(state_dir)
        .wrap_err_with(|| format!("create temporary NATS port file in {}", state_dir.display()))?;
    writeln!(temp_file, "{port}")?;
    temp_file.as_file().sync_all()?;
    temp_file
        .persist(&path)
        .map_err(|error| error.error)
        .wrap_err_with(|| format!("persist NATS port to {}", path.display()))?;
    Ok(())
}

/// Test whether a loopback TCP port can be bound at this instant.
/// Cross-checkout starts are serialized by `NatsStartupLock` until the chosen
/// NATS process passes protocol readiness.
pub fn port_is_available(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn allocator_keeps_free_persisted_port_and_falls_forward_on_collision() {
        assert_eq!(choose_available(4_300, |port| port == 4_300), Some(4_300));
        assert_eq!(choose_available(4_300, |port| port == 4_301), Some(4_301));
    }

    #[test]
    fn allocator_wraps_inside_the_shared_range() {
        assert_eq!(choose_available(5_221, |port| port == 4_222), Some(4_222));
    }

    #[test]
    fn allocator_preserves_explicit_ports_and_reports_exhaustion() {
        assert_eq!(
            choose_available(44_309, |port| port == 44_309),
            Some(44_309)
        );
        assert_eq!(choose_available(4_300, |_| false), None);
    }

    #[test]
    fn occupied_listener_survives_persisted_fallback_selection() -> Result<()> {
        let holder = TcpListener::bind(("127.0.0.1", 0))?;
        let occupied = holder.local_addr()?.port();
        let state = tempfile::tempdir()?;
        let lock = NatsStartupLock::acquire_at(state.path())?;
        let selected = choose_and_persist(&lock, state.path(), occupied, port_is_available)?;
        assert_ne!(selected, occupied);
        assert_eq!(persisted_port(state.path()), Some(selected));
        assert!(std::net::TcpStream::connect(("127.0.0.1", occupied)).is_ok());
        assert!(TcpListener::bind(("127.0.0.1", occupied)).is_err());
        Ok(())
    }

    #[test]
    fn chosen_port_is_persisted_for_later_shells() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let lock = NatsStartupLock::acquire_at(temp.path())?;
        let selected = choose_and_persist(&lock, temp.path(), 4_300, |port| port == 4_301)?;
        assert_eq!(selected, 4_301);
        assert_eq!(persisted_port(temp.path()), Some(4_301));
        let moved = choose_and_persist(&lock, temp.path(), selected, |port| port == 4_302)?;
        assert_eq!(moved, 4_302);
        assert_eq!(persisted_port(temp.path()), Some(4_302));
        Ok(())
    }

    #[test]
    fn startup_lock_serializes_independent_open_descriptors() -> Result<()> {
        let runtime_dir = tempfile::tempdir()?;
        let first = NatsStartupLock::acquire_at(runtime_dir.path())?;
        let contender = OpenOptions::new()
            .read(true)
            .write(true)
            .open(runtime_dir.path().join(NATS_PORT_LOCK))?;
        let result = unsafe { libc::flock(contender.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        assert_eq!(
            result, -1,
            "a second descriptor must not acquire the held lock"
        );
        assert_eq!(
            std::io::Error::last_os_error().kind(),
            std::io::ErrorKind::WouldBlock
        );

        let lock_dir = runtime_dir.path().to_path_buf();
        let (attempting_tx, attempting_rx) = mpsc::channel();
        let (acquired_tx, acquired_rx) = mpsc::channel();

        let second = std::thread::spawn(move || -> Result<()> {
            attempting_tx
                .send(())
                .expect("test receiver stays alive while the second lock waits");
            let lock = NatsStartupLock::acquire_at(&lock_dir)?;
            acquired_tx
                .send(())
                .expect("test receiver stays alive until the second lock is acquired");
            drop(lock);
            Ok(())
        });

        attempting_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second thread should reach the lock acquisition before the timeout");
        drop(first);
        acquired_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second lock should acquire after the first lock is dropped");
        second
            .join()
            .expect("second lock thread should not panic")?;
        Ok(())
    }
}
