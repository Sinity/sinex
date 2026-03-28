#![doc = include_str!("../docs/systemd_integration.md")]

//! Modern systemd/journald integration using the `nix` crate.

use color_eyre::eyre::{Result, eyre};
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Systemd cgroup path base
const SYSTEMD_CGROUP_BASE: &str = "/sys/fs/cgroup/systemd";
const SYSTEMD_SLICE_BASE: &str = "/sys/fs/cgroup/unified/system.slice";

/// Represents a systemd unit with its properties
#[derive(Debug, Clone)]
pub struct SystemdUnit {
    pub name: String,
    pub unit_type: SystemdUnitType,
    pub state: SystemdUnitState,
    pub sub_state: String,
    pub description: Option<String>,
    pub pid: Option<u32>,
    pub memory_usage: Option<u64>,
    pub cpu_usage: Option<Duration>,
}

/// Systemd unit types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemdUnitType {
    Service,
    Timer,
    Socket,
    Target,
    Mount,
    Device,
    Scope,
    Slice,
    Other,
}

/// Systemd unit states
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemdUnitState {
    Active,
    Inactive,
    Failed,
    Activating,
    Deactivating,
    Reloading,
    Unknown,
}

/// Systemd monitor that reads unit state via the cgroup filesystem.
#[derive(Clone)]
pub struct SystemdMonitor {
    cgroup_base: PathBuf,
}

impl SystemdMonitor {
    /// Create a new systemd monitor
    pub fn new() -> Result<Self> {
        // Determine the correct cgroup path
        let cgroup_base = if Path::new(SYSTEMD_SLICE_BASE).exists() {
            PathBuf::from(SYSTEMD_SLICE_BASE)
        } else if Path::new(SYSTEMD_CGROUP_BASE).exists() {
            PathBuf::from(SYSTEMD_CGROUP_BASE)
        } else {
            return Err(eyre!("Cannot find systemd cgroup directory"));
        };

        Ok(Self { cgroup_base })
    }

    /// List all systemd service units by reading cgroup
    pub fn list_service_units(&self) -> Result<Vec<String>> {
        let mut services = Vec::new();

        // Read service units from cgroup
        let service_path = self.cgroup_base.join("system.slice");
        if service_path.exists() {
            for entry in std::fs::read_dir(&service_path)? {
                let entry = entry?;
                let name = entry.file_name().into_string().map_err(|name| {
                    eyre!(
                        "Failed to decode systemd unit name as UTF-8 in '{}': {:?}",
                        service_path.display(),
                        name
                    )
                })?;
                if name.ends_with(".service") {
                    services.push(name);
                }
            }
        }

        Ok(services)
    }

    /// Get unit status by reading cgroup and proc
    pub fn get_unit_status(&self, unit_name: &str) -> Result<SystemdUnit> {
        let unit_path = self.cgroup_base.join("system.slice").join(unit_name);

        let unit_type = SystemdUnitType::from_name(unit_name);
        let state = self.read_unit_state(&unit_path)?;
        let pid = self.read_unit_main_pid(&unit_path)?;
        let memory_usage = self.read_memory_usage(&unit_path)?;
        let cpu_usage = self.read_cpu_usage(&unit_path)?;

        Ok(SystemdUnit {
            name: unit_name.to_string(),
            unit_type,
            state,
            sub_state: String::new(), // Would need D-Bus for detailed sub-state
            description: None,
            pid,
            memory_usage,
            cpu_usage,
        })
    }

    /// Read unit state from cgroup
    fn read_unit_state(&self, unit_path: &Path) -> Result<SystemdUnitState> {
        if !unit_path.exists() {
            return Ok(SystemdUnitState::Inactive);
        }

        // Check if there are any processes in the cgroup
        let tasks_file = unit_path.join("cgroup.procs");
        if tasks_file.exists() {
            let contents = std::fs::read_to_string(&tasks_file)?;
            if contents.trim().is_empty() {
                Ok(SystemdUnitState::Inactive)
            } else {
                Ok(SystemdUnitState::Active)
            }
        } else {
            Ok(SystemdUnitState::Unknown)
        }
    }

    /// Read main PID from cgroup
    fn read_unit_main_pid(&self, unit_path: &Path) -> Result<Option<u32>> {
        let tasks_file = unit_path.join("cgroup.procs");
        if tasks_file.exists() {
            let contents = std::fs::read_to_string(&tasks_file)?;
            if let Some(first_line) = contents.lines().next()
            {
                let first_line = first_line.trim();
                if first_line.is_empty() {
                    return Ok(None);
                }
                let pid = first_line.parse::<u32>().map_err(|error| {
                    eyre!(
                        "Failed to parse unit main PID from {}: '{}' ({error})",
                        tasks_file.display(),
                        first_line
                    )
                })?;
                return Ok(Some(pid));
            }
        }
        Ok(None)
    }

    /// Read memory usage from cgroup
    fn read_memory_usage(&self, unit_path: &Path) -> Result<Option<u64>> {
        let memory_file = unit_path.join("memory.current");
        if memory_file.exists() {
            let contents = std::fs::read_to_string(&memory_file)?;
            let raw = contents.trim();
            if raw.is_empty() {
                return Ok(None);
            }
            let bytes = raw.parse::<u64>().map_err(|error| {
                eyre!(
                    "Failed to parse unit memory usage from {}: '{}' ({error})",
                    memory_file.display(),
                    raw
                )
            })?;
            return Ok(Some(bytes));
        }
        Ok(None)
    }

    /// Read CPU usage from cgroup
    fn read_cpu_usage(&self, unit_path: &Path) -> Result<Option<Duration>> {
        let cpu_file = unit_path.join("cpu.stat");
        if cpu_file.exists() {
            let contents = std::fs::read_to_string(&cpu_file)?;
            for line in contents.lines() {
                if line.starts_with("usage_usec")
                {
                    let Some(value) = line.split_whitespace().nth(1) else {
                        return Err(eyre!(
                            "unit cpu.stat entry '{}' in {} is missing usage value",
                            line,
                            cpu_file.display()
                        ));
                    };
                    let usecs = value.parse::<u64>().map_err(|error| {
                        eyre!(
                            "Failed to parse unit CPU usage from {}: '{}' ({error})",
                            cpu_file.display(),
                            value
                        )
                    })?;
                    return Ok(Some(Duration::from_micros(usecs)));
                }
            }
        }
        Ok(None)
    }

    /// Send signal to a unit's main process
    pub fn signal_unit(&self, unit_name: &str, signal: Signal) -> Result<()> {
        let unit = self.get_unit_status(unit_name)?;
        if let Some(pid) = unit.pid {
            signal::kill(Pid::from_raw(pid as i32), signal)?;
            info!("Sent signal {:?} to {} (PID {})", signal, unit_name, pid);
        } else {
            return Err(eyre!("Unit {} has no main PID", unit_name));
        }
        Ok(())
    }
}

impl SystemdUnitType {
    /// Determine unit type from unit name
    #[must_use]
    pub fn from_name(name: &str) -> Self {
        if name.ends_with(".service") {
            Self::Service
        } else if name.ends_with(".timer") {
            Self::Timer
        } else if name.ends_with(".socket") {
            Self::Socket
        } else if name.ends_with(".target") {
            Self::Target
        } else if name.ends_with(".mount") || name.ends_with(".automount") {
            Self::Mount
        } else if name.ends_with(".device") {
            Self::Device
        } else if name.ends_with(".scope") {
            Self::Scope
        } else if name.ends_with(".slice") {
            Self::Slice
        } else {
            Self::Other
        }
    }
}

/// Journal reader that tails journal files directly without journald IPC.
pub struct JournalReader {
    journal_path: PathBuf,
    file: Option<File>,
    last_position: u64,
}

impl JournalReader {
    /// Create a new journal reader
    pub fn new() -> Result<Self> {
        // Try common journal locations
        let journal_path = if Path::new("/run/log/journal").exists() {
            PathBuf::from("/run/log/journal")
        } else if Path::new("/var/log/journal").exists() {
            PathBuf::from("/var/log/journal")
        } else {
            return Err(eyre!("Cannot find systemd journal directory"));
        };

        Ok(Self {
            journal_path,
            file: None,
            last_position: 0,
        })
    }

    /// Open the system journal for reading
    pub fn open_system_journal(&mut self) -> Result<()> {
        // Find the system journal file
        let machine_id = std::fs::read_to_string("/etc/machine-id")?
            .trim()
            .to_string();

        let journal_dir = self.journal_path.join(&machine_id);
        if !journal_dir.exists() {
            return Err(eyre!(
                "Journal directory not found for machine {}",
                machine_id
            ));
        }

        // Find the current system.journal file
        let system_journal = journal_dir.join("system.journal");
        if system_journal.exists() {
            let file = OpenOptions::new().read(true).open(&system_journal)?;

            // Seek to end for following new entries
            let metadata = file.metadata()?;
            self.last_position = metadata.len();

            self.file = Some(file);
            info!("Opened system journal at {:?}", system_journal);
            Ok(())
        } else {
            Err(eyre!("System journal file not found"))
        }
    }

    /// Read new entries from the journal (simplified text reading)
    /// Note: For production use, you'd want to parse the binary journal format
    pub async fn read_new_entries(&mut self) -> Result<Vec<String>> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| eyre!("Journal not opened"))?;

        // Check if file has grown
        let metadata = file.metadata()?;
        let current_size = metadata.len();

        if current_size > self.last_position {
            // Seek to last read position
            file.seek(SeekFrom::Start(self.last_position))?;

            let mut entries = Vec::new();
            let reader = BufReader::new(file);

            for (line_index, line) in reader.lines().enumerate() {
                let entry = line.map_err(|error| {
                    eyre!(
                        "Failed to read journal line {} from {:?}: {}",
                        line_index + 1,
                        self.journal_path,
                        error
                    )
                })?;
                entries.push(entry);
            }

            self.last_position = current_size;
            Ok(entries)
        } else {
            Ok(Vec::new())
        }
    }

    /// Follow journal for new entries
    pub async fn follow_journal(mut self, tx: mpsc::Sender<String>) -> Result<()> {
        self.open_system_journal()?;

        loop {
            let entries = self.read_new_entries().await?;
            if !forward_journal_entries(&tx, entries).await {
                return Ok(());
            }

            // Poll interval: 100ms provides responsive state change detection
            // without excessive CPU usage (10 checks/sec is reasonable for systemd units)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn forward_journal_entries(tx: &mpsc::Sender<String>, entries: Vec<String>) -> bool {
    for entry in entries {
        if tx.send(entry).await.is_err() {
            info!("Journal follower: receiver dropped");
            return false;
        }
    }

    true
}

/// Helper to monitor systemd unit changes via cgroup inotify
/// Note: This would ideally use inotify via nix crate for efficiency
pub struct SystemdChangeMonitor {
    monitor: SystemdMonitor,
    known_units: HashMap<String, SystemdUnit>,
}

impl SystemdChangeMonitor {
    pub fn new() -> Result<Self> {
        Ok(Self {
            monitor: SystemdMonitor::new()?,
            known_units: HashMap::new(),
        })
    }

    /// Poll for unit changes (simplified - real implementation would use inotify)
    pub async fn poll_changes(&mut self) -> Result<Vec<SystemdChange>> {
        let mut changes = Vec::new();

        // Get current units (spawn_blocking for /proc reads)
        let monitor = self.monitor.clone();
        let units = tokio::task::spawn_blocking(move || monitor.list_service_units())
            .await
            .map_err(|e| eyre!("Task join error: {}", e))??;

        for unit_name in units.clone() {
            // Spawn blocking for cgroup/proc reads
            let monitor = self.monitor.clone();
            let unit_name_clone = unit_name.clone();
            match tokio::task::spawn_blocking(move || monitor.get_unit_status(&unit_name_clone))
                .await
                .map_err(|e| eyre!("Task join error: {}", e))?
            {
                Ok(current_unit) => {
                    if let Some(known_unit) = self.known_units.get(&unit_name) {
                        // Check for state changes
                        if known_unit.state != current_unit.state {
                            changes.push(SystemdChange::StateChanged {
                                unit: unit_name.clone(),
                                old_state: known_unit.state.clone(),
                                new_state: current_unit.state.clone(),
                            });
                        }
                    } else {
                        // New unit discovered
                        changes.push(SystemdChange::UnitAdded {
                            unit: unit_name.clone(),
                            state: current_unit.state.clone(),
                        });
                    }
                    self.known_units.insert(unit_name, current_unit);
                }
                Err(e) => {
                    warn!("Failed to get status for {}: {}", unit_name, e);
                }
            }
        }

        // Check for removed units
        let current_units: std::collections::HashSet<_> = units.into_iter().collect();
        let removed: Vec<_> = self
            .known_units
            .keys()
            .filter(|k| !current_units.contains(k.as_str()))
            .cloned()
            .collect();

        for unit_name in removed {
            changes.push(SystemdChange::UnitRemoved {
                unit: unit_name.clone(),
            });
            self.known_units.remove(&unit_name);
        }

        Ok(changes)
    }
}

#[cfg(test)]
mod tests {
    use super::{JournalReader, SystemdMonitor, forward_journal_entries};
    use std::fs::OpenOptions;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};
    use xtask::sandbox::sinex_test;

    fn temp_path(prefix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "{prefix}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock before epoch")
                .as_nanos()
        ))
    }

    fn write_unit_file(
        unit_path: &Path,
        file_name: &str,
        contents: &[u8],
    ) -> color_eyre::eyre::Result<()> {
        std::fs::create_dir_all(unit_path)?;
        std::fs::write(unit_path.join(file_name), contents)?;
        Ok(())
    }

    #[sinex_test]
    async fn forward_journal_entries_stops_when_receiver_drops() -> TestResult<()> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);

        let forwarded =
            forward_journal_entries(&tx, vec!["entry-1".to_string(), "entry-2".to_string()]).await;

        assert!(!forwarded);
        Ok(())
    }

    #[sinex_test]
    async fn forward_journal_entries_delivers_all_entries() -> TestResult<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);

        let forwarded =
            forward_journal_entries(&tx, vec!["entry-1".to_string(), "entry-2".to_string()]).await;

        assert!(forwarded);
        assert_eq!(rx.recv().await.as_deref(), Some("entry-1"));
        assert_eq!(rx.recv().await.as_deref(), Some("entry-2"));
        Ok(())
    }

    #[sinex_test]
    async fn journal_reader_reads_new_entries_and_advances_position() -> TestResult<()> {
        let temp_dir = std::env::temp_dir().join(format!(
            "sinex-systemd-journal-test-{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&temp_dir)?;
        let journal_file = temp_dir.join("system.journal");
        std::fs::write(&journal_file, b"entry-1\nentry-2\n")?;

        let file = OpenOptions::new().read(true).open(&journal_file)?;
        let mut reader = JournalReader {
            journal_path: temp_dir.clone(),
            file: Some(file),
            last_position: 0,
        };

        let entries = reader.read_new_entries().await?;

        assert_eq!(entries, vec!["entry-1".to_string(), "entry-2".to_string()]);
        assert_eq!(reader.last_position, std::fs::metadata(&journal_file)?.len());
        std::fs::remove_dir_all(&temp_dir)?;
        Ok(())
    }

    #[sinex_test]
    async fn journal_reader_rejects_invalid_utf8_without_advancing_position() -> TestResult<()> {
        let temp_dir = std::env::temp_dir().join(format!(
            "sinex-systemd-journal-test-{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&temp_dir)?;
        let journal_file = temp_dir.join("system.journal");
        std::fs::write(&journal_file, b"entry-1\n\xffentry-2\n")?;

        let file = OpenOptions::new().read(true).open(&journal_file)?;
        let mut reader = JournalReader {
            journal_path: temp_dir.clone(),
            file: Some(file),
            last_position: 0,
        };

        let error = reader
            .read_new_entries()
            .await
            .expect_err("invalid UTF-8 journal lines must fail honestly");

        assert!(error.to_string().contains("Failed to read journal line 2"));
        assert_eq!(reader.last_position, 0);
        std::fs::remove_dir_all(&temp_dir)?;
        Ok(())
    }

    #[sinex_test]
    async fn systemd_monitor_rejects_invalid_cgroup_pid() -> TestResult<()> {
        let temp_dir = temp_path("sinex-systemd-cgroup-pid");
        let unit_path = temp_dir.join("broken.service");
        write_unit_file(&unit_path, "cgroup.procs", b"abc\n")?;

        let monitor = SystemdMonitor {
            cgroup_base: temp_dir.clone(),
        };

        let error = monitor
            .read_unit_main_pid(&unit_path)
            .expect_err("invalid cgroup pid must fail honestly");

        assert!(error.to_string().contains("Failed to parse unit main PID"));
        assert!(error.to_string().contains("abc"));
        std::fs::remove_dir_all(&temp_dir)?;
        Ok(())
    }

    #[sinex_test]
    async fn systemd_monitor_rejects_invalid_memory_usage() -> TestResult<()> {
        let temp_dir = temp_path("sinex-systemd-memory");
        let unit_path = temp_dir.join("broken.service");
        write_unit_file(&unit_path, "memory.current", b"nan\n")?;

        let monitor = SystemdMonitor {
            cgroup_base: temp_dir.clone(),
        };

        let error = monitor
            .read_memory_usage(&unit_path)
            .expect_err("invalid memory.current must fail honestly");

        assert!(error.to_string().contains("Failed to parse unit memory usage"));
        assert!(error.to_string().contains("nan"));
        std::fs::remove_dir_all(&temp_dir)?;
        Ok(())
    }

    #[sinex_test]
    async fn systemd_monitor_rejects_invalid_cpu_usage() -> TestResult<()> {
        let temp_dir = temp_path("sinex-systemd-cpu");
        let unit_path = temp_dir.join("broken.service");
        write_unit_file(&unit_path, "cpu.stat", b"usage_usec nope\n")?;

        let monitor = SystemdMonitor {
            cgroup_base: temp_dir.clone(),
        };

        let error = monitor
            .read_cpu_usage(&unit_path)
            .expect_err("invalid cpu.stat usage_usec must fail honestly");

        assert!(error.to_string().contains("Failed to parse unit CPU usage"));
        assert!(error.to_string().contains("nope"));
        std::fs::remove_dir_all(&temp_dir)?;
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_test]
    async fn systemd_monitor_rejects_non_utf8_unit_names() -> TestResult<()> {
        use std::os::unix::ffi::OsStringExt;

        let temp_dir = temp_path("sinex-systemd-units");
        let service_dir = temp_dir.join("system.slice");
        std::fs::create_dir_all(&service_dir)?;
        let invalid_name = std::ffi::OsString::from_vec(vec![
            b's', b'i', b'n', b'e', b'x', b'-', 0xff, b'.', b's', b'e', b'r', b'v', b'i', b'c',
            b'e',
        ]);
        std::fs::write(service_dir.join(invalid_name), [])?;

        let monitor = SystemdMonitor {
            cgroup_base: temp_dir.clone(),
        };

        let error = monitor
            .list_service_units()
            .expect_err("non-utf8 unit names must fail honestly");

        assert!(error.to_string().contains("decode systemd unit name as UTF-8"));
        std::fs::remove_dir_all(&temp_dir)?;
        Ok(())
    }
}

/// Represents a change in systemd units
#[derive(Debug, Clone)]
pub enum SystemdChange {
    UnitAdded {
        unit: String,
        state: SystemdUnitState,
    },
    UnitRemoved {
        unit: String,
    },
    StateChanged {
        unit: String,
        old_state: SystemdUnitState,
        new_state: SystemdUnitState,
    },
}
