#![doc = include_str!("../docs/systemd_integration.md")]

//! Modern systemd/journald integration using the `nix` crate.

use color_eyre::eyre::{eyre, Result};
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

/// Modern systemd monitor using cgroup filesystem
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
                let name = entry.file_name().to_string_lossy().to_string();
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
            if let Some(first_line) = contents.lines().next() {
                if let Ok(pid) = first_line.trim().parse::<u32>() {
                    return Ok(Some(pid));
                }
            }
        }
        Ok(None)
    }

    /// Read memory usage from cgroup
    fn read_memory_usage(&self, unit_path: &Path) -> Result<Option<u64>> {
        let memory_file = unit_path.join("memory.current");
        if memory_file.exists() {
            let contents = std::fs::read_to_string(&memory_file)?;
            if let Ok(bytes) = contents.trim().parse::<u64>() {
                return Ok(Some(bytes));
            }
        }
        Ok(None)
    }

    /// Read CPU usage from cgroup
    fn read_cpu_usage(&self, unit_path: &Path) -> Result<Option<Duration>> {
        let cpu_file = unit_path.join("cpu.stat");
        if cpu_file.exists() {
            let contents = std::fs::read_to_string(&cpu_file)?;
            for line in contents.lines() {
                if line.starts_with("usage_usec") {
                    if let Some(value) = line.split_whitespace().nth(1) {
                        if let Ok(usecs) = value.parse::<u64>() {
                            return Ok(Some(Duration::from_micros(usecs)));
                        }
                    }
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

/// Modern journal reader using direct file access
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

            for line in reader.lines() {
                match line {
                    Ok(entry) => entries.push(entry),
                    Err(e) => {
                        warn!("Error reading journal line: {}", e);
                        break;
                    }
                }
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
            for entry in entries {
                if tx.send(entry).await.is_err() {
                    info!("Journal follower: receiver dropped");
                    break;
                }
            }

            // Poll interval
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
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

        // Get current units
        let units = self.monitor.list_service_units()?;

        for unit_name in units.clone() {
            match self.monitor.get_unit_status(&unit_name) {
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
