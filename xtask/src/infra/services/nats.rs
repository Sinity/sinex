use color_eyre::eyre::{Result, WrapErr, bail};
use std::fs;
use std::path::PathBuf;
use std::process::Command;

/// Kill stale nats-server processes that may be orphaned.
///
/// This helps prevent "port already in use" errors and cleans up
/// processes that accumulate during development.
pub fn cleanup_stale_nats_processes(target_port: u16, verbose: bool) -> Result<usize> {
    let mut killed = 0;

    // Find all nats-server processes
    let output = Command::new("pgrep").args(["-f", "nats-server"]).output();

    let pids: Vec<u32> = match output {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|line| line.trim().parse().ok())
            .collect(),
        _ => return Ok(0), // No nats-server processes found
    };

    if pids.is_empty() {
        return Ok(0);
    }

    // Check each process to see if it's using our target port or is orphaned
    for pid in pids {
        // Check if this process is using the target port
        let lsof_output = Command::new("lsof")
            .args(["-p", &pid.to_string(), "-i", &format!(":{target_port}")])
            .output();

        let uses_target_port =
            lsof_output.is_ok_and(|out| out.status.success() && !out.stdout.is_empty());

        // Also check if the process has been running for a long time (> 2 hours)
        // as a heuristic for "orphaned"
        let is_old = is_process_old(pid, 2 * 3600);

        if uses_target_port || is_old {
            if verbose {
                if uses_target_port {
                    eprintln!("⚠️  Killing stale nats-server (PID {pid}) using port {target_port}");
                } else {
                    eprintln!("⚠️  Killing old nats-server (PID {pid}) running > 2 hours");
                }
            }

            // Send SIGTERM first for graceful shutdown
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }

            // Wait briefly for termination
            std::thread::sleep(std::time::Duration::from_millis(500));

            // Check if still running, send SIGKILL if needed
            if unsafe { libc::kill(pid as i32, 0) } == 0 {
                unsafe {
                    libc::kill(pid as i32, libc::SIGKILL);
                }
            }

            killed += 1;
        }
    }

    Ok(killed)
}

/// Check if a process has been running longer than the given threshold (in seconds).
fn is_process_old(pid: u32, threshold_secs: u64) -> bool {
    // Read process start time from /proc on Linux
    #[cfg(target_os = "linux")]
    {
        let stat_path = format!("/proc/{pid}/stat");
        if let Ok(stat) = fs::read_to_string(&stat_path) {
            // The 22nd field is starttime in clock ticks since boot
            let fields: Vec<&str> = stat.split_whitespace().collect();
            if fields.len() > 21
                && let Ok(start_ticks) = fields[21].parse::<u64>()
            {
                // Get system uptime
                if let Ok(uptime_str) = fs::read_to_string("/proc/uptime")
                    && let Some(uptime_secs) = uptime_str
                        .split_whitespace()
                        .next()
                        .and_then(|s| s.parse::<f64>().ok())
                {
                    // Clock ticks per second (usually 100)
                    let ticks_per_sec = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as u64;
                    let process_uptime_secs = uptime_secs as u64 - (start_ticks / ticks_per_sec);
                    return process_uptime_secs > threshold_secs;
                }
            }
        }
    }

    // On non-Linux, use ps command as fallback
    #[cfg(not(target_os = "linux"))]
    {
        let output = Command::new("ps")
            .args(["-o", "etime=", "-p", &pid.to_string()])
            .output();

        if let Ok(out) = output {
            if out.status.success() {
                let etime = String::from_utf8_lossy(&out.stdout);
                // Parse elapsed time format: [[DD-]HH:]MM:SS
                if let Some(secs) = parse_etime(&etime.trim()) {
                    return secs > threshold_secs;
                }
            }
        }
    }

    false
}

/// Parse ps etime format: [[DD-]HH:]MM:SS -> seconds
#[cfg(not(target_os = "linux"))]
fn parse_etime(etime: &str) -> Option<u64> {
    let parts: Vec<&str> = etime.split(':').collect();
    match parts.len() {
        2 => {
            // MM:SS
            let mins: u64 = parts[0].parse().ok()?;
            let secs: u64 = parts[1].parse().ok()?;
            Some(mins * 60 + secs)
        }
        3 => {
            // HH:MM:SS or DD-HH:MM:SS
            let first = parts[0];
            if first.contains('-') {
                // DD-HH:MM:SS
                let day_hour: Vec<&str> = first.split('-').collect();
                let days: u64 = day_hour[0].parse().ok()?;
                let hours: u64 = day_hour[1].parse().ok()?;
                let mins: u64 = parts[1].parse().ok()?;
                let secs: u64 = parts[2].parse().ok()?;
                Some(days * 86400 + hours * 3600 + mins * 60 + secs)
            } else {
                // HH:MM:SS
                let hours: u64 = first.parse().ok()?;
                let mins: u64 = parts[1].parse().ok()?;
                let secs: u64 = parts[2].parse().ok()?;
                Some(hours * 3600 + mins * 60 + secs)
            }
        }
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub struct NatsConfig {
    pub port: u16,
    pub config_file: PathBuf,
    pub data_dir: PathBuf,
    pub pid_file: PathBuf,
    pub log_file: PathBuf,
}

pub struct NatsManager {
    config: NatsConfig,
}

impl NatsManager {
    #[must_use]
    pub fn new(config: NatsConfig) -> Self {
        Self { config }
    }

    pub fn generate_config(&self) -> Result<()> {
        let store_dir = self.config.data_dir.join("jetstream");
        let expected_conf = format!(
            r#"# sinex-dev isolated NATS configuration
port = {}
jetstream {{
    store_dir = "{}"
    max_mem = 256MB
    max_file = 1GB
}}
"#,
            self.config.port,
            store_dir.display()
        );

        // Check if existing config matches expected (handles port changes)
        if self.config.config_file.exists()
            && let Ok(existing) = fs::read_to_string(&self.config.config_file)
            && existing == expected_conf
        {
            return Ok(());
        }
        // Port or config changed - regenerate

        if let Some(parent) = self.config.config_file.parent() {
            fs::create_dir_all(parent)?;
        }

        // Ensure store dir exists, though NATS might create it
        fs::create_dir_all(&store_dir)?;

        fs::write(&self.config.config_file, expected_conf)?;
        Ok(())
    }

    pub fn start(&self, verbose: bool) -> Result<()> {
        if self.is_running() {
            if verbose {
                println!("NATS already running");
            }
            return Ok(());
        }

        // Clean up any stale nats-server processes that might be blocking our port
        let cleaned = cleanup_stale_nats_processes(self.config.port, verbose)?;
        if cleaned > 0 && verbose {
            println!("Cleaned up {cleaned} stale nats-server process(es)");
        }

        if verbose {
            println!("Starting NATS on port {}...", self.config.port);
        }

        if let Some(parent) = self.config.log_file.parent() {
            fs::create_dir_all(parent)?;
        }
        let log_file = fs::File::create(&self.config.log_file)?;

        let child = self
            .nats_command()
            .args([
                "-js",
                "-c",
                self.config
                    .config_file
                    .to_str()
                    .expect("config file path must be valid UTF-8"),
            ])
            .stdout(log_file.try_clone()?)
            .stderr(log_file)
            .spawn()
            .context("Failed to start NATS")?;

        if let Some(parent) = self.config.pid_file.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&self.config.pid_file, child.id().to_string())?;

        // Wait for port
        for _ in 0..30 {
            if std::net::TcpStream::connect(format!("127.0.0.1:{}", self.config.port)).is_ok() {
                if verbose {
                    println!("NATS started");
                }
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }

        bail!("NATS failed to start within 15 seconds")
    }

    pub fn stop(&self, verbose: bool) -> Result<()> {
        if !self.is_running() {
            if verbose {
                println!("NATS not running");
            }
            // Cleanup pid file if it exists but process doesn't
            if self.config.pid_file.exists() {
                let _ = fs::remove_file(&self.config.pid_file);
            }
            return Ok(());
        }

        if verbose {
            println!("Stopping NATS...");
        }

        if let Some(pid) = self.read_pid() {
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
            // Wait for exit
            for _ in 0..40 {
                if !self.is_running_pid(pid) {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(250));
            }
        }

        let _ = fs::remove_file(&self.config.pid_file);

        if verbose {
            println!("NATS stopped");
        }

        Ok(())
    }

    #[must_use]
    pub fn is_running(&self) -> bool {
        if let Some(pid) = self.read_pid() {
            self.is_running_pid(pid)
        } else {
            false
        }
    }

    #[must_use] 
    pub fn read_pid(&self) -> Option<u32> {
        fs::read_to_string(&self.config.pid_file)
            .ok()
            .and_then(|s| s.trim().parse().ok())
    }

    fn is_running_pid(&self, pid: u32) -> bool {
        if unsafe { libc::kill(pid as i32, 0) } != 0 {
            return false;
        }
        // On Linux, verify the process is actually nats-server and not a recycled PID
        // (e.g. after a machine restart). /proc/<pid>/cmdline is NUL-separated args.
        #[cfg(target_os = "linux")]
        {
            if let Ok(cmdline) = std::fs::read_to_string(format!("/proc/{pid}/cmdline"))
                && !cmdline.contains("nats-server") {
                    // Stale PID file: a different process now holds this PID
                    let _ = std::fs::remove_file(&self.config.pid_file);
                    return false;
                }
        }
        true
    }

    fn nats_command(&self) -> Command {
        if let Ok(path) = std::env::var("NATS_SERVER_BIN") {
            Command::new(path)
        } else {
            Command::new("nats-server")
        }
    }
}
