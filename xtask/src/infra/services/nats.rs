use color_eyre::eyre::{Result, WrapErr, bail};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Kill stale nats-server processes that may be orphaned.
///
/// This helps prevent "port already in use" errors and cleans up
/// processes that accumulate during development.
pub fn cleanup_stale_nats_processes(target_port: u16, verbose: bool) -> Result<usize> {
    let mut killed = 0;

    // Find all nats-server processes
    let pids = parse_nats_pgrep_output(Command::new("pgrep").args(["-f", "nats-server"]).output())?;

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

fn parse_nats_pgrep_output(output: std::io::Result<std::process::Output>) -> Result<Vec<u32>> {
    let output = output.wrap_err("failed to inspect running nats-server processes with pgrep")?;
    match output.status.code() {
        Some(0) => Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| line.trim().parse().ok())
            .collect()),
        Some(1) => Ok(Vec::new()),
        _ => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let detail = stderr.trim();
            let suffix = if detail.is_empty() {
                String::new()
            } else {
                format!(" ({detail})")
            };
            bail!("pgrep -f nats-server exited unsuccessfully{suffix}");
        }
    }
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

fn parse_listener_port(listener: &str) -> Option<u16> {
    listener.rsplit(':').next()?.parse::<u16>().ok()
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NatsPidState {
    Missing,
    Running(u32),
    Stale(u32),
}

fn remove_service_file(path: &Path, label: &str) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .wrap_err_with(|| format!("failed to remove {label} {}", path.display())),
    }
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
        match self.pid_state()? {
            NatsPidState::Missing => {}
            NatsPidState::Stale(pid) => {
                if verbose {
                    println!("Removing stale NATS pid file for dead or foreign PID {pid}");
                }
                self.remove_pid_file_if_present("stale NATS pid file")?;
            }
            NatsPidState::Running(pid) => {
                if let Some(actual_port) = self.listener_port_for_pid(pid)? {
                    if actual_port == self.config.port {
                        if verbose {
                            println!("NATS already running");
                        }
                        return Ok(());
                    }

                    if verbose {
                        println!(
                            "Restarting NATS on port {} to converge on {}",
                            actual_port, self.config.port
                        );
                    }
                    self.stop(verbose)?;
                } else {
                    if verbose {
                        println!("Restarting NATS because its listener port could not be verified");
                    }
                    self.stop(verbose)?;
                }
            }
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
        let pid = match self.pid_state()? {
            NatsPidState::Missing => {
                if verbose {
                    println!("NATS not running");
                }
                return Ok(());
            }
            NatsPidState::Stale(pid) => {
                if verbose {
                    println!("Cleaning up stale NATS pid file for dead or foreign PID {pid}");
                }
                self.remove_pid_file_if_present("stale NATS pid file")?;
                return Ok(());
            }
            NatsPidState::Running(pid) => pid,
        };

        if verbose {
            println!("Stopping NATS...");
        }

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

        self.remove_pid_file_if_present("NATS pid file")?;

        if verbose {
            println!("NATS stopped");
        }

        Ok(())
    }

    #[must_use]
    pub fn is_running(&self) -> bool {
        match self.pid_state() {
            Ok(NatsPidState::Running(_)) => true,
            Ok(NatsPidState::Missing | NatsPidState::Stale(_)) => false,
            Err(error) => {
                tracing::warn!(path = %self.config.pid_file.display(), error = %error, "failed to inspect nats pid file");
                false
            }
        }
    }

    #[must_use]
    pub fn read_pid(&self) -> Option<u32> {
        match self.read_pid_result() {
            Ok(pid) => pid,
            Err(error) => {
                tracing::warn!(path = %self.config.pid_file.display(), error = %error, "failed to read nats pid file");
                None
            }
        }
    }

    fn is_running_pid(&self, pid: u32) -> bool {
        if unsafe { libc::kill(pid as i32, 0) } != 0 {
            return false;
        }
        // On Linux, verify the process is actually nats-server and not a recycled PID
        // (e.g. after a machine restart). /proc/<pid>/cmdline is NUL-separated args.
        #[cfg(target_os = "linux")]
        {
            match std::fs::read_to_string(format!("/proc/{pid}/cmdline")) {
                Ok(cmdline) => {
                    if !cmdline.contains("nats-server") {
                        return false;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return false,
                Err(error) => {
                    tracing::warn!(pid, error = %error, "failed to read nats process command line");
                }
            }
        }
        true
    }

    fn listener_port_for_pid(&self, pid: u32) -> Result<Option<u16>> {
        listener_port_for_pid_probe(pid, Command::new("ss").args(["-ltnp"]).output())
    }

    fn nats_command(&self) -> Command {
        if let Ok(path) = std::env::var("NATS_SERVER_BIN") {
            Command::new(path)
        } else {
            Command::new("nats-server")
        }
    }

    fn read_pid_result(&self) -> Result<Option<u32>> {
        if !self.config.pid_file.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&self.config.pid_file).wrap_err_with(|| {
            format!("failed to read NATS pid file {}", self.config.pid_file.display())
        })?;
        let pid_str = content.trim();
        if pid_str.is_empty() {
            bail!("NATS pid file {} is empty", self.config.pid_file.display());
        }

        let pid = pid_str.parse::<u32>().wrap_err_with(|| {
            format!("failed to parse NATS pid from {}", self.config.pid_file.display())
        })?;
        Ok(Some(pid))
    }

    fn pid_state(&self) -> Result<NatsPidState> {
        let Some(pid) = self.read_pid_result()? else {
            return Ok(NatsPidState::Missing);
        };

        if self.is_running_pid(pid) {
            Ok(NatsPidState::Running(pid))
        } else {
            Ok(NatsPidState::Stale(pid))
        }
    }

    fn remove_pid_file_if_present(&self, label: &str) -> Result<()> {
        remove_service_file(&self.config.pid_file, label)
    }
}

fn listener_port_for_pid_probe(
    pid: u32,
    output: std::io::Result<std::process::Output>,
) -> Result<Option<u16>> {
    let output = output.wrap_err("failed to inspect NATS listeners with ss")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let detail = stderr.trim();
        let suffix = if detail.is_empty() {
            String::new()
        } else {
            format!(" ({detail})")
        };
        bail!("ss -ltnp exited unsuccessfully while inspecting NATS listeners{suffix}");
    }

    let pid_marker = format!("pid={pid}");
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| line.contains("LISTEN"))
        .filter(|line| line.contains("nats-server"))
        .find(|line| line.contains(&pid_marker))
        .and_then(|line| line.split_whitespace().nth(3))
        .and_then(parse_listener_port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    fn test_manager(root: &tempfile::TempDir) -> NatsManager {
        NatsManager::new(NatsConfig {
            port: 4222,
            config_file: root.path().join("nats.conf"),
            data_dir: root.path().join("data"),
            pid_file: root.path().join("run/nats.pid"),
            log_file: root.path().join("run/nats.log"),
        })
    }

    #[sinex_test]
    async fn parses_ipv4_and_wildcard_listener_ports() -> TestResult<()> {
        assert_eq!(parse_listener_port("*:4321"), Some(4321));
        assert_eq!(parse_listener_port("127.0.0.1:4250"), Some(4250));
        assert_eq!(parse_listener_port("[::]:4222"), Some(4222));
        Ok(())
    }

    #[sinex_test]
    async fn rejects_non_numeric_listener_ports() -> TestResult<()> {
        assert_eq!(parse_listener_port("*"), None);
        assert_eq!(parse_listener_port("localhost:http"), None);
        Ok(())
    }

    #[sinex_test]
    async fn listener_port_for_pid_probe_reports_ss_spawn_failures() -> TestResult<()> {
        let error =
            listener_port_for_pid_probe(123, Err(std::io::Error::other("ss exploded"))).unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("failed to inspect NATS listeners with ss"));
        assert!(message.contains("ss exploded"));
        Ok(())
    }

    #[sinex_test]
    async fn listener_port_for_pid_probe_reports_ss_exit_failures() -> TestResult<()> {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;

            let error = listener_port_for_pid_probe(
                123,
                Ok(std::process::Output {
                    status: std::process::ExitStatus::from_raw(256),
                    stdout: Vec::new(),
                    stderr: b"permission denied".to_vec(),
                }),
            )
            .unwrap_err();
            let message = format!("{error:#}");
            assert!(message.contains("ss -ltnp exited unsuccessfully"));
            assert!(message.contains("permission denied"));
        }
        Ok(())
    }

    #[sinex_test]
    async fn listener_port_for_pid_probe_extracts_matching_port() -> TestResult<()> {
        let port = listener_port_for_pid_probe(
            123,
            Ok(std::process::Output {
                status: std::process::ExitStatus::default(),
                stdout: br#"State  Recv-Q Send-Q Local Address:Port Peer Address:PortProcess
LISTEN 0      4096   127.0.0.1:4222      0.0.0.0:*    users:(("nats-server",pid=123,fd=7))
"#
                .to_vec(),
                stderr: Vec::new(),
            }),
        )?;
        assert_eq!(port, Some(4222));
        Ok(())
    }

    #[sinex_test]
    async fn parse_nats_pgrep_output_reports_spawn_failures() -> TestResult<()> {
        let error = parse_nats_pgrep_output(Err(std::io::Error::other("pgrep exploded"))).unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("failed to inspect running nats-server processes with pgrep"));
        assert!(message.contains("pgrep exploded"));
        Ok(())
    }

    #[sinex_test]
    async fn parse_nats_pgrep_output_treats_exit_one_as_no_matches() -> TestResult<()> {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;

            let pids = parse_nats_pgrep_output(Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(256),
                stdout: Vec::new(),
                stderr: Vec::new(),
            }))?;
            assert!(pids.is_empty());
        }
        Ok(())
    }

    #[sinex_test]
    async fn parse_nats_pgrep_output_reports_exit_failures() -> TestResult<()> {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;

            let error = parse_nats_pgrep_output(Ok(std::process::Output {
                status: std::process::ExitStatus::from_raw(512),
                stdout: Vec::new(),
                stderr: b"permission denied".to_vec(),
            }))
            .unwrap_err();
            let message = format!("{error:#}");
            assert!(message.contains("pgrep -f nats-server exited unsuccessfully"));
            assert!(message.contains("permission denied"));
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_read_pid_result_reports_malformed_pid_file() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let manager = test_manager(&temp);
        fs::create_dir_all(manager.config.pid_file.parent().unwrap())?;
        fs::write(&manager.config.pid_file, "not-a-pid\n")?;

        let error = manager.read_pid_result().unwrap_err();
        assert!(format!("{error:#}").contains("failed to parse NATS pid"));
        Ok(())
    }

    #[sinex_test]
    async fn test_remove_service_file_reports_remove_failures() -> TestResult<()> {
        let temp = tempfile::tempdir()?;

        let error = remove_service_file(temp.path(), "test pid file").unwrap_err();
        assert!(format!("{error:#}").contains("failed to remove test pid file"));
        Ok(())
    }
}
