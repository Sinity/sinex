use crate::process::ProcessBuilder;
use color_eyre::eyre::{Result, WrapErr};
use serde::{Deserialize, Serialize};
use std::process::Command;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Environment {
    pub timestamp: String,
    pub hostname: String,
    pub uname: String,
    pub kernel: String,
    pub arch: String,
    pub os: String,
    pub cpu_model: String,
    pub cpu_cores: u32,
    pub cpu_threads: u32,
    pub memory_total_kb: u64,
    pub memory_available_kb: u64,
    pub load_avg: String,
    pub rustc_version: String,
    pub cargo_version: String,
    pub rustup_toolchain: String,
    pub postgres_version: String,
    pub database_url_masked: String,
    pub nats_url: String,
    pub git_sha: String,
    pub git_sha_short: String,
    pub git_branch: String,
    pub git_dirty: bool,
    pub probe_issues: Vec<String>,
}

impl Environment {
    pub(super) fn capture() -> Self {
        let mut probe_issues = Vec::new();
        Self {
            timestamp: sinex_primitives::temporal::Timestamp::now().format_rfc3339(),
            hostname: capture_probe(&mut probe_issues, "hostname", hostname(), "unknown"),
            uname: capture_probe(&mut probe_issues, "uname", uname(), "unknown"),
            kernel: capture_probe(&mut probe_issues, "kernel", kernel_version(), "unknown"),
            arch: std::env::consts::ARCH.to_string(),
            os: capture_probe(&mut probe_issues, "os_release", os_release(), "unknown"),
            cpu_model: capture_probe(&mut probe_issues, "cpu_model", cpu_model(), "unknown"),
            cpu_cores: num_cpus::get_physical() as u32,
            cpu_threads: num_cpus::get() as u32,
            memory_total_kb: capture_probe(
                &mut probe_issues,
                "memory_total_kb",
                memory_total_kb(),
                0_u64,
            ),
            memory_available_kb: capture_probe(
                &mut probe_issues,
                "memory_available_kb",
                memory_available_kb(),
                0_u64,
            ),
            load_avg: capture_probe(&mut probe_issues, "load_average", load_average(), "unknown"),
            rustc_version: capture_probe(
                &mut probe_issues,
                "rustc_version",
                rustc_version(),
                "unknown",
            ),
            cargo_version: capture_probe(
                &mut probe_issues,
                "cargo_version",
                cargo_version(),
                "unknown",
            ),
            rustup_toolchain: capture_probe(
                &mut probe_issues,
                "rustup_toolchain",
                rustup_toolchain(),
                "unknown",
            ),
            postgres_version: capture_probe(
                &mut probe_issues,
                "postgres_version",
                postgres_version(),
                "unknown",
            ),
            database_url_masked: database_url_masked(),
            nats_url: std::env::var("NATS_URL").unwrap_or_else(|_| "unset".to_string()),
            git_sha: capture_probe(&mut probe_issues, "git_sha", git_sha(false), "unknown"),
            git_sha_short: capture_probe(
                &mut probe_issues,
                "git_sha_short",
                git_sha(true),
                "unknown",
            ),
            git_branch: capture_probe(&mut probe_issues, "git_branch", git_branch(), "unknown"),
            git_dirty: capture_probe(&mut probe_issues, "git_dirty", git_dirty(), false),
            probe_issues,
        }
    }

    pub(super) fn write_to_file(&self, path: &std::path::Path) -> Result<()> {
        let content = self.format_text();
        std::fs::write(path, content)
            .with_context(|| format!("Failed to write environment to {}", path.display()))
    }

    pub(super) fn format_text(&self) -> String {
        format!(
            r"# Environment snapshot - {}

## System
hostname={}
uname={}
kernel={}
arch={}
os={}

## Hardware
cpu_model={}
cpu_cores={}
cpu_threads={}
memory_total_kb={}
memory_available_kb={}

## Load
load_avg={}

## Rust toolchain
rustc_version={}
cargo_version={}
rustup_toolchain={}

## Database
postgres_version={}
database_url_masked={}

## NATS
nats_url={}

## Git
git_sha={}
git_sha_short={}
git_branch={}
git_dirty={}
",
            self.timestamp,
            self.hostname,
            self.uname,
            self.kernel,
            self.arch,
            self.os,
            self.cpu_model,
            self.cpu_cores,
            self.cpu_threads,
            self.memory_total_kb,
            self.memory_available_kb,
            self.load_avg,
            self.rustc_version,
            self.cargo_version,
            self.rustup_toolchain,
            self.postgres_version,
            self.database_url_masked,
            self.nats_url,
            self.git_sha,
            self.git_sha_short,
            self.git_branch,
            self.git_dirty,
        ) + &format_probe_issues(&self.probe_issues)
    }
}

fn capture_probe<T>(
    issues: &mut Vec<String>,
    label: &str,
    result: Result<T, String>,
    fallback: impl Into<T>,
) -> T {
    match result {
        Ok(value) => value,
        Err(error) => {
            issues.push(format!("{label}: {error}"));
            fallback.into()
        }
    }
}

fn format_probe_issues(issues: &[String]) -> String {
    if issues.is_empty() {
        return String::new();
    }

    let mut formatted = String::from("\n## Probe issues\n");
    for issue in issues {
        formatted.push_str("- ");
        formatted.push_str(issue);
        formatted.push('\n');
    }
    formatted
}

fn command_stdout(program: &str, args: &[&str]) -> Result<String, String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .map_err(|error| format!("failed to run {program}: {error}"))?;
    if !output.status.success() {
        return Err(format_command_failure(program, args, &output));
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() {
        Err(format!(
            "{program} returned success but produced empty stdout"
        ))
    } else {
        Ok(stdout)
    }
}

fn format_command_failure(program: &str, args: &[&str], output: &std::process::Output) -> String {
    let status = output
        .status
        .code()
        .map_or_else(|| "signal".to_string(), |code| code.to_string());
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let detail = if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        format!("stdout: {stdout}")
    } else {
        "no output".to_string()
    };
    let rendered_args = if args.is_empty() {
        String::new()
    } else {
        format!(" {}", args.join(" "))
    };
    format!("{program}{rendered_args} exited with status {status}: {detail}")
}

fn read_file(path: &str) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|error| format!("failed to read {path}: {error}"))
}

fn parse_meminfo_value(key: &str) -> Result<u64, String> {
    let content = read_file("/proc/meminfo")?;
    let line = content
        .lines()
        .find(|line| line.starts_with(key))
        .ok_or_else(|| format!("missing {key} entry in /proc/meminfo"))?;
    let value = line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| format!("malformed {key} entry in /proc/meminfo"))?;
    value
        .parse::<u64>()
        .map_err(|error| format!("failed to parse {key} value '{value}': {error}"))
}

fn hostname() -> Result<String, String> {
    command_stdout("hostname", &[])
}

fn uname() -> Result<String, String> {
    command_stdout("uname", &["-a"])
}

fn kernel_version() -> Result<String, String> {
    command_stdout("uname", &["-r"])
}

fn os_release() -> Result<String, String> {
    let content = read_file("/etc/os-release")?;
    content
        .lines()
        .find(|line| line.starts_with("PRETTY_NAME="))
        .map(|line| {
            line.trim_start_matches("PRETTY_NAME=")
                .trim_matches('"')
                .to_string()
        })
        .ok_or_else(|| "missing PRETTY_NAME in /etc/os-release".to_string())
}

fn cpu_model() -> Result<String, String> {
    let content = read_file("/proc/cpuinfo")?;
    content
        .lines()
        .find(|line| line.starts_with("model name"))
        .and_then(|line| line.split(':').nth(1))
        .map(|s| s.trim().to_string())
        .ok_or_else(|| "missing model name in /proc/cpuinfo".to_string())
}

fn memory_total_kb() -> Result<u64, String> {
    parse_meminfo_value("MemTotal:")
}

fn memory_available_kb() -> Result<u64, String> {
    parse_meminfo_value("MemAvailable:")
}

fn load_average() -> Result<String, String> {
    read_file("/proc/loadavg").map(|content| content.trim().to_string())
}

fn rustc_version() -> Result<String, String> {
    command_stdout("rustc", &["--version"])
}

fn cargo_version() -> Result<String, String> {
    ProcessBuilder::cargo()
        .arg("--version")
        .run_stdout()
        .map_err(|error| error.to_string())
}

fn rustup_toolchain() -> Result<String, String> {
    command_stdout("rustup", &["show", "active-toolchain"])
}

fn postgres_version() -> Result<String, String> {
    command_stdout("psql", &["--version"])
}

fn database_url_masked() -> String {
    std::env::var("DATABASE_URL").ok().map_or_else(
        || "unset".to_string(),
        |url| {
            if let Some(idx) = url.find("://") {
                let scheme = &url[..idx + 3];
                if let Some(host_idx) = url[idx + 3..].find('@') {
                    format!("{}***@{}", scheme, &url[idx + 3 + host_idx + 1..])
                } else {
                    url
                }
            } else {
                url
            }
        },
    )
}

fn git_sha(short: bool) -> Result<String, String> {
    let mut cmd = Command::new("git");
    cmd.arg("rev-parse");
    if short {
        cmd.arg("--short");
    }
    cmd.arg("HEAD");

    let output = cmd
        .output()
        .map_err(|error| format!("failed to run git rev-parse: {error}"))?;
    if !output.status.success() {
        return Err(format_command_failure(
            "git",
            &["rev-parse", "HEAD"],
            &output,
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_branch() -> Result<String, String> {
    command_stdout("git", &["rev-parse", "--abbrev-ref", "HEAD"])
}

fn git_dirty() -> Result<bool, String> {
    let status = Command::new("git")
        .args(["diff", "--quiet"])
        .status()
        .map_err(|error| format!("failed to run git diff --quiet: {error}"))?;
    match status.code() {
        Some(0) => Ok(false),
        Some(1) => Ok(true),
        Some(code) => Err(format!(
            "git diff --quiet exited with unexpected status {code}"
        )),
        None => Err("git diff --quiet terminated by signal".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{Environment, command_stdout, database_url_masked, format_probe_issues};
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn command_stdout_reports_non_zero_exit() -> crate::sandbox::TestResult<()> {
        let error = command_stdout("sh", &["-c", "echo boom >&2; exit 7"])
            .expect_err("non-zero exit should be reported");
        assert!(error.contains("status 7"), "unexpected error: {error}");
        assert!(error.contains("boom"), "unexpected error: {error}");
        Ok(())
    }

    #[sinex_test]
    async fn format_text_includes_probe_issues() -> crate::sandbox::TestResult<()> {
        let env = Environment {
            timestamp: "2026-03-27T00:00:00Z".to_string(),
            hostname: "host".to_string(),
            uname: "uname".to_string(),
            kernel: "kernel".to_string(),
            arch: "x86_64".to_string(),
            os: "NixOS".to_string(),
            cpu_model: "cpu".to_string(),
            cpu_cores: 1,
            cpu_threads: 1,
            memory_total_kb: 1024,
            memory_available_kb: 512,
            load_avg: "0.0 0.0 0.0".to_string(),
            rustc_version: "rustc".to_string(),
            cargo_version: "cargo".to_string(),
            rustup_toolchain: "toolchain".to_string(),
            postgres_version: "psql".to_string(),
            database_url_masked: "postgres://***@db/sinex".to_string(),
            nats_url: "nats://127.0.0.1:4222".to_string(),
            git_sha: "abc".to_string(),
            git_sha_short: "abc".to_string(),
            git_branch: "master".to_string(),
            git_dirty: false,
            probe_issues: vec!["hostname: failed".to_string()],
        };

        let text = env.format_text();
        assert!(text.contains("## Probe issues"));
        assert!(text.contains("hostname: failed"));
        Ok(())
    }

    #[sinex_test]
    async fn database_url_masked_redacts_credentials() -> crate::sandbox::TestResult<()> {
        let old = std::env::var_os("DATABASE_URL");
        unsafe {
            std::env::set_var("DATABASE_URL", "postgres://user:secret@example.test/sinex");
        }
        let masked = database_url_masked();
        match old {
            Some(value) => unsafe { std::env::set_var("DATABASE_URL", value) },
            None => unsafe { std::env::remove_var("DATABASE_URL") },
        }
        assert_eq!(masked, "postgres://***@example.test/sinex");
        assert_eq!(
            format_probe_issues(&["boom".to_string()]),
            "\n## Probe issues\n- boom\n"
        );
        Ok(())
    }
}
