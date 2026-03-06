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
}

impl Environment {
    pub(super) fn capture() -> Self {
        Self {
            timestamp: sinex_primitives::temporal::Timestamp::now().format_rfc3339(),
            hostname: hostname().unwrap_or_else(|| "unknown".to_string()),
            uname: uname().unwrap_or_else(|| "unknown".to_string()),
            kernel: kernel_version().unwrap_or_else(|| "unknown".to_string()),
            arch: std::env::consts::ARCH.to_string(),
            os: os_release().unwrap_or_else(|| "unknown".to_string()),
            cpu_model: cpu_model().unwrap_or_else(|| "unknown".to_string()),
            cpu_cores: num_cpus::get_physical() as u32,
            cpu_threads: num_cpus::get() as u32,
            memory_total_kb: memory_total_kb().unwrap_or(0),
            memory_available_kb: memory_available_kb().unwrap_or(0),
            load_avg: load_average().unwrap_or_else(|| "unknown".to_string()),
            rustc_version: rustc_version().unwrap_or_else(|| "unknown".to_string()),
            cargo_version: cargo_version().unwrap_or_else(|| "unknown".to_string()),
            rustup_toolchain: rustup_toolchain().unwrap_or_else(|| "unknown".to_string()),
            postgres_version: postgres_version().unwrap_or_else(|| "unknown".to_string()),
            database_url_masked: database_url_masked(),
            nats_url: std::env::var("NATS_URL").unwrap_or_else(|_| "unset".to_string()),
            git_sha: git_sha(false).unwrap_or_else(|| "unknown".to_string()),
            git_sha_short: git_sha(true).unwrap_or_else(|| "unknown".to_string()),
            git_branch: git_branch().unwrap_or_else(|| "unknown".to_string()),
            git_dirty: git_dirty().unwrap_or(false),
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
        )
    }
}

fn hostname() -> Option<String> {
    Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
}

fn uname() -> Option<String> {
    Command::new("uname")
        .arg("-a")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
}

fn kernel_version() -> Option<String> {
    Command::new("uname")
        .arg("-r")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
}

fn os_release() -> Option<String> {
    std::fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|content| {
            content
                .lines()
                .find(|line| line.starts_with("PRETTY_NAME="))
                .map(|line| {
                    line.trim_start_matches("PRETTY_NAME=")
                        .trim_matches('"')
                        .to_string()
                })
        })
}

fn cpu_model() -> Option<String> {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|content| {
            content
                .lines()
                .find(|line| line.starts_with("model name"))
                .and_then(|line| line.split(':').nth(1))
                .map(|s| s.trim().to_string())
        })
}

fn memory_total_kb() -> Option<u64> {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|content| {
            content
                .lines()
                .find(|line| line.starts_with("MemTotal:"))
                .and_then(|line| {
                    line.split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse::<u64>().ok())
                })
        })
}

fn memory_available_kb() -> Option<u64> {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|content| {
            content
                .lines()
                .find(|line| line.starts_with("MemAvailable:"))
                .and_then(|line| {
                    line.split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse::<u64>().ok())
                })
        })
}

fn load_average() -> Option<String> {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .map(|s| s.trim().to_string())
}

fn rustc_version() -> Option<String> {
    Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
}

fn cargo_version() -> Option<String> {
    ProcessBuilder::cargo().arg("--version").run_stdout().ok()
}

fn rustup_toolchain() -> Option<String> {
    Command::new("rustup")
        .args(["show", "active-toolchain"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
}

fn postgres_version() -> Option<String> {
    Command::new("psql")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
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

fn git_sha(short: bool) -> Option<String> {
    let mut cmd = Command::new("git");
    cmd.arg("rev-parse");
    if short {
        cmd.arg("--short");
    }
    cmd.arg("HEAD");

    cmd.output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
}

fn git_branch() -> Option<String> {
    Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
}

fn git_dirty() -> Option<bool> {
    Command::new("git")
        .args(["diff", "--quiet"])
        .status()
        .ok()
        .map(|status| !status.success())
}
