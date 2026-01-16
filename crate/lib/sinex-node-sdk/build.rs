use chrono::{DateTime, Utc};
use std::env;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    emit_rerun_directives();

    let version = env::var("NODE_VERSION")
        .or_else(|_| env::var("CARGO_PKG_VERSION"))
        .unwrap_or_else(|_| "0.0.0".to_string());

    let commit_hash = env::var("NODE_COMMIT_HASH")
        .or_else(|_| env::var("GIT_HASH"))
        .ok()
        .or_else(|| git_output(&["rev-parse", "--short=8", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_string());

    let commit_count = env::var("NODE_COMMIT_COUNT")
        .ok()
        .or_else(|| git_output(&["rev-list", "--count", "HEAD"]))
        .unwrap_or_else(|| "0".to_string());

    let branch = env::var("NODE_BRANCH")
        .ok()
        .or_else(|| git_output(&["rev-parse", "--abbrev-ref", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_string());

    let is_dirty = env::var("NODE_IS_DIRTY")
        .ok()
        .or_else(|| git_is_dirty().map(|dirty| dirty.to_string()))
        .unwrap_or_else(|| "true".to_string());

    let build_timestamp = env::var("NODE_BUILD_TIMESTAMP")
        .ok()
        .or_else(build_timestamp_from_env)
        .unwrap_or_else(current_timestamp);

    let full_version = env::var("NODE_FULL_VERSION").unwrap_or_else(|_| {
        if commit_hash == "unknown" {
            version.clone()
        } else {
            format!("{version}+{commit_hash}")
        }
    });

    let binary_hash = env::var("NODE_BINARY_HASH")
        .or_else(|_| env::var("BINARY_HASH"))
        .ok()
        .unwrap_or_else(|| commit_hash.clone());

    emit("NODE_VERSION", &version);
    emit("NODE_FULL_VERSION", &full_version);
    emit("NODE_COMMIT_HASH", &commit_hash);
    emit("NODE_COMMIT_COUNT", &commit_count);
    emit("NODE_BRANCH", &branch);
    emit("NODE_BUILD_TIMESTAMP", &build_timestamp);
    emit("NODE_IS_DIRTY", &is_dirty);
    emit("NODE_BINARY_HASH", &binary_hash);
    emit("GIT_HASH", &commit_hash);
}

fn emit_rerun_directives() {
    for key in [
        "NODE_VERSION",
        "NODE_FULL_VERSION",
        "NODE_COMMIT_HASH",
        "NODE_COMMIT_COUNT",
        "NODE_BRANCH",
        "NODE_BUILD_TIMESTAMP",
        "NODE_IS_DIRTY",
        "NODE_BINARY_HASH",
        "GIT_HASH",
        "BINARY_HASH",
        "SOURCE_DATE_EPOCH",
    ] {
        println!("cargo:rerun-if-env-changed={}", key);
    }

    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
    println!("cargo:rerun-if-changed=.git/packed-refs");
}

fn emit(key: &str, value: &str) {
    println!("cargo:rustc-env={key}={value}");
}

fn git_output(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn git_is_dirty() -> Option<bool> {
    let output = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(!output.stdout.is_empty())
}

fn build_timestamp_from_env() -> Option<String> {
    let epoch = env::var("SOURCE_DATE_EPOCH").ok()?;
    let seconds: i64 = epoch.parse().ok()?;
    if seconds < 0 {
        return None;
    }
    let when = UNIX_EPOCH + Duration::from_secs(seconds as u64);
    Some(DateTime::<Utc>::from(when).to_rfc3339())
}

fn current_timestamp() -> String {
    DateTime::<Utc>::from(SystemTime::now()).to_rfc3339()
}
