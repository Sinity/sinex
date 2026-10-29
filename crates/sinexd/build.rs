use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=SINEX_BUILD");
    println!("cargo:rerun-if-changed=../../.git");
    println!("cargo:rerun-if-changed=../");
    for name in ["HEAD", "index"] {
        if let Ok(path) = Command::new("git")
            .args(["rev-parse", "--git-path", name])
            .output()
            && path.status.success()
        {
            println!(
                "cargo:rerun-if-changed={}",
                String::from_utf8_lossy(&path.stdout).trim()
            );
        }
    }
    let supplied = std::env::var("SINEX_BUILD").ok();
    let build = supplied.or_else(|| {
        let commit = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .ok()?;
        if !commit.status.success() {
            return None;
        }
        let commit = String::from_utf8(commit.stdout).ok()?;
        let status = Command::new("git")
            .args(["status", "--porcelain", "--untracked-files=normal"])
            .output()
            .ok()?;
        if !status.status.success() {
            return None;
        }
        Some(format!(
            "{};dirty={}",
            commit.trim(),
            !status.stdout.is_empty()
        ))
    });
    if let Some(build) = build {
        println!("cargo:rustc-env=SINEX_BUILD={build}");
    }
}
