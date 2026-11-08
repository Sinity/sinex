use std::process::Command;

// The build identity comes from SINEX_BUILD: Nix supplies the real revision
// and the development shell supplies a fixed one, so ordinary edits and Git
// operations never rebuild sinexd. Plain Cargo outside both reads Git once.
fn main() {
    println!("cargo:rerun-if-env-changed=SINEX_BUILD");
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
