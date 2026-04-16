//! Ephemeral Cargo workspace for xtask integration testing.
//!
//! Creates a minimal, isolated Cargo workspace in a temporary directory, allowing
//! integration tests to run real `xtask check` / `xtask build` subprocesses
//! against controlled workspace state without touching the real sinex workspace.
//!
//! # Design
//!
//! - Fully isolated: separate `CARGO_TARGET_DIR` and `SINEX_STATE_DIR` so no
//!   cross-contamination with the real workspace's target/ or history DB.
//! - Mutation methods inject specific defects (compile errors, clippy warnings,
//!   format errors) so each test exercises a known, reproducible condition.
//! - `env_overrides()` returns the env vars to pass to the subprocess.
//! - `state_dir()` exposes the history DB path so tests can read it back.
//!
//! # Usage
//!
//! ```rust,no_run
//! # use xtask::sandbox::EphemeralWorkspace;
//! # fn example() -> color_eyre::eyre::Result<()> {
//! let ws = EphemeralWorkspace::new()?;
//! ws.inject_compile_error("ws-lib")?;
//!
//! let output = std::process::Command::new("xtask")
//!     .args(["check", "--json"])
//!     .current_dir(ws.dir())
//!     .envs(ws.env_overrides())
//!     .output()?;
//!
//! assert_ne!(output.status.code(), Some(0));
//! # Ok(())
//! # }
//! ```

use std::fs;
use std::path::{Path, PathBuf};

use color_eyre::eyre::{Result, WrapErr};
use tempfile::TempDir;

/// The name of the single library crate created by `EphemeralWorkspace::new()`.
pub const DEFAULT_CRATE: &str = "ws-lib";

/// An isolated Cargo workspace in a temporary directory.
///
/// On drop, the temp directories are automatically cleaned up.
pub struct EphemeralWorkspace {
    /// Root of the ephemeral Cargo workspace (set as `current_dir` for subprocesses).
    workspace_dir: TempDir,
    /// Separate target directory (avoids polluting the real workspace's target/).
    target_dir: TempDir,
    /// Separate sinex state directory (isolates history DB from real workspace).
    state_dir: TempDir,
}

impl EphemeralWorkspace {
    /// Create a minimal workspace: a root `Cargo.toml` with one member crate (`ws-lib`).
    pub fn new() -> Result<Self> {
        let workspace_dir = tempfile::tempdir().context("create workspace tempdir")?;
        let target_dir = tempfile::tempdir().context("create target tempdir")?;
        let state_dir = tempfile::tempdir().context("create state tempdir")?;

        let ws = Self {
            workspace_dir,
            target_dir,
            state_dir,
        };

        ws.write_workspace_toml()?;
        ws.create_member_crate(DEFAULT_CRATE)?;

        Ok(ws)
    }

    /// Root directory of the ephemeral workspace. Set as `current_dir` for subprocesses.
    pub fn dir(&self) -> &Path {
        self.workspace_dir.path()
    }

    /// Path to the isolated target directory.
    pub fn target_dir(&self) -> &Path {
        self.target_dir.path()
    }

    /// Path to the isolated sinex state directory.
    ///
    /// After running a subprocess xtask command with `env_overrides()`, the
    /// history DB is at `state_dir().join("xtask-history.db")`.
    pub fn state_dir(&self) -> &Path {
        self.state_dir.path()
    }

    /// Path to the history SQLite DB created by a subprocess xtask command.
    pub fn history_db_path(&self) -> PathBuf {
        self.state_dir.path().join("xtask-history.db")
    }

    /// Environment variable overrides to pass to subprocess xtask invocations.
    ///
    /// These redirect `CARGO_TARGET_DIR` and `SINEX_STATE_DIR` to isolated
    /// temp directories so the subprocess doesn't touch real workspace state.
    pub fn env_overrides(&self) -> Vec<(String, String)> {
        vec![
            (
                "CARGO_TARGET_DIR".to_string(),
                self.target_dir.path().display().to_string(),
            ),
            (
                "SINEX_STATE_DIR".to_string(),
                self.state_dir.path().display().to_string(),
            ),
        ]
    }

    /// Inject a compile error into `crate_name/src/lib.rs`.
    ///
    /// Appends a type mismatch (`let _x: i32 = "not_an_int";`) that cargo will
    /// reject with `E0308`. The original valid content is preserved above the error.
    pub fn inject_compile_error(&self, crate_name: &str) -> Result<&Self> {
        let lib_rs = self.crate_src_lib(crate_name);
        let existing = read_existing_text_file(&lib_rs, "inject_compile_error")?;
        let mutated = format!(
            "{existing}\n\
             // EphemeralWorkspace: injected compile error\n\
             #[allow(dead_code)]\n\
             fn _injected_error() {{\n\
             \x20   let _x: i32 = \"not_an_int\";\n\
             }}\n"
        );
        fs::write(&lib_rs, mutated)
            .with_context(|| format!("inject_compile_error into {}", lib_rs.display()))?;
        Ok(self)
    }

    /// Inject a clippy warning into `crate_name/src/lib.rs`.
    ///
    /// Appends a function containing `let v = Vec::<i32>::new(); v.len() == 0`
    /// which triggers `clippy::len_zero` (prefer `.is_empty()`).
    pub fn inject_clippy_warning(&self, crate_name: &str) -> Result<&Self> {
        let lib_rs = self.crate_src_lib(crate_name);
        let existing = read_existing_text_file(&lib_rs, "inject_clippy_warning")?;
        let mutated = format!(
            "{existing}\n\
             // EphemeralWorkspace: injected clippy warning (clippy::len_zero)\n\
             #[allow(dead_code)]\n\
             fn _clippy_warning() -> bool {{\n\
             \x20   let v = Vec::<i32>::new();\n\
             \x20   v.len() == 0\n\
             }}\n"
        );
        fs::write(&lib_rs, mutated)
            .with_context(|| format!("inject_clippy_warning into {}", lib_rs.display()))?;
        Ok(self)
    }

    /// Inject a format error into `crate_name/src/lib.rs`.
    ///
    /// Appends code with deliberately bad `rustfmt` formatting (missing spaces,
    /// compact expression style) that `cargo fmt --check` will reject.
    pub fn inject_format_error(&self, crate_name: &str) -> Result<&Self> {
        let lib_rs = self.crate_src_lib(crate_name);
        let existing = read_existing_text_file(&lib_rs, "inject_format_error")?;
        // Deliberately bad formatting: rustfmt would rewrite this
        let mutated = format!(
            "{existing}\n\
             // EphemeralWorkspace: injected format error\n\
             #[allow(dead_code)] fn _fmt_error(){{let x=1+2;let _=x;}}\n"
        );
        fs::write(&lib_rs, mutated)
            .with_context(|| format!("inject_format_error into {}", lib_rs.display()))?;
        Ok(self)
    }

    /// Add an unused dependency to `crate_name/Cargo.toml`.
    ///
    /// The dep is added but never imported, triggering `cargo-machete` or
    /// `unused-deps` analysis tools. Does NOT trigger a compile error by itself.
    pub fn inject_unused_dep(&self, crate_name: &str, dep: &str, version: &str) -> Result<&Self> {
        let cargo_toml = self.crate_cargo_toml(crate_name);
        let existing = read_existing_text_file(&cargo_toml, "inject_unused_dep")?;
        let mutated = format!("{existing}\n[dependencies]\n{dep} = \"{version}\"\n");
        fs::write(&cargo_toml, mutated)
            .with_context(|| format!("inject_unused_dep into {}", cargo_toml.display()))?;
        Ok(self)
    }

    /// Break a crate by deleting its `src/lib.rs`.
    ///
    /// Cargo will fail with "can't find crate root". Useful for testing
    /// partial-workspace failure paths.
    pub fn break_crate(&self, crate_name: &str) -> Result<&Self> {
        let lib_rs = self.crate_src_lib(crate_name);
        fs::remove_file(&lib_rs)
            .with_context(|| format!("break_crate: remove {}", lib_rs.display()))?;
        Ok(self)
    }

    /// Add a second member crate to the workspace.
    ///
    /// Creates a new `[crate_name]/Cargo.toml` + `src/lib.rs` and adds the
    /// member to the workspace root `Cargo.toml`.
    pub fn add_member(&self, crate_name: &str) -> Result<&Self> {
        self.create_member_crate(crate_name)?;

        // Append member to workspace Cargo.toml
        let ws_toml = self.workspace_dir.path().join("Cargo.toml");
        let existing = fs::read_to_string(&ws_toml)?;
        let mutated = existing.replace(
            &format!("members = [\"{DEFAULT_CRATE}\"]"),
            &format!("members = [\"{DEFAULT_CRATE}\", \"{crate_name}\"]"),
        );
        fs::write(&ws_toml, mutated)?;
        Ok(self)
    }

    // ─── private helpers ──────────────────────────────────────────────────────

    fn write_workspace_toml(&self) -> Result<()> {
        let content = format!(
            "[workspace]\n\
             resolver = \"2\"\n\
             members = [\"{DEFAULT_CRATE}\"]\n"
        );
        fs::write(self.workspace_dir.path().join("Cargo.toml"), content)
            .context("write workspace Cargo.toml")
    }

    fn create_member_crate(&self, crate_name: &str) -> Result<()> {
        let crate_dir = self.workspace_dir.path().join(crate_name);
        let src_dir = crate_dir.join("src");
        fs::create_dir_all(&src_dir).with_context(|| format!("create {crate_name}/src/"))?;

        let cargo_toml = format!(
            "[package]\n\
             name = \"{crate_name}\"\n\
             version = \"0.1.0\"\n\
             edition = \"2024\"\n"
        );
        fs::write(crate_dir.join("Cargo.toml"), cargo_toml)
            .with_context(|| format!("write {crate_name}/Cargo.toml"))?;

        fs::write(
            src_dir.join("lib.rs"),
            "// EphemeralWorkspace: minimal lib\n",
        )
        .with_context(|| format!("write {crate_name}/src/lib.rs"))?;

        Ok(())
    }

    fn crate_src_lib(&self, crate_name: &str) -> PathBuf {
        self.workspace_dir
            .path()
            .join(crate_name)
            .join("src")
            .join("lib.rs")
    }

    fn crate_cargo_toml(&self, crate_name: &str) -> PathBuf {
        self.workspace_dir
            .path()
            .join(crate_name)
            .join("Cargo.toml")
    }
}

fn read_existing_text_file(path: &std::path::Path, operation: &str) -> Result<String> {
    fs::read_to_string(path)
        .with_context(|| format!("{operation}: read existing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn inject_compile_error_surfaces_unreadable_lib_rs() -> TestResult<()> {
        let workspace = EphemeralWorkspace::new()?;
        let lib_rs = workspace.crate_src_lib(DEFAULT_CRATE);
        std::fs::remove_file(&lib_rs)?;
        std::fs::create_dir(&lib_rs)?;

        let error = workspace
            .inject_compile_error(DEFAULT_CRATE)
            .expect_err("directory lib.rs should surface");
        let message = format!("{error:#}");
        assert!(message.contains("inject_compile_error: read existing"));
        assert!(message.contains(lib_rs.display().to_string().as_str()));
        Ok(())
    }

    #[sinex_test]
    async fn inject_unused_dep_surfaces_unreadable_manifest() -> TestResult<()> {
        let workspace = EphemeralWorkspace::new()?;
        let cargo_toml = workspace.crate_cargo_toml(DEFAULT_CRATE);
        std::fs::remove_file(&cargo_toml)?;
        std::fs::create_dir(&cargo_toml)?;

        let error = workspace
            .inject_unused_dep(DEFAULT_CRATE, "serde_json", "1")
            .expect_err("directory Cargo.toml should surface");
        let message = format!("{error:#}");
        assert!(message.contains("inject_unused_dep: read existing"));
        assert!(message.contains(cargo_toml.display().to_string().as_str()));
        Ok(())
    }
}
