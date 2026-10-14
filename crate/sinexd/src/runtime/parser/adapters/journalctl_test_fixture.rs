//! An isolated binary journal. Never reads or changes the operator journal.

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use xtask::sandbox::prelude::EnvGuard;

pub(crate) const UNAVAILABLE_CURSOR: &str =
    "s=00000000000000000000000000000001;i=1;b=00000000000000000000000000000002;m=1;t=1;x=1";

pub(crate) struct JournalFixture {
    pub cursors: Vec<String>,
    pub env: EnvGuard,
    _directory: tempfile::TempDir,
}

impl JournalFixture {
    pub fn finite_query(&self) -> xtask::sandbox::TestResult<()> {
        let wrapper = self._directory.path().join("journalctl");
        let script = std::fs::read_to_string(&wrapper)?;
        // The adapter's first argument is -f. Remove only that argument so
        // the real child can successfully reach EOF with the same filter/cursor.
        std::fs::write(
            wrapper,
            script.replacen("\nexec ", "\ntest \"$1\" = -f || exit 2\nshift\nexec ", 1),
        )?;
        Ok(())
    }

    pub fn delay_start(&self) -> xtask::sandbox::TestResult<()> {
        let wrapper = self._directory.path().join("journalctl");
        let script = std::fs::read_to_string(&wrapper)?;
        std::fs::write(wrapper, script.replacen("\nexec ", "\nsleep 6\nexec ", 1))?;
        Ok(())
    }

    pub fn new() -> xtask::sandbox::TestResult<Self> {
        let directory = tempfile::tempdir()?;
        let executable = std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
            .map(|path| path.join("journalctl"))
            .find(|path| path.is_file())
            .ok_or_else(|| {
                color_eyre::eyre::eyre!("journalctl is required for the synthetic journal fixture")
            })?
            .canonicalize()?;
        let root = executable
            .parent()
            .and_then(std::path::Path::parent)
            .ok_or_else(|| color_eyre::eyre::eyre!("journalctl has no package root"))?;
        let remote = [
            root.join("lib/systemd/systemd-journal-remote"),
            root.join("libexec/systemd/systemd-journal-remote"),
        ]
        .into_iter()
        .find(|path| path.is_file())
        .ok_or_else(|| {
            color_eyre::eyre::eyre!(
                "systemd-journal-remote is required for the synthetic journal fixture"
            )
        })?;
        let journal = directory.path().join("synthetic.journal");
        let mut child = Command::new(remote)
            .arg("--split-mode=none")
            .arg(format!("--output={}", journal.display()))
            .arg("-")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        let mut input = child
            .stdin
            .take()
            .ok_or_else(|| color_eyre::eyre::eyre!("missing journal fixture input"))?;
        for n in 1..=2 {
            writeln!(
                input,
                "__REALTIME_TIMESTAMP={}\n__MONOTONIC_TIMESTAMP={}\n_BOOT_ID=00000000000000000000000000000002\n_MACHINE_ID=00000000000000000000000000000003\n_SYSTEMD_UNIT=sinex-synthetic-fixture.service\nMESSAGE=synthetic journal record {n}\n",
                1_704_067_200_000_000_u64 + n * 1_000_000,
                n * 1_000_000
            )?;
        }
        drop(input);
        let result = child.wait_with_output()?;
        assert!(
            result.status.success(),
            "journal fixture import failed: {:?}",
            result
        );
        let result = Command::new(&executable)
            .arg(format!("--file={}", journal.display()))
            .args(["--no-pager", "--output=json"])
            .output()?;
        assert!(result.status.success());
        let cursors = String::from_utf8(result.stdout)?
            .lines()
            .map(|line| {
                let value: serde_json::Value = serde_json::from_str(line)?;
                Ok(value["__CURSOR"]
                    .as_str()
                    .ok_or_else(|| color_eyre::eyre::eyre!("fixture cursor missing"))?
                    .to_string())
            })
            .collect::<xtask::sandbox::TestResult<Vec<_>>>()?;
        assert_eq!(cursors.len(), 2);
        let quote = |path: &std::path::Path| {
            format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
        };
        let wrapper = directory.path().join("journalctl");
        std::fs::write(
            &wrapper,
            format!(
                "#!/bin/sh\nexec {} --file={} \"$@\"\n",
                quote(&executable),
                quote(&journal)
            ),
        )?;
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700))?;
        let mut env = EnvGuard::new();
        env.set(
            "PATH",
            format!("{}:{}", directory.path().display(), std::env::var("PATH")?),
        );
        Ok(Self {
            cursors,
            env,
            _directory: directory,
        })
    }
}
