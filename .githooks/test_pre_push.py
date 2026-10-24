"""Run with python3 .githooks/test_pre_push.py; all Nix calls are synthetic."""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

HOOK = Path(
    os.environ.get("PRE_PUSH_HOOK", str(Path(__file__).with_name("pre-push")))
).resolve()
ZERO = "0" * 40
CHECKS = [
    "develop --command cargo fmt --all -- --check",
    "develop --command cargo run --locked --offline --quiet -p xtask -- boundaries",
]


class PrePush(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="pre-push-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.git("init", "--initial-branch=master", "-q")
        self.git("config", "user.name", "Synthetic Fixture")
        self.git("config", "user.email", "fixture@example.invalid")
        self.commit("README.md")
        self.base = self.git("rev-parse", "HEAD")
        self.git("update-ref", "refs/remotes/origin/master", self.base)
        tools = self.root / "tools"
        tools.mkdir()
        fake = tools / "nix"
        fake.write_text(
            '#!/bin/sh\nprintf "%s\\n" "$*" >> "$HOOK_NIX_LOG"\nexit "${HOOK_NIX_EXIT:-0}"\n'
        )
        fake.chmod(0o755)
        self.log = self.root / "nix.log"
        self.env = dict(
            os.environ,
            PATH=str(tools) + os.pathsep + os.environ["PATH"],
            HOOK_NIX_LOG=str(self.log),
        )

    def git(self, *args):
        return subprocess.run(
            ["git", "-C", str(self.root), *args],
            check=True,
            text=True,
            capture_output=True,
        ).stdout.strip()

    def commit(self, path):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("synthetic change\n")
        self.git("add", path)
        self.git("commit", "-qm", "synthetic change")

    def push(self, *, local=None, remote=None):
        local = local or self.git("rev-parse", "HEAD")
        remote = self.base if remote is None else remote
        return subprocess.run(
            ["bash", str(HOOK)],
            cwd=self.root,
            env=self.env,
            text=True,
            input=f"refs/heads/topic {local} refs/heads/topic {remote}\n",
            capture_output=True,
            check=False,
        )

    def calls(self):
        return self.log.read_text().splitlines() if self.log.exists() else []

    def test_metadata_and_docs_skip_cargo(self):
        self.commit(".agentctl/project.toml")
        self.commit("docs/development.md")
        self.assertEqual(self.push().returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_relevant_changes_keep_both_original_checks(self):
        paths = [
            "Cargo.toml",
            "Cargo.lock",
            "flake.nix",
            "flake.lock",
            "rust-toolchain",
            "rust-toolchain.toml",
            "rustfmt.toml",
            ".rustfmt.toml",
            ".cargo/config.toml",
            "build.rs",
            "crates/example/src/lib.rs",
            "xtask/src/main.rs",
        ]
        for path in paths:
            with self.subTest(path=path):
                self.commit(path)
                result = self.push()
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.calls(), CHECKS)
                self.log.unlink()
                self.base = self.git("rev-parse", "HEAD")

    def test_new_branch_compares_with_origin_master(self):
        self.commit(".agentctl/project.toml")
        self.assertEqual(self.push(remote=ZERO).returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_branch_deletion_runs_no_cargo(self):
        self.assertEqual(self.push(local=ZERO).returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_non_head_ref_keeps_conservative_checks(self):
        self.commit(".agentctl/project.toml")
        self.assertEqual(self.push(local=self.base).returncode, 0)
        self.assertEqual(self.calls(), CHECKS)

    def test_missing_merge_base_keeps_conservative_checks(self):
        self.git("update-ref", "-d", "refs/remotes/origin/master")
        self.assertEqual(self.push(remote=ZERO).returncode, 0)
        self.assertEqual(self.calls(), CHECKS)

    def test_failed_check_refuses_push(self):
        self.commit("Cargo.toml")
        self.env["HOOK_NIX_EXIT"] = "7"
        self.assertEqual(self.push().returncode, 7)
        self.assertEqual(self.calls(), CHECKS[:1])

    def test_whitespace_check_still_runs_for_metadata(self):
        self.commit(".agentctl/project.toml")
        (self.root / ".agentctl/project.toml").write_text("synthetic trailing space \n")
        self.assertNotEqual(self.push().returncode, 0)
        self.assertEqual(self.calls(), [])


if __name__ == "__main__":
    unittest.main()
