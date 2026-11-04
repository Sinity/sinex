"""Synthetic repository identity checks; no Kitty process is needed."""
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("watcher", Path(__file__).with_name("sinex_watcher.py"))
watcher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(watcher)

class IdentityTests(unittest.TestCase):
    def test_worktrees_share_capture_identity_and_missing_repo_stays_unknown(self):
        with tempfile.TemporaryDirectory(prefix="sinex-identity-", dir=os.environ.get("TMPDIR", "/realm/tmp/work")) as tmp:
            root = Path(tmp) / "repo"
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            subprocess.run(["git", "-C", str(root), "-c", "user.name=Example", "-c", "user.email=example@example.invalid",
                            "commit", "-q", "--allow-empty", "-m", "Synthetic root"], check=True)
            tree = Path(tmp) / "worktree"
            subprocess.run(["git", "-C", str(root), "worktree", "add", "-q", "-b", "work", str(tree)], check=True)
            main = watcher._git_identity(str(root))
            linked = watcher._git_identity(str(tree))
            self.assertEqual(main["common_dir"], linked["common_dir"])
            self.assertEqual(main["root_commits"], linked["root_commits"])
            self.assertEqual(linked["branch"], "work")
            self.assertIsNone(watcher._git_identity(tmp))
            self.assertIsNone(watcher._git_identity(None))

class ExecutionTokenTests(unittest.TestCase):
    def test_a_command_carries_the_token_its_shell_set_and_forgets_it_after(self):
        sent = []
        saved = watcher._write, watcher._git_identity
        watcher._write = sent.append
        watcher._git_identity = lambda cwd: None
        self.addCleanup(lambda: setattr(watcher, "_write", saved[0]))
        self.addCleanup(lambda: setattr(watcher, "_git_identity", saved[1]))

        class Window:
            id = 7
            cwd_of_child = None
            title = "zsh"

            def cmd_output(self, as_ansi=True):
                return "out"

        window = Window()
        token = "01900000-0000-7000-8000-000000000001"
        watcher.on_set_user_var(None, window, {"key": "sinex_execution", "value": token})
        watcher.on_cmd_startstop(None, window, {"is_start": True, "cmdline": "ls"})
        watcher.on_cmd_startstop(None, window, {"is_start": False, "cmdline": "ls", "exit_status": 0})
        watcher.on_cmd_startstop(None, window, {"is_start": True, "cmdline": "pwd"})
        self.assertEqual([line.get("execution") for line in sent], [token, token, None])
        watcher.on_set_user_var(None, window, {"key": "other", "value": "x"})
        self.assertNotIn(window.id, watcher._execution)

if __name__ == "__main__":
    unittest.main()
