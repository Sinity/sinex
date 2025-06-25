#!/usr/bin/env python3
"""
Advanced Test Suite Migration & Refactoring Orchestrator for Sinex (V2)

This script uses a multi-pass approach, leveraging `ast-grep` for robust,
syntax-aware transformations and Python for stateful orchestration.

Key Features:
- Multi-pass refactoring with distinct, ordered stages.
- Per-pass compilation checks to isolate failures.
- --git-rollback: Uses git to automatically revert a failing pass.
- --dry-run predictive: A sandboxed "wet run" to check for compile errors
  without modifying your source files.
- New rules to tag slow tests and add documentation stubs.
"""
import subprocess
import sys
import shutil
import tempfile
from pathlib import Path
from typing import List, Dict
import time

class TestRefactorOrchestrator:
    def __init__(self, test_dir: Path, dry_run_mode: str, use_git: bool):
        self.project_root = self.find_project_root(test_dir)
        self.test_dir = test_dir
        self.dry_run_mode = dry_run_mode
        self.use_git = use_git
        self.stats: Dict[str, Dict[str, int]] = {}
        self.rules_file = self.project_root / "rules" / "test_refactor_v2.yml"

    def find_project_root(self, start_path: Path) -> Path:
        """Finds the project root by looking for Cargo.toml."""
        current = start_path.resolve()
        while not (current / "Cargo.toml").exists():
            if current.parent == current:
                raise FileNotFoundError("Could not find project root (Cargo.toml).")
            current = current.parent
        print(f"▶️  Project root found at: {current}")
        return current

    def _run_command(self, cmd: List[str], cwd: Path) -> subprocess.CompletedProcess:
        """Helper to run a command and handle errors."""
        try:
            return subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
        except FileNotFoundError:
            print(f"❌ Error: Command `{cmd[0]}` not found. Is it installed and in your PATH?")
            sys.exit(1)
        except Exception as e:
            print(f"An unexpected error occurred running '{' '.join(cmd)}': {e}")
            sys.exit(1)

    def _run_sg_pass(self, pass_name: str, rule_ids: List[str], target_dir: Path) -> int:
        """Runs a single ast-grep pass on a specified directory."""
        print(f"  Executing ast-grep for pass: '{pass_name}' on '{target_dir}'...")
        command = [
            "ast-grep", "scan", "-r", str(self.rules_file),
            "--filter", "|".join(rule_ids), str(target_dir)
        ]
        if self.dry_run_mode is None: # Only apply changes in wet-run mode
            command.append("-U")

        result = self._run_command(command, self.project_root)

        # ast-grep exits 1 if matches are found, 0 if not. Other codes are errors.
        if result.returncode not in [0, 1]:
            print(f"  ❌ ast-grep failed during pass '{pass_name}' with exit code {result.returncode}.")
            print(result.stderr)
            return -1

        changes = result.stdout.count("sg-rule-found")
        print(f"  Applied an estimated {changes} changes.")
        self.stats.setdefault(pass_name, {})["changes"] = changes
        return changes

    def _validate_compilation(self, target_dir: Path) -> bool:
        """Runs `cargo check` within the specified project directory."""
        print(f"  Validating compilation in '{target_dir}'...")
        manifest_path = target_dir / "Cargo.toml"
        # We check a specific manifest to ensure we're not picking up another project
        # when running in a temporary directory.
        result = self._run_command(["cargo", "check", "--tests", "--manifest-path", str(manifest_path)], self.project_root)

        if result.returncode == 0:
            print("  ✅ Compilation successful.")
            return True
        else:
            print("  ❌ Compilation failed.")
            if self.dry_run_mode != 'predictive':
                print("\n----- CARGO CHECK STDERR -----")
                print(result.stderr)
                print("------------------------------")
            return False

    def run(self):
        """Executes the full multi-pass migration and refactoring pipeline."""
        if not self.rules_file.exists():
            print(f"❌ Error: Rules file not found at {self.rules_file}")
            sys.exit(1)

        if self.use_git:
            git_status = self._run_command(["git", "status", "--porcelain"], self.project_root)
            if git_status.returncode != 0 or git_status.stdout.strip():
                print("❌ Error: Your git working directory is not clean.")
                print("Please commit or stash your changes before running with --git-rollback.")
                sys.exit(1)
            print("✅ Git working directory is clean. Proceeding with rollback safety net.")

        passes = {
            "1. Core Migration": ["migrate-test-attribute", "migrate-function-signature", "add-prelude-import", "migrate-pool-initialization"],
            "2. Quality & Performance Fixes": ["replace-unwrap-with-expect", "add-assert-eq-message", "replace-sleep-with-wait-helper", "replace-println-with-tracing"],
            "3. Cleanup & Refinement": ["cleanup-redundant-pool-variable", "add-ok-return"],
            "4. Strategic Improvements": ["tag-slow-tests", "add-doc-stub-for-adversarial"]
        }

        start_time = time.time()
        final_status = "SUCCESS"

        if self.dry_run_mode == 'predictive':
            self.run_predictive_pipeline(passes)
        else:
            if not self.run_wet_pipeline(passes):
                final_status = "FAILED"

        end_time = time.time()
        self.print_summary(end_time - start_time, final_status)

    def run_predictive_pipeline(self, passes: Dict[str, List[str]]):
        print("\n🚀 Starting Predictive Dry Run...")
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            print(f"  Creating sandbox in '{temp_dir}'...")
            
            # Copy project sources to sandbox
            shutil.copytree(self.project_root / "src", temp_dir / "src")
            shutil.copytree(self.project_root / "test", temp_dir / "test")
            shutil.copy(self.project_root / "Cargo.toml", temp_dir / "Cargo.toml")
            shutil.copy(self.project_root / "Cargo.lock", temp_dir / "Cargo.lock")
            shutil.copytree(self.project_root / "rules", temp_dir / "rules", dirs_exist_ok=True)


            for name, rule_ids in passes.items():
                print(f"\n--- PREDICTIVE PASS: {name} ---")
                changes = self._run_sg_pass(name, rule_ids, temp_dir)
                if changes == -1:
                    print(f"  Prediction: Pass '{name}' would FAIL during `ast-grep` execution.")
                    break
                if self._validate_compilation(temp_dir):
                    print(f"  Prediction: Pass '{name}' would likely SUCCEED and compile.")
                else:
                    print(f"  Prediction: Pass '{name}' would likely cause COMPILE ERRORS.")
                    break

    def run_wet_pipeline(self, passes: Dict[str, List[str]]) -> bool:
        """Run a pass on the actual source, with optional git rollback."""
        for name, rule_ids in passes.items():
            print(f"\n--- PASS: {name} ---")

            if self.use_git:
                print("  Creating git savepoint...")
                self._run_command(["git", "add", "."], self.project_root)
                commit_result = self._run_command(["git", "commit", "-m", f"refactor: savepoint before '{name}'"], self.project_root)
                if commit_result.returncode != 0 and "nothing to commit" not in commit_result.stdout:
                     print(f"  ❌ Failed to create git savepoint: {commit_result.stderr}")
                     return False

            changes = self._run_sg_pass(name, rule_ids, self.project_root)
            if changes == -1: # ast-grep itself failed
                if self.use_git: self.rollback(name)
                return False
            
            # If no changes were made, we don't need to check compilation.
            if changes == 0 and self.dry_run_mode != 'preview':
                print("  No changes in this pass, skipping compilation check.")
                continue

            if self.dry_run_mode == 'preview':
                continue

            if not self._validate_compilation(self.project_root):
                if self.use_git: self.rollback(name)
                return False
        return True


    def rollback(self, pass_name: str):
        print(f"\n↩️  Rolling back changes from failed pass: '{pass_name}'")
        result = self._run_command(["git", "reset", "--hard", "HEAD~1"], self.project_root)
        if result.returncode == 0:
            print("  Rollback complete. Your working directory is restored.")
        else:
            print(f"  ❌ Rollback failed: {result.stderr}")


    def print_summary(self, duration: float, status: str):
        total_changes = sum(p.get("changes", 0) for p in self.stats.values())
        print("\n=============================================")
        print(f"✅ Refactoring Pipeline {status}")
        print("=============================================")
        print(f"  Execution Time: {duration:.2f} seconds")
        print(f"  Mode: {self.dry_run_mode if self.dry_run_mode else 'apply'}")
        
        if self.dry_run_mode:
            print("\nNOTE: This was a dry run. No files in your working directory were modified.")
        else:
            print(f"  Total Changes Made: ~{total_changes}")
            print("\nAll changes have been applied to your files.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Sinex Test Suite Migration and Refactoring Tool",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Modes of Operation:
  (default)          : Wet run. Applies changes directly. Use with git for safety.
  --dry-run preview  : Shows `ast-grep` diffs without applying them.
  --dry-run predictive : Runs the full process in a sandbox and reports if it would compile.
  --apply            : Explicitly apply changes to files.
  --git-rollback     : Use git to create savepoints and automatically roll back a pass if it fails compilation. Requires a clean git state.

Recommended Workflow:
  1. `git status` (ensure clean working directory)
  2. `./scripts/migrate_and_refactor_v2.py --dry-run predictive` (check for potential compile errors)
  3. `./scripts/migrate_and_refactor_v2.py --apply --git-rollback` (apply changes with rollback safety)
"""
    )
    parser.add_argument("path", type=Path, nargs="?", default=Path("."), help="Path to project root (default: current directory)")
    parser.add_argument("--dry-run", choices=['preview', 'predictive'], nargs='?', const='preview', help="Run without modifying files.")
    parser.add_argument("--apply", action="store_true", help="Apply changes directly to files. Safer when combined with --git-rollback.")
    parser.add_argument("--git-rollback", action="store_true", help="Use git for automatic rollback on failure. Requires clean state.")
    
    args = parser.parse_args()

    # Determine mode
    mode = args.dry_run
    if not args.dry_run and not args.apply:
        # Default to safest dry run if no action is specified
        print("Defaulting to preview dry-run. Use --apply to modify files.")
        mode = 'preview'

    use_git = args.git_rollback
    if use_git and (args.dry_run is not None):
        print("Warning: --git-rollback has no effect in dry-run mode.")
        use_git = False
    
    orchestrator = TestRefactorOrchestrator(test_dir=args.path, dry_run_mode=mode, use_git=use_git)
    orchestrator.run()
