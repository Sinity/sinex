#!/usr/bin/env python3
"""
Orchestrates the multi-pass test suite refactoring using ast-grep.
"""
import subprocess
import sys
from pathlib import Path
import argparse

def run_ast_grep(test_dir: Path, apply_changes: bool):
    """Runs the full ast-grep scan with iterative updates."""
    sgconfig_path = Path(__file__).parent.parent / "sgconfig.yml"
    if not sgconfig_path.exists():
        print(f"❌ Error: sgconfig.yml not found at {sgconfig_path}")
        print("Please create it in the project root with `ruleDirs: [rules]`")
        sys.exit(1)

    command = ["ast-grep", "scan"]
    if apply_changes:
        command.append("-U")

    print(f"🚀 Executing: {' '.join(command)} {test_dir}")
    print("This may take a few moments as ast-grep applies rules iteratively...")

    try:
        result = subprocess.run(
            command + [str(test_dir)],
            capture_output=True,
            text=True,
            check=False # Don't throw on non-zero exit, as `scan` exits 1 when changes are found
        )
        print("\n--- ast-grep stdout ---")
        print(result.stdout if result.stdout else "(No output)")
        if result.stderr:
            print("\n--- ast-grep stderr ---")
            print(result.stderr)
        
        if result.returncode > 1:
            print(f"\n❌ ast-grep exited with error code {result.returncode}.")
        else:
            print("\n✅ ast-grep pass completed.")

    except FileNotFoundError:
        print("\n❌ Error: `ast-grep` command not found.")
        print("Please ensure ast-grep is installed and in your system's PATH.")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sinex Test Suite Migration Tool")
    parser.add_argument("path", type=Path, nargs="?", default=Path("test"), help="Directory to refactor")
    parser.add_argument("--apply", action="store_true", help="Apply changes to files")
    args = parser.parse_args()

    run_ast_grep(test_dir=args.path, apply_changes=args.apply)

    if not args.apply:
        print("\n✨ This was a dry-run. To apply changes, use the --apply flag.")
    else:
        print("\n✨ Refactoring applied. Please review changes and run `cargo check --tests`.")