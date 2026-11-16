"""Temporary window-plan measurement; remove after collecting its artifact."""
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path.cwd()
OUTPUT = Path(os.environ["RUNNER_TEMP"]) / "store-verification"


def explain():
    name = "store_window_" + uuid.uuid4().hex
    template = os.environ["DEVKIT_PG_TEMPLATE"]
    subprocess.run([
        "createdb", "--maintenance-db=postgres", "--strategy=file_copy",
        "--template", template, name,
    ], check=True)
    try:
        print("EXPLAIN on a disposable clone of the test template", flush=True)
        subprocess.run([
            "psql", "--no-psqlrc", "--set=ON_ERROR_STOP=1", "--dbname", name,
            "--file", str(ROOT / ".github/explain-window.sql"),
        ], check=True)
    finally:
        subprocess.run([
            "dropdb", "--maintenance-db=postgres", "--if-exists", "--force", name,
        ], check=True)


def main():
    if "--inside-database" in sys.argv:
        explain()
        return
    OUTPUT.mkdir(parents=True, exist_ok=True)
    log = OUTPUT / "explain-window.log"
    with log.open("w") as output:
        result = subprocess.run([
            "cargo", "xtask", "db", "exec", "--", sys.executable,
            str(Path(__file__).resolve()), "--inside-database",
        ], stdout=output, stderr=subprocess.STDOUT)
    print(log.read_text(), flush=True)
    (OUTPUT / "summary.json").write_text(json.dumps({
        "query_plan_exit_status": result.returncode,
    }, indent=2) + "\n")
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
