#!/usr/bin/env python3
"""Run end-to-end scenarios: `xtask scenario NAME... [--package DIR]`.

A scenario is a module in scenarios/ with an `exercise(h)` function that
drives the real daemon, CLI and MCP server through harness.Harness `h`.

Started by xtask, which provides a PostgreSQL cluster (PGHOST, PGPORT) and the
binaries to test (SINEX_BIN_DIR). Each scenario gets a fresh database; several
scenarios run in parallel (SINEX_SCENARIO_JOBS, default 8).
"""
import concurrent.futures
import importlib
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

from harness import Harness, create_template

SCENARIO_DIR = Path(__file__).resolve().parent/'scenarios'
sys.path.insert(0, str(SCENARIO_DIR))
# Every scenarios/NAME.py with an `exercise(h)` is a scenario; `_name.py` files are helpers.
SCENARIOS = sorted(p.stem for p in SCENARIO_DIR.glob('*.py') if not p.stem.startswith('_'))


def run_one(name, packaged):
    """Run one scenario in this process; return True if it passed."""
    print(f'{name}:', flush=True)
    started = time.monotonic()
    h = Harness(name, packaged)
    failure = None
    try:
        h.create_database(h['database'])
        importlib.import_module(name).exercise(h)
    except Exception:
        failure = traceback.format_exc()
        print(failure, flush=True)
    finally:
        kept = h.close(failure)
    elapsed = time.monotonic() - started
    print(f'{name}: FAILED after {elapsed:.0f}s; logs in {kept}' if failure else f'{name}: passed in {elapsed:.0f}s', flush=True)
    return failure is None


def run_child(name):
    """Run one scenario in a child process; its output is printed in one piece."""
    child = subprocess.run([sys.executable, __file__, name], stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, text=True)
    print(child.stdout, end='', flush=True)
    return child.returncode == 0


def main(names):
    if names == ['all']:
        names = SCENARIOS
    unknown = [n for n in names if n not in SCENARIOS]
    if unknown:
        sys.exit(f'unknown scenario {", ".join(unknown)}; known: all, {", ".join(SCENARIOS)}')
    if 'SINEX_TEMPLATE_READY' not in os.environ:
        create_template()
        os.environ['SINEX_TEMPLATE_READY'] = '1'
    if len(names) == 1:
        sys.exit(0 if run_one(names[0], 'SINEX_PACKAGED' in os.environ) else 1)
    # Scenarios have separate databases, directories and ports, so they run side by side.
    jobs = int(os.environ.get('SINEX_SCENARIO_JOBS', '8'))
    with concurrent.futures.ThreadPoolExecutor(jobs) as pool:
        passed = dict(zip(names, pool.map(run_child, names)))
    failed = [name for name, ok in passed.items() if not ok]
    if failed:
        sys.exit(f'failed: {", ".join(failed)}')


if __name__ == '__main__':
    main(sys.argv[1:])
