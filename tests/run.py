#!/usr/bin/env python3
"""Run end-to-end scenarios: `xtask scenario NAME... [--package DIR]`.

A scenario is a module in scenarios/ with an `exercise(h)` function that
drives the real daemon, CLI and MCP server through harness.Harness `h`.

Started by xtask, which provides a PostgreSQL cluster (PGHOST, PGPORT) and the
binaries to test (SINEX_BIN_DIR). Each scenario gets a fresh database.
"""
import importlib
import os
from pathlib import Path
import sys
import time
import traceback

from harness import Harness

SCENARIO_DIR = Path(__file__).resolve().parent/'scenarios'
sys.path.insert(0, str(SCENARIO_DIR))
# Every scenarios/NAME.py with an `exercise(h)` is a scenario; `_name.py` files are helpers.
SCENARIOS = sorted(p.stem for p in SCENARIO_DIR.glob('*.py') if not p.stem.startswith('_'))


def main(names):
    if names == ['all']:
        names = SCENARIOS
    unknown = [n for n in names if n not in SCENARIOS]
    if unknown:
        sys.exit(f'unknown scenario {", ".join(unknown)}; known: all, {", ".join(SCENARIOS)}')
    packaged = "SINEX_PACKAGED" in os.environ
    failed = []
    for name in names:
        print(f'{name}:', flush=True)
        started = time.monotonic()
        h = Harness(name, packaged)
        failure = None
        try:
            h.create_database(h['database'])
            importlib.import_module(name).exercise(h)
        except Exception:
            failure = traceback.format_exc()
            print(failure, file=sys.stderr, flush=True)
            failed.append(name)
        finally:
            kept = h.close(failure)
        elapsed = time.monotonic() - started
        print(f'{name}: FAILED after {elapsed:.0f}s; logs in {kept}' if failure else f'{name}: passed in {elapsed:.0f}s', flush=True)
    if failed:
        sys.exit(f'failed: {", ".join(failed)}')


if __name__ == '__main__':
    main(sys.argv[1:])
