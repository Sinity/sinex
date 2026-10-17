#!/usr/bin/env python3
"""Cheap harness-front-door refusals; no runtime, package build or provider access."""
from pathlib import Path
import subprocess
import sys

root=Path(__file__).resolve().parents[1]
command=[sys.executable,str(root/'tests/foundation.py')]
pin='/synthetic/not-opened-package-pin.json'
cases=[
    (['--source-investigation','--packaged'],'requires an explicit immutable package pin'),
    (['--source-investigation','--package-pin',pin],'package pin requires --packaged'),
    (['--source-investigation','--packaged','--package-pin',pin,'--atuin'],'owns its scenario selector'),
    (['--source-investigation','--packaged','--package-pin',pin,'--session-predecessor-pin',pin],'owns its scenario selector'),
    (['--source-investigation','--packaged','--package-pin',pin,'--recall-coverage-before-pin',pin],'recall coverage contrast requires'),
    (['--package-pin',pin],'source investigation'),
]
for args,expected in cases:
    result=subprocess.run(command+args,text=True,capture_output=True)
    assert result.returncode==2 and expected in result.stderr, (args,result.returncode,result.stdout,result.stderr)
    assert '"owned_state"' not in result.stdout, 'refusal must precede scratch/runtime ownership'
help_result=subprocess.run(command+['--help'],text=True,capture_output=True)
assert help_result.returncode==0 and 'source investigation' in ' '.join(help_result.stdout.split())
print(f'{len(cases)} parser refusals and help passed')
