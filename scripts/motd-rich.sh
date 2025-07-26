#!/usr/bin/env bash
# Enhanced MOTD using rich for beautiful terminal graphics

# Get all the data first
DATABASE_NAME="${DATABASE_NAME:-sinex_dev}"
DATABASE_URL="${DATABASE_URL:-postgresql:///$DATABASE_NAME?host=/run/postgresql}"

# Create a Python script with rich formatting
cat > /tmp/sinex-motd.py << 'PYTHON_SCRIPT'
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.progress import Progress, BarColumn, TextColumn
from rich.text import Text
from rich import box
import os
import json
import subprocess
from datetime import datetime
import psycopg2

console = Console()

def run_cmd(cmd):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=1)
        return result.stdout.strip() if result.returncode == 0 else None
    except:
        return None

# Database status
db_name = os.environ.get('DATABASE_NAME', 'sinex_dev')
db_url = os.environ.get('DATABASE_URL', f'postgresql:///{db_name}?host=/run/postgresql')
try:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'migrations'")
    migration_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM core.events WHERE ts_ingest > NOW() - INTERVAL '1 hour'")
    event_count = cur.fetchone()[0]
    conn.close()
    db_status = "🟢"
    migration_info = f"{migration_count} applied"
except:
    db_status = "🔴"
    migration_info = "not connected"
    event_count = 0

# Cache status
cache_dir = os.path.expanduser("~/.cache/sccache")
if os.path.exists(cache_dir):
    size = run_cmd(f"du -sh {cache_dir} | cut -f1")
    cache_info = f"🟢 {size}"
else:
    cache_info = "🟢 initializing"

# sccache stats
sccache_output = run_cmd("sccache --show-stats 2>/dev/null")
cache_hits = "0%"
if sccache_output:
    for line in sccache_output.split('\n'):
        if "Cache hits" in line and "%" in line:
            cache_hits = line.split()[-1]
            break

# Git tracker daemon status
git_status_json = run_cmd("./scripts/git-state-tracker.sh status 2>/dev/null")
git_daemon_status = "🔴 stopped"
git_info = ""
git_startup_msg = os.environ.get('GIT_DAEMON_MSG', '')

if git_status_json:
    try:
        status = json.loads(git_status_json)
        if status.get('status') == 'running':
            git_daemon_status = "🟢 running"
            snapshots = int(run_cmd("git stash list 2>/dev/null | grep -c 'auto-snapshot'") or "0")
            git_info = f"{snapshots} snapshots"
    except:
        pass
elif git_startup_msg == 'started':
    git_daemon_status = "🟢 running"
    git_info = "just started"
elif git_startup_msg == 'failed to start':
    git_daemon_status = "🔴 failed"

# Compilation daemon status
compile_json = run_cmd("./scripts/compile-daemon.sh status 2>/dev/null")
compile_daemon_status = "🔴 stopped"
compile_info = ""
compile_startup_msg = os.environ.get('COMPILE_DAEMON_MSG', '')

if compile_json:
    try:
        status = json.loads(compile_json)
        if status.get('status') != 'no_data':
            compile_daemon_status = "🟢 running"
            last_complete = os.path.expanduser("~/.sinex-compile-state/last-complete.json")
            if os.path.exists(last_complete):
                with open(last_complete) as f:
                    data = json.load(f)
                    errors = data.get('error_count', 0)
                    warnings = data.get('warning_count', 0)
                    elapsed = data.get('elapsed_ms', 0) / 1000
                    
                    if errors > 0:
                        compile_daemon_status = f"🔴 {errors} errors"
                    elif warnings > 0:
                        compile_daemon_status = f"🟡 {warnings} warnings"
                    else:
                        compile_daemon_status = "🟢 clean"
                    
                    compile_info = f"{elapsed:.1f}s"
    except:
        pass
elif compile_startup_msg == 'started':
    compile_daemon_status = "🟢 running"
    compile_info = "just started"
elif compile_startup_msg == 'failed to start':
    compile_daemon_status = "🔴 failed"

# Test results
test_latest = os.path.expanduser("~/.sinex-analytics/test-runs/latest.json")
test_status = ""
test_info = ""
if os.path.exists(test_latest):
    try:
        with open(test_latest) as f:
            data = json.load(f)
            passed = data.get('summary', {}).get('passed', 0)
            failed = data.get('summary', {}).get('failed', 0)
            duration = data.get('duration_ms', 0) / 1000
            test_type = data.get('test_type', 'tests')
            
            if failed == 0:
                test_status = f"🟢 {passed} passed"
            else:
                test_status = f"🔴 {failed}/{passed+failed} failed"
            
            test_info = f"{test_type}, {duration:.1f}s"
    except:
        pass

# Create beautiful layout
layout = Layout()
layout.split_column(
    Layout(name="header", size=3),
    Layout(name="body"),
    Layout(name="footer", size=4)
)

# Header
header = Text("🚀 SINEX Development Environment", style="bold cyan", justify="center")
layout["header"].update(Panel(header, box=box.DOUBLE_EDGE, style="cyan"))

# Body - split into two columns
layout["body"].split_row(
    Layout(name="left"),
    Layout(name="right")
)

# Left column - Environment
env_table = Table(show_header=False, box=None, expand=True, padding=(0,1))
env_table.add_column("Label", style="dim")
env_table.add_column("Value", style="white")
env_table.add_row("Database:", f"{db_status} {db_name}")
env_table.add_row("Cache:", cache_info)
env_table.add_row("Migrations:", migration_info)
env_table.add_row("Events (1h):", f"{event_count:,}")

layout["left"].update(Panel(
    env_table,
    title="[bold cyan]Environment[/bold cyan]",
    border_style="cyan",
    box=box.ROUNDED
))

# Right column - Daemons
daemons_table = Table(show_header=False, box=None, expand=True, padding=(0,1))
daemons_table.add_column("Label", style="dim") 
daemons_table.add_column("Value", style="white")
daemons_table.add_row("Git Daemon:", git_daemon_status)
if git_info:
    daemons_table.add_row("", git_info)
daemons_table.add_row("Compile Daemon:", compile_daemon_status)
if compile_info:
    daemons_table.add_row("", compile_info)
if test_status:
    daemons_table.add_row("Last tests:", test_status)
    if test_info:
        daemons_table.add_row("", test_info)
daemons_table.add_row("Cache hits:", cache_hits)

layout["right"].update(Panel(
    daemons_table,
    title="[bold magenta]Daemons[/bold magenta]",
    border_style="magenta",
    box=box.ROUNDED
))

# Footer - Quick commands
footer_text = """[dim]Quick Start:[/dim]
  [bold]just[/bold]         → Show essential commands
  [bold]just dev[/bold]     → Format, check & test  
  [bold]just monitor[/bold] → Launch dev dashboard"""

layout["footer"].update(footer_text)

# Print the layout
console.print(layout)
PYTHON_SCRIPT

# Run the Python script
cd /realm/project/sinex 2>/dev/null || cd .
python /tmp/sinex-motd.py