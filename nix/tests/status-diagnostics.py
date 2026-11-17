import time


def show_status_failure(machine, default_command, measured_command):
    # Only a failed assertion enables statement logging. The measured request
    # cannot satisfy the original default-budget assertion.
    commands = [
        "sudo -u postgres psql -d sinex -c \"ALTER SYSTEM SET log_min_duration_statement = 0\" -c \"SELECT pg_reload_conf()\"",
        default_command,
        measured_command,
        "sudo -u postgres psql -d sinex -c \"ALTER SYSTEM RESET log_min_duration_statement\" -c \"SELECT pg_reload_conf()\"",
        "journalctl -u postgresql.service --no-pager -n 400",
        "journalctl -u sinexd.service --no-pager -n 200",
        "if test -f /var/lib/sinex/backend-probes.log; then cat /var/lib/sinex/backend-probes.log; fi",
        "sudo -u postgres psql -d sinex -x -c \"SELECT pid, application_name, state, wait_event_type, wait_event, pg_blocking_pids(pid) AS blockers, query FROM pg_stat_activity WHERE datname = 'sinex'\"",
    ]
    for command in commands:
        try:
            started = time.monotonic()
            code, output = machine.execute(command, timeout=65)
            print(f"status failure diagnostic: {command}: exit {code}, seconds={time.monotonic() - started:.6f}")
            print(output)
        except Exception as error:
            print(f"status failure diagnostic unavailable: {error}")
