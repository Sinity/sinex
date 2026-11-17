import time
import os
import pathlib
import shlex


def capture_original_status(machine, command):
    # Observe the first request itself, without a warm-up or statement logging.
    # The collector reads backend metadata only and terminates after three seconds.
    trace = "/tmp/sinex-original-status.jsonl"
    sql = r"""SELECT json_build_object('at',clock_timestamp(),'backends',
      (SELECT coalesce(json_agg(json_build_object('pid',pid,'state',state,
        'query_start',query_start,'query',left(query,2000),
        'wait_type',wait_event_type,'wait',wait_event,
        'blockers',pg_blocking_pids(pid))),'[]'::json)
       FROM pg_stat_activity WHERE datname='sinex' AND application_name='sinex.api'));
\watch 0.01
"""
    machine.succeed(f"test ! -e {trace}")
    machine.succeed("printf %s " + shlex.quote(sql) + f" > {trace}.sql")
    machine.succeed(
        "systemd-run --quiet --collect --service-type=exec --unit=sinex-status-phase "
        "--property=User=postgres --property=RuntimeMaxSec=3s "
        f"--property=StandardOutput=append:{trace} "
        f"--property=StandardError=append:{trace}.stderr "
        f"/run/current-system/sw/bin/psql -XAtq -d sinex -f {trace}.sql"
    )
    machine.succeed(f"timeout 3s sh -c 'until test -s {trace}; do sleep 0.01; done'")
    failed = False
    try:
        return machine.succeed(command)
    except Exception:
        failed = True
        raise
    finally:
        try:
            machine.execute("systemctl stop sinex-status-phase.service", timeout=5)
            code, samples = machine.execute(f"cat {trace}")
            error_code, errors = machine.execute(f"cat {trace}.stderr")
            artifact = pathlib.Path(os.environ["out"]) / "original-status-phase.jsonl"
            artifact.write_text(samples)
            artifact.with_suffix(".stderr").write_text(errors)
            print(f"original status phase retained at {artifact}, read_exit={code}")
            if failed:
                print(samples)
                print(errors)
        except Exception as error:
            print(f"original status phase collection failed: {error}")


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
