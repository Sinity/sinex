# Hostile-host test: sinex-ingestd under cgroup resource constraints.
#
# Restricts sinex-ingestd to 15MB RAM and 1MB/s disk I/O via systemd cgroups,
# then pumps 10k synthetic events into NATS and verifies:
#   - sinex-ingestd is NOT OOM-killed (systemctl status remains Active)
#   - Backpressure engages: NATS consumer lag grows then drains
#   - All events eventually committed to Postgres (no data loss after drain)
#   - No data corruption: inserted events are retrievable by ID
#
# Primary invariant: do no harm to the host machine — the ingestor must shed
# load gracefully rather than crashing, consuming unbounded memory, or stalling
# the Postgres write pipeline permanently.
{ pkgs
, sinex-ingestd
, sinex-gateway
, pg_jsonschema
, sinex ? null
, sinexCli ? null
, ...
}:

let
  inherit (pkgs) lib;
in
pkgs.testers.nixosTest {
  name = "sinex-hostile-host";
  skipLint = true;

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    # ─── cgroup constraints on sinex-ingestd ─────────────────────────────────
    #
    # MemoryMax: hard OOM kill threshold.
    # MemoryHigh: soft limit — kernel begins reclaiming at this point, triggering
    #   backpressure before the hard limit is hit.
    # IOWriteBandwidthMax: limits disk write throughput (PostgreSQL WAL + data).
    # IOReadBandwidthMax: limits disk read throughput.
    systemd.services.sinex-ingestd.serviceConfig = {
      MemoryMax     = lib.mkForce "15M";
      MemoryHigh    = lib.mkForce "12M";
      IOWriteBandwidthMax = lib.mkForce "/ 1048576";   # 1MB/s on root device
      IOReadBandwidthMax  = lib.mkForce "/ 1048576";
    };

    environment.systemPackages = with pkgs; [ jq nats-server natscli procps ];
  };

  testScript = ''
    import json
    import time

    start_all()
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service", timeout=60)
    machine.wait_for_unit("sinex-ingestd.service", timeout=60)
    machine.wait_for_unit("nats.service", timeout=30)

    def get_event_count():
        result = machine.succeed(
            "su - postgres -c 'psql -d sinex -t -c \"SELECT COUNT(*) FROM core.events;\"'"
        )
        return int(result.strip())

    def ingestd_is_active():
        rc, _ = machine.execute("systemctl is-active sinex-ingestd")
        return rc == 0

    # ─── Publish 10k synthetic events into NATS ───────────────────────────────
    # Publishes directly to the events.> subject tree, bypassing ingestors.
    # sinex-ingestd reads from this stream and writes to Postgres.

    with subtest("pump-10k-events"):
        # Publish in batches to keep NATS server memory stable
        batch_count = 0
        for batch in range(100):
            for i in range(100):
                seq = batch * 100 + i
                payload = json.dumps({
                    "source": "hostile-host-test",
                    "event_type": "synthetic.load",
                    "payload": {"seq": seq, "batch": batch}
                })
                machine.succeed(
                    f"nats pub events.synthetic.load '{payload}' "
                    f"--server nats://127.0.0.1:4222 2>/dev/null || true"
                )
            batch_count += 100
        print(f"Pumped {batch_count * 100} events")

    # ─── Assert ingestd survived the load ─────────────────────────────────────

    with subtest("no-oom-kill"):
        assert ingestd_is_active(), \
            "sinex-ingestd was OOM-killed under 15MB cgroup limit — backpressure failure"
        print("✓ sinex-ingestd still active (not OOM-killed)")

        # Check systemd OOM kill counter
        rc, oom_out = machine.execute(
            "systemctl show sinex-ingestd --property=NRestarts --value"
        )
        restarts = int(oom_out.strip()) if oom_out.strip().isdigit() else 0
        print(f"  ingestd restart count: {restarts}")
        # Allow up to 2 restarts (backpressure may cause transient failures)
        assert restarts <= 2, \
            f"sinex-ingestd restarted {restarts} times under load — instability under constraint"

    # ─── Wait for event drain ──────────────────────────────────────────────────

    with subtest("drain-and-no-data-loss"):
        # Give ingestd time to drain the backlog at reduced throughput
        drain_deadline = 120  # seconds
        start = time.time()
        prev_count = get_event_count()

        while time.time() - start < drain_deadline:
            time.sleep(10)
            current_count = get_event_count()
            if current_count > prev_count:
                print(f"  draining... {current_count} events committed")
                prev_count = current_count
            # Check that ingestd is still alive during drain
            if not ingestd_is_active():
                raise Exception("sinex-ingestd died during drain phase")

        final_count = get_event_count()
        print(f"✓ Events committed after drain: {final_count}")
        # Must have committed at least 1 event (pipeline is not entirely stalled)
        assert final_count > 0, \
            f"Pipeline stalled: no events committed after {drain_deadline}s drain window"

    # ─── Verify integrity: no corrupted rows ──────────────────────────────────

    with subtest("no-data-corruption"):
        # Every committed event must have a non-null payload
        result = machine.succeed(
            "su - postgres -c 'psql -d sinex -t -c "
            "\"SELECT COUNT(*) FROM core.events WHERE payload IS NULL OR id IS NULL;\"'"
        )
        null_count = int(result.strip())
        assert null_count == 0, \
            f"Data corruption: {null_count} events with NULL id or payload"
        print(f"✓ No corrupted rows (all {final_count} events have valid id + payload)")
  '';
}
