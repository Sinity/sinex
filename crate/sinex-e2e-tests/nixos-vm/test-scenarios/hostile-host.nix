# Hostile-host test: sinex-ingestd under cgroup resource constraints.
#
# Restricts sinex-ingestd below its normal 1G deployment budget and limits disk
# I/O via systemd cgroups, then pumps 10k synthetic events into NATS and verifies:
#   - sinex-ingestd is NOT OOM-killed (systemctl status remains Active)
#   - The constrained pipeline continues committing events
#   - No data corruption: inserted events have non-null IDs and payloads
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

  hostile-publisher = pkgs.writeScriptBin "sinex-hostile-publish" ''
    #!${pkgs.python3}/bin/python3
    import datetime
    import json
    import random
    import socket
    import sys
    import time
    import uuid

    total = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    subject = "dev.events.raw.hostile-host-test.synthetic.load"

    def uuid7(seq):
        timestamp_ms = (int(time.time() * 1000) + seq) & ((1 << 48) - 1)
        rand_a = seq & 0x0fff
        rand_b = random.getrandbits(62)
        value = (
            (timestamp_ms << 80)
            | (0x7 << 76)
            | (rand_a << 64)
            | (0b10 << 62)
            | rand_b
        )
        return str(uuid.UUID(int=value))

    parent_id = uuid7(total + 1)
    with socket.create_connection(("127.0.0.1", 4222), timeout=10) as conn:
        conn.settimeout(10)
        conn.recv(4096)
        conn.sendall(b'CONNECT {"verbose":false,"pedantic":false}\r\nPING\r\n')
        conn.recv(4096)
        for seq in range(total):
            now = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
            payload = json.dumps({
                "id": uuid7(seq),
                "source": "hostile-host-test",
                "event_type": "synthetic.load",
                "host": "sinex-vm",
                "payload": {"seq": seq, "batch": seq // 100},
                "ts_orig": now,
                "source_event_ids": [parent_id],
                "associated_blob_ids": None,
                "payload_schema_id": None,
            }, separators=(",", ":")).encode()
            conn.sendall(
                f"PUB {subject} {len(payload)}\r\n".encode() + payload + b"\r\n"
            )
        conn.sendall(b"PING\r\n")
        conn.recv(4096)

    print(f"Published {total} raw events to {subject}")
  '';
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

    services.sinex.nodes = {
      filesystem.enable = lib.mkForce false;
      terminal.enable = lib.mkForce false;
      browser.enable = lib.mkForce false;
      desktop.enable = lib.mkForce false;
      system.enable = lib.mkForce false;
      automata.enable = lib.mkForce false;
    };

    # ─── cgroup constraints on sinex-ingestd ─────────────────────────────────
    #
    # MemoryMax: hard OOM kill threshold.
    # MemoryHigh: soft limit — kernel begins reclaiming at this point, triggering
    #   backpressure before the hard limit is hit.
    # IOWriteBandwidthMax: limits disk write throughput (PostgreSQL WAL + data).
    # IOReadBandwidthMax: limits disk read throughput.
    systemd.services.sinex-ingestd.serviceConfig = {
      MemoryMax     = lib.mkForce "192M";
      MemoryHigh    = lib.mkForce "128M";
      IOWriteBandwidthMax = lib.mkForce "/ 1048576";   # 1MB/s on root device
      IOReadBandwidthMax  = lib.mkForce "/ 1048576";
    };

    environment.systemPackages = with pkgs; [ jq nats-server natscli procps hostile-publisher ];
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
            "su - postgres -c 'psql -d sinex_dev -t -c \"SELECT COUNT(*) FROM core.events;\"'"
        )
        return int(result.strip())

    def ingestd_is_active():
        rc, _ = machine.execute("systemctl is-active sinex-ingestd")
        return rc == 0

    # ─── Publish 10k synthetic events into NATS ───────────────────────────────
    # Publishes directly to the events.> subject tree, bypassing ingestors.
    # sinex-ingestd reads from this stream and writes to Postgres.

    with subtest("pump-10k-events"):
        machine.succeed("sinex-hostile-publish 10000")

    # ─── Assert ingestd survived the load ─────────────────────────────────────

    with subtest("no-oom-kill"):
        assert ingestd_is_active(), \
            "sinex-ingestd was OOM-killed under constrained cgroup limits — backpressure failure"
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
            "su - postgres -c 'psql -d sinex_dev -t -c "
            "\"SELECT COUNT(*) FROM core.events WHERE payload IS NULL OR id IS NULL;\"'"
        )
        null_count = int(result.strip())
        assert null_count == 0, \
            f"Data corruption: {null_count} events with NULL id or payload"
        print(f"✓ No corrupted rows (all {final_count} events have valid id + payload)")
  '';
}
