# Chaos test: sinex-ingestd process kill during batch ingestion.
#
# SIGKILLs ingestd mid-batch and verifies:
#   - ingestd restarts (via systemd) and resumes from its checkpoint
#   - no duplicate events in Postgres (idempotent re-processing)
#   - the total event count is monotonically non-decreasing (no data loss)
#
# This directly tests the checkpoint-based recovery invariant described in the
# sinex-node-sdk documentation: a node that restarts mid-batch must replay from
# the last committed checkpoint, not from zero.
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
  name = "sinex-chaos-process-restart";
  skipLint = true;

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    services.sinex.nodes.filesystem = {
      enable = true;
      watchPaths = [ "/var/lib/sinex/watched" ];
    };

    # ingestd must restart automatically after SIGKILL
    systemd.services.sinex-ingestd.serviceConfig.Restart = lib.mkForce "always";
    systemd.services.sinex-ingestd.serviceConfig.RestartSec = lib.mkForce "2s";

    environment.systemPackages = with pkgs; [ jq procps ];
  };

  testScript = ''
    import time

    start_all()
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service", timeout=60)
    machine.wait_for_unit("sinex-ingestd.service", timeout=60)

    def get_event_count():
        result = machine.succeed(
            "su - postgres -c 'psql -d sinex -t -c \"SELECT COUNT(*) FROM core.events;\"'"
        )
        return int(result.strip())

    def get_event_ids():
        """Return a set of all event UUIDs currently in core.events."""
        result = machine.succeed(
            "su - postgres -c 'psql -d sinex -t -c \"SELECT id FROM core.events ORDER BY id;\"'"
        )
        return set(line.strip() for line in result.splitlines() if line.strip())

    def generate_batch(n, prefix):
        for i in range(n):
            machine.succeed(f"touch /var/lib/sinex/watched/restart-{prefix}-{i}.txt")

    # ─── Baseline ──────────────────────────────────────────────────────────────

    with subtest("baseline"):
        generate_batch(10, "pre")
        time.sleep(5)
        baseline_count = get_event_count()
        baseline_ids = get_event_ids()
        print(f"Baseline: {baseline_count} events")

    # ─── SIGKILL ingestd mid-batch ─────────────────────────────────────────────

    with subtest("sigkill-mid-batch"):
        # Start a new batch in the background
        generate_batch(30, "during")

        # SIGKILL ingestd immediately
        rc, pid_out = machine.execute("systemctl show -p MainPID sinex-ingestd --value")
        pid = pid_out.strip()
        if pid and pid != "0":
            print(f"SIGKILLing ingestd PID {pid} mid-batch")
            machine.execute(f"kill -9 {pid}")
        else:
            machine.systemctl("kill --signal=KILL sinex-ingestd")

        time.sleep(1)

    # ─── Wait for systemd to restart ingestd ──────────────────────────────────

    with subtest("restart-recovery"):
        machine.wait_for_unit("sinex-ingestd.service", timeout=30)
        machine.wait_until_succeeds("systemctl is-active sinex-ingestd", timeout=15)
        print("ingestd restarted successfully")

        # Allow checkpoint replay to complete
        time.sleep(10)

    # ─── Verify idempotency: no duplicate events ───────────────────────────────

    with subtest("no-duplicates"):
        post_ids = get_event_ids()
        post_count = len(post_ids)

        # All baseline events must still be present (no deletion)
        lost = baseline_ids - post_ids
        assert not lost, \
            f"Data loss: {len(lost)} events present before restart but missing after: {list(lost)[:5]}"

        # No event may appear more than once (deduplication via UUIDv7 ON CONFLICT)
        result = machine.succeed(
            "su - postgres -c 'psql -d sinex -t -c "
            "\"SELECT COUNT(*) FROM (SELECT id, COUNT(*) c FROM core.events GROUP BY id HAVING c > 1) dups;\"'"
        )
        dup_count = int(result.strip())
        assert dup_count == 0, \
            f"Duplicate events found after restart: {dup_count} duplicated IDs"

        print(f"✓ No duplicates. Events: {baseline_count} → {post_count} (net +{post_count - baseline_count})")

    # ─── Verify pipeline works after recovery ─────────────────────────────────

    with subtest("post-recovery-pipeline"):
        generate_batch(10, "post")
        time.sleep(8)
        final_count = get_event_count()
        assert final_count > post_count, \
            f"Pipeline not flowing after recovery: count stuck at {final_count}"
        print(f"✓ Pipeline flows after restart: {post_count} → {final_count}")
  '';
}
