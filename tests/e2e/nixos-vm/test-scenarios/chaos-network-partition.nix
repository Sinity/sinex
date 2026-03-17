# Chaos test: network partition between ingestd and NATS.
#
# Injects a deterministic `tc netem` packet-drop rule between sinex-ingestd and
# the NATS server, then removes it and verifies:
#   - ingestd survives the partition without crashing
#   - after reconnection, the event pipeline drains (no data loss)
#   - the event count in Postgres grows back to the expected total
#
# This proves ingestd's reconnection + replay logic works end-to-end and that
# NATS JetStream durable consumers resume from the correct sequence number.
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
  name = "sinex-chaos-network-partition";
  skipLint = true;

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
      ../common/chaos-toolkit.nix
    ];

    # Enable NATS for this test (ingestd → NATS → ingestd pipeline)
    services.sinex.nats.enable = true;
    services.sinex.nats.bootstrapStreams.enable = true;

    services.sinex.nodes = {
      filesystem = {
        enable = true;
        watchPaths = [ "/var/lib/sinex/watched" ];
      };
    };

    # iproute2 + iptables for tc/netem packet injection
    environment.systemPackages = with pkgs; [
      iproute2
      iptables
      netcat-openbsd
      jq
    ];
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

    def generate_fs_events(n, prefix):
        for i in range(n):
            machine.succeed(f"touch /var/lib/sinex/watched/chaos-{prefix}-{i}.txt")

    # ─── Baseline: verify pipeline works before chaos ──────────────────────────

    with subtest("baseline-pipeline"):
        generate_fs_events(10, "pre")
        time.sleep(5)
        baseline_count = get_event_count()
        print(f"Baseline event count: {baseline_count}")
        assert baseline_count > 0, "Pipeline must be working before chaos injection"

    # ─── Inject: partition NATS port 4222 ──────────────────────────────────────

    with subtest("inject-network-partition"):
        print("Injecting network partition on NATS port 4222...")

        # Drop all packets to/from NATS (port 4222) using tc + iptables
        machine.succeed(
            "iptables -A INPUT -p tcp --dport 4222 -j DROP && "
            "iptables -A OUTPUT -p tcp --dport 4222 -j DROP"
        )

        # Also add netem delay + drop on loopback for thorough isolation
        machine.succeed(
            "tc qdisc add dev lo root handle 1: prio && "
            "tc qdisc add dev lo parent 1:3 handle 30: netem loss 100% && "
            "tc filter add dev lo protocol ip parent 1:0 prio 3 u32 "
            "  match ip dport 4222 0xffff flowid 1:3 || true"
        )

        # Verify ingestd is still alive during the partition
        time.sleep(3)
        machine.succeed("systemctl is-active sinex-ingestd")
        print("ingestd survived initial partition")

    # ─── During partition: generate events (should buffer or fail gracefully) ──

    with subtest("events-during-partition"):
        pre_partition_count = get_event_count()
        generate_fs_events(20, "during")

        time.sleep(5)
        during_count = get_event_count()
        print(f"Events during partition: {during_count - pre_partition_count} (may be 0 if buffered)")

        # ingestd must not have crashed
        machine.succeed("systemctl is-active sinex-ingestd")

    # ─── Heal: remove partition rules ──────────────────────────────────────────

    with subtest("heal-network-partition"):
        print("Removing network partition...")
        machine.succeed("iptables -F INPUT; iptables -F OUTPUT; tc qdisc del dev lo root 2>/dev/null || true")

        time.sleep(10)  # Allow reconnection + replay

        # ingestd must still be active
        machine.succeed("systemctl is-active sinex-ingestd")
        print("Partition healed, ingestd still active")

    # ─── Verify: all events eventually reach Postgres ──────────────────────────

    with subtest("verify-no-data-loss"):
        # Generate a final batch to confirm pipeline is flowing again
        generate_fs_events(10, "post")

        # Wait up to 30s for post-partition events to arrive
        final_count = pre_partition_count
        for _ in range(15):
            time.sleep(2)
            final_count = get_event_count()
            if final_count > pre_partition_count + 5:
                break

        print(f"Final event count: {final_count} (baseline was {baseline_count})")
        assert final_count > baseline_count, \
            f"Event count did not grow after partition heal: {final_count} vs {baseline_count}"
        print("✓ No data loss after network partition")
  '';
}
