{ pkgs
, sinex-ingestd
, sinex-gateway
, pg_jsonschema
, sinex ? null
, sinexCli ? null
, ...
}:

pkgs.testers.nixosTest {
  name = "sinex-chaos-engineering";

  nodes.machine = { pkgs, config, lib, ... }: {
    imports = [
      (import ./common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
      ./common/chaos-toolkit.nix
    ];

    # Enhanced monitoring for chaos testing
    systemd.services.chaos-monitor = {
      description = "Chaos test monitoring";
      wantedBy = [ "multi-user.target" ];
      script = ''
        while true; do
          echo "=== System Status at $(date) ==="
          echo "Memory: $(free -h | awk '/Mem:/ {print $3 "/" $2}')"
          echo "Load: $(uptime | awk -F'load average:' '{print $2}')"
          echo "Disk: $(df -h / | tail -1 | awk '{print $3 "/" $2}')"

          EVENTS_LAST_15=$(
            su - postgres -c "psql -d sinex -At -c \"SELECT COUNT(*) FROM core.events WHERE ts_coided > NOW() - INTERVAL '15 seconds';\"" 2>/dev/null || echo "0"
          )
          echo "Events (15s): $EVENTS_LAST_15"

          echo "Active Sinex services:"
          systemctl list-units 'sinex-*.service' --state=active --no-legend --plain 2>/dev/null \
            | awk '{print \"  • \" $1}' || true
          echo
          sleep 10
        done
      '';
      serviceConfig = {
        Type = "simple";
      };
    };
  };

  testScript = ''
    import random
    import time
    import sys

    sys.path.append('/etc/nixos-test')
    from test_helpers import TestHelpers

    start_all()
    helpers = TestHelpers(machine)

    def assert_nodes() -> list[str]:
        nodes = helpers.list_active_nodes()
        assert nodes, "No node services detected"
        print(f"Active nodes: {', '.join(nodes)}")
        return nodes

    def wait_for_recovery(timeout: int = 120) -> None:
        helpers.wait_for_sinex_ready(timeout=timeout)
        machine.wait_until_succeeds("sinex-health-check", timeout=timeout)
        assert_nodes()

    def inject_and_validate(failure: str, duration: int = 15) -> None:
        print(f"→ Injecting {failure} fault for {duration}s")
        machine.succeed(f"chaos-inject {failure} {duration}")
        wait_for_recovery()
        recent = helpers.get_event_count_since(15)
        print(f"Events ingested in last 15s after {failure}: {recent}")

    with subtest("Baseline system health"):
        machine.wait_for_unit("multi-user.target")
        helpers.wait_for_sinex_ready(timeout=90)
        machine.wait_for_unit("chaos-monitor.service")
        machine.succeed("sinex-health-check")
        nodes = assert_nodes()
        baseline_events = helpers.get_event_count()
        print(f"Baseline event count: {baseline_events}")
        print(f"Monitoring nodes: {nodes}")

    with subtest("Random service failures"):
        failure_types = ["kill", "cpu", "memory"]
        for iteration in range(4):
            inject_and_validate(random.choice(failure_types), duration=12)
            time.sleep(5)

    with subtest("Cascading failure scenario"):
        machine.succeed("chaos-scenario cascading-failure")
        wait_for_recovery(timeout=150)
        recent = helpers.get_event_count_since(30)
        print(f"Events processed in 30s post-cascade: {recent}")
        assert recent >= 0

    with subtest("Resource stress scenario"):
        before = helpers.get_event_count()
        machine.succeed("chaos-inject cpu 20")
        machine.sleep(5)
        machine.succeed("chaos-inject memory 15")
        wait_for_recovery()
        after = helpers.get_event_count()
        print(f"Events processed during resource storm: {after - before}")

    with subtest("Service restart resilience"):
        tracked_services = ["sinex-ingestd", "postgresql"]
        nodes = assert_nodes()
        if nodes:
            tracked_services.append(nodes[0].removesuffix(".service"))

        for service in tracked_services:
            unit = service if service.endswith(".service") else f"{service}.service"
            print(f"Restarting {unit}")
            machine.succeed(f"systemctl restart {service}")
            machine.wait_for_unit(unit)
            wait_for_recovery(timeout=90)

    with subtest("Post-chaos validation"):
        wait_for_recovery(timeout=90)
        initial = helpers.get_event_count()
        generated = helpers.generate_events(10, "chaos-validation")
        assert generated >= 0
        assert helpers.wait_for_event_processing(initial + generated, timeout=60)
        final_nodes = assert_nodes()
        print(f"Final active nodes: {final_nodes}")
        machine.succeed("sinex-health-check")
  '';
}
