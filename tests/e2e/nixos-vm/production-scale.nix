{ pkgs
, sinex-ingestd
, sinex-gateway
, pg_jsonschema
, sinex ? null
, sinexCli ? null
, ...
}:

pkgs.testers.nixosTest {
  name = "sinex-production-scale";

  nodes.machine = { pkgs, config, lib, ... }: {
    imports = [
      (import ./common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
      ./common/production-load.nix
    ];

    services.sinex = {
      lifecycle.maintenance.enable = lib.mkForce true;
      observability.monitoring.enable = lib.mkDefault false;

      nodes = {
        filesystem = {
          enable = lib.mkForce true;
          instances = lib.mkDefault 2;
          watchPaths = lib.mkDefault [ "/watched" ];
        };
        terminal = {
          enable = lib.mkForce true;
          instances = lib.mkDefault 1;
        };
        system = {
          enable = lib.mkForce true;
          instances = lib.mkDefault 1;
        };
      };
    };

    # Tune for production-scale testing
    virtualisation.memorySize = 8192; # 8GB RAM
    virtualisation.cores = 4;

    # PostgreSQL tuning for sustained load
    services.postgresql.settings = {
      max_connections = 200;
      shared_buffers = "2GB";
      effective_cache_size = "6GB";
      work_mem = "32MB";
      maintenance_work_mem = "512MB";
      checkpoint_completion_target = 0.9;
      wal_buffers = "16MB";
      random_page_cost = 1.1;
      effective_io_concurrency = 200;
    };

    # Increase system limits
    systemd.services.sinex-ingestd.serviceConfig = {
      LimitNOFILE = 65536;
      LimitNPROC = 4096;
    };
  };

  testScript = ''
    import json
    import statistics
    import time
    import sys

    sys.path.append('/etc/nixos-test')
    from test_helpers import TestHelpers

    start_all()
    helpers = TestHelpers(machine)

    def ensure_ready(timeout: int = 120):
        helpers.wait_for_sinex_ready(timeout=timeout)
        machine.wait_until_succeeds("sinex-health-check", timeout=timeout)
        nodes = helpers.list_active_nodes()
        print(f"Active nodes: {nodes}")
        return nodes

    with subtest("Production environment readiness"):
        machine.wait_for_unit("multi-user.target")
        nodes = ensure_ready()
        assert nodes, "Expected node services to be active"
        baseline_events = helpers.get_event_count()
        print(f"Baseline events: {baseline_events}")

    with subtest("Filesystem watcher scalability"):
        machine.succeed("su - test -c 'mkdir -p /var/lib/sinex/watched/scale'")
        for i in range(50):
            machine.succeed(f"su - test -c 'mkdir -p /var/lib/sinex/watched/scale/dir{i}'")

        before = helpers.get_event_count()
        produced = helpers.generate_events(200, prefix="scale", path="/var/lib/sinex/watched/scale")
        assert produced >= 0
        assert helpers.wait_for_event_processing(before + produced, timeout=90)
        recent = helpers.get_event_count_since(30)
        print(f"Events ingested in last 30s after scaling: {recent}")

    with subtest("High-frequency filesystem ingestion"):
        baseline = helpers.get_event_count()
        machine.succeed("production-load-generator --filesystem 4000 20")
        time.sleep(5)
        total = helpers.get_event_count()
        ingested = max(0, total - baseline)
        rate = ingested / 20 if ingested else 0
        print(f"Ingested {ingested} events (~{rate:.1f}/s)")
        assert rate > 150, f"Ingestion rate too low: {rate:.1f}/s"

    with subtest("Mixed workload performance"):
        metrics = []
        machine.succeed("production-load-generator --mixed 1500 40 &")
        for _ in range(8):
            time.sleep(5)
            try:
                metric_json = machine.succeed("sinex-metrics")
                metrics.append(json.loads(metric_json))
            except Exception as exc:
                print(f"Metric collection failed: {exc}")

        if metrics:
            avg_ingestion = statistics.mean(m["ingestion_rate"] for m in metrics)
            max_memory = max(m["memory_usage"] for m in metrics)
            avg_latency = statistics.mean(m["query_latency_ms"] for m in metrics)
            print(
                f"Average ingestion: {avg_ingestion:.1f}/s, "
                f"max memory: {max_memory} MB, "
                f"avg latency: {avg_latency:.1f} ms"
            )
            assert max_memory < 7800, "Memory usage exceeded allocation"
            assert avg_latency < 1500, "Latency regressed under load"

    with subtest("Sustained load soak (2 minutes)"):
        baseline = helpers.get_event_count()
        machine.succeed("production-load-generator --mixed 800 120 &")

        for minute in range(2):
            time.sleep(60)
            try:
                machine.succeed("sinex-health-check")
                print(f"Minute {minute + 1}: health check passed")
            except Exception as exc:
                print(f"Minute {minute + 1}: health check reported issue: {exc}")

        total = helpers.get_event_count()
        print(f"Events ingested during soak: {total - baseline}")

    with subtest("Query performance under contention"):
        machine.succeed("production-load-generator --filesystem 1200 45 &")
        query_times = []
        queries = [
            "SELECT COUNT(*) FROM core.events;",
            "SELECT source, COUNT(*) FROM core.events GROUP BY source LIMIT 10;",
        ]

        for _ in range(3):
            for sql in queries:
                start_ts = time.time()
                machine.succeed(
                    "su - postgres -c \"psql -d sinex -At -c \\\"%s\\\"\"" % sql.replace('"', '\\"')
                )
                query_times.append((time.time() - start_ts) * 1000)
            time.sleep(8)

        if query_times:
            avg_time = statistics.mean(query_times)
            worst_time = max(query_times)
            print(f"Query timings (ms): avg={avg_time:.1f}, max={worst_time:.1f}")
            assert worst_time < 6000, "Worst-case query latency exceeded tolerance"

    with subtest("Post-load validation"):
        nodes = ensure_ready()
        before = helpers.get_event_count()
        produced = helpers.generate_events(20, "final")
        assert helpers.wait_for_event_processing(before + produced, timeout=60)
        final_nodes = helpers.list_active_nodes()
        print(f"Final nodes: {final_nodes}")
        machine.succeed("sinex-health-check")
  '';
}
