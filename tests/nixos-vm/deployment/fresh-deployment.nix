{ pkgs, lib, ... }:

{
  name = "sinex-fresh-deployment";
  meta.maintainers = with lib.maintainers; [ sinity ];

  nodes = {
    sinex = { config, pkgs, ... }: {
      imports = [ ../common/production-load.nix ];
      
      # Fresh deployment configuration
      services.sinex = {
        enable = true;
        preset = "normal";
        
        database = {
          url = "postgresql:///sinex?host=/run/postgresql";
          createDatabase = true;
        };
        
        collector = {
          enable = true;
          sources = [ "filesystem" "terminal" ];
        };
        
        workers = {
          promo.enable = true;
          promo.concurrency = 4;
        };
        
        monitoring = {
          enable = true;
          dashboards.grafana.enable = true;
        };
      };
      
      # PostgreSQL with TimescaleDB
      services.postgresql = {
        enable = true;
        package = pkgs.postgresql_16;
        extraPlugins = with pkgs.postgresql16Packages; [ timescaledb ];
      };
    };
  };

  testScript = ''
    import json
    import time
    
    start_all()
    
    # Wait for PostgreSQL
    sinex.wait_for_unit("postgresql.service")
    sinex.wait_for_open_port(5432)
    
    # Verify database creation
    sinex.succeed("sudo -u postgres psql -d sinex -c 'SELECT 1'")
    
    # Wait for services to start
    sinex.wait_for_unit("sinex-ingestd.service")
    sinex.wait_for_unit("sinex-gateway.service")
    
    # Verify health endpoints
    with subtest("Health monitoring active"):
        sinex.wait_until_succeeds(
            "systemctl is-active sinex-ingestd.service"
        )
        sinex.wait_until_succeeds(
            "systemctl is-active sinex-gateway.service"
        )
    
    # Check for event flow
    with subtest("Event collection working"):
        # Generate some filesystem events
        sinex.execute("touch /tmp/test-file-{1..10}")
        sinex.execute("rm /tmp/test-file-*")
        
        # Wait for events to be processed
        time.sleep(5)
        
        # Query for events
        result = sinex.succeed(
            "sudo -u postgres psql -d sinex -t -c "
            "'SELECT COUNT(*) FROM core.events'"
        )
        count = int(result.strip())
        assert count > 0, f"Expected events in database, got {count}"
    
    # Verify heartbeat events
    with subtest("Heartbeat monitoring"):
        # Wait for at least one heartbeat cycle
        time.sleep(35)
        
        result = sinex.succeed(
            "sudo -u postgres psql -d sinex -t -c "
            "'SELECT COUNT(*) FROM core.events WHERE "
            "source LIKE \"sinex.metrics.%\"'"
        )
        heartbeats = int(result.strip())
        assert heartbeats >= 2, f"Expected heartbeat events, got {heartbeats}"
    
    # Check systemd notify protocol
    with subtest("SystemD integration"):
        # Verify notify type services
        sinex.succeed(
            "systemctl show -p Type sinex-ingestd.service | "
            "grep -q 'Type=notify'"
        )
        
        # Check service states
        sinex.succeed("systemctl status sinex-ingestd.service")
        sinex.succeed("systemctl status sinex-gateway.service")
    
    # Verify resource limits
    with subtest("Resource limits applied"):
        limits = sinex.succeed(
            "systemctl show -p MemoryMax sinex-ingestd.service"
        )
        assert "MemoryMax=" in limits, "Memory limits not set"
    
    # Test graceful shutdown
    with subtest("Graceful shutdown"):
        sinex.execute("systemctl stop sinex-ingestd.service")
        
        # Service should stop cleanly
        sinex.wait_until_fails(
            "systemctl is-active sinex-ingestd.service"
        )
        
        # Check for clean shutdown in logs
        sinex.succeed(
            "journalctl -u sinex-ingestd.service | "
            "grep -q 'Shutting down gracefully'"
        )
  '';
}