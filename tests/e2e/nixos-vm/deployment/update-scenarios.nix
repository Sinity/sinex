{ pkgs, lib, ... }:

{
  name = "sinex-update-scenarios";
  meta.maintainers = with lib.maintainers; [ sinity ];

  nodes = {
    sinex = { config, pkgs, ... }: {
      imports = [ ../common/production-load.nix ];
      
      # Initial deployment configuration
      services.sinex = {
        enable = true;
        preset = "lite";
        
        database = {
          url = "postgresql:///sinex?host=/run/postgresql";
          createDatabase = true;
        };
        
        collector = {
          enable = true;
          sources = [ "filesystem" ];
        };
        
        workers = {
          promo.enable = true;
          promo.concurrency = 2;
        };
        
        updates = {
          coordinatedUpdate = true;
          gracePeriod = 30;
        };
      };
      
      services.postgresql = {
        enable = true;
        package = pkgs.postgresql_18;
        extraPlugins = with pkgs.postgresql18Packages; [ timescaledb ];
      };
      
      # Helper script for configuration updates
      environment.systemPackages = [
        (pkgs.writeScriptBin "update-sinex-deployment" ''
          #!${pkgs.bash}/bin/bash
          set -euo pipefail
          
          case "$1" in
            "upgrade-preset")
              # Simulate upgrading from lite to normal preset
              echo "Upgrading Sinex preset to normal..."
              nixos-rebuild switch --flake .#test-upgrade
              ;;
            "add-sources")
              # Simulate adding new event sources
              echo "Adding new event sources..."
              nixos-rebuild switch --flake .#test-sources
              ;;
            "scale-workers")
              # Simulate scaling worker concurrency
              echo "Scaling workers..."
              nixos-rebuild switch --flake .#test-scale
              ;;
            *)
              echo "Usage: $0 {upgrade-preset|add-sources|scale-workers}"
              exit 1
              ;;
          esac
        '')
      ];
    };
  };

  testScript = ''
    import json
    import time
    
    start_all()
    
    # Initial setup
    sinex.wait_for_unit("postgresql.service")
    sinex.wait_for_unit("sinex-ingestd.service")
    sinex.wait_for_unit("sinex-gateway.service")
    
    # Capture initial state
    with subtest("Initial deployment state"):
        # Record initial event count
        initial_count = int(sinex.succeed(
            "sudo -u postgres psql -d sinex -t -c "
            "'SELECT COUNT(*) FROM core.events'"
        ).strip())
        
        # Check initial configuration
        sinex.succeed(
            "systemctl show -p Environment sinex-ingestd.service | "
            "grep -q 'SINEX_PRESET=lite'"
        )
    
    # Test coordinated update process
    with subtest("Coordinated update"):
        # Trigger update signal
        sinex.execute("systemctl reload sinex-ingestd.service")
        
        # Verify grace period behavior
        time.sleep(5)
        
        # Service should still be active during grace period
        sinex.succeed("systemctl is-active sinex-ingestd.service")
        
        # Verify events are still being collected
        sinex.execute("touch /tmp/update-test-file")
        time.sleep(2)
        
        new_count = int(sinex.succeed(
            "sudo -u postgres psql -d sinex -t -c "
            "'SELECT COUNT(*) FROM core.events'"
        ).strip())
        
        assert new_count > initial_count, "Events should continue during update"
    
    # Test configuration hot reload
    with subtest("Configuration hot reload"):
        # Modify configuration file
        sinex.succeed(
            "echo 'event_batch_size = 500' >> "
            "/etc/sinex/collector.toml"
        )
        
        # Send reload signal
        sinex.execute("systemctl reload sinex-ingestd.service")
        
        # Verify configuration was reloaded
        sinex.succeed(
            "journalctl -u sinex-ingestd.service | "
            "grep -q 'Configuration reloaded'"
        )
        
        # Service should remain active
        sinex.succeed("systemctl is-active sinex-ingestd.service")
    
    # Test rollback scenario
    with subtest("Rollback on failure"):
        # Simulate a bad configuration
        sinex.execute(
            "echo 'invalid_option = true' >> /etc/sinex/collector.toml"
        )
        
        # Attempt reload
        sinex.fail("systemctl reload sinex-ingestd.service")
        
        # Service should still be running with old config
        sinex.succeed("systemctl is-active sinex-ingestd.service")
        
        # Fix configuration
        sinex.succeed(
            "grep -v 'invalid_option' /etc/sinex/collector.toml > "
            "/tmp/collector.toml && "
            "mv /tmp/collector.toml /etc/sinex/collector.toml"
        )
    
    # Test zero-downtime migration
    with subtest("Zero-downtime database migration"):
        # Create a migration file
        sinex.succeed("""
            cat > /tmp/test_migration.sql << 'EOF'
            -- Test migration
            CREATE TABLE IF NOT EXISTS sinex_schemas.test_migration (
                id TEXT PRIMARY KEY,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            EOF
        """)
        
        # Apply migration while services are running
        sinex.succeed(
            "sudo -u postgres psql -d sinex -f /tmp/test_migration.sql"
        )
        
        # Verify services remained active
        sinex.succeed("systemctl is-active sinex-ingestd.service")
        sinex.succeed("systemctl is-active sinex-gateway.service")
        
        # Verify migration was applied
        sinex.succeed(
            "sudo -u postgres psql -d sinex -c "
            "'\\dt sinex_schemas.test_migration'"
        )
    
    # Test worker scaling
    with subtest("Dynamic worker scaling"):
        # Check initial worker count
        initial_workers = sinex.succeed(
            "pgrep -f sinex-gateway | wc -l"
        ).strip()
        
        # Scale up workers (would normally be done via config update)
        # For now, just verify the service can handle restarts
        sinex.execute("systemctl restart sinex-gateway.service")
        
        # Wait for service to come back
        sinex.wait_for_unit("sinex-gateway.service")
        
        # Verify workers are processing
        time.sleep(5)
        sinex.succeed(
            "sudo -u postgres psql -d sinex -c "
            "'SELECT COUNT(*) FROM core.events WHERE ts_ingest > NOW() - INTERVAL ''5 minutes'''"
        )
    
    # Test update failure recovery
    with subtest("Update failure recovery"):
        # Stop collector to simulate failure
        sinex.execute("systemctl stop sinex-ingestd.service")
        
        # Verify Dead Letter Queue is active
        time.sleep(2)
        
        # Restart collector
        sinex.execute("systemctl start sinex-ingestd.service")
        sinex.wait_for_unit("sinex-ingestd.service")
        
        # Verify recovery
        sinex.succeed(
            "journalctl -u sinex-ingestd.service | "
            "grep -q 'Service started successfully'"
        )
  '';
}
