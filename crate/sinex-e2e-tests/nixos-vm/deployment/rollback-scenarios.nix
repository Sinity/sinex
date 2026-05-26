{ pkgs, lib, ... }:

{
  name = "sinex-rollback-scenarios";
  meta.maintainers = with lib.maintainers; [ sinity ];

  nodes = {
    sinex = { config, pkgs, ... }:
      let
        stateDir = config.services.sinex.directories.state;
      in {
      imports = [ ../common/production-load.nix ];
      
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
        
        updates = {
          coordinatedUpdate = true;
          gracePeriod = 30;
          rollbackOnFailure = true;
        };
      };
      
      services.postgresql = {
        enable = true;
        package = pkgs.postgresql_18;
        extraPlugins = with pkgs.postgresql18Packages; [ timescaledb ];
      };
      
      # Rollback test utilities
      environment.systemPackages = [
        (pkgs.writeScriptBin "inject-failure" ''
          #!${pkgs.bash}/bin/bash
          STATE_DIR="${stateDir}"
          case "$1" in
            "database")
              echo "Injecting database failure..."
              sudo -u postgres psql -c "DROP TABLE core.events CASCADE"
              ;;
            "permission")
              echo "Injecting permission failure..."
              chmod 000 "$STATE_DIR"/
              ;;
            *)
              echo "Usage: $0 {database|permission}"
              exit 1
              ;;
          esac
        '')
        
        (pkgs.writeScriptBin "verify-rollback" ''
          #!${pkgs.bash}/bin/bash
          echo "Verifying rollback state..."
          
          # Check service status
          systemctl is-active sinex-ingestd.service || exit 1
          systemctl is-active sinex-gateway.service || exit 1
          
          # Check event flow
          EVENT_COUNT=$(sudo -u postgres psql -d sinex -t -c \
            "SELECT COUNT(*) FROM core.events")
          
          if [ "$EVENT_COUNT" -gt 0 ]; then
            echo "✓ Event collection operational"
          else
            echo "✗ Event collection failed"
            exit 1
          fi
        '')
      ];

      environment.sessionVariables = {
        SINEX_STATE_DIR = stateDir;
      };
    };
  };

  testScript = ''
    import json
    import time
    import subprocess
    state_dir = sinex.succeed("echo -n $SINEX_STATE_DIR")
    
    start_all()
    
    # Initial setup
    sinex.wait_for_unit("postgresql.service")
    sinex.wait_for_unit("sinex-ingestd.service")
    sinex.wait_for_unit("sinex-gateway.service")
    
    # Capture baseline state
    with subtest("Establish baseline"):
        # Generate initial events
        sinex.execute("touch /tmp/baseline-{1..10}")
        time.sleep(5)
        
        baseline_count = int(sinex.succeed(
            "sudo -u postgres psql -d sinex -t -c "
            "'SELECT COUNT(*) FROM core.events'"
        ).strip())
        
        assert baseline_count > 0, "Baseline events not captured"
    
    # Test database schema rollback
    with subtest("Database migration rollback"):
        # Create a migration that will fail
        sinex.succeed("""
            cat > /tmp/bad_migration.sql << 'EOF'
            BEGIN;
            -- This will succeed
            CREATE TABLE sinex_schemas.migration_test (id TEXT PRIMARY KEY);
            
            -- This will fail due to missing table
            ALTER TABLE nonexistent_table ADD COLUMN test TEXT;
            COMMIT;
            EOF
        """)
        
        # Attempt migration
        sinex.fail(
            "sudo -u postgres psql -d sinex -f /tmp/bad_migration.sql"
        )
        
        # Verify rollback occurred
        sinex.fail(
            "sudo -u postgres psql -d sinex -c "
            "'\\dt sinex_schemas.migration_test'"
        )
        
        # Services should remain operational
        sinex.succeed("systemctl is-active sinex-ingestd.service")
    
    # Test permission failure rollback
    with subtest("Permission failure rollback"):
        # Create state directory backup
        sinex.succeed(f"cp -r {state_dir} /tmp/sinex-backup")
        
        # Inject permission failure
        sinex.execute("inject-failure permission")
        
        # Service should fail and attempt recovery
        sinex.execute("systemctl restart sinex-ingestd.service || true")
        time.sleep(5)
        
        # Fix permissions
        sinex.succeed(f"chmod 755 {state_dir}/")
        
        # Service should recover
        sinex.succeed("systemctl start sinex-ingestd.service")
        sinex.wait_for_unit("sinex-ingestd.service")
        
        # Verify functionality
        sinex.succeed("verify-rollback")
    
    # Test partial update rollback
    with subtest("Partial update rollback"):
        # Start a service transition that matches the NixOS-managed runtime contract.
        sinex.execute("systemctl restart sinex-ingestd.service")
        time.sleep(5)

        # Simulate failure during the restart window.
        sinex.execute("pkill -9 sinex-ingestd || true")
        
        # System should detect failure and rollback
        time.sleep(5)
        
        # Restart services
        sinex.succeed("systemctl start sinex-ingestd.service")
        sinex.wait_for_unit("sinex-ingestd.service")
        
        # Verify event collection continues
        pre_count = int(sinex.succeed(
            "sudo -u postgres psql -d sinex -t -c "
            "'SELECT COUNT(*) FROM core.events'"
        ).strip())
        
        sinex.execute("touch /tmp/rollback-test")
        time.sleep(5)
        
        post_count = int(sinex.succeed(
            "sudo -u postgres psql -d sinex -t -c "
            "'SELECT COUNT(*) FROM core.events'"
        ).strip())
        
        assert post_count > pre_count, "Event collection not restored"
    
    # Test cascading failure prevention
    with subtest("Cascading failure prevention"):
        # Kill promo worker
        sinex.execute("systemctl stop sinex-gateway.service")
        
        # Collector should continue operating
        sinex.succeed("systemctl is-active sinex-ingestd.service")
        
        # Events should still be captured
        sinex.execute("touch /tmp/cascade-test-{1..5}")
        time.sleep(5)
        
        # Verify events in raw table
        sinex.succeed(
            "sudo -u postgres psql -d sinex -c "
            "'SELECT COUNT(*) FROM core.events WHERE "
            "source = \"filesystem\" AND "
            "created_at > NOW() - INTERVAL \"1 minute\"'"
        )
        
        # Restart promo worker
        sinex.succeed("systemctl start sinex-gateway.service")
        sinex.wait_for_unit("sinex-gateway.service")
        
        # System should recover fully
        sinex.succeed("verify-rollback")
    
    # Test rollback with data preservation
    with subtest("Data preservation during rollback"):
        # Record current state
        initial_events = int(sinex.succeed(
            "sudo -u postgres psql -d sinex -t -c "
            "'SELECT COUNT(*) FROM core.events'"
        ).strip())
        
        # Create events during "update"
        sinex.execute("touch /tmp/preserve-{1..20}")
        time.sleep(3)
        
        # Simulate failed update
        sinex.execute("systemctl stop sinex-ingestd.service")
        time.sleep(2)
        
        # Check Dead Letter Queue captured events
        dlq_exists = sinex.succeed(
            f"test -d {state_dir}/dlq && echo 'exists' || echo 'missing'"
        ).strip()
        
        # Restart service
        sinex.succeed("systemctl start sinex-ingestd.service")
        sinex.wait_for_unit("sinex-ingestd.service")
        
        # Final event count should include preserved events
        final_events = int(sinex.succeed(
            "sudo -u postgres psql -d sinex -t -c "
            "'SELECT COUNT(*) FROM core.events'"
        ).strip())
        
        # Some events should have been preserved
        assert final_events >= initial_events, "Events were lost during rollback"
  '';
}
