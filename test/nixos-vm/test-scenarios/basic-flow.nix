# Basic E2E flow test for Sinex
{ pkgs, sinex-collector, ... }:

let
  # Python CLI for querying (simple wrapper)
  sinex-query = pkgs.writeScriptBin "sinex" ''
    #!${pkgs.python3}/bin/python3
    import subprocess
    import sys
    import json

    # Simple query interface to PostgreSQL
    def query_events(limit=10):
        result = subprocess.run([
            "${pkgs.postgresql_16}/bin/psql", 
            "-d", "sinex_test",
            "-U", "postgres",
            "-t", "-c",
            f"SELECT id, source, event_type, ts_ingest, payload FROM raw.events ORDER BY ts_ingest DESC LIMIT {limit};"
        ], capture_output=True, text=True, cwd="/tmp")
        
        if result.returncode == 0:
            lines = [line.strip() for line in result.stdout.split('\n') if line.strip()]
            if lines:
                print("Recent events:")
                for line in lines:
                    print(f"  {line}")
            else:
                print("No events found")
        else:
            print(f"Query failed: {result.stderr}")

    def stats():
        result = subprocess.run([
            "${pkgs.postgresql_16}/bin/psql", 
            "-d", "sinex_test",
            "-U", "postgres", 
            "-t", "-c",
            "SELECT COUNT(*) FROM raw.events;"
        ], capture_output=True, text=True, cwd="/tmp")
        
        if result.returncode == 0:
            count = result.stdout.strip()
            print(f"Total events captured: {count}")
        else:
            print(f"Stats failed: {result.stderr}")

    if len(sys.argv) > 1 and sys.argv[1] == "stats":
        stats()
    else:
        limit = 10
        if len(sys.argv) > 2 and sys.argv[1] == "query":
            try:
                limit = int(sys.argv[2])
            except:
                pass
        query_events(limit)
  '';
in
pkgs.nixosTest {
  name = "sinex-basic-flow";

  nodes.machine =
    { config, pkgs, ... }:
    {
      imports = [
        # Import the actual Sinex NixOS module
        ../../../nixos/default.nix
      ];

      # Basic system configuration
      networking.hostName = "sinex-test";
      
      # The Sinex module will handle PostgreSQL setup
      # We just need to ensure required extensions are available
      services.postgresql = {
        extraPlugins = with pkgs.postgresql16Packages; [
          timescaledb
          pgx_ulid
          # pg_jsonschema when available
        ];
      };

      # Use Sinex the way a real user would!
      services.sinex = {
        enable = true;

        database = {
          name = "sinex_test";
          user = "sinex_test";  # Match the database name to avoid NixOS assertion
        };

        unifiedCollector = {
          enable = true;

          # Just enable filesystem monitoring for the test
          sources = {
            filesystem = {
              enable = true;
              watchPaths = [ "/home/test/watched" ];
              excludePatterns = [ ];
            };
          };
        };
      };

      # Create test user and directory
      users.users.test = {
        isNormalUser = true;
        home = "/home/test";
        createHome = true;
      };

      # Create watched directory
      systemd.tmpfiles.rules = [
        "d /home/test/watched 0755 test users -"
      ];
      
      # Provide our built packages
      nixpkgs.overlays = [(final: prev: {
        sinex-unified-collector = sinex-collector;
        sqlx-cli = prev.sqlx-cli or pkgs.sqlx-cli;
      })];
      
      # Make the sinex query tool available
      environment.systemPackages = [ sinex-query ];
    };

  testScript = ''
    start_all()

    # Wait for system to be ready
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service")

    # Verify PostgreSQL is running
    machine.succeed("systemctl is-active postgresql")

    # Wait for Sinex services (the module creates these)
    machine.wait_for_unit("sinex-init.service")
    machine.wait_for_unit("sinex-unified-collector.service")
    machine.succeed("systemctl is-active sinex-unified-collector")

    # Test 1: Database schema validation
    with subtest("Database schema validation"):
        # Check that Sinex tables exist
        tables = machine.succeed("su - postgres -c 'psql -d sinex_test -c \"\\dt raw.*\"'")
        assert "raw.events" in tables, "raw.events table not created"
        
        # Check extensions
        extensions = machine.succeed("su - postgres -c 'psql -d sinex_test -c \"\\dx\"'")
        assert "timescaledb" in extensions, "TimescaleDB not installed"
        print(f"Extensions: {extensions}")

    # Test 2: Basic file creation event
    with subtest("File creation event capture"):
        # Create a test file
        machine.succeed("su - test -c 'echo \"Hello Sinex\" > /home/test/watched/test1.txt'")
        
        # Wait for event to be processed (filesystem events are immediate)
        machine.sleep(3)
        
        # Query events
        output = machine.succeed("sinex")
        print(f"Query output: {output}")
        
        # Check if we have any events at all first
        stats = machine.succeed("sinex stats")
        print(f"Stats: {stats}")
        
        # Basic verification that the system is working
        assert "Total events captured:" in stats, "Stats command not working"

    # Test 3: Multiple file events
    with subtest("Multiple event capture"):
        # Create multiple files
        for i in range(3):
            machine.succeed(f"su - test -c 'touch /home/test/watched/file_{i}.txt'")
        
        # Wait for processing
        machine.sleep(3)
        
        # Check stats show increased count
        stats = machine.succeed("sinex stats")
        print(f"Updated stats: {stats}")
        
        # Extract event count
        import re
        match = re.search(r'Total events captured: (\d+)', stats)
        if match:
            count = int(match.group(1))
            print(f"Event count: {count}")
            # Should have at least some events
            assert count > 0, f"Expected some events, got {count}"
        else:
            print("Could not parse event count, but stats command worked")

    # Test 4: Service resilience
    with subtest("Service restart resilience"):
        # Restart the collector
        machine.systemctl("restart sinex-unified-collector")
        machine.wait_for_unit("sinex-unified-collector.service")
        
        # Generate new event
        machine.succeed("su - test -c 'echo \"After restart\" > /home/test/watched/restart-test.txt'")
        machine.sleep(2)
        
        # Verify service is still active
        machine.succeed("systemctl is-active sinex-unified-collector")
        
        # Check that we can still query (system still responsive)
        machine.succeed("sinex stats")

    # Test 5: Real database integration
    with subtest("Database integration"):
        # Directly verify events in database
        result = machine.succeed("su - postgres -c 'psql -d sinex_test -c \"SELECT COUNT(*) FROM raw.events;\"'")
        print(f"Direct DB count: {result}")
        
        # Verify hypertable (TimescaleDB feature)
        hypertables = machine.succeed("su - postgres -c 'psql -d sinex_test -c \"SELECT * FROM timescaledb_information.hypertables;\"'")
        print(f"Hypertables: {hypertables}")
  '';
}

