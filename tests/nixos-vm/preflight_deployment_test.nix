# NixOS VM integration test for Sinex Pre-Flight Verification deployment
# Tests the complete Pre-Flight Verification Model in a real NixOS environment

{ pkgs, lib, ... }:

with lib;

let
  # Test configurations for different scenarios
  baseConfig = {
    system.stateVersion = "24.05";
    
    # Basic system setup
    boot.loader.systemd-boot.enable = true;
    boot.loader.efi.canTouchEfiVariables = true;
    
    # Network for package downloads
    networking.hostName = "sinex-preflight-test";
    networking.networkmanager.enable = true;
    
    # Users
    users.users.testuser = {
      isNormalUser = true;
      extraGroups = [ "wheel" ];
      password = "test";
    };
    
    # System packages needed for testing
    environment.systemPackages = with pkgs; [
      postgresql
      curl
      jq
      systemd
      util-linux
    ];
    
    # Enable SSH for debugging if needed
    services.openssh = {
      enable = true;
      settings.PermitRootLogin = "yes";
    };
    users.users.root.password = "root";
  };
  
  # Sinex configuration with pre-flight verification enabled
  sinexConfig = {
    services.sinex = {
      enable = true;
      targetUser = "testuser";
      
      # Enable pre-flight verification with test-friendly settings
      preflightVerification = {
        enable = true;
        timeout = 120;
        skipPhases = [ ]; # Test all phases
        failureAction = "abort"; # Strict testing
        recordResults = true;
        notifications.enable = false; # Disable for testing
      };
      
      # Database configuration
      database = {
        autoSetup = true;
        name = "sinex";
        user = "sinex";
        host = "localhost";
        port = 5432;
        migration.enable = true;
      };
      
      # Use lite preset for faster testing
      preset = "lite";
      
      # Event source configuration for testing
      eventSources = {
        filesystem = {
          enable = true;
          watchPaths = [ "/tmp/sinex-test" ];
        };
        
        # Disable complex sources for VM testing
        clipboard.enable = false;
        kittyScrollback.enable = false;
        asciinema.enable = false;
        hyprland.enable = false;
        atuin.enable = false;
      };

      dlq = {
        enable = true;
        failureStoragePath = "/var/lib/sinex/failures";
        maxRetries = 2;
        retryDelaySecs = 10;
        cleanup = {
          enable = true;
          maxAge = "1d";
          maxFiles = 100;
        };
      };
      
      # Promotion worker configuration
      promoWorker = {
        enable = true;
        pollInterval = 2;
        batchSize = 50;
      };
      
      # Update configuration for testing
      update = {
        enable = true;
        gracePeriod = 10;
        healthCheckTimeout = 30;
        rollbackOnFailure = true;
        preserveData = true;
      };
      
      # Monitoring for testing
      monitoring = {
        enable = true;
        logging.enable = true;
        alerting.enable = false; # Disable for testing
      };
    };
  };

in {
  name = "sinex-preflight-deployment";
  
  meta = {
    description = "Test Sinex Pre-Flight Verification deployment in NixOS VM";
    maintainers = [ "sinex-team" ];
  };
  
  nodes = {
    # Test machine with standard Sinex deployment
    sinex-machine = { ... }: mkMerge [
      baseConfig
      sinexConfig
    ];
    
    # Test machine for rollback scenarios
    sinex-rollback-test = { ... }: mkMerge [
      baseConfig
      sinexConfig
      {
        # Intentionally misconfigure something to test rollback
        services.sinex.database.port = 9999; # Invalid port
      }
    ];
    
    # Test machine for resource constraints
    sinex-resource-test = { ... }: mkMerge [
      baseConfig
      sinexConfig
      {
        # Constrain resources to test resource verification
        virtualisation.memorySize = 512; # Low memory
        virtualisation.diskSize = 2048; # Small disk
      }
    ];
  };
  
  testScript = ''
    import json
    import time
    
    # Start all machines
    sinex_machine.start()
    sinex_rollback_test.start()
    sinex_resource_test.start()
    
    # Wait for machines to boot
    sinex_machine.wait_for_unit("multi-user.target")
    sinex_rollback_test.wait_for_unit("multi-user.target")
    sinex_resource_test.wait_for_unit("multi-user.target")
    
    print("=== Test 1: Standard Pre-Flight Verification Deployment ===")
    
    # Test 1.1: Verify PostgreSQL is running
    sinex_machine.wait_for_unit("postgresql.service")
    sinex_machine.succeed("systemctl is-active postgresql")
    
    # Test 1.2: Run pre-flight verification manually
    print("Running manual pre-flight verification...")
    result = sinex_machine.succeed("sinex-preflight verify --output json --timeout 120")
    verification_report = json.loads(result)
    
    assert verification_report["overall_status"] == "PASS", f"Pre-flight verification failed: {verification_report}"
    print(f"✓ Pre-flight verification passed with phases: {list(verification_report['phases'].keys())}")
    
    # Test 1.3: Start Sinex services (should trigger pre-flight verification)
    print("Starting Sinex services with pre-flight verification...")
    sinex_machine.succeed("systemctl start sinex-preflight.service")
    sinex_machine.wait_for_unit("sinex-preflight.service")
    
    # Start collector (depends on pre-flight verification)
    sinex_machine.succeed("systemctl start sinex-ingestd.service")
    sinex_machine.wait_for_unit("sinex-ingestd.service")
    
    # Start promotion worker
    sinex_machine.succeed("systemctl start sinex-gateway.service")
    sinex_machine.wait_for_unit("sinex-gateway.service")
    
    # Test 1.4: Verify services are healthy
    print("Verifying service health...")
    sinex_machine.succeed("systemctl is-active sinex-ingestd")
    sinex_machine.succeed("systemctl is-active sinex-gateway")
    
    # Test 1.5: Test database connectivity and heartbeats
    print("Testing database operations...")
    sinex_machine.succeed("""
        export DATABASE_URL="postgresql://sinex@localhost:5432/sinex"
        psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM component_heartbeats WHERE component_name = 'unified-collector'"
    """)
    
    # Test 1.6: Test event ingestion
    print("Testing event ingestion...")
    sinex_machine.succeed("mkdir -p /tmp/sinex-test")
    sinex_machine.succeed("echo 'test content' > /tmp/sinex-test/test-file.txt")
    
    # Wait for event to be processed
    time.sleep(5)
    
    # Verify event was captured
    sinex_machine.succeed("""
        export DATABASE_URL="postgresql://sinex@localhost:5432/sinex"
        psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM core.events WHERE source LIKE '%filesystem%'" | grep -q '[1-9]'
    """)
    
    print("✓ Standard deployment test passed")
    
    print("=== Test 2: Coordinated Update with Pre-Flight Verification ===")
    
    # Test 2.1: Run coordinated update
    print("Testing coordinated update process...")
    sinex_machine.succeed("systemctl start sinex-update.service")
    sinex_machine.wait_for_unit("sinex-update.service")
    
    # Test 2.2: Verify services restarted successfully
    sinex_machine.succeed("systemctl is-active sinex-ingestd")
    sinex_machine.succeed("systemctl is-active sinex-gateway")
    
    # Test 2.3: Verify update was recorded in database
    sinex_machine.succeed("""
        export DATABASE_URL="postgresql://sinex@localhost:5432/sinex"
        psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM component_heartbeats WHERE component_name = 'sinex-deployment' AND status = 'success'" | grep -q '[1-9]'
    """)
    
    print("✓ Coordinated update test passed")
    
    print("=== Test 3: Pre-Flight Verification Failure and Rollback ===")
    
    # Test 3.1: Test with intentionally broken configuration
    print("Testing pre-flight verification failure handling...")
    sinex_rollback_test.wait_for_unit("multi-user.target")
    
    # Try to start pre-flight verification (should fail due to bad database port)
    result = sinex_rollback_test.fail("systemctl start sinex-preflight.service")
    print("✓ Pre-flight verification correctly failed with bad configuration")
    
    # Test 3.2: Verify collector doesn't start without pre-flight verification
    result = sinex_rollback_test.fail("systemctl start sinex-ingestd.service")
    print("✓ Collector correctly refused to start without pre-flight verification")
    
    print("=== Test 4: Resource Constraint Testing ===")
    
    # Test 4.1: Test resource verification with constraints
    print("Testing resource verification with constraints...")
    sinex_resource_test.wait_for_unit("multi-user.target")
    
    # Run resource check specifically
    result = sinex_resource_test.succeed("sinex-preflight resource-check --output json")
    resource_report = json.loads(result)
    
    print(f"Resource check status: {resource_report['status']}")
    print(f"Available memory: {resource_report['details']['memory']['available_gb']} GB")
    
    # Should pass or warn, but not fail completely
    assert resource_report["status"] in ["PASS", "WARNING"], f"Resource check failed unexpectedly: {resource_report}"
    
    print("✓ Resource constraint test passed")
    
    print("=== Test 5: Verification Phases Testing ===")
    
    # Test 5.1: Test individual verification phases
    phases = ["database", "extensions", "migrations", "resources", "configuration", "services"]
    
    for phase in phases:
        print(f"Testing {phase} phase...")
        # Test with only this phase (skip others)
        skip_args = " ".join([f"--skip {p}" for p in phases if p != phase])
        result = sinex_machine.succeed(f"sinex-preflight verify {skip_args} --output json --timeout 60")
        phase_report = json.loads(result)
        
        assert phase in phase_report["phases"], f"Phase {phase} not found in report"
        phase_status = phase_report["phases"][phase]["status"]
        assert phase_status in ["PASS", "WARNING"], f"Phase {phase} failed: {phase_status}"
        
        print(f"✓ {phase} phase: {phase_status}")
    
    print("=== Test 6: Concurrent Verification Testing ===")
    
    # Test 6.1: Run multiple verifications concurrently
    print("Testing concurrent verification runs...")
    sinex_machine.succeed("""
        for i in {1..3}; do
            sinex-preflight verify --timeout 60 --output json > /tmp/verify_$i.json &
        done
        wait
    """)
    
    # Check all succeeded
    for i in range(1, 4):
        sinex_machine.succeed(f"test -f /tmp/verify_{i}.json")
        result = sinex_machine.succeed(f"cat /tmp/verify_{i}.json")
        report = json.loads(result)
        assert report["overall_status"] == "PASS", f"Concurrent verification {i} failed"
    
    print("✓ Concurrent verification test passed")
    
    print("=== Test 7: Performance and Monitoring ===")
    
    # Test 7.1: Verify performance metrics are collected
    print("Testing performance monitoring...")
    result = sinex_machine.succeed("sinex-preflight verify --output json --timeout 60")
    perf_report = json.loads(result)
    
    assert "duration_ms" in perf_report, "Should report total duration"
    assert "system_info" in perf_report, "Should include system information"
    
    # Check individual phase timings
    for phase_name, phase_data in perf_report["phases"].items():
        assert "duration_ms" in phase_data, f"Phase {phase_name} should report duration"
        assert phase_data["duration_ms"] > 0, f"Phase {phase_name} should have positive duration"
    
    print(f"✓ Performance monitoring test passed - total duration: {perf_report['duration_ms']}ms")
    
    print("=== Test 8: Configuration Management ===")
    
    # Test 8.1: Test configuration file handling
    print("Testing configuration file handling...")
    sinex_machine.succeed("""
        cat > /tmp/test-preflight-config.toml << 'EOF'
        [verification]
        timeout = 90
        skip_phases = ["resources"]
        
        [database]
        timeout_seconds = 20
        EOF
    """)
    
    result = sinex_machine.succeed("SINEX_CONFIG=/tmp/test-preflight-config.toml sinex-preflight verify --output json")
    config_report = json.loads(result)
    
    # Should have skipped resources phase
    assert "resources" not in config_report["phases"], "Resources phase should be skipped per config"
    
    print("✓ Configuration management test passed")
    
    print("=== Test 9: Error Recovery and Diagnostics ===")
    
    # Test 9.1: Test error reporting
    print("Testing error reporting and diagnostics...")
    
    # Test with invalid database URL
    result = sinex_machine.fail("""
        DATABASE_URL="postgresql://invalid:5432/nonexistent" sinex-preflight verify --output json --timeout 30
    """)
    print("✓ Error handling test passed - invalid database correctly rejected")
    
    # Test 9.2: Test diagnostic information
    result = sinex_machine.succeed("sinex-preflight report --detailed --output json")
    diag_report = json.loads(result)
    
    assert "recent_verifications" in diag_report or "verification_count" in diag_report, "Should provide diagnostic information"
    
    print("✓ Diagnostics test passed")
    
    print("=== All Tests Completed Successfully ===")
    
    # Final verification - ensure all services are still healthy
    sinex_machine.succeed("systemctl is-active sinex-ingestd")
    sinex_machine.succeed("systemctl is-active sinex-gateway")
    sinex_machine.succeed("systemctl is-active postgresql")
    
    print("✓ Final health check passed - all services remain active")
    
    # Clean up test files
    sinex_machine.succeed("rm -rf /tmp/sinex-test /tmp/verify_*.json /tmp/test-preflight-config.toml")
    
    print("🎉 Sinex Pre-Flight Verification deployment test completed successfully!")
  '';
}