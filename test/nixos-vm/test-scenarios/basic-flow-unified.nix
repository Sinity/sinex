# Basic E2E flow test using unified test abstractions
{ pkgs, sinex-collector, sinex-promo-worker, pg_jsonschema, sinex-test-bridge, ... }:

let
  inherit (pkgs) lib;
  
  # Python test bridge client
  testBridgeClient = pkgs.writeText "test_bridge_client.py" (builtins.readFile ../common/test_bridge_client.py);
  
  # Test scenario definition (could be loaded from YAML/JSON)
  testScenario = {
    name = "basic-event-flow";
    description = "Test basic event generation and verification using unified abstractions";
    steps = [
      {
        name = "Generate filesystem events";
        type = "generate_events";
        source = "filesystem";
        event_type = "file.created";
        count = 10;
        payload_template = {
          path = "/home/test/watched/test_{{index}}.txt";
          action = "created";
        };
        interval = 100; # ms
      }
      {
        name = "Wait for events";
        type = "wait_for";
        condition = "event_count";
        count = 10;
        source = "filesystem";
        timeout = 10;
      }
      {
        name = "Generate shell history events";
        type = "generate_events";
        source = "shell";
        event_type = "command.executed";
        count = 5;
        payload_template = {
          command = "test-command-{{index}}";
          exit_code = 0;
        };
      }
      {
        name = "Wait for all events";
        type = "wait_for";
        condition = "event_count";
        count = 15;
        timeout = 10;
      }
    ];
  };
in
pkgs.nixosTest {
  name = "sinex-basic-flow-unified";
  
  skipLint = true;

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [ 
      (import ../common/test-base.nix { 
        inherit config pkgs lib sinex-collector sinex-promo-worker pg_jsonschema; 
      })
    ];

    # Enable test bridge service
    systemd.services.sinex-test-bridge = {
      description = "Sinex Test Bridge Service";
      wantedBy = [ "multi-user.target" ];
      after = [ "postgresql.service" "sinex-unified-collector.service" ];
      
      serviceConfig = {
        ExecStart = "${sinex-test-bridge}/bin/sinex-test-bridge";
        Restart = "on-failure";
        RestartSec = "5s";
        Environment = [
          "DATABASE_URL=postgresql:///sinex?host=/run/postgresql"
          "REDIS_URL=redis://localhost:6379"
          "RUST_LOG=info"
        ];
      };
    };

    # Additional packages for test execution
    environment.systemPackages = with pkgs; [
      python3
      curl
      jq
    ];
  };

  testScript = ''
    import json
    import sys
    
    # Add test bridge client to Python path
    sys.path.insert(0, '/tmp')
    
    start_all()
    
    # Copy test bridge client
    machine.copy_from_host("${testBridgeClient}", "/tmp/test_bridge_client.py")
    
    # Import the client
    machine.execute("cd /tmp && python3 -c 'import test_bridge_client'")
    
    # Wait for services
    with subtest("System initialization"):
        machine.wait_for_unit("multi-user.target")
        machine.wait_for_unit("postgresql.service", timeout=60)
        machine.wait_for_unit("sinex-unified-collector.service", timeout=60)
        machine.wait_for_unit("sinex-test-bridge.service", timeout=60)
        
        # Wait for bridge to be ready
        machine.wait_until_succeeds(
            "curl -s http://localhost:8899/health | jq -r .status | grep -q healthy",
            timeout=30
        )
    
    # Execute test scenario using unified abstractions
    test_scenario = ${builtins.toJSON testScenario}
    
    with subtest("Execute unified test scenario"):
        # Create test script that uses the bridge client
        test_script = """
import sys
sys.path.insert(0, '/tmp')
from test_bridge_client import TestBridgeClient
import json

# Initialize client
client = TestBridgeClient()

# Load scenario
scenario = json.loads('${builtins.toJSON testScenario}')

print(f"Running scenario: {scenario['name']}")
print(f"Description: {scenario.get('description', 'N/A')}")

# Execute each step
for i, step in enumerate(scenario['steps']):
    print(f"\\nStep {i+1}: {step['name']}")
    
    try:
        result = client.run_scenario_step(step)
        print(f"✓ Step completed successfully")
        if result:
            print(f"  Result: {result}")
    except Exception as e:
        print(f"✗ Step failed: {e}")
        raise

# Final verification
print("\\nFinal verification...")
total_count = client.count_events()
print(f"Total events in database: {total_count}")

# Query recent events
recent_events = client.query_events(limit=5)
print("\\nRecent events:")
for event in recent_events:
    print(f"  - {event.id}: {event.source}/{event.event_type}")

# Verify minimum event count
client.assert_event_count(15, comparison="greater_than_or_equal")
print("\\n✓ All assertions passed!")
"""
        
        machine.succeed(f"cat > /tmp/test_scenario.py << 'EOF'\n{test_script}\nEOF")
        
        # Run the test scenario
        output = machine.succeed("cd /tmp && python3 test_scenario.py")
        print(output)
    
    # Traditional VM test operations can still be mixed in
    with subtest("Additional VM-specific tests"):
        # Test service resilience
        machine.systemctl("restart sinex-unified-collector")
        machine.wait_for_unit("sinex-unified-collector.service")
        
        # Verify service recovered
        machine.succeed("systemctl is-active sinex-unified-collector")
        
        # Use bridge to verify events still being processed
        machine.succeed("""
cd /tmp && python3 -c "
from test_bridge_client import TestBridgeClient
client = TestBridgeClient()
# Generate event after restart
event_id = client.create_event('test', 'service.restarted', {'service': 'sinex-unified-collector'})
print(f'Created event: {event_id}')
# Verify it was stored
result = client.wait_for_events(16, timeout_seconds=5)
print(f'Events after restart: {result.actual_count}')
"
        """)
        
        print("✓ Service resilient to restarts")
    
    # Performance metrics from bridge
    with subtest("Collect performance metrics"):
        metrics = machine.succeed("""
cd /tmp && python3 -c "
from test_bridge_client import TestBridgeClient
import json
client = TestBridgeClient()

# Query database for metrics
rows = client.query_database(
    'SELECT COUNT(*) as total, MIN(ts_ingest) as first, MAX(ts_ingest) as last FROM core.events'
)
print(json.dumps(rows[0], indent=2))
"
        """)
        print(f"Performance metrics:\\n{metrics}")
    
    print("\\n✅ Unified test completed successfully!")
  '';
}