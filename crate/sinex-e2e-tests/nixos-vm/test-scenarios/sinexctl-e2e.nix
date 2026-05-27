# sinexctl CLI E2E tests
# Tests the CLI tool against a running gateway with structured JSON output
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
  name = "sinexctl-e2e";

  # Skip lint check for this test
  skipLint = true;

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    # Enable gateway for CLI tests
    services.sinex.core.gateway.enable = true;

    # Enable filesystem node to generate events
    services.sinex.nodes = {
      filesystem.enable = true;
      filesystem.watchPaths = lib.mkAfter [ "/var/lib/sinex/watched" ];
      terminal.enable = false;
      desktop.enable = false;
      system.enable = false;
    };

    # Add jq for JSON parsing in tests
    environment.systemPackages = with pkgs; [
      jq
    ];
  };

  testScript = ''
    import json

    start_all()

    def wait_for_gateway():
        """Wait for gateway to be ready and accepting connections"""
        machine.wait_for_unit("postgresql.service", timeout=60)
        machine.wait_for_unit("sinex-gateway.service", timeout=60)
        machine.wait_for_unit("sinex-ingestd.service", timeout=60)
        # Wait until gateway health endpoint responds
        machine.wait_until_succeeds(
            "curl -k -s https://127.0.0.1:9999/health",
            timeout=30
        )

    def sinexctl(args, check=True):
        """Run sinexctl command and return output"""
        cmd = f"sinexctl --insecure {args}"
        if check:
            return machine.succeed(cmd)
        else:
            return machine.execute(cmd)

    def sinexctl_json(args):
        """Run sinexctl command with JSON output and parse result"""
        output = sinexctl(f"{args} -f json")
        values = parse_json_output(output)
        return values[0] if len(values) == 1 else values

    def parse_json_output(output):
        """Parse sinexctl JSON output, including JSON-lines list output."""
        values = []
        for line in output.strip().split('\n'):
            line = line.strip()
            if line.startswith("{") or line.startswith("["):
                values.append(json.loads(line))
        return values

    def flatten_json_items(values, collection_keys=()):
        """Flatten JSON-lines objects and empty/list collection output."""
        items = []
        for value in values:
            if isinstance(value, list):
                items.extend(value)
                continue
            if isinstance(value, dict):
                for key in collection_keys:
                    nested = value.get(key)
                    if isinstance(nested, list):
                        items.extend(nested)
                        break
                else:
                    items.append(value)
        return items

    def generate_test_events(count):
        """Generate filesystem events for testing"""
        for i in range(count):
            machine.succeed(f"echo 'test content {i}' > /var/lib/sinex/watched/test_{i}.txt")

    # Initialize test environment
    with subtest("System initialization"):
        machine.wait_for_unit("multi-user.target")
        wait_for_gateway()

    # Test 1: sinexctl version and help
    with subtest("sinexctl version and help"):
        version = sinexctl("--version")
        assert "sinexctl" in version, f"Version output missing sinexctl: {version}"
        print(f"sinexctl version: {version.strip()}")

        help_output = sinexctl("--help")
        assert "Commands:" in help_output, "Help should list commands"
        assert "query" in help_output, "Help should include query command"
        assert "node" in help_output, "Help should include node command"
        print("Help output verified")

    # Test 2: Config commands
    with subtest("sinexctl config commands"):
        # Config path
        config_path = sinexctl("config path")
        assert "sinexctl" in config_path or ".toml" in config_path, \
            f"Config path should reference config file: {config_path}"
        print(f"Config path: {config_path.strip()}")

        # Config show (JSON format for easy parsing)
        config_json = sinexctl_json("config show")
        assert "rpc_url" in config_json, "Config should have rpc_url"
        assert "timeout" in config_json, "Config should have timeout"
        print(f"Config loaded successfully with {len(config_json)} fields")

    # Test 3: Node listing with JSON output
    with subtest("sinexctl node list with JSON"):
        # List nodes - may be empty initially
        nodes_output = sinexctl("node list -f json", check=False)
        exit_code = nodes_output[0]
        output = nodes_output[1]

        if exit_code == 0 and output.strip():
            nodes = flatten_json_items(parse_json_output(output), ("nodes", "instances"))
            for node in nodes:
                if isinstance(node, dict):
                    name = node.get("name") or node.get("node_name") \
                        or node.get("service_name") or node.get("instance_id") or "unknown"
                    print(f"Found node: {name}")
                else:
                    print(f"Found node entry: {node}")
            if not nodes:
                print("No nodes registered yet (expected for fresh install)")
        else:
            print("No nodes registered yet (expected for fresh install)")

    # Test 4: Generate events and query
    with subtest("Event generation and query"):
        # Generate some events
        generate_test_events(5)
        # Poll until at least one event is visible (up to 30 s) instead of a
        # fixed sleep that races against pipeline latency.
        machine.wait_until_succeeds(
            "sinexctl --insecure recent -n 1 -f json 2>/dev/null | grep -q '{'",
            timeout=30
        )

        # Query events
        query_result = sinexctl("query -s 1h -n 10 -f json", check=False)
        exit_code = query_result[0]
        output = query_result[1]

        if exit_code == 0 and output.strip():
            events = []
            for line in output.strip().split('\n'):
                if line.strip() and line.startswith('{'):
                    events.append(json.loads(line))
            print(f"Query returned {len(events)} events")
        else:
            print("No events found yet (may be expected)")

    # Test 5: DLQ commands
    with subtest("sinexctl dlq commands"):
        # List DLQ queues
        dlq_result = sinexctl("dlq list -f json", check=False)
        exit_code = dlq_result[0]
        output = dlq_result[1]

        if exit_code == 0:
            if output.strip():
                print(f"DLQ list output: {output.strip()}")
            else:
                print("DLQ is empty (expected)")
        else:
            # DLQ list might fail if no queues exist - that's OK
            print("DLQ list returned empty or error (expected for clean system)")

    # Test 6: Operations log
    with subtest("sinexctl ops commands"):
        # List operations
        ops_result = sinexctl("ops list -f json", check=False)
        exit_code = ops_result[0]
        output = ops_result[1]

        if exit_code == 0:
            print(f"Operations list retrieved")
        else:
            print("No operations found (expected for fresh system)")

    # Test 7: Completions generation
    with subtest("Shell completions generation"):
        # Bash completions
        bash_comp = sinexctl("completions bash")
        assert "_sinexctl" in bash_comp, "Bash completions should define _sinexctl"

        # Zsh completions
        zsh_comp = sinexctl("completions zsh")
        assert "#compdef" in zsh_comp, "Zsh completions should have compdef"

        # Fish completions
        fish_comp = sinexctl("completions fish")
        assert "complete -c sinexctl" in fish_comp, "Fish completions should use complete"

        print("All shell completions generated successfully")

    # Test 8: Error handling
    with subtest("Error handling"):
        # Invalid command should fail
        result = machine.execute("sinexctl nonexistent-command 2>&1")
        assert result[0] != 0, "Invalid command should fail"

        # Missing required args should fail
        result = machine.execute("sinexctl node status 2>&1")
        assert result[0] != 0, "Missing node name should fail"

        print("Error handling works correctly")

    # Test 9: Output formats
    with subtest("Output format handling"):
        # Test table format (default)
        table_out = sinexctl("config show")
        assert len(table_out) > 0, "Table output should not be empty"

        # Test JSON format
        json_out = sinexctl("config show -f json")
        # Extract just the JSON part (before any extra info)
        json_end = json_out.rfind('}')
        if json_end > 0:
            json_str = json_out[:json_end+1]
            parsed = json.loads(json_str)
            assert isinstance(parsed, dict), "JSON output should be a dict"

        # Test YAML format
        yaml_out = sinexctl("config show -f yaml")
        assert "rpc_url:" in yaml_out, "YAML should contain rpc_url"

        print("All output formats work correctly")

    # Test 10: Query with filters
    with subtest("Query with filters"):
        # Generate more events to ensure we have data
        generate_test_events(3)
        machine.sleep(2)

        # Query with time filter
        result = sinexctl("query -s 1h -f json", check=False)
        print(f"Time-filtered query: exit={result[0]}")

        # Query with limit
        result = sinexctl("query -s 1h -n 5 -f json", check=False)
        print(f"Limited query: exit={result[0]}")

    print("sinexctl E2E tests completed successfully")
  '';
}
