# mTLS Enforcement E2E Test
# Tests gateway mTLS client certificate verification
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

  # TLS fixtures directory
  tlsFixtures = ./tls-fixtures;

  sinexPackage = if sinex != null then sinex else sinex-ingestd;
  sinexCliPackage = sinexCli;
in
pkgs.testers.nixosTest {
  name = "sinex-mtls-enforcement";

  skipLint = true;

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    # Override gateway configuration to enable mTLS
    services.sinex.core.gateway = {
      enable = true;
      requireClientTLS = true;
      listenAddress = "0.0.0.0:9999";
      tlsCertFile = "${tlsFixtures}/server-cert.pem";
      tlsKeyFile = "${tlsFixtures}/server-key.pem";
      tlsClientCAFile = "${tlsFixtures}/ca-cert.pem";
    };

    # Additional packages for testing
    environment.systemPackages = with pkgs; [
      openssl
      curl
      jq
    ];

    # Copy TLS fixtures to the VM
    environment.etc = {
      "sinex-test/ca-cert.pem".source = "${tlsFixtures}/ca-cert.pem";
      "sinex-test/client-cert.pem".source = "${tlsFixtures}/client-cert.pem";
      "sinex-test/client-key.pem".source = "${tlsFixtures}/client-key.pem";
      "sinex-test/expired-client-cert.pem".source = "${tlsFixtures}/expired-client-cert.pem";
      "sinex-test/expired-client-key.pem".source = "${tlsFixtures}/expired-client-key.pem";
    };
  };

  testScript = ''
    import json
    import time

    start_all()

    with subtest("System initialization"):
        machine.wait_for_unit("multi-user.target")
        machine.wait_for_unit("postgresql.service", timeout=60)
        machine.wait_for_unit("sinex-schema-apply.service", timeout=60)
        machine.wait_for_unit("sinex-ingestd.service", timeout=60)
        machine.wait_for_unit("sinex-gateway.service", timeout=60)
        machine.wait_for_open_port(9999, timeout=30)
        print("✓ All services started")

    with subtest("mTLS: Request without client cert fails"):
        # Attempt connection without client certificate
        # Should fail with TLS handshake error
        result = machine.fail(
            "curl -v --cacert /etc/sinex-test/ca-cert.pem "
            "https://localhost:9999/health 2>&1"
        )
        print("✓ Request without client cert rejected (expected)")
        print(f"Error output: {result}")

    with subtest("mTLS: Request with valid client cert succeeds"):
        # Connection with valid client certificate
        result = machine.succeed(
            "curl --cert /etc/sinex-test/client-cert.pem "
            "--key /etc/sinex-test/client-key.pem "
            "--cacert /etc/sinex-test/ca-cert.pem "
            "https://localhost:9999/health"
        )
        print(f"✓ Request with valid client cert succeeded")
        print(f"Response: {result}")

        # Verify response is valid JSON
        health_status = json.loads(result)
        assert "status" in health_status or "jsonrpc" in health_status, "Invalid health response"
        print(f"✓ Health check response valid")

    with subtest("mTLS: Request with expired client cert fails"):
        # Attempt connection with expired client certificate
        # Should fail during certificate verification
        result = machine.fail(
            "curl -v --cert /etc/sinex-test/expired-client-cert.pem "
            "--key /etc/sinex-test/expired-client-key.pem "
            "--cacert /etc/sinex-test/ca-cert.pem "
            "https://localhost:9999/health 2>&1"
        )
        print("✓ Request with expired client cert rejected (expected)")
        print(f"Error output: {result}")

    with subtest("mTLS: Full JSON-RPC request with valid cert"):
        # Test a full JSON-RPC request with valid authentication
        rpc_request = {
            "jsonrpc": "2.0",
            "method": "ping",
            "params": {},
            "id": 1
        }

        # Create JSON file
        machine.succeed(f"cat > /tmp/rpc_request.json << 'EOF'\n{json.dumps(rpc_request)}\nEOF")

        # Send JSON-RPC request
        result = machine.succeed(
            "curl -X POST "
            "--cert /etc/sinex-test/client-cert.pem "
            "--key /etc/sinex-test/client-key.pem "
            "--cacert /etc/sinex-test/ca-cert.pem "
            "-H 'Content-Type: application/json' "
            "-H 'Authorization: Bearer test-admin-token:admin' "
            "-d @/tmp/rpc_request.json "
            "https://localhost:9999/rpc"
        )

        print(f"✓ JSON-RPC request with mTLS succeeded")
        print(f"Response: {result}")

        # Verify response
        rpc_response = json.loads(result)
        assert rpc_response.get("jsonrpc") == "2.0", "Invalid JSON-RPC response"
        assert "result" in rpc_response or "error" in rpc_response, "Malformed RPC response"
        print(f"✓ JSON-RPC response valid")

    with subtest("mTLS: Verify certificate details"):
        # Inspect the server certificate
        result = machine.succeed(
            "openssl s_client -connect localhost:9999 "
            "-cert /etc/sinex-test/client-cert.pem "
            "-key /etc/sinex-test/client-key.pem "
            "-CAfile /etc/sinex-test/ca-cert.pem "
            "< /dev/null 2>&1 | openssl x509 -noout -subject -issuer"
        )
        print(f"✓ Server certificate verified:")
        print(result)

        # Verify client certificate
        client_cert_info = machine.succeed(
            "openssl x509 -in /etc/sinex-test/client-cert.pem -noout -subject -dates"
        )
        print(f"✓ Client certificate info:")
        print(client_cert_info)

    with subtest("mTLS: Connection attempt with wrong CA"):
        # Try to connect with a different CA (should fail)
        # First, generate a rogue CA and client cert
        machine.succeed("""
            cd /tmp
            openssl genrsa -out rogue-ca-key.pem 2048 2>/dev/null
            openssl req -new -x509 -key rogue-ca-key.pem -out rogue-ca-cert.pem -days 1 -subj "/CN=Rogue CA" 2>/dev/null
            openssl genrsa -out rogue-client-key.pem 2048 2>/dev/null
            openssl req -new -key rogue-client-key.pem -out rogue-client-csr.pem -subj "/CN=rogue-client" 2>/dev/null
            openssl x509 -req -in rogue-client-csr.pem -CA rogue-ca-cert.pem -CAkey rogue-ca-key.pem \
                -CAcreateserial -out rogue-client-cert.pem -days 1 2>/dev/null
        """)

        # Attempt connection with rogue client cert
        result = machine.fail(
            "curl -v --cert /tmp/rogue-client-cert.pem "
            "--key /tmp/rogue-client-key.pem "
            "--cacert /tmp/rogue-ca-cert.pem "
            "https://localhost:9999/health 2>&1"
        )
        print("✓ Request with untrusted CA rejected (expected)")
        print(f"Error snippet: {result[:200]}")

    with subtest("mTLS: Gateway logs show mTLS enforcement"):
        # Check gateway logs for mTLS-related messages
        logs = machine.succeed("journalctl -u sinex-gateway.service -n 100 --no-pager")
        print("Gateway logs (last 100 lines):")
        print(logs)

        # Verify gateway started with TLS
        assert "RPC server listening on TLS" in logs or "TLS" in logs, "Gateway not running in TLS mode"
        print("✓ Gateway logs confirm TLS mode")

    print("\n✅ mTLS enforcement test completed successfully!")
  '';
}
