# Replay lifecycle smoke test
# Verifies: plan → preview → approve → execute → completed
# Category: smoke
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
  name = "replay-smoke";

  skipLint = true;

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    # Enable gateway (required for replay RPC)
    services.sinex.gateway.enable = true;

    # Enable filesystem node to generate real events
    services.sinex.nodes = {
      filesystem.enable = true;
      filesystem.watchPaths = lib.mkAfter [ "/var/lib/sinex/watched" ];
      terminal.enable = false;
      desktop.enable = false;
      system.enable = false;
    };

    environment.systemPackages = with pkgs; [ jq ];
  };

  testScript = ''
    import json
    import time

    start_all()

    def wait_for_services():
        machine.wait_for_unit("multi-user.target")
        machine.wait_for_unit("postgresql.service", timeout=60)
        machine.wait_for_unit("sinex-gateway.service", timeout=60)
        machine.wait_for_unit("sinex-ingestd.service", timeout=60)
        machine.wait_until_succeeds(
            "curl -k -s https://127.0.0.1:9999/health",
            timeout=30
        )

    def sinexctl(args, check=True):
        cmd = f"sinexctl --insecure {args}"
        if check:
            return machine.succeed(cmd)
        else:
            return machine.execute(cmd)

    def sinexctl_json(args):
        output = sinexctl(f"{args} -f json")
        lines = output.strip().split('\n')
        if len(lines) == 1:
            return json.loads(lines[0])
        else:
            return [json.loads(line) for line in lines if line.strip()]

    # ── Initialize ───────────────────────────────────────────
    with subtest("System initialization"):
        wait_for_services()

    # ── Generate events ──────────────────────────────────────
    with subtest("Generate filesystem events"):
        machine.succeed("mkdir -p /var/lib/sinex/watched")
        for i in range(5):
            machine.succeed(
                f"echo 'replay smoke test {i}' > /var/lib/sinex/watched/replay_test_{i}.txt"
            )
        # Wait for events to be ingested
        machine.wait_until_succeeds(
            "test $(sinexctl --insecure query --source fs-ingestor -f json 2>/dev/null | jq length) -ge 5",
            timeout=60
        )

    # ── Replay lifecycle ─────────────────────────────────────
    with subtest("Full replay lifecycle"):
        # Plan
        plan_output = sinexctl("replay plan --node fs-ingestor --since 1h -f json")
        plan = json.loads(plan_output.strip().split('\n')[-1])
        op_id = plan.get("operation_id", plan.get("operation", {}).get("operation_id"))
        assert op_id is not None, f"Failed to get operation_id from plan: {plan}"
        print(f"Replay plan created: {op_id}")

        # Preview
        preview_output = sinexctl(f"replay preview {op_id} -f json")
        preview = json.loads(preview_output.strip().split('\n')[-1])
        total = preview.get("total_events", preview.get("preview", {}).get("total_events", 0))
        assert total >= 5, f"Preview should find >=5 events, got {total}"
        print(f"Preview: {total} events in scope")

        # Approve
        sinexctl(f"replay approve {op_id}")
        print("Replay approved")

        # Execute
        sinexctl(f"replay execute {op_id}")

        # Wait for completion
        for attempt in range(60):
            status_output = sinexctl(f"replay status {op_id} -f json")
            status = json.loads(status_output.strip().split('\n')[-1])
            state = status.get("state", status.get("operation", {}).get("state"))
            if state == "Completed":
                print(f"Replay completed after {attempt + 1} polls")
                break
            elif state in ("Failed", "Cancelled"):
                raise Exception(f"Replay ended in {state}: {status}")
            time.sleep(1)
        else:
            raise Exception(f"Replay did not complete within 60s, last state: {state}")

    # ── Verify consistency ───────────────────────────────────
    with subtest("Event consistency after replay"):
        events = sinexctl("query --source fs-ingestor -f json")
        event_list = [json.loads(line) for line in events.strip().split('\n') if line.strip()]
        assert len(event_list) >= 5, \
            f"Should have >=5 events after replay, got {len(event_list)}"
        print(f"Event count after replay: {len(event_list)} (consistent)")
  '';
}
