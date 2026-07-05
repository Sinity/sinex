# Replay lifecycle smoke test
# Verifies: plan → preview → approve → execute → completed
# Category: smoke
{ pkgs
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
        inherit config pkgs lib pg_jsonschema sinex sinexCli;
      })
    ];

    # Enable API (required for replay RPC)
    services.sinex.core.api.enable = true;

    # Enable filesystem source runtime to generate real events
    services.sinex.sources = {
      filesystem.enable = true;
      filesystem.watchPaths = lib.mkAfter [ "/var/lib/sinex/watched" ];
      terminal.enable = false;
      desktop.enable = false;
      system.enable = false;
    };
    services.sinex.automata = {
      enable = true;
      canonicalizer.enable = true;
      healthAggregator.enable = true;
      analyticsAutomaton.enable = true;
      sessionDetector.enable = true;
    };

    environment.systemPackages = with pkgs; [ jq ];
  };

  testScript = ''
    import json
    import re
    import time

    start_all()

    def wait_for_services():
        machine.wait_for_unit("multi-user.target")
        machine.wait_for_unit("postgresql.service", timeout=60)
        machine.wait_for_unit("sinexd.service", timeout=180)
        machine.fail("systemctl list-unit-files 'sinex-filesystem-*.service' 'sinex-*automaton.service' 'sinex-canonicalizer.service' --no-legend --plain | grep -v '^$'")
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
        output = sinexctl(f"{args} --format json")
        return parse_json_output(output)

    def parse_json_output(output):
        clean = re.sub(r"\x1b\[[0-9;]*m", "", output).strip()
        try:
            return json.loads(clean)
        except json.JSONDecodeError:
            pass

        lines = clean.splitlines()
        for start in range(len(lines)):
            candidate = "\n".join(lines[start:]).strip()
            if not candidate or candidate[0] not in "[{":
                continue
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue

        raise ValueError(f"no JSON payload found in output: {clean[-400:]}")

    def event_cards_from_result(result):
        if isinstance(result, dict):
            return result.get("payload", {}).get("cards", []) or result.get("events", [])
        if isinstance(result, list):
            return result
        return []

    def event_path(card):
        for key in ("path", "old_path", "new_path"):
            value = card.get("payload", {}).get(key) or card.get(key)
            if isinstance(value, dict):
                value = value.get("path") or value.get("display") or value.get("value")
            if isinstance(value, str):
                return value
        text = json.dumps(card, sort_keys=True)
        for candidate in ("replay_test_0.txt", "replay_test_1.txt"):
            if candidate in text:
                return candidate
        return None

    def payload_from_result(result):
        if isinstance(result, dict):
            return result.get("payload", result)
        return result

    def operation_from_result(result):
        payload = payload_from_result(result)
        if isinstance(payload, dict):
            return payload.get("operation", payload)
        return payload

    def preview_from_result(result):
        payload = payload_from_result(result)
        if isinstance(payload, dict):
            return payload.get("preview", payload)
        return payload

    def wait_for_query_event_count(source, minimum, timeout=60):
        deadline = time.time() + timeout
        last_result = None
        last_error = None

        while time.time() < deadline:
            try:
                result = sinexctl_json(f"events query --source {source}")
                events = event_cards_from_result(result)
                last_result = result
                if len(events) >= minimum:
                    return events
            except Exception as exc:
                last_error = repr(exc)
            time.sleep(1)

        raise Exception(
            f"Timed out waiting for >={minimum} events from {source}; "
            f"last_result={last_result!r}; last_error={last_error!r}"
        )

    # ── Initialize ───────────────────────────────────────────
    with subtest("System initialization"):
        wait_for_services()

    # ── Generate events ──────────────────────────────────────
    with subtest("Generate filesystem events"):
        machine.succeed("mkdir -p /var/lib/sinex/watched")
        machine.succeed(
            "echo 'replay smoke test 0' > /var/lib/sinex/watched/replay_test_0.txt"
        )
        # Wait for filesystem create+modify observations to be ingested.
        wait_for_query_event_count("fs-watcher", 2, timeout=60)

        machine.succeed(
            "echo 'replay smoke test 1' > /var/lib/sinex/watched/replay_test_1.txt"
        )
        wait_for_query_event_count("fs-watcher", 4, timeout=60)

    # ── Replay lifecycle ─────────────────────────────────────
    with subtest("Full replay lifecycle"):
        # Plan
        plan_output = sinexctl("ops replay plan --source fs-watcher --since 1h --format json")
        plan = parse_json_output(plan_output)
        operation = operation_from_result(plan)
        op_id = operation.get("operation_id")
        assert op_id is not None, f"Failed to get operation_id from plan: {plan}"
        print(f"Replay plan created: {op_id}")

        # Preview
        preview_output = sinexctl(f"ops replay preview {op_id} --format json")
        preview = parse_json_output(preview_output)
        preview_payload = preview_from_result(preview)
        total = preview_payload.get("total_events", 0)
        assert total >= 4, f"Preview should find >=4 events, got {total}"
        print(f"Preview: {total} events in scope")

        # Approve
        sinexctl(f"ops replay approve {op_id}")
        print("Replay approved")

        # Execute
        sinexctl(f"ops replay execute {op_id}")

        # Wait for completion
        for attempt in range(60):
            status_output = sinexctl(f"ops replay status {op_id} --format json")
            status = parse_json_output(status_output)
            status_operation = operation_from_result(status)
            state = status_operation.get("state")
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
        events = parse_json_output(sinexctl("events query --source fs-watcher --format json"))
        event_list = event_cards_from_result(events)
        assert len(event_list) >= 2, \
            f"Should have at least one visible event per replayed file after replay, got {len(event_list)}"
        paths = {path for path in (event_path(card) for card in event_list) if path}
        assert any("replay_test_0.txt" in path for path in paths), \
            f"Replay output for replay_test_0.txt missing: {event_list!r}"
        assert any("replay_test_1.txt" in path for path in paths), \
            f"Replay output for replay_test_1.txt missing: {event_list!r}"
        print(f"Event count after replay: {len(event_list)} across paths {sorted(paths)}")
  '';
}
