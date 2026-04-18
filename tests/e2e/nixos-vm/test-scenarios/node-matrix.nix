# Node constellation coverage test for Sinex
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
  name = "sinex-node-matrix";

  nodes.machine = { config, pkgs, lib, ... }: {
    imports = [
      (import ../common/test-base.nix {
        inherit config pkgs lib sinex-ingestd sinex-gateway pg_jsonschema sinex sinexCli;
      })
    ];

    services.sinex = {
      nodes = {
        coordination.enable = lib.mkForce true;

        filesystem = {
          enable = lib.mkForce true;
          instances = lib.mkForce 2;
          watchPaths = lib.mkForce [ "/var/lib/sinex/watched" ];
        };
        terminal = {
          enable = lib.mkForce true;
          instances = lib.mkForce 1;
        };
        desktop = {
          enable = lib.mkForce true;
          instances = lib.mkForce 1;
        };
        system = {
          enable = lib.mkForce true;
          instances = lib.mkForce 1;
        };
        document = {
          enable = lib.mkForce true;
          allowedRoots = lib.mkForce [ "/home/test/Documents" ];
        };

        automata = {
          enable = lib.mkForce true;
          canonicalizer.enable = lib.mkForce true;
          healthAggregator.enable = lib.mkForce true;
          analyticsAutomaton.enable = lib.mkForce true;
          sessionDetector.enable = lib.mkForce true;
        };
      };
    };

    system.activationScripts.sinexActivitywatchFixture = ''
      mkdir -p /home/test/.local/share/activitywatch/aw-server-rust
      rm -f /home/test/.local/share/activitywatch/aw-server-rust/sqlite.db
      ${pkgs.sqlite}/bin/sqlite3 /home/test/.local/share/activitywatch/aw-server-rust/sqlite.db <<'SQL'
CREATE TABLE buckets (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);
CREATE TABLE events (
  bucketrow INTEGER NOT NULL,
  starttime INTEGER NOT NULL,
  endtime INTEGER NOT NULL,
  data TEXT,
  FOREIGN KEY(bucketrow) REFERENCES buckets(id)
);
INSERT INTO buckets (id, name) VALUES
  (1, 'aw-watcher-window_sinex-vm'),
  (2, 'aw-watcher-web_sinex-vm'),
  (3, 'aw-watcher-afk_sinex-vm');
INSERT INTO events (bucketrow, starttime, endtime, data) VALUES
  (1, 1000000000, 4000000000, '{"app":"kitty","title":"node-matrix"}'),
  (2, 5000000000, 9000000000, '{"app":"Firefox","title":"Docs","url":"https://example.com"}'),
  (3, 10000000000, 16000000000, '{"status":"afk"}');
SQL
      chown -R test:users /home/test/.local/share/activitywatch
    '';
  };

  testScript = ''
    machine.start()
    machine.wait_for_unit("multi-user.target")

    # Core hubs
    for unit in ["sinex-ingestd.service", "sinex-gateway.service", "nats.service"]:
        machine.wait_for_unit(unit)
        machine.succeed(f"systemctl is-active {unit}")

    # Event source nodes
    nodes = [
        "sinex-filesystem-1.service",
        "sinex-filesystem-2.service",
        "sinex-terminal-1.service",
        "sinex-desktop-1.service",
        "sinex-system-1.service"
    ]
    for unit in nodes:
        machine.wait_for_unit(unit)
        machine.succeed(f"systemctl is-active {unit}")

    machine.wait_for_unit("sinex-document-scan.timer")
    machine.succeed("systemctl is-active sinex-document-scan.timer")
    machine.succeed("test \"$(systemctl show -p LoadState --value sinex-document-scan.service)\" = loaded")

    # Automata
    automata = [
        "sinex-canonicalizer.service",
        "sinex-health-automaton.service",
        "sinex-analytics-automaton.service",
        "sinex-session-detector.service"
    ]
    for unit in automata:
        machine.wait_for_unit(unit)
        machine.succeed(f"systemctl is-active {unit}")

    with subtest("Runtime proof for deployable surfaces"):
        machine.succeed("su - test -c 'echo node-matrix > /var/lib/sinex/watched/node-matrix.txt'")
        machine.succeed("su - test -c 'echo node_matrix_cmd >> /home/test/.zsh_history'")
        machine.succeed("su - test -c 'echo node_matrix_bash >> /home/test/.bash_history'")
        machine.succeed(
            "sinexctl --insecure verify --gateway-smoke --automata-smoke --document-smoke --source-proof --historical-proof"
        )

    # Verify generated units metadata exposed via option
    generated = machine.succeed("nixos-option sinex._generatedUnits")
    assert "sinex-filesystem-1" in generated
    assert "sinex-terminal-1" in generated
    assert "sinex-document-scan" not in generated
  '';
}
