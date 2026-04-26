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
  sinexPackage = if sinex != null then sinex else sinex-ingestd;
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
        browser = {
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

    system.activationScripts.sinexTargetHistoryFixture = ''
      mkdir -p /home/test/.local/share/atuin
      mkdir -p /home/test/.local/share/fish
      mkdir -p /home/test/.local/share/qutebrowser/webengine

      cat > /home/test/.zsh_history <<'EOF'
: 1700100000:0;echo node_matrix_zsh_fixture
EOF
      cat > /home/test/.bash_history <<'EOF'
echo node_matrix_bash_fixture
EOF

      rm -f /home/test/.local/share/atuin/history.db
      ${pkgs.sqlite}/bin/sqlite3 /home/test/.local/share/atuin/history.db <<'SQL'
CREATE TABLE history (
  id TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  duration INTEGER NOT NULL,
  exit INTEGER NOT NULL,
  command TEXT NOT NULL,
  cwd TEXT NOT NULL,
  session TEXT NOT NULL,
  hostname TEXT NOT NULL,
  deleted_at INTEGER
);
INSERT INTO history (id, timestamp, duration, exit, command, cwd, session, hostname, deleted_at)
VALUES
  ('node-matrix-atuin-1', 1700100000000000000, 50000000, 0, 'echo node_matrix_atuin_fixture', '/home/test', 'node-matrix', 'sinex-vm', NULL);
SQL

      rm -f /home/test/.local/share/fish/fish_history
      ${pkgs.sqlite}/bin/sqlite3 /home/test/.local/share/fish/fish_history <<'SQL'
CREATE TABLE history (
  command TEXT NOT NULL,
  "when" INTEGER
);
INSERT INTO history (command, "when")
VALUES ('echo node_matrix_fish_fixture', 1700100000);
SQL

      rm -f /home/test/.local/share/qutebrowser/history.sqlite
      ${pkgs.sqlite}/bin/sqlite3 /home/test/.local/share/qutebrowser/history.sqlite <<'SQL'
CREATE TABLE History (
  url TEXT NOT NULL,
  title TEXT NOT NULL,
  atime INTEGER NOT NULL,
  redirect INTEGER NOT NULL DEFAULT 0
);
INSERT INTO History (url, title, atime, redirect)
VALUES ('https://example.com/node-matrix-qute', 'Node Matrix Qutebrowser', 1700100000, 0);
SQL

      rm -f /home/test/.local/share/qutebrowser/webengine/History
      ${pkgs.sqlite}/bin/sqlite3 /home/test/.local/share/qutebrowser/webengine/History <<'SQL'
CREATE TABLE urls (
  id INTEGER PRIMARY KEY,
  url LONGVARCHAR,
  title LONGVARCHAR
);
CREATE TABLE visits (
  id INTEGER PRIMARY KEY,
  url INTEGER NOT NULL,
  visit_time INTEGER NOT NULL,
  external_referrer_url LONGVARCHAR,
  transition INTEGER DEFAULT 0 NOT NULL,
  visit_duration INTEGER DEFAULT 0 NOT NULL
);
INSERT INTO urls (id, url, title)
VALUES (1, 'https://example.com/node-matrix-chromium', 'Node Matrix Chromium');
INSERT INTO visits (id, url, visit_time, external_referrer_url, transition, visit_duration)
VALUES (1, 1, 13344473601000000, NULL, 805306368, 1000000);
SQL

      chown -R test:users /home/test/.zsh_history /home/test/.bash_history /home/test/.local/share/atuin /home/test/.local/share/fish /home/test/.local/share/qutebrowser
      chmod 0644 /home/test/.zsh_history /home/test/.bash_history /home/test/.local/share/atuin/history.db /home/test/.local/share/fish/fish_history /home/test/.local/share/qutebrowser/history.sqlite /home/test/.local/share/qutebrowser/webengine/History
    '';

    # VM tests only need process liveness here. Keep browser aligned with the
    # filesystem VM override from common/test-base.nix so wait_for_unit does not
    # depend on sd_notify readiness timing.
    systemd.services.sinex-browser-1.serviceConfig.Type = lib.mkForce "simple";
    systemd.services.sinex-browser-1.serviceConfig.TimeoutStartSec = lib.mkForce "infinity";
  };

  testScript = ''
    import shlex
    import re

    def collect_unit_logs(units, output_dir="/tmp/sinex-vm-failure-logs"):
        machine.succeed(f"mkdir -p {output_dir}")
        for unit in units:
            safe_name = re.sub(r"[^A-Za-z0-9_.-]", "_", unit)
            machine.execute(
                f"journalctl -u {unit} -n 250 --no-pager > {output_dir}/{safe_name}.log 2>&1 || true"
            )
        listing = machine.succeed(f"ls -1 {output_dir} 2>/dev/null || true")
        print(f"Captured service logs under {output_dir}:\n{listing}")

    def assert_no_failed_sinex_units():
        failed = machine.succeed(
            "systemctl list-units 'sinex-*.service' --state=failed --no-legend --plain 2>/dev/null || true"
        ).strip()
        if not failed:
            return
        units = []
        for line in failed.splitlines():
            parts = line.split()
            if parts:
                units.append(parts[0])
        collect_unit_logs(units)
        raise AssertionError(f"Failed sinex units detected:\n{failed}")

    machine.start()
    machine.wait_for_unit("multi-user.target")

    # Core hubs
    for unit in ["postgresql.service", "nats.service", "sinex-ingestd.service", "sinex-gateway.service"]:
        machine.wait_for_unit(unit, timeout=120)
        machine.succeed(f"systemctl is-active {unit}")

    with subtest("Deployment readiness ordering"):
        for unit in ["sinex-schema-apply.service", "sinex-blob-init.service"]:
            machine.wait_for_unit(unit)
        machine.succeed("systemctl show -p Result --value sinex-schema-apply.service | grep '^success$'")
        machine.succeed("systemctl show -p Result --value sinex-blob-init.service | grep '^success$'")
        machine.succeed("test -s /etc/sinex/gateway-admin-token")
        machine.succeed("su - postgres -c 'psql -d sinex -At -c \"SELECT 1\"' | grep '^1$'")
        machine.succeed("su - postgres -c \"psql -d sinex_dev -At -c \\\"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'events'\\\"\" | grep '^1$'")

    terminal_source_units = [
        "sinex-source@terminal.atuin-history.service",
        "sinex-source@terminal.bash-history.service",
        "sinex-source@terminal.fish-history.service",
        "sinex-source@terminal.zsh-history.service",
    ]

    # Event source nodes
    nodes = [
        "sinex-filesystem-1.service",
        "sinex-filesystem-2.service",
        "sinex-browser-1.service",
        "sinex-desktop-1.service",
        "sinex-system-1.service"
    ] + terminal_source_units
    for unit in nodes:
        machine.wait_for_unit(unit, timeout=120)
        machine.succeed(f"systemctl is-active {unit}")

    machine.wait_for_unit("sinex-document-scan.timer", timeout=120)
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
        machine.wait_for_unit(unit, timeout=120)
        machine.succeed(f"systemctl is-active {unit}")

    with subtest("Target-user bridge access"):
        for unit in [
            "sinex-terminal-target-access.service",
            "sinex-browser-target-access.service",
            "sinex-desktop-target-access.service",
        ]:
            machine.succeed(f"systemctl start {unit}")
            machine.wait_for_unit(unit, timeout=120)
            machine.succeed(f"systemctl show -p Result --value {unit} | grep '^success$'")
        for path in [
            "/home/test/.zsh_history",
            "/home/test/.bash_history",
            "/home/test/.local/share/atuin/history.db",
            "/home/test/.local/share/fish/fish_history",
            "/home/test/.local/share/qutebrowser/history.sqlite",
            "/home/test/.local/share/qutebrowser/webengine/History",
        ]:
            machine.succeed(f"su -s /bin/sh -c 'test -r {path}' sinex")
        machine.succeed("su -s /bin/sh -c 'test -x /run/user/1000' sinex")

    with subtest("Runtime proof for deployable surfaces"):
        machine.succeed("su - test -c 'echo node-matrix > /var/lib/sinex/watched/node-matrix.txt'")
        machine.succeed("su - test -c 'echo node_matrix_cmd >> /home/test/.zsh_history'")
        machine.succeed("su - test -c 'echo node_matrix_bash >> /home/test/.bash_history'")
        desktop_scan_config = '{"activitywatch_db_path":"/home/test/.local/share/activitywatch/aw-server-rust/sqlite.db","clipboard_enabled":false,"window_manager_enabled":false,"window_manager_type":"Hyprland","clipboard_poll_interval_secs":1,"require_hyprland":false}'
        desktop_scan_command = (
            "set -euo pipefail; "
            "mkdir -p /var/lib/sinex/desktop-history-proof; "
            "scan_until=$(date -u +%Y-%m-%dT%H:%M:%SZ); "
            "DATABASE_URL=postgresql://sinex@127.0.0.1:5432/sinex_dev "
            "SINEX_NATS_URL=nats://127.0.0.1:4222 "
            "${sinexPackage}/bin/sinex-desktop-ingestor "
            "--service-name sinex-desktop-history-proof "
            "--work-dir /var/lib/sinex/desktop-history-proof "
            f"--node-config {shlex.quote(desktop_scan_config)} "
            'scan --from none --until "$scan_until"'
        )
        machine.succeed(f"su -s /bin/sh -c {shlex.quote(desktop_scan_command)} sinex")
        machine.wait_until_succeeds(
            "su - postgres -c \"psql -d sinex_dev -tAc \\\"SELECT COUNT(*) FROM core.events WHERE source = 'activitywatch' AND event_type = 'window.active'\\\"\" | grep -Eq '^[1-9][0-9]*$'",
            timeout=60,
        )
        machine.wait_until_succeeds(
            "sinexctl --insecure verify --document-smoke --source-proof --historical-proof",
            timeout=120,
        )
        assert_no_failed_sinex_units()

    with subtest("Managed service restart proof"):
        restart_units = [
            "sinex-ingestd.service",
            "sinex-gateway.service",
            "sinex-browser-1.service",
        ] + terminal_source_units
        for unit in restart_units:
            machine.systemctl(f"restart {unit}")
            machine.wait_for_unit(unit, timeout=60)
            machine.succeed(f"systemctl is-active {unit}")
        machine.succeed("sinexctl --insecure verify")
        assert_no_failed_sinex_units()

    with subtest("Managed node units are generated"):
        unit_files = machine.succeed("systemctl list-unit-files 'sinex-*.service' --no-legend --plain")
        for unit in [
            "sinex-filesystem-1.service",
            "sinex-filesystem-2.service",
            "sinex-browser-1.service",
            "sinex-desktop-1.service",
            "sinex-system-1.service",
        ] + terminal_source_units:
            assert unit in unit_files
  '';
}
