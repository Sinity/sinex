# The deployed stack on one host: PostgreSQL, NATS, sinexd, the journald
# capture and the session service, with a file change reaching the database
# through the session's NATS ingress, a source read from a private home
# directory, and a declared backfill.
self: {
  name = "sinex-stack";
  nodes.machine = { pkgs, ... }: {
    imports = [ self.nixosModules.default ];
    virtualisation = { memorySize = 3072; cores = 2; diskSize = 4096; };
    environment.etc."sinex-test/nats-daemon".text = "daemon secret: with @ and /";
    environment.etc."sinex-test/nats-session".text = "session-secret";
    environment.systemPackages = [ pkgs.curl ];
    users.users.alice = { isNormalUser = true; linger = true; };
    environment.etc."sinex-test/zsh_history".text = ''
      : 1700000000:0;echo hello
      : 1700000005:0;ls
    '';
    environment.etc."sinex-test/library_export.csv".text = ''
      Book Id,Title,Author,ISBN,ISBN13,My Rating,Average Rating,Date Read,Date Added,Bookshelves,Exclusive Shelf
      185,T,A,,,4,4.1,2020/01/02,2019/12/01,"x, y",read
    '';
    systemd.tmpfiles.rules = [
      "d /home/alice/notes 0755 alice users -"
      # A shell history only its owner can read.
      "C /home/alice/.zsh_history 0600 alice users - /etc/sinex-test/zsh_history"
    ];
    services.sinex = {
      enable = true;
      nats.daemonPasswordFile = "/etc/sinex-test/nats-daemon";
      journald.enable = true;
      readAnyFile = true;
      sources."terminal.zsh-history" = {
        enable = true;
        paths = [ "/home/alice/.zsh_history" ];
      };
      backfills.books = [ "goodreads=/etc/sinex-test/library_export.csv" ];
      session = {
        enable = true;
        user = "alice";
        passwordFile = "/etc/sinex-test/nats-session";
        watch = [ "/home/alice/notes" ];
        wantedBy = [ "default.target" ];
      };
    };
  };
  testScript = ''
    def count(where):
        return int(machine.succeed(
            f"sudo -u postgres psql -d sinex -tAc \"select count(*) from core.events where {where}\""
        ).strip())

    machine.wait_for_unit("sinex-nats.service")
    print(machine.succeed("sed 's/password: \"[^\"]*\"/password: <hidden>/g' /run/sinex-nats/nats.conf"))
    machine.wait_for_unit("sinex-db-setup.service")
    machine.wait_for_unit("sinexd.service")
    machine.wait_for_unit("sinex-capture-journald.service")
    machine.wait_for_unit("sinex-session.service", user="alice")

    with subtest("PostgreSQL is set up as the module says"):
        machine.succeed("sudo -u postgres psql -tAc 'show wal_compression' | grep -qx lz4")
        machine.succeed("sudo -u postgres psql -tAc 'show full_page_writes' | grep -qx on")
        # PostgreSQL 18 made log_connections a list; off may show as empty.
        shown = machine.succeed("sudo -u postgres psql -tAc 'show log_connections'").strip()
        assert shown in ("", "off", "false", "no"), f"log_connections is {shown!r}"
        machine.succeed("sudo -u postgres psql -d sinex -tAc \"select 1 from pg_extension where extname='timescaledb'\" | grep -qx 1")

    with subtest("NATS refuses a producer outside its own subjects"):
        machine.succeed("grep -q 'session-machine' /run/sinex-nats/nats.conf")
        machine.fail("grep -rq session-secret /etc/systemd/system/sinex-nats.service /etc/systemd/system/sinexd.service")

    with subtest("a file change reaches the database through the ingress"):
        machine.succeed("su alice -c 'echo hello > /home/alice/notes/a.txt'")
        machine.wait_until_succeeds(
            "test $(sudo -u postgres psql -d sinex -tAc \"select count(*) from core.events where event_type = 'file.changed'\") -gt 0",
            timeout=180,
        )

    with subtest("the API answers with a token, and refuses without one"):
        machine.succeed(
            "curl -sf -H \"Authorization: Bearer $(cat /var/lib/sinex/tokens/read.token)\" http://127.0.0.1:7171/v1/commands"
        )
        machine.fail("curl -sf http://127.0.0.1:7171/v1/commands -X POST -d '{}'")

    with subtest("sinexd reads a file only the operator can read"):
        machine.succeed("test $(stat -c %a /home/alice) = 700")
        machine.wait_until_succeeds(
            "test $(sudo -u postgres psql -d sinex -tAc \"select count(*) from core.events where event_type = 'command.imported'\") -gt 0",
            timeout=120,
        )

    with subtest("the private-mode switch is one file the operator can set"):
        machine.succeed("su alice -c 'SINEX_PRIVATE_MODE=/run/sinex/private-mode sinexctl private on'")
        machine.succeed("test -e /run/sinex/private-mode")
        machine.succeed("su alice -c 'SINEX_PRIVATE_MODE=/run/sinex/private-mode sinexctl private off'")

    with subtest("a declared backfill stops sinexd, imports, and starts it again"):
        machine.succeed("systemctl start sinex-backfill-books.service")
        assert count("event_type = 'book.status'") == 2
        machine.wait_until_succeeds("systemctl is-active sinexd.service", timeout=120)

    with subtest("sinexd comes back after a crash"):
        machine.succeed("systemctl kill -s KILL sinexd.service")
        machine.wait_until_succeeds("systemctl is-active sinexd.service", timeout=120)
  '';
}
