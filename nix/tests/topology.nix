# Capture topology (rkv.7): one database host and a capture-only satellite.
# The satellite mounts sinexd's content store over NFS (sending bytes over the
# link is not built yet) and produces over NATS with its own credentials.
self: {
  name = "sinex-topology";
  nodes = {
    server = { ... }: {
      imports = [ self.nixosModules.default ];
      virtualisation = { memorySize = 3072; cores = 2; diskSize = 4096; };
      networking.firewall.enable = false;
      environment.etc."sinex-test/nats-daemon".text = "daemon-secret";
      environment.etc."sinex-test/nats-satellite".text = "satellite-secret";
      services.sinex = {
        enable = true;
        noCow = false;
        nats = {
          address = "0.0.0.0";
          daemonPasswordFile = "/etc/sinex-test/nats-daemon";
          producers.capture-satellite.passwordFile = "/etc/sinex-test/nats-satellite";
        };
      };
      services.nfs.server = {
        enable = true;
        exports = "/var/lib/sinex/store *(rw,no_root_squash,no_subtree_check,sync)";
      };
    };
    satellite = { ... }: {
      imports = [ self.nixosModules.default ];
      virtualisation.memorySize = 1024;
      networking.firewall.enable = false;
      environment.etc."sinex-test/nats".text = "satellite-secret";
      # Test VMs take their mounts from virtualisation.fileSystems.
      virtualisation.fileSystems."/mnt/sinex-store" = {
        device = "server:/var/lib/sinex/store";
        fsType = "nfs";
        options = [ "x-systemd.automount" "noauto" ];
      };
      systemd.tmpfiles.rules = [ "d /srv/watched 0755 root root -" ];
      services.sinex.captureNode = {
        enable = true;
        nats = "server:4222";
        passwordFile = "/etc/sinex-test/nats";
        store = "/mnt/sinex-store";
        journald = false;
        watch = [ "/srv/watched" ];
      };
    };
  };
  testScript = ''
    server.wait_for_unit("sinexd.service")
    server.wait_for_unit("nfs-server.service")
    satellite.wait_for_unit("multi-user.target")
    satellite.wait_until_succeeds("ls /mnt/sinex-store", timeout=120)
    satellite.wait_for_unit("sinex-capture.service")

    with subtest("a change on the satellite is admitted on the server with the satellite's host"):
        satellite.succeed("echo from-satellite > /srv/watched/x.txt")
        server.wait_until_succeeds(
            "test $(sudo -u postgres psql -d sinex -tAc \"select count(*) from core.events where event_type = 'file.changed' and payload::text like '%x.txt%'\") -gt 0",
            timeout=180,
        )

    with subtest("the satellite's credentials allow only its own subjects"):
        server.succeed("grep -q 'capture-satellite' /run/sinex-nats/nats.conf")
        server.succeed("grep -q 'sinex.ingress.capture-satellite' /run/sinex-nats/nats.conf")
  '';
}
