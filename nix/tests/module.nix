# Evaluate the generated public options without probing the evaluation host.
self: pkgs:
let
  lib = pkgs.lib;
  evaluate = extra: (self.inputs.nixpkgs.lib.nixosSystem {
    system = pkgs.stdenv.hostPlatform.system;
    modules = [ self.nixosModules.default ({ ... }: {
      nixpkgs.pkgs = pkgs;
      boot.loader.grub.enable = false;
      fileSystems."/" = { device = "/dev/vda"; fsType = "ext4"; };
      system.stateVersion = "26.05";
      users.users.alice.isNormalUser = true;
      services.sinex = {
        enable = true;
        package = pkgs.hello;
        nats.daemonPasswordFile = "/run/fixture-daemon";
        session = { user = "alice"; passwordFile = "/run/fixture-session"; };
      };
    }) extra ];
  }).config;
  config = evaluate {
    services.sinex = {
      session.stateDir = "/realm/state/sinex-session/alice";
      exclude."desktop.clipboard".reason = "Synthetic operator exclusion";
      sources."terminal.zsh-history".paths = [ "/host-declared/history" ];
      discovery = {
        inputRoots.projects = [ "/subjects/project" ];
        inputs.spotify = [ "/exports/current" ];
        environment.ZSH_HISTFILE = "/host-declared/history";
      };
      backfills.spotify = [ "spotify=/exports/earlier" ];
    };
  };
  plan = builtins.fromJSON config.environment.etc."sinex/sources.json".text;
  fails = extra: !(builtins.tryEval (evaluate extra).system.build.toplevel.drvPath).success;
  zoneScripts = extra:
    let checked = evaluate (lib.recursiveUpdate {
      services.sinex.backfills.zone = [ "zsh=/synthetic/history" ];
    } extra);
    in [ checked.systemd.services.sinexd.script checked.systemd.services.sinex-backfill-zone.script ];
  producerGrants = extra: builtins.fromJSON (evaluate extra).environment.etc."sinex/producer-grants.json".text;
  configuredSessionGrant = (producerGrants {
    services.sinex = {
      host = "fixture";
      session.producer = "session-fixture";
      nats.producers.session-fixture = {
        passwordFile = "/run/fixture-session";
        writeFamily = "sinex";
        attestedHost = "fixture";
        writeSourcePrefixes = [ "machine.file-watch" ];
        writeKindPrefixes = [ "file.changed" ];
        writeClasses = [ "guarded" ];
      };
    };
  }).session-fixture;
  defaultSessionGrant = (producerGrants {
    services.sinex.session.producer = "session-fixture";
  }).session-fixture;
  tests = [
    (builtins.tryEval (evaluate { }).system.build.toplevel.drvPath).success
    (defaultSessionGrant.write == null)
    (defaultSessionGrant.kind_prefixes == [ ] && defaultSessionGrant.classes == [ ])
    (configuredSessionGrant.write.family == "sinex")
    (configuredSessionGrant.write.host == "fixture")
    (configuredSessionGrant.write.source_prefixes == [ "machine.file-watch" ])
    (configuredSessionGrant.write.kind_prefixes == [ "file.changed" ])
    (configuredSessionGrant.write.classes == [ "guarded" ])
    (configuredSessionGrant.write.canonical_prefixes == [ ])
    (configuredSessionGrant.kind_prefixes == [ ] && configuredSessionGrant.classes == [ ])
    (lib.all (lib.hasInfix "--zone Europe/Warsaw") (zoneScripts {
      time.timeZone = "Europe/Warsaw";
    }))
    (lib.all (lib.hasInfix "--zone America/New_York") (zoneScripts {
      time.timeZone = "Europe/Warsaw";
      services.sinex.zone = "America/New_York";
    }))
    (lib.all (script: !(lib.hasInfix "--zone " script)) (zoneScripts { }))
    (plan.exclude."desktop.clipboard".reason == "Synthetic operator exclusion")
    (plan.exclude."terminal.atuin-history" == null)
    (!(plan.sources."terminal.atuin-history" ? enable))
    (plan.sources."terminal.zsh-history".paths == [ "/host-declared/history" ])
    (plan.discovery.inputs.spotify == [ "/exports/current" "/exports/earlier" ])
    (plan.discovery.home == "/home/alice")
    ((evaluate { }).services.sinex.session.stateDir == "/home/alice/.local/state/sinex-session")
    (!(builtins.hasAttr "/home/alice/.local/state/sinex-session" ((evaluate { }).systemd.tmpfiles.settings.sinex-session or { })))
    (lib.hasInfix "mkdir -p -- /home/alice/.local/state/sinex-session" (evaluate { }).systemd.user.services.sinex-session.preStart)
    (plan.discovery.environment.ZSH_HISTFILE == "/host-declared/history")
    (config.systemd.user.services.sinex-session.serviceConfig.UMask == "0077")
    (lib.hasInfix "--state /realm/state/sinex-session/alice" config.systemd.user.services.sinex-session.script)
    (!(lib.hasInfix "--clipboard" config.systemd.user.services.sinex-session.script))
    (config.systemd.tmpfiles.settings.sinex-session."/realm/state/sinex-session/alice".d.mode == "0700")
    (fails { services.sinex.exclude."desktop.clipboard".reason = " \t\n"; })
    (fails { services.sinex.exclude."no.such.source".reason = "fixture"; })
    (fails { services.sinex.sources."terminal.atuin-history".enable = true; })
    (fails { services.sinex.session.clipboard = true; })
    (fails { services.sinex.session.enable = true; })
    (fails { services.sinex.journald.enable = true; })
    (fails { services.sinex.captureNode.journald = true; })
    (!(evaluate { services.sinex.exclude."machine.journald".reason = "fixture"; }).systemd.services ? sinex-capture-journald)
  ];
in
assert lib.all (result: result) tests;
pkgs.runCommand "sinex-module-tests" { } ''
  mkdir "$out"
''
