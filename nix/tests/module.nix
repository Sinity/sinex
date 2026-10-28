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
  tests = [
    (builtins.tryEval (evaluate { }).system.build.toplevel.drvPath).success
    (plan.exclude."desktop.clipboard".reason == "Synthetic operator exclusion")
    (plan.exclude."terminal.atuin-history" == null)
    (!(plan.sources."terminal.atuin-history" ? enable))
    (plan.sources."terminal.zsh-history".paths == [ "/host-declared/history" ])
    (plan.discovery.inputs.spotify == [ "/exports/current" "/exports/earlier" ])
    (plan.discovery.home == "/home/alice")
    ((evaluate { }).services.sinex.session.stateDir == "/home/alice/.local/state/sinex-session")
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
