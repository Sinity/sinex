{ config, lib, ... }:

with lib;

let
  cfg = config.services.sinex;
  secretDir = ../../secret;
  available = builtins.pathExists secretDir;
  files = if available then builtins.readDir secretDir else {};
  ageFiles = filterAttrs (name: kind: kind == "regular" && hasSuffix ".age" name) files;
  serviceUser = cfg.users.satellites;
  defaultOwner = serviceUser;

  mkSpec = filename: {
    file = secretDir + "/" + filename;
    owner = defaultOwner;
    group = defaultOwner;
    mode = "0400";
    path = "/run/agenix/" + removeSuffix ".age" filename;
  };

  specs = mapAttrs' (filename: _: nameValuePair (removeSuffix ".age" filename) (mkSpec filename)) ageFiles;

  nonExport = [
    "sinex-local-db"
    "sinex-remote-db"
    "sinex-remote-nats-ca"
    "sinex-remote-nats-cert"
    "sinex-remote-nats-key"
  ];

  mkExport = name: spec:
    let envName = toUpper (replaceStrings ["-" "."] ["_" "_"] name);
    in optionalString (!elem name nonExport) ''
      if [[ -r "${spec.path}" ]]; then
        export ${envName}="$(<${spec.path})"
      fi
    '';

  exportScript = concatStringsSep "\n" (filter (s: s != "") (mapAttrsToList mkExport specs));

  defaultIdentities = [ "/etc/ssh/ssh_host_ed25519_key" ]
    ++ optional (builtins.pathExists "/home/${defaultOwner}/.ssh/id_ed25519") "/home/${defaultOwner}/.ssh/id_ed25519";

in {
  options.sinex.secrets = {
    paths = mkOption {
      type = types.attrsOf types.path;
      description = "Resolved secret paths.";
      readOnly = true;
      default = {};
    };
    exportScript = mkOption {
      type = types.str;
      description = "Shell snippet exporting decrypted secrets.";
      readOnly = true;
      default = "";
    };
  };

  config = {
    age.identityPaths = mkDefault defaultIdentities;
    age.secrets = specs;
    sinex.secrets.paths = mapAttrs (_: spec: spec.path) specs;
    sinex.secrets.exportScript = exportScript;
  };
}
