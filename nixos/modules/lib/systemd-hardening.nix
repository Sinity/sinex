{ lib }:

with lib;

{
  mkHelperServiceConfig =
    {
      user,
      group ? user,
      type ? "oneshot",
      remainAfterExit ? false,
      protectHome ? true,
      privateTmp ? true,
      readWritePaths ? [],
      readOnlyPaths ? [],
      restrictAddressFamilies ? [ "AF_UNIX" "AF_INET" "AF_INET6" ],
      extra ? {},
    }:
    {
      User = user;
      Group = group;
      Type = type;
      ProtectSystem = "strict";
      ProtectHome = protectHome;
      PrivateTmp = privateTmp;
      NoNewPrivileges = true;
      RestrictSUIDSGID = true;
      RemoveIPC = true;
      ProtectKernelTunables = true;
      ProtectKernelModules = true;
      ProtectKernelLogs = true;
      ProtectClock = true;
      ProtectControlGroups = true;
      RestrictRealtime = true;
      LockPersonality = true;
      MemoryDenyWriteExecute = true;
      RestrictNamespaces = true;
      SystemCallArchitectures = "native";
      RestrictAddressFamilies = restrictAddressFamilies;
      SystemCallFilter = [ "@system-service" "~@privileged" ];
      SystemCallErrorNumber = "EPERM";
      UMask = "0077";
    }
    // optionalAttrs remainAfterExit { RemainAfterExit = true; }
    // optionalAttrs (readWritePaths != []) { ReadWritePaths = readWritePaths; }
    // optionalAttrs (readOnlyPaths != []) { ReadOnlyPaths = readOnlyPaths; }
    // extra;
}
