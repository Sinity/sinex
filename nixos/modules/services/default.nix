# Sinex Services - Consolidated Service Management
# This module provides the new consolidated approach to Sinex service management
# eliminating duplication and improving co-location of related services
{ config, lib, pkgs, ... }:

with lib;

{
  imports = [
    ./core-services.nix
    ./maintenance-services.nix
  ];
  
  options.services.sinex.serviceManagement = {
    serviceGroups = mkOption {
      type = types.submodule {
        options = {
          core = mkOption {
            type = types.bool;
            default = true;
            description = "Enable the satellite constellation (ingestd, gateway, satellites)";
          };
          
          maintenance = mkOption {
            type = types.bool;
            default = true;
            description = "Enable maintenance services (cleanup, monitoring, git-annex)";
          };
          
          monitoring = mkOption {
            type = types.bool;
            default = true;
            description = "Enable monitoring stack (Prometheus, Grafana, exporters)";
          };
        };
      };
      default = {};
      description = "Control which service groups are enabled";
    };
  };
  
  config = mkIf config.services.sinex.enable (
    let
      cfg = config.services.sinex;
    in
    {
      assertions = [
        {
          assertion = cfg.targetUser != null;
          message = "services.sinex.targetUser must be set for the Sinex deployment";
        }
        {
          assertion = cfg.serviceManagement.serviceGroups.maintenance ->
                     (cfg.database.autoSetup || config.services.postgresql.enable);
          message = "Database must be managed or explicitly enabled for maintenance services";
        }
      ];
    }
  );
}
