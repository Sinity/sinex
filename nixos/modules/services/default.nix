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
    consolidatedMode = mkOption {
      type = types.bool;
      default = true;
      description = ''
        Enable consolidated service management mode.
        This replaces the old scattered service definitions with a unified approach.
        Set to false to use legacy service definitions (not recommended).
      '';
    };
    
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
          assertion = cfg.serviceManagement.consolidatedMode -> (cfg.targetUser != null);
          message = "services.sinex.targetUser must be set when using consolidated service management";
        }
        {
          assertion = cfg.serviceManagement.serviceGroups.maintenance ->
                     (cfg.database.autoSetup || config.services.postgresql.enable);
          message = "Database must be managed or explicitly enabled for maintenance services";
        }
        {
          assertion = cfg.serviceManagement.consolidatedMode;
          message = "Legacy Sinex service definitions have been removed; consolidatedMode must remain true.";
        }
      ];
    }
  );
}
