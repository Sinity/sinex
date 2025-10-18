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
            description = "Enable core runtime services (collector, worker, coordinator)";
          };
          
          maintenance = mkOption {
            type = types.bool;
            default = true;
            description = "Enable maintenance services (cleanup, monitoring, git-annex)";
          };
          
          monitoring = mkOption {
            type = types.bool;
            default = config.services.sinex.monitoring.enable or false;
            description = "Enable monitoring and health check services";
          };
        };
      };
      default = {};
      description = "Control which service groups are enabled";
    };
  };
  
  config = mkIf config.services.sinex.enable {
    # Add configuration validation
    assertions = [
      {
        assertion = config.services.sinex.serviceManagement.consolidatedMode -> 
                   (config.services.sinex.targetUser != null);
        message = "services.sinex.targetUser must be set when using consolidated service management";
      }
      
      {
        assertion = config.services.sinex.serviceManagement.serviceGroups.maintenance ->
                   (config.services.sinex.database.autoSetup || config.services.postgresql.enable);
        message = "Database must be managed or explicitly enabled for maintenance services";
      }
    ];
    
    # Warning about legacy service definitions
    warnings = optional (!config.services.sinex.serviceManagement.consolidatedMode) ''
      You are using legacy Sinex service definitions. Consider migrating to 
      consolidated service management by setting:
      services.sinex.serviceManagement.consolidatedMode = true;
    '';
  };
}
