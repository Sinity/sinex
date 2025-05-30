# Example usage of the sinex NixOS module
{ config, pkgs, ... }:

{
  # Import the module
  imports = [
    ./default.nix
  ];

  # Enable and configure the service
  services.sinex = {
    enable = true;
    
    # Specify the user who runs the graphical session
    systemUser = "sinity"; # Replace with your actual username
    
    # Database configuration (optional, these are defaults)
    database = {
      name = "exocortex";
      url = "postgresql://localhost/exocortex";
      ensureExists = true;
    };
    
    # Enable specific ingestors
    ingestors = {
      hyprland = {
        enable = true;
        interval = 5; # Poll every 5 seconds
      };
    };
  };
  
  # Ensure PostgreSQL is enabled (required for the database)
  services.postgresql = {
    enable = true;
    enableTCPIP = true;
  };
}