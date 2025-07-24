# Example NixOS configuration with Sinex Hot Standby Coordination
# This configuration demonstrates running multiple instances of each satellite 
# service for zero-downtime upgrades and automatic failover.

{ config, lib, pkgs, ... }:

{
  imports = [
    ./modules
  ];

  services.sinex = {
    enable = true;
    
    # Database configuration
    database = {
      enable = true;
      autoSetup = true;
      name = "sinex_prod";
      user = "sinex";
    };

    # Satellite coordination with hot standby
    satellite = {
      enable = true;
      logLevel = "info";
      
      database.url = "postgresql:///sinex_prod?host=/run/postgresql";
      redis.url = "redis://localhost:6379";

      # Coordination system configuration
      coordination = {
        enable = true;
        heartbeatInterval = 30;      # Heartbeat every 30 seconds
        leadershipTimeout = 120;     # Wait 2 minutes for leadership
        handoffTimeout = 60;         # 1 minute for graceful handoff
      };

      # Core services (single instance - leadership not needed)
      coreServices.enable = true;

      # Event source satellites with hot standby (multiple instances)
      eventSources = {
        filesystem = {
          enable = true;
          instances = 3;             # 3 instances: 1 leader + 2 standbys
          memoryLimit = "256M";
          environment = [
            "COORDINATION_PREFLIGHT_CHECKS=database,redis,disk_space"
          ];
        };

        terminal = {
          enable = true; 
          instances = 2;             # 2 instances: 1 leader + 1 standby
          memoryLimit = "256M";
        };

        desktop = {
          enable = true;
          instances = 2;             # 2 instances: 1 leader + 1 standby  
          memoryLimit = "256M";
        };

        system = {
          enable = true;
          instances = 2;             # 2 instances: 1 leader + 1 standby
          memoryLimit = "384M";
        };
      };

      # Automaton satellites
      automata = {
        canonicalCommandSynthesizer = {
          enable = true;
          consumerGroup = "canonical-synthesizers";
          batchSize = 50;
          memoryLimit = "512M";
        };

        healthAggregator = {
          enable = true;
          consumerGroup = "health-aggregators"; 
          batchSize = 50;
          memoryLimit = "512M";
        };
      };
    };

    # Monitoring setup
    monitoring = {
      enable = true;
      grafana.enable = true;
      prometheus.enable = true;
    };
  };

  # System-level coordination monitoring
  systemd.services = {
    # Monitor coordination health
    sinex-coordination-monitor = {
      description = "Monitor Sinex Coordination System";
      wantedBy = [ "multi-user.target" ];
      after = [ "postgresql.service" ];
      
      serviceConfig = {
        Type = "simple";
        User = "sinex";
        Restart = "on-failure";
        RestartSec = "30s";
      };
      
      script = ''
        while true; do
          echo "$(date): Checking coordination system health..."
          
          # Check for healthy instances
          ${pkgs.postgresql}/bin/psql "postgresql:///sinex_prod?host=/run/postgresql" -c "
            SELECT 
              service_name,
              COUNT(*) as instance_count,
              MAX(last_heartbeat) as latest_heartbeat,
              NOW() - MAX(last_heartbeat) as heartbeat_age
            FROM core.satellite_instances 
            WHERE last_heartbeat > NOW() - INTERVAL '2 minutes'
            GROUP BY service_name;
          "
          
          # Check leadership status
          ${pkgs.postgresql}/bin/psql "postgresql:///sinex_prod?host=/run/postgresql" -c "
            SELECT 
              service_name,
              version,
              NOW() - last_heartbeat as heartbeat_age
            FROM core.service_leadership;
          "
          
          sleep 60
        done
      '';
    };
  };

  # Networking for Redis
  networking.firewall.allowedTCPPorts = [ 6379 ];

  # Log management
  services.journald.extraConfig = ''
    SystemMaxUse=1G
    SystemKeepFree=2G
    MaxRetentionSec=7day
  '';

  # Performance tuning for coordination
  boot.kernel.sysctl = {
    # Increase connection limits for advisory locks
    "net.core.somaxconn" = 65536;
    "net.ipv4.tcp_max_syn_backlog" = 65536;
    
    # PostgreSQL performance
    "vm.overcommit_memory" = 2;
    "vm.overcommit_ratio" = 80;
  };

  # Backup coordination state
  services.postgresqlBackup = {
    enable = true;
    databases = [ "sinex_prod" ];
    startAt = "*-*-* 02:00:00";
    location = "/var/backup/postgresql";
  };
}

# Expected behavior with this configuration:
#
# 1. Each service type will have multiple instances running:
#    - sinex-fs-watcher-1, sinex-fs-watcher-2, sinex-fs-watcher-3
#    - sinex-terminal-satellite-1, sinex-terminal-satellite-2  
#    - sinex-desktop-satellite-1, sinex-desktop-satellite-2
#    - sinex-system-satellite-1, sinex-system-satellite-2
#
# 2. Only ONE instance of each service type will be the leader and process events
#
# 3. The other instances will be in hot standby, monitoring for:
#    - Leader failure (heartbeat timeout)
#    - New version deployment (version-based takeover)
#    - Manual leadership signals
#
# 4. During upgrades:
#    - New version instances start in standby
#    - New version challenges current leader
#    - Graceful handoff occurs (current leader finishes work, new leader takes over)
#    - Old version instances are stopped
#
# 5. During failures:
#    - Standby instances detect leader failure within 30-60 seconds
#    - Automatic leadership election occurs
#    - New leader begins processing immediately
#
# 6. Monitoring shows:
#    - Which instance is the leader for each service
#    - Heartbeat status of all instances
#    - Recent coordination events (handoffs, failures, elections)