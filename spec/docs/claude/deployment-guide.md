# Sinex Deployment Guide

## NixOS Module Configuration

The Sinex system is deployed via a comprehensive NixOS module that provides declarative configuration for all components.

### Basic Configuration

```nix
{
  services.sinex = {
    enable = true;
    
    # Database Configuration
    database = {
      name = "sinex";
      host = "/run/postgresql"; 
      user = "sinex";
      createLocally = true;
    };
    
    # Blob Storage
    blobstore = {
      path = "/var/lib/sinex/blobstore";
      enableGitAnnex = true;
    };
    
    # UnifiedCollector Configuration  
    unifiedCollector = {
      enable = true;
      configPath = "/etc/sinex/collector.toml";
      enableBrowserHistory = true;
      enableHyprlandIPC = true;
      enableFilesystemWatcher = true;
      enableClipboardMonitor = false;
    };
    
    # Worker Configuration
    workers = {
      enable = true;
      parallelism = 2;
      maxRetries = 3;
      batchSize = 100;
    };
    
    # Routing Cache
    routingCache = {
      batchRefresh = "5m";
      enableMaterializedView = true;
    };
    
    # Observability
    metrics = {
      enable = true;
      port = 9090;
      endpoint = "/metrics";
    };
  };
}
```

### Advanced Configuration

```nix
{
  services.sinex = {
    enable = true;
    
    # Production Database Setup
    database = {
      name = "sinex_prod";
      host = "db.example.com";
      port = 5432;
      user = "sinex_user";
      passwordFile = "/run/secrets/sinex-db-password";
      sslMode = "require";
      createLocally = false;
    };
    
    # Distributed Blob Storage
    blobstore = {
      path = "/data/sinex/blobs";
      enableGitAnnex = true;
      remotes = [
        {
          name = "backup";
          url = "s3://sinex-backup/blobs";
          type = "s3";
        }
        {
          name = "mirror";
          url = "/mnt/backup/sinex-blobs";
          type = "directory";
        }
      ];
    };
    
    # Collector with Custom Sources
    unifiedCollector = {
      enable = true;
      configPath = "/etc/sinex/production-collector.toml";
      sources = {
        browserHistory = {
          enable = true;
          browsers = ["chrome" "firefox"];
          pollInterval = "30s";
        };
        hyprland = {
          enable = true;
          socketPath = "/run/user/1000/hypr/.socket.sock";
        };
        filesystem = {
          enable = true;
          watchPaths = ["/home/user/Documents" "/home/user/Code"];
          excludePatterns = ["*.tmp" "node_modules"];
        };
        terminal = {
          enable = true;
          backends = ["atuin" "kitty"];
        };
      };
    };
    
    # High-Performance Worker Setup
    workers = {
      enable = true;
      parallelism = 8;
      maxRetries = 5;
      batchSize = 500;
      deadLetterQueue = {
        enable = true;
        maxAge = "7d";
      };
      agents = [
        {
          name = "activity-segmentation";
          type = "segmentation";
          parallelism = 2;
          eventTypes = ["window.focus" "terminal.command"];
        }
        {
          name = "content-processor";
          type = "nlp";
          parallelism = 1;
          eventTypes = ["clipboard.text" "browser.page"];
        }
      ];
    };
    
    # Performance Tuning
    routingCache = {
      batchRefresh = "1m";
      enableMaterializedView = true;
      refreshConcurrency = 4;
    };
    
    # Full Observability Stack
    metrics = {
      enable = true;
      port = 9090;
      endpoint = "/metrics";
      
      # Custom metrics
      exporters = {
        workQueue = true;
        agentLag = true;
        processingRate = true;
        errorRate = true;
      };
      
      # Integration with monitoring
      grafana = {
        enable = true;
        dashboardPath = "/etc/sinex/grafana-dashboard.json";
      };
      
      prometheus = {
        scrapeInterval = "15s";
        retentionTime = "30d";
      };
    };
    
    # Security Configuration
    security = {
      enableSandboxing = true;
      appArmorProfile = "sinex-restricted";
      
      # Process isolation
      systemdSecurity = {
        DynamicUser = true;
        PrivateTmp = true;
        ProtectSystem = "strict";
        ProtectHome = "read-only";
        NoNewPrivileges = true;
      };
      
      # Database security
      databaseSecurity = {
        enableRowLevelSecurity = true;
        encryptionAtRest = true;
        auditLogging = true;
      };
    };
  };
}
```

## Deployment Models

### Single-User Development

```nix
{
  services.sinex = {
    enable = true;
    database.createLocally = true;
    unifiedCollector.enable = true;
    workers.parallelism = 1;
    blobstore.path = "/home/user/.local/share/sinex";
  };
}
```

### Multi-User + Agents Production

```nix
{
  # Central database server
  services.postgresql = {
    enable = true;
    package = pkgs.postgresql_16;
    extraPlugins = with pkgs.postgresql16Packages; [
      timescaledb
      pgvector
    ];
  };
  
  # Multiple user collectors
  services.sinex = {
    enable = true;
    database = {
      host = "central-db.internal";
      createLocally = false;
    };
    
    # User-specific collectors
    userCollectors = {
      alice = {
        enable = true;
        configPath = "/etc/sinex/users/alice.toml";
        dataPartition = "alice";
      };
      bob = {
        enable = true; 
        configPath = "/etc/sinex/users/bob.toml";
        dataPartition = "bob";
      };
    };
    
    # Centralized workers
    workers = {
      enable = true;
      parallelism = 16;
      processAllPartitions = true;
    };
  };
}
```

## Service Management

### SystemD Integration

The NixOS module creates these systemd services:

- `sinex-unified-collector.service` - Main event collection
- `sinex-promo-worker.service` - Event processing worker
- `sinex-router.service` - Work queue routing (batch mode)
- `sinex-metrics.service` - Prometheus metrics exporter

### Service Dependencies

```
postgresql.service
    ↓
sinex-database-init.service
    ↓
sinex-unified-collector.service
sinex-router.service  
    ↓
sinex-promo-worker.service
```

### Management Commands

```bash
# Service control
sudo systemctl start sinex-unified-collector
sudo systemctl status sinex-promo-worker
sudo systemctl restart sinex-router

# Configuration reload
sudo systemctl reload sinex-unified-collector

# Logs
journalctl -u sinex-promo-worker -f
journalctl -u sinex-unified-collector --since="1 hour ago"

# Database management
sudo -u sinex psql sinex
sudo -u sinex sinex-cli migrate
```

## Configuration Files

### Collector Configuration (`/etc/sinex/collector.toml`)

```toml
[database]
url = "postgresql:///sinex?host=/run/postgresql"

[sources.browser_history]
enabled = true
poll_interval = "30s"
browsers = ["chrome", "firefox"]

[sources.hyprland]
enabled = true  
socket_path = "/run/user/1000/hypr/.socket.sock"

[sources.filesystem]
enabled = true
watch_paths = ["/home/user/Documents"]
exclude_patterns = ["*.tmp", ".git"]

[blobstore]
enabled = true
path = "/var/lib/sinex/blobstore"
```

### Worker Configuration

```toml
[worker]
parallelism = 4
batch_size = 100
max_retries = 3

[queue]
poll_interval = "1s"
max_batch_size = 500

[dead_letter_queue]
enabled = true
max_attempts = 5
```

## Monitoring & Troubleshooting

### Health Checks

```bash
# Service health
curl http://localhost:9090/health

# Queue status  
sinex-cli queue status

# Agent status
sinex-cli agent list
sinex-cli agent status activity-segmentation

# Database status
sinex-cli db status
```

### Common Issues

1. **Database Connection**: Check PostgreSQL service and connection strings
2. **Schema Issues**: Run `sinex-cli migrate` to apply latest schema
3. **Queue Backlog**: Monitor work_queue depth via metrics
4. **Worker Errors**: Check dead letter queue for failed events
5. **Collector Issues**: Verify source permissions and configuration

This deployment guide provides the foundation for both development and production Sinex installations using the declarative NixOS module system.