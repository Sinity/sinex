# Sinex Customization Guide

This guide shows common settings you might want to customize even when using presets.

## Basic Pattern

```nix
services.sinex = {
  enable = true;
  preset = "normal";  # Start with preset defaults
  
  # Then override specific settings:
  targetUser = "myuser";
  blobStorage.repositoryPath = "/my/custom/path";
  # ... other customizations
};
```

## Common Customizations by Category

### 🗂️ Paths and Storage

```nix
services.sinex = {
  preset = "normal";
  
  # Custom user and storage locations
  targetUser = "alice";
  
  # Git-annex blob storage location
  blobStorage.repositoryPath = "/storage/exocortex-blobs";
  blobStorage.healthCheck.wantedSize = "500G";  # Adjust for your disk
  
  # Custom state directories
  directories = {
    state = "/data/sinex/state";
    cache = "/tmp/sinex-cache";
    logs = "/var/log/exocortex";
  };
  
  # Database location
  database = {
    name = "my_brain";
    host = "db.local";  # Remote database
    # user = "my_brain" (auto-derived)
  };
};
```

### 📁 Filesystem Monitoring

```nix
services.sinex = {
  preset = "normal";
  
  unifiedCollector.sources.filesystem = {
    enable = true;
    watchPaths = [
      "~/Work"           # Work projects
      "~/Personal"       # Personal files
      "~/Research"       # Research materials
      "/media/archive"   # External storage
      "/shared/docs"     # Network shares
    ];
    excludePatterns = [
      "*.tmp"
      "*.cache"
      "*/.git/*"
      "*/node_modules/*"
      "*/.venv/*"
      "*/__pycache__/*"
      "*/target/*"       # Rust build artifacts
      "*/dist/*"         # Build outputs
      "*.log"            # Log files
    ];
  };
};
```

### 🔒 Privacy Controls

```nix
services.sinex = {
  preset = "normal";
  
  unifiedCollector.sources = {
    # Disable sensitive event sources
    clipboard.enable = false;           # Privacy: no clipboard capture
    
    # Selective D-Bus monitoring
    dbus = {
      extractNotifications = true;      # Keep notifications
      extractMedia = true;              # Keep media events
      extractPower = true;              # Keep power events
      extractNetwork = false;           # Privacy: no network events
      extractPolicykit = false;         # Privacy: no auth events
      extractMounts = false;            # Privacy: no mount events
    };
    
    # Limited terminal capture
    kittyScrollback = {
      enable = true;
      captureOnCommand = false;         # Privacy: manual capture only
      maxScrollbackLines = 1000;        # Limit data captured
    };
    
    # No automatic recording
    asciinema = {
      enable = true;
      autoRecord = false;               # Privacy: manual recording only
    };
  };
};
```

### ⚡ Performance Tuning

```nix
services.sinex = {
  preset = "lite";  # Start light, then tune up specific sources
  
  # High-performance shell history monitoring
  unifiedCollector.sources.atuin.pollInterval = 1;  # Very frequent
  
  # Moderate filesystem monitoring
  unifiedCollector.sources.filesystem.watchPaths = [ "~/ActiveProjects" ];
  
  # Database tuning for performance
  database.connectionPool = {
    maxConnections = 50;
    minConnections = 15;
    connectionTimeout = 30;
  };
  
  # Aggressive promotion worker
  promoWorker = {
    pollInterval = 1;
    batchSize = 1000;
  };
  
  # Shorter retention for performance
  unifiedCollector.dlq.cleanup = {
    maxAge = "7d";
    maxFiles = 10000;
  };
};
```

### 🖥️ Development Setup

```nix
services.sinex = {
  preset = "normal";
  
  # Development-specific settings
  unifiedCollector = {
    logLevel = "debug";
    sources = {
      # Focus on code-related events
      filesystem.watchPaths = [
        "~/code"
        "~/projects" 
        "~/experiments"
      ];
      filesystem.excludePatterns = [
        "*/.git/*"
        "*/node_modules/*"
        "*/target/*"
        "*/.next/*"
        "*/.nuxt/*"
        "*/dist/*"
        "*/build/*"
      ];
      
      # Capture terminal activity (useful for debugging)
      kittyScrollback.captureOnCommand = true;
      asciinema.autoRecord = true;
    };
  };
  
  # Development database
  database.name = "sinex_dev";
  
  # Enable full observability for debugging
  monitoring = {
    enable = true;
    observabilityStack.enable = true;
    dashboards.grafana.enable = true;
    logging.level = "debug";
  };
};
```

### 🌐 Network/Remote Setup

```nix
services.sinex = {
  preset = "normal";
  
  # Remote database
  database = {
    name = "exocortex";
    host = "brain.mydomain.com";
    port = 5432;
    user = "exocortex";
    passwordFile = "/run/secrets/sinex-db-password";
  };
  
  # Network-accessible monitoring
  monitoring.observabilityStack = {
    enable = true;
    listenAddress = "0.0.0.0";  # CAUTION: Exposes to network
    prometheusPort = 9090;
    grafanaPort = 3000;
  };
  
  # Custom blob storage (maybe network attached)
  blobStorage.repositoryPath = "/mnt/nas/exocortex-blobs";
};

# Don't forget firewall rules for network access!
networking.firewall.allowedTCPPorts = [ 9090 3000 ];
```

### 🔧 Resource Constraints

```nix
services.sinex = {
  preset = "lite";  # Start conservative
  
  # Minimal resource usage
  database.connectionPool = {
    maxConnections = 5;
    minConnections = 2;
  };
  
  # Slower polling to save CPU
  unifiedCollector.sources = {
    atuin.pollInterval = 15;
    clipboard.pollInterval = 5000;  # Very slow
    kittyScrollback.enable = false;  # Disable expensive sources
    dbus = {
      extractNotifications = true;
      extractMedia = false;          # Disable high-volume events
      extractAll = false;
    };
  };
  
  # Small storage limits
  blobStorage.healthCheck.wantedSize = "10G";
  
  # Aggressive cleanup
  unifiedCollector.dlq.cleanup = {
    maxAge = "2d";
    maxFiles = 500;
  };
  
  # Disable observability stack to save resources
  monitoring.observabilityStack.enable = false;
};
```

### 📊 Data Retention Policies

```nix
services.sinex = {
  preset = "normal";
  
  # Custom retention policies
  unifiedCollector.dlq.cleanup = {
    enable = true;
    maxAge = "90d";      # Keep DLQ files longer
    maxFiles = 100000;   # Allow more files
  };
  
  # Blob storage maintenance
  blobStorage.maintenance = {
    gcSchedule = "weekly";     # Regular cleanup
    fsckSchedule = "monthly";  # Integrity checks
  };
  
  # Monitoring retention
  monitoring = {
    observabilityStack.retentionTime = "180d";  # 6 months of metrics
    logging.retention = {
      maxAge = "90d";
      maxFiles = 50;
      maxSize = "500M";
    };
  };
};
```

## Quick Reference: Most Common Overrides

```nix
services.sinex = {
  enable = true;
  preset = "normal";  # Choose: lite, normal, max
  
  # Almost always customized:
  targetUser = "yourname";
  blobStorage.repositoryPath = "/your/preferred/path";
  
  # Often customized:
  database.name = "your_db_name";
  unifiedCollector.sources.filesystem.watchPaths = [ "~/YourDirs" ];
  
  # Privacy customizations:
  unifiedCollector.sources.clipboard.enable = false;  # If privacy-sensitive
  
  # Performance customizations:
  database.connectionPool.maxConnections = 25;  # Tune for your hardware
  blobStorage.healthCheck.wantedSize = "100G";  # Tune for your disk
};
```