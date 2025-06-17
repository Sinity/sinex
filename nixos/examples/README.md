# Modularized NixOS Configuration Examples

This directory contains examples of how to use the new modularized Sinex NixOS configuration.

## The Problem: full.nix was huge

The original `full.nix` file was over 1000 lines because it defined every single option inline:

```nix
# Old approach - massive repetition
services.sinex.unifiedCollector.sources.atuin = {
  enable = mkOption {
    type = types.bool;
    default = true;
    description = "Enable Atuin shell history monitoring";
  };
  pollInterval = mkOption {
    type = types.int;
    default = 3;
    description = "Polling interval in seconds for Atuin";
  };
  databasePath = mkOption {
    type = types.str;
    default = "~/.local/share/atuin/history.db";
    description = "Path to Atuin database file";
  };
};
# ... repeat this pattern for every single event source, health check, etc.
```

This pattern was repeated for:
- 8+ event sources (filesystem, clipboard, terminals, etc.)
- Health checks for each component  
- Restart policies and resource limits
- Database connection options
- Monitoring and alerting rules
- Blob storage configuration

## The Solution: Modularization

The new modular approach uses utility functions and preset configurations:

### Utility Functions Eliminate Repetition

```nix
# health-checks.nix - reusable patterns
mkHealthCheckOptions = { defaultPort, serviceName, ... }: {
  enable = mkOption { ... };
  port = mkOption { default = defaultPort; ... };
  interval = mkOption { ... };
  # ... common health check options
};

# event-sources.nix - reusable event source pattern
mkEventSource = { name, defaultPollInterval, ... }: {
  enable = mkOption { default = true; ... };
  pollInterval = mkOption { default = defaultPollInterval; ... };
  # ... common event source options
};
```

### Preset Configurations

Instead of manually configuring every option, users can choose presets:

```nix
services.sinex = {
  enable = true;
  preset = "max";  # Automatically configures all components
  
  # Only specify what differs from the preset
  blobStorage.repositoryPath = "/realm/annex";
  database.name = "sinex";
};
```

### Module Structure

The configuration is split into focused modules:

- `modules/default.nix` - Main options and preset logic
- `modules/database.nix` - Database configuration 
- `modules/event-sources.nix` - Event source definitions
- `modules/health-checks.nix` - Health check utilities
- `modules/blob-storage.nix` - Git-annex configuration
- `modules/monitoring.nix` - Observability features

## Usage Examples

### Simple Configuration (50 lines vs 1000+)

See `simple-config.nix` for a complete configuration that replaces the massive `full.nix`:

### With Full Observability Stack

See `with-observability.nix` for Sinex with Prometheus + Grafana monitoring:

### Preset + Customizations

See `preset-with-customizations.nix` for how to use presets while overriding specific settings:

```nix
{
  imports = [ ../modules ];
  
  services.sinex = {
    enable = true;
    preset = "max";
    targetUser = "sinity";
    database.name = "sinex";
    blobStorage.repositoryPath = "/realm/annex";
  };
}
```

### With Full Observability Stack

```nix
{
  imports = [ ../modules ];
  
  services.sinex = {
    enable = true;
    preset = "normal";
    
    # Enable complete monitoring stack
    monitoring = {
      enable = true;
      observabilityStack.enable = true;  # Prometheus + Grafana + exporters
      dashboards.grafana.enable = true;  # Pre-built dashboards
    };
  };
}
```

## 📖 Detailed Customization Guide

See `CUSTOMIZATION-GUIDE.md` for comprehensive examples of:
- **Storage & Paths**: Custom git-annex locations, filesystem monitoring paths
- **Privacy Controls**: Disabling sensitive event sources, selective monitoring
- **Performance Tuning**: Resource optimization, polling intervals, connection pools
- **Development Setup**: Debug configurations, code-focused monitoring
- **Network Setup**: Remote databases, network-accessible monitoring
- **Resource Constraints**: Minimal configurations for low-resource systems

### Available Presets

- **lite**: Lightweight capture (core events, minimal resources)
- **normal**: Standard comprehensive capture (good default) - **DEFAULT**
- **max**: Maximum data capture (everything, high frequency)

### Monitoring Commands

Once deployed, monitor your system with:

```bash
# Check service status
systemctl status sinex-unified-collector sinex-promo-worker

# View recent events  
./cli/exo.py query 20

# Check metrics endpoints
curl localhost:2112/metrics  # Unified collector
curl localhost:2113/metrics  # Promotion worker

# Monitor health endpoints
curl localhost:8080/health   # Collector health
curl localhost:8081/health   # Worker health

# Check database
just psql  # Direct database connection
./cli/exo.py agent list  # List registered agents

# View system health logs
journalctl -u sinex-system-health -f

# Monitor resource usage
tail -f /var/log/sinex/resource-usage.log

# Observability stack (if enabled)
sinex-metrics    # Check Prometheus + Grafana status
sinex-logs       # Interactive log viewer for all services
# Open Grafana dashboard at http://localhost:3000
# Open Prometheus at http://localhost:9090
```

## Benefits of Modularization

1. **Reduced Repetition**: Utility functions eliminate copy-paste code
2. **Easier Maintenance**: Changes in one module propagate everywhere
3. **Better Defaults**: Preset configurations work out of the box
4. **Focused Concerns**: Each module handles one aspect of configuration
5. **User-Friendly**: Simple configurations for common use cases
6. **Extensible**: Easy to add new event sources or features

## Migration from full.nix

Replace your old configuration:

```nix
# Old way
{ imports = [ ./sinex/nixos/full.nix ]; }

# New way  
{
  imports = [ ./sinex/nixos/modules ];
  services.sinex = {
    enable = true;
    preset = "normal";  # or "lite", "max" 
    # Only configure what you need to change
  };
}
```

The modular approach achieves the same functionality with significantly less code and much better maintainability.