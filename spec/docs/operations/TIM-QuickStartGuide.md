# TIM: Quick Start Guide

- **TIM Identifier**: TIM-QuickStartGuide
- **Category**: Operations
- **Status**: Implemented (Core)
- **Target Component**: New Users, Operators
- **Prerequisites**: NixOS system or Nix with flakes
- **Linked TIMs**: 
  - TIM-OperationsManual
  - TIM-ServiceManagement
  - TIM-NixOSDeploymentModule

## Overview

This guide helps you get Sinex running quickly, from zero to a functional event capture system in under 15 minutes.

## Prerequisites Check

```bash
# 1. Verify Nix is installed
nix --version
# Expected: nix (Nix) 2.18 or higher

# 2. Check flakes are enabled
nix show-config | grep experimental-features
# Should include "flakes nix-command"

# 3. Verify PostgreSQL is available
which psql
# If not, it will be installed automatically
```

## Quick Install Options

### Option A: Development Environment (Recommended for Testing)

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/sinex.git
cd sinex

# 2. Enter development shell (auto-configures everything)
nix develop

# 3. Run the collector
just unified

# 4. In another terminal, check it's working
just query
```

That's it! Sinex is now capturing filesystem events.

### Option B: NixOS System Module (Production)

Add to your NixOS configuration:

```nix
# flake.nix
{
  inputs.sinex.url = "github:yourusername/sinex";
  
  outputs = { self, nixpkgs, sinex }: {
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      modules = [
        sinex.nixosModules.default
        {
          # Enable Sinex with default settings
          services.sinex = {
            enable = true;
            preset = "lite";  # Start small
          };
        }
      ];
    };
  };
}
```

Then rebuild:
```bash
sudo nixos-rebuild switch
```

### Option C: Quick Test Without Installation

```bash
# Run directly from git
nix run github:yourusername/sinex#sinex-collector -- --dry-run

# This runs in dry-run mode (no database needed)
```

## 5-Minute Configuration

### 1. Basic Event Sources

Create `~/.config/sinex/collector.toml`:

```toml
# Minimal configuration - just filesystem events
[[event_sources]]
name = "filesystem"
enabled = true

[event_sources.config.filesystem]
paths = ["/home/youruser/Documents"]
recursive = true
include_patterns = ["*.txt", "*.md", "*.pdf"]
```

### 2. Start Capturing

```bash
# If using development environment
just unified

# If using NixOS module
sudo systemctl start sinex-unified-collector
```

### 3. Verify It's Working

```bash
# Check service status
systemctl status sinex-unified-collector

# View recent events (last 10)
just query

# View specific source events
./cli/exo.py query --source filesystem --limit 20

# Watch events in real-time
watch -n 1 "./cli/exo.py query --limit 5"
```

## Common First Tasks

### Enable More Event Sources

Edit `~/.config/sinex/collector.toml`:

```toml
# Add terminal capture
[[event_sources]]
name = "terminal"
enabled = true

[event_sources.config.terminal]
# Auto-detects terminal type

# Add clipboard monitoring
[[event_sources]]
name = "clipboard"
enabled = true

[event_sources.config.clipboard]
poll_interval_ms = 1000
capture_images = true
```

### Access Monitoring Dashboard

If you enabled Grafana:

```bash
# Open Grafana
xdg-open http://localhost:3000

# Default dashboards:
# - Sinex Overview
# - Event Pipeline
# - System Health
```

### Set Up Automatic Backups

```nix
# In your NixOS configuration
services.sinex = {
  enable = true;
  backup = {
    enable = true;
    schedule = "daily";
    retention = "7d";
  };
};
```

## Quick Troubleshooting

### Service Won't Start

```bash
# Check logs
journalctl -u sinex-unified-collector -n 50

# Common fix: database connection
sudo systemctl restart postgresql
```

### No Events Appearing

```bash
# Verify collector is running
ps aux | grep sinex-collector

# Check configuration
sinex-collector --validate-config

# Test with minimal config
sinex-collector --dry-run
```

### Database Connection Failed

```bash
# For development
nix develop  # This sets up PostgreSQL automatically

# For production
sudo -u postgres createuser sinex
sudo -u postgres createdb sinex_dev -O sinex
```

## Essential Commands Cheat Sheet

```bash
# Service management
systemctl {start|stop|status} sinex-unified-collector

# Query events
just query                    # Last 10 events
just query 50                # Last 50 events
./cli/exo.py query --help    # All query options

# Check health
curl http://localhost:2113/health
systemctl is-active sinex-unified-collector

# View logs
journalctl -fu sinex-unified-collector  # Follow logs
journalctl -u sinex-unified-collector --since "10 min ago"

# Database access
psql $DATABASE_URL           # Direct SQL access
```

## Next Steps

### 10-Minute Enhancements

1. **Enable More Sources**
   ```toml
   # Window manager events
   [[event_sources]]
   name = "hyprland"
   enabled = true
   ```

2. **Add Git-Annex for Large Files**
   ```bash
   ./script/init_git_annex.sh
   export SINEX_ANNEX_PATH=/path/to/annex
   ```

3. **Increase Performance**
   ```nix
   services.sinex.preset = "normal";  # From "lite"
   ```

### 30-Minute Power User Setup

1. **Custom Event Sources**
   - See `config/examples/` for templates
   - Create source-specific configurations

2. **Query Shortcuts**
   ```bash
   # Add to ~/.bashrc
   alias sq='./cli/exo.py query'
   alias sqf='./cli/exo.py query --source filesystem'
   alias sqt='./cli/exo.py query --source terminal'
   ```

3. **Monitoring Stack**
   ```nix
   services.sinex = {
     monitoring = {
       observabilityStack.enable = true;
       dashboards.grafana.enable = true;
     };
   };
   ```

## Quick Performance Tips

### For Laptops/Limited Resources

```nix
services.sinex = {
  preset = "lite";
  collector.resources = {
    memoryMax = "512M";
    cpuQuota = "50%";
  };
};
```

### For Desktops/Servers

```nix
services.sinex = {
  preset = "max";
  database.tuning = "performance";
};
```

## Getting Help

### Built-in Help

```bash
# CLI help
./cli/exo.py --help
./cli/exo.py query --help

# Validate configuration
sinex-collector --validate-config
```

### Documentation

- Operations Manual: `spec/docs/operations/TIM-OperationsManual.md`
- Troubleshooting: `spec/docs/operations/TIM-TroubleshootingGuide.md`
- Architecture: `spec/STAD.md`

### Real-time Metrics

```bash
# Collector metrics
curl http://localhost:2113/metrics

# Event flow stats
./cli/exo.py sources
```

## Example: 5-Minute Personal Archive

Here's how to set up a basic personal data archive:

```bash
# 1. Create config
cat > ~/.config/sinex/collector.toml << 'EOF'
# Capture file changes in important directories
[[event_sources]]
name = "filesystem"
enabled = true

[event_sources.config.filesystem]
paths = [
  "~/Documents",
  "~/Pictures",
  "~/Downloads"
]
recursive = true
exclude_patterns = ["*.tmp", "*.cache", ".git/**"]

# Capture terminal sessions
[[event_sources]]
name = "terminal"
enabled = true

# Capture clipboard
[[event_sources]]
name = "clipboard"
enabled = true

[event_sources.config.clipboard]
capture_images = true
sensitive_content_detection = true
EOF

# 2. Start collector
nix develop
just unified

# 3. Verify capture
# Create a test file
echo "Test content" > ~/Documents/test.txt

# Query events (should see filesystem event)
just query

# Copy some text
echo "Test clipboard" | xclip -selection clipboard

# Query again (should see clipboard event)
sleep 2
just query
```

You now have a working personal data archive!

## FAQ

**Q: How much disk space will this use?**
A: Approximately 1-5GB per day depending on activity. Metrics events are auto-cleaned after 30 days.

**Q: Can I pause event capture?**
A: Yes: `systemctl stop sinex-unified-collector`

**Q: Is my data secure?**
A: Events are stored locally in PostgreSQL. No external connections unless you configure them.

**Q: Can I exclude sensitive data?**
A: Yes, use exclude patterns in source configurations and enable sensitive content detection.

**Q: How do I export my data?**
A: Use `pg_dump` for full backups or `exo.py` queries for selective export.

Remember: Start simple, add sources gradually, and monitor resource usage!