# Sinex Observability Migration

## Moved to Sinex Modules

The following Sinex-specific observability components have been moved from `sinnix/module/automation/observability.nix` to `sinex/nixos/modules/monitoring.nix`:

### ✅ Moved Components

1. **Prometheus scrape configs for Sinex services**:
   - `sinex_unified_collector` (port 2112)
   - `sinex_promo_worker` (port 2113)

2. **PostgreSQL exporter configuration**:
   - For monitoring Sinex database specifically

3. **Grafana configuration**:
   - Sinex-specific datasource (`Prometheus-Sinex`)
   - Dashboard provisioning for Sinex dashboards
   - Sinex dashboard folder setup

4. **Dashboard files**:
   - `sinex-dashboard.json` → `sinex/nixos/modules/sinex-dashboard.json`

5. **Convenience scripts**:
   - `sinex-metrics` script for checking Sinex observability status
   - `sinex-logs` script for interactive Sinex service log viewing

6. **Home-manager user configuration**:
   - Environment variables (`PROMETHEUS_URL`, `GRAFANA_URL`)
   - User bin scripts activation

## Usage in Sinex

Enable the observability stack in your Sinex configuration:

```nix
services.sinex = {
  enable = true;
  preset = "normal";  # or "max" - both include observability
  
  # Or explicit control:
  monitoring = {
    enable = true;
    observabilityStack.enable = true;
    dashboards.grafana.enable = true;
  };
};
```

## What Should Remain in Sinnix

**NOTHING** - Since Prometheus/Grafana are only for Sinex monitoring right now, the entire `sinnix/module/automation/observability.nix` file should be:

1. **Completely removed**, OR
2. **Disabled by commenting out the import in the module list**

## Rationale

- **Prometheus**: Only scraping Sinex services → belongs in Sinex
- **Grafana**: Only showing Sinex dashboards → belongs in Sinex
- **Node exporter**: Only valuable if consumed by Prometheus → belongs with Sinex
- **PostgreSQL exporter**: Only monitoring Sinex database → belongs in Sinex
- **All convenience scripts**: Sinex-specific (`sinex-metrics`, `sinex-logs`) → belong in Sinex
- **Firewall rules**: Only for Sinex monitoring ports → belong in Sinex

## Cleanup Required

**Delete the entire file**: `sinnix/module/automation/observability.nix`

**AND remove from module imports**: In your sinnix module list, remove or comment out:
```nix
# ./automation/observability.nix  # Now handled by Sinex itself
```

## Future System Monitoring

If you later want system-level monitoring for non-Sinex services, create a separate system monitoring configuration that doesn't conflict with Sinex's observability stack.