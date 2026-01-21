# Desktop Ingestor Environment Variables

Environment variables specific to `sinex-desktop-ingestor`.

## Configuration

```bash
# Require Hyprland window manager (fail if not detected)
SINEX_DESKTOP_REQUIRE_HYPRLAND=true

# Skip DBus RPATH setting during build
SINEX_SKIP_DBUS_RPATH=true
```

## System Detection

The desktop ingestor also reads these system variables:

```bash
# Hyprland window manager instance signature (set by Hyprland)
HYPRLAND_INSTANCE_SIGNATURE="..."
```

## See Also

- Global env vars: `docs/current/configuration/environment-variables.md`
- Node SDK: `crate/lib/sinex-node-sdk/docs/`
