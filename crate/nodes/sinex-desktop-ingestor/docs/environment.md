# Desktop Ingestor Environment Variables

Environment variables specific to `sinex-desktop-ingestor`.

## Configuration

```bash
# Require Hyprland window manager (fail if not detected)
SINEX_DESKTOP_REQUIRE_HYPRLAND=true

# Override the user runtime directory that contains /hypr/<instance>/
SINEX_HYPRLAND_RUNTIME_DIR=/run/user/1000

# Override Hyprland instance selection when multiple instances exist
SINEX_HYPRLAND_INSTANCE_SIGNATURE=abc123def456

# Override sockets directly when the runtime layout is non-standard
SINEX_HYPRLAND_EVENT_SOCKET=/run/user/1000/hypr/abc123def456/.socket2.sock
SINEX_HYPRLAND_COMMAND_SOCKET=/run/user/1000/hypr/abc123def456/.socket.sock

# Skip DBus RPATH setting during build
SINEX_SKIP_DBUS_RPATH=true
```

## System Detection

The desktop ingestor also reads these system variables:

```bash
# Hyprland window manager instance signature (set by Hyprland)
HYPRLAND_INSTANCE_SIGNATURE="..."

# Standard desktop runtime directory used as a fallback when
# SINEX_HYPRLAND_RUNTIME_DIR is not set
XDG_RUNTIME_DIR=/run/user/1000
```

## See Also

- Deployment config: `nixos/modules/README.md`
- Node SDK: `crate/lib/sinex-node-sdk/docs/`
