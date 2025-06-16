# Wayland/GUI Environment Fixes for VM Testing

## Issues Fixed

### 1. greetd Configuration Error
**Problem**: The original configuration used `cage -s ${pkgs.weston}/bin/weston` which is invalid:
- `cage -s` doesn't exist (cage doesn't accept `-s` flag)
- Cannot run two Wayland compositors simultaneously
- greetd service was failing on startup

**Solution**: Removed greetd entirely and replaced with direct Weston systemd service:
```nix
systemd.services.weston-headless = {
  description = "Weston Wayland compositor (headless mode for testing)";
  wantedBy = [ "multi-user.target" ];
  after = [ "systemd-user-sessions.service" ];
  
  serviceConfig = {
    ExecStart = "${pkgs.weston}/bin/weston --backend=headless-backend --width=1920 --height=1080";
    Restart = "always";
    RestartSec = "2";
    User = "test";
    Group = "users";
    Environment = [
      "WAYLAND_DISPLAY=wayland-1"
      "XDG_RUNTIME_DIR=/run/user/1000"
      "XDG_SESSION_TYPE=wayland"
    ];
  };
}
```

### 2. Runtime Directory Setup
**Problem**: XDG_RUNTIME_DIR and Wayland socket creation was incomplete.

**Solution**: Added proper tmpfiles rules and preStart script:
```nix
systemd.tmpfiles.rules = [
  "d /run/user 0755 root root -"
  "d /run/user/1000 0700 test users -"
];

preStart = ''
  mkdir -p /run/user/1000
  chown test:users /run/user/1000
  chmod 0700 /run/user/1000
'';
```

### 3. Service Dependencies
**Problem**: Sinex services could start before Wayland compositor was ready.

**Solution**: Added service dependencies:
```nix
systemd.services.sinex-unified-collector = {
  after = lib.mkAfter [ "weston-headless.service" ];
  wants = lib.mkAfter [ "weston-headless.service" ];
};
```

### 4. Test Environment Setup
**Problem**: Tests didn't wait for Wayland to be ready and had incorrect environment variables.

**Solution**: 
- Added wait for Wayland socket: `machine.wait_until_succeeds("test -e /run/user/1000/wayland-1")`
- Fixed environment variables in test commands
- Proper user context for GUI operations

## Key Changes Made

1. **Replaced greetd with direct Weston service** - More reliable for headless testing
2. **Fixed runtime directory permissions** - Proper XDG_RUNTIME_DIR setup
3. **Added service dependencies** - Ensures startup order
4. **Updated test script** - Waits for Wayland readiness
5. **Fixed environment variables** - Consistent XDG_RUNTIME_DIR and WAYLAND_DISPLAY

## Testing Results Expected

With these fixes:
- ✅ greetd service failures should be eliminated
- ✅ Wayland compositor starts successfully in headless mode
- ✅ wl-clipboard operations work in VM
- ✅ Kitty terminal can start and create sockets
- ✅ Sinex clipboard monitoring functions properly
- ✅ Sinex Kitty scrollback capture works

## Alternative Approaches Considered

1. **Fixing greetd configuration** - More complex, not needed for testing
2. **Using X11 instead of Wayland** - Would require different tools (xclip vs wl-clipboard)
3. **Mock/stub GUI environment** - Wouldn't test real integration

The chosen approach provides a real Wayland environment suitable for testing while being simpler and more reliable than display managers designed for interactive use.