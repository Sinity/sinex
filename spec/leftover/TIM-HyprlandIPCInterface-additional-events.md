# TIM-HyprlandIPCInterface: Additional Event Types (Not Implemented)

## Unimplemented Hyprland Event Types

The current implementation handles core window management events but doesn't capture these additional event types available in Hyprland IPC:

### Window State Events
- `fullscreen>>STATE` - Fullscreen mode changes (0/1)
- `changefloatingmode>>WINDOWADDRESS,FLOATING_STATE` - Float state changes
- `minimize>>WINDOWADDRESS,MINIMIZED_STATE` - Window minimize/restore (v0.33.0+)
- `urgent>>WINDOWADDRESS` - Window urgency hints
- `windowtitle>>WINDOWADDRESS` - Title changes (requires hyprctl query)

### Monitor Events  
- `focusedmon>>MONITORNAME,WORKSPACENAME` - Monitor focus changes
- `monitoradded>>MONITORNAME` - Monitor hotplug
- `monitorremoved>>MONITORNAME` - Monitor disconnect

### Layer Shell Events
- `openlayer>>LAYER_NAMESPACE` - Panel/notification layers
- `closelayer>>LAYER_NAMESPACE` - Layer removal

### Input Events
- `submap>>SUBMAPNAME` - Keybinding mode changes (e.g., "resize" mode)

### System Events
- `screencast>>OWNER_STATE,SCREENCAST_STATE` - Screen recording status

### Legacy/Alternative Events
- `activewindow>>WINDOWCLASS,WINDOWTITLE` - Legacy focus event (less specific)
- `activewindowv2>>WINDOWADDRESS` - Newer focus event with address only
- `movewindow>>WINDOWADDRESS,WORKSPACENAME` - Legacy move event
- `movewindowv2>>WINDOWADDRESS,WORKSPACEID,WORKSPACENAME` - Enhanced move event

## Implementation Considerations

### Event Augmentation Strategy
Many events only provide `WINDOWADDRESS`. Full implementation would:
1. Maintain local window state cache
2. Query `hyprctl -j clients` for missing details
3. Merge event data with cached/queried state
4. Update cache on state changes

### Version Compatibility
- Minimum recommended: Hyprland v0.33.1+
- Some events added in later versions
- Should log Hyprland version on startup

### Performance Optimization
- Cache hyprctl results to avoid redundant queries
- Batch queries when multiple events arrive
- Use async queries to avoid blocking event stream

## Rationale for Non-Implementation

These events weren't implemented because:
1. Core window focus/workspace events cover main use cases
2. Additional events add complexity without clear immediate value
3. State caching and augmentation requires significant architecture
4. Some events are for specialized use cases (screencast, layers)