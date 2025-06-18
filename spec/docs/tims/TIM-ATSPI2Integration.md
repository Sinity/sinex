# TIM - AT-SPI2 Accessibility Integration

**Category**: Event Source  
**Maturity Level**: L2 - Ready for Implementation  
**Implementation Status**: 0% - Not Started  

## Status Dashboard

### MVP Specification
- [ ] Basic AT-SPI2 dbus interface detection (0%)
- [ ] Focus change event capture (0%)  
- [ ] Text selection event capture (0%)
- [ ] Application switch detection (0%)
- [ ] Core accessibility event schema definition (0%)

### Enhanced Features  
- [ ] Screen reader compatibility testing (0%)
- [ ] Keyboard navigation pattern detection (0%)
- [ ] UI element state change tracking (0%)
- [ ] Cross-application accessibility workflow analysis (0%)
- [ ] Performance optimization for high-frequency events (0%)

### Implementation Checklist
- [ ] Create `accessibility.rs` event source module
- [ ] Implement `AccessibilityEventSource` with AT-SPI2 bindings
- [ ] Define event schemas for focus, selection, and state changes
- [ ] Add accessibility event types to `sinex-events/lib.rs`
- [ ] Create database migration for accessibility event tables
- [ ] Implement filtering for noise reduction (rapid fire events)
- [ ] Add integration tests for AT-SPI2 event capture
- [ ] Document accessibility privacy considerations
- [ ] Add configuration options for event filtering
- [ ] Performance benchmark with screen reader software

## Overview

AT-SPI2 (Assistive Technology Service Provider Interface) integration enables comprehensive accessibility event capture across the Linux desktop environment. This provides insights into how users interact with applications through assistive technologies and keyboard navigation patterns.

## Current Implementation Status

**Verification against codebase:**
- ✅ **Database Infrastructure**: LLM and AI tables exist for processing accessibility data
- ❌ **Event Source**: No AT-SPI2 event source found in `crate/sinex-events/src/`
- ❌ **Event Types**: No accessibility-related event types in `lib.rs`
- ❌ **Configurations**: No accessibility configs in `config/` directory
- ❌ **Migrations**: No accessibility-specific database migrations

## Motivation

Understanding accessibility patterns provides valuable insights into:
- User interaction workflows and preferences
- Application accessibility compliance
- Screen reader usage patterns 
- Keyboard navigation efficiency
- Cross-application workflow analysis

## Technical Requirements

### Core Components

1. **AccessibilityEventSource**
   - AT-SPI2 dbus connection management
   - Event filtering and throttling
   - Error recovery for accessibility service disruptions

2. **Event Types**
   - Focus change events (window, widget, document)
   - Text selection and cursor movement
   - Application state transitions
   - Screen reader speech events
   - Keyboard navigation patterns

3. **Privacy Controls**
   - Configurable event filtering
   - PII redaction for text content
   - Opt-out mechanisms for sensitive applications

### Integration Points

- **Window Manager Events**: Correlate with existing window focus tracking
- **Terminal Events**: Connect with terminal accessibility features  
- **Clipboard Events**: Track accessibility-driven copy/paste actions
- **DBus Events**: Leverage existing dbus monitoring infrastructure

## Implementation Architecture

### Event Source Structure
```rust
pub struct AccessibilityEventSource {
    connection: Connection,
    event_filter: AccessibilityFilter,
    privacy_config: PrivacyConfig,
}

#[async_trait]
impl EventSource for AccessibilityEventSource {
    type Config = AccessibilityConfig;
    const SOURCE_NAME: &'static str = "accessibility";
    
    async fn initialize(ctx: EventSourceContext) -> Result<Self>;
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()>;
}
```

### Event Schema Examples
```rust
#[derive(Serialize, Deserialize)]
pub struct FocusChanged {
    pub source_application: String,
    pub target_application: String, 
    pub element_type: String,        // "window", "button", "text_field"
    pub element_role: String,        // AT-SPI role
    pub element_name: Option<String>, // Redacted if sensitive
}

#[derive(Serialize, Deserialize)]
pub struct TextSelectionChanged {
    pub application: String,
    pub selection_start: i32,
    pub selection_end: i32,
    pub selected_text_hash: Option<String>, // SHA256 if enabled
}
```

## Configuration

### Basic Configuration
```toml
[accessibility]
enabled = true
at_spi_service_timeout = 5000  # milliseconds
max_events_per_second = 100

[accessibility.privacy]
capture_text_content = false
capture_element_names = true
redact_password_fields = true
excluded_applications = ["1password", "bitwarden"]

[accessibility.filtering]
focus_debounce_ms = 250
ignore_rapid_selection_changes = true
min_selection_length = 3
```

## Privacy Considerations

### Data Sensitivity Levels
1. **Low**: Focus changes between applications
2. **Medium**: UI element interactions and navigation patterns  
3. **High**: Text selection content and form field interactions
4. **Critical**: Password fields, secure input areas

### Default Privacy Stance
- Focus events: Captured by default
- Element names: Captured with filtering
- Text content: **Not captured** by default
- Form fields: Application name only

## Performance Considerations

### Event Volume Management
- AT-SPI2 can generate high-frequency events
- Implement debouncing for rapid focus changes
- Use sampling for text selection events
- Batch similar events within time windows

### Resource Usage
- Monitor dbus connection health
- Implement connection recovery mechanisms  
- Limit memory usage for event buffering
- Graceful degradation when AT-SPI2 unavailable

## Testing Strategy

### Unit Tests
- AT-SPI2 connection establishment
- Event filtering and privacy controls
- Configuration validation
- Error recovery mechanisms

### Integration Tests  
- Real accessibility service interaction
- Event correlation with window manager
- Screen reader compatibility testing
- Performance under high event load

### System Tests
- End-to-end accessibility workflow capture
- Cross-application interaction patterns
- Privacy compliance validation
- Resource usage monitoring

## Success Metrics

### Functional Success
- Reliable capture of accessibility events across desktop session
- Effective filtering reduces noise while preserving meaningful interactions  
- Zero data leakage from sensitive applications or form fields
- Seamless integration with existing event processing pipeline

### Performance Success  
- <100ms latency for accessibility event processing
- <50MB memory usage for event source
- Handles >1000 accessibility events/second without loss
- Graceful degradation when accessibility services unavailable

### Privacy Success
- Zero sensitive text content captured without explicit configuration
- Configurable exclusion lists prevent monitoring sensitive applications
- Clear audit trail of what accessibility data is being captured
- Compliance with accessibility user expectations and privacy norms

## Dependencies

### System Requirements
- AT-SPI2 accessibility service (standard on Linux desktops)
- DBus connectivity (existing in Sinex)
- Screen reader software for testing (optional)

### Rust Crates
- `atspi` - AT-SPI2 Rust bindings
- `dbus` - DBus communication (existing dependency)
- `serde` - Event serialization (existing)
- `tokio` - Async runtime (existing)

## Future Enhancements

### Advanced Analytics
- Machine learning on accessibility usage patterns
- Predictive UI navigation suggestions
- Application accessibility scoring
- Cross-application workflow optimization

### Integration Opportunities  
- Voice control event correlation
- Eye tracking integration potential
- Gesture recognition coordination
- Multi-modal interaction analysis

## References

- [AT-SPI2 Documentation](https://www.freedesktop.org/wiki/Accessibility/AT-SPI2/)
- [Linux Accessibility HOWTO](https://tldp.org/HOWTO/Accessibility-HOWTO/)
- [GNOME Accessibility Guide](https://help.gnome.org/users/gnome-help/stable/a11y.html)
- [Rust AT-SPI Bindings](https://docs.rs/atspi/latest/atspi/)