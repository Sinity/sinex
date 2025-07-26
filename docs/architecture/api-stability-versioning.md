# API Stability and Versioning Requirements

## Overview

Proactive management of external dependencies and their versions is crucial for Sinex's long-term maintainability. This document outlines version requirements for key dependencies and strategies for managing API instability.

## Key Version Requirements

### System Dependencies

**Window Manager**:
- Hyprland: v0.33.1+ (IPC features/stability)
- Plugin API is unstable, requires careful management

**Browser Extensions**:
- Chrome/Chromium: Chrome 88+ (Manifest V3 mandatory)
- Firefox: Firefox 101+ (MV3 support)

**Linux Kernel Features**:
- eBPF (advanced): Kernel 5.8+ (ring buffers, CO-RE)
- fanotify (admin privs): Pre-v5.1/v5.12 behavior differs
- inotify (watch limits): Kernels 5.15+ have better defaults

**Database**:
- PostgreSQL: Version 15+ recommended (14+ minimum for replication)
- Extension compatibility matrices must be respected:
  - pgvector
  - pgsodium
  - pg_jsonschema
  - TimescaleDB
  - pgx_ulid
  - AGE

**Version Control**:
- Git: v2.25+ (modern sparse checkout)
- git-annex has its own Git version compatibility

**Platform-Specific**:
- macOS FSEvents: File-level events reliable since macOS 10.7+

## Strategies for Managing API Instability

### 1. Dependency Pinning

**Nix Flakes**:
- `flake.lock` pins Nixpkgs and other inputs
- Primary mechanism for reproducible environments
- Commit lock files to version control

**Language-Specific Lock Files**:
- Rust: `Cargo.lock`
- Python: `poetry.lock` or pinned `requirements.txt`
- Node.js: `package-lock.json`
- All lock files must be committed

**Plugin Management**:
- Build C++ plugins against specific Hyprland commits/tags
- Version plugin APIs explicitly

### 2. Abstraction Layers

Create internal facades for volatile external APIs:
- Hyprland IPC/plugin interfaces
- AT-SPI2 accessibility APIs
- Browser extension APIs

Benefits:
- Localize external API changes to adapters
- Enable easier testing with mocks
- Provide consistent internal interfaces

### 3. Versioned Schemas and APIs

**Event Payloads**:
- Use versioned schemas in event system
- Maintain backward compatibility
- Document schema evolution

**Internal APIs**:
- Use semantic versioning for inter-component APIs
- Clearly mark breaking changes
- Provide migration guides

**Database Schema**:
- Managed by versioned SQL migrations
- Never modify existing migrations
- Test rollback procedures

### 4. Comprehensive Testing

**CI/CD Pipeline**:
- Test against specific, pinned dependency versions
- NixOS VM tests with exact system configurations
- Regular dependency update cycles with full regression testing

**Update Process**:
- Dedicated PRs for dependency updates
- Run full test suite before merging
- Document any behavior changes

### 5. Fallback Mechanisms

For critical components relying on unstable APIs:

**Design Fallbacks**:
- AT-SPI2 fails → OCR fallback option
- Hyprland IPC unavailable → Basic X11 capture
- Browser extension blocked → Manual export

**Error Handling**:
- Log errors with detailed context
- Emit structured error events
- Allow disabling unstable features

**User Communication**:
- Clear error messages
- Suggested workarounds
- Configuration options

### 6. Ongoing Maintenance

**Monitoring**:
- Track changelogs of key dependencies
- Subscribe to security advisories
- Monitor performance metrics

**Proactive Adaptation**:
- Allocate time for dependency updates
- Test beta versions of critical dependencies
- Participate in upstream discussions

**Observability**:
- Prometheus metrics for API call failures
- Log aggregation for error patterns
- Performance degradation alerts

## Implementation Guidelines

### Version Specification

Always specify versions explicitly:
```nix
# Good
hyprland = pkgs.hyprland.overrideAttrs {
  version = "0.33.1";
  # ...
};

# Bad
hyprland = pkgs.hyprland;
```

### API Wrapper Pattern

```rust
// Abstract external API behind trait
trait WindowManager {
    fn get_active_window(&self) -> Result<Window>;
    fn list_workspaces(&self) -> Result<Vec<Workspace>>;
}

// Implement for specific versions
struct HyprlandV033 { /* ... */ }
impl WindowManager for HyprlandV033 { /* ... */ }
```

### Migration Support

When APIs change:
1. Support both old and new versions temporarily
2. Log deprecation warnings
3. Provide clear migration timeline
4. Update documentation

## Testing Strategy

### Dependency Matrix Testing

Test against:
- Minimum supported versions
- Latest stable versions
- Beta/RC versions (non-blocking)

### Integration Test Isolation

- Mock external APIs where possible
- Use Docker/NixOS containers for real API testing
- Separate unit tests from integration tests

## Documentation Requirements

For each external dependency:
- Document minimum version required
- List known compatibility issues
- Provide troubleshooting guides
- Include upgrade procedures

This structured approach ensures Sinex remains maintainable and evolvable despite external API changes.