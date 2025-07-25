# Sinex Documentation

## Quick Links

- [Quick Start Guide](guides/quick-start.md) - Get Sinex running in 15 minutes
- [Operations Manual](operations/manual.md) - Day-to-day operations procedures
- [Troubleshooting Guide](operations/troubleshooting.md) - Common issues and solutions
- [Project Status](project-status.md) - Current implementation status and roadmap
- [Security Guide](security.md) - Security best practices and threat mitigation

## Documentation Structure

### `/docs/guides/`
User-facing guides and tutorials:
- Quick Start Guide
- Installation Guide
- Configuration Guide

### `/docs/operations/`
Operational documentation for system administrators:
- Operations Manual
- Troubleshooting Guide
- Disaster Recovery Plan

### `/docs/architecture/`
Technical architecture documentation:
- System Architecture
- Database Schema
- Satellite Implementation Patterns
- Future designs (Tagging System, Event Relations)

### `/docs/archive/`
Historical documentation and design specs:
- Original TIMs (Technical Implementation Modules)
- ADRs (Architecture Decision Records)
- Migration guides and deprecated designs

## Development Documentation

For development-specific documentation, see:
- `/spec/` - Active specifications and design documents
- `/crate/*/README.md` - Individual crate documentation
- Rustdoc - Run `cargo doc --open` for API documentation
- `/nixos/README.md` - NixOS deployment and development guide