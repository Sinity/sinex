# Preflight Verification

Preflight verification system for node deployment.

This module provides comprehensive preflight checks to ensure nodes can
operate correctly before they begin processing events. Preflight verification
prevents runtime failures by validating all dependencies and prerequisites.

## Verification Categories

- **Configuration**: Validate all required configuration values
- **Database**: Check database connectivity and schema compatibility
- **Resources**: Verify filesystem access, permissions, and disk space
- **Services**: Ensure external services (NATS, ingestd) are reachable

## Usage

Preflight checks are automatically run by the node SDK before starting
event processing. Failed checks will prevent node startup with detailed
error information.
