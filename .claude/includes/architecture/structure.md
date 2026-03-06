## Project Structure

```
crate/
├── lib/
│   ├── sinex-primitives/    # Foundation: types, validation, error handling, domain types, IDs
│   ├── sinex-db/            # Database pools, repositories, query helpers
│   ├── sinex-node-sdk/      # Node runtime framework
│   ├── sinex-services/      # Business logic services
│   ├── sinex-schema/        # DB schema + declarative apply engine (library only)
│   └── sinex-macros/        # Proc macros
├── core/
│   ├── sinex-ingestd/       # Ingestion daemon
│   └── sinex-gateway/       # API gateway
└── nodes/
    ├── sinex-fs-ingestor/
    ├── sinex-terminal-ingestor/
    ├── sinex-desktop-ingestor/
    ├── sinex-system-ingestor/
    ├── sinex-document-ingestor/
    ├── sinex-terminal-command-canonicalizer/
    ├── sinex-analytics-automaton/
    └── sinex-health-automaton/

docs/
├── current/                 # Active documentation
│   ├── architecture/        # Architecture docs
│   ├── configuration/       # Environment variables, config
│   ├── testing/             # Testing guides
│   └── getting-started.md   # Onboarding
├── planning/                # Design documents
└── vision/                  # Future direction

tests/
├── e2e/                     # End-to-end pipeline tests
└── ci/                      # CI infrastructure tests

crate/cli/                   # Unified CLI (sinexctl binary)

.config/                     # Tool configuration
├── nextest.toml             # Test runner config
├── clippy.toml              # Lint configuration
├── deny.toml                # Dependency audit
└── ast-grep/                # Code patterns
xtask/                       # Build automation (xtask)
├── src/
│   ├── sandbox/             # Test infrastructure (feature-gated)
│   └── ...
└── Cargo.toml
```
