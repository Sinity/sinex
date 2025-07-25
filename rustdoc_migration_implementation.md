# Rustdoc Migration: Implementation Guide

This guide provides a practical roadmap for migrating Sinex's `/spec/` documentation to rustdoc.

## Phase 1: Infrastructure Setup (Week 1)

### 1.1 Rustdoc Configuration

Create `.cargo/config.toml` additions:
```toml
[doc]
browser.protocol = "https"
browser.host = "sinex.dev"

[build]
rustdocflags = [
    "--html-logo-url", "https://sinex.dev/logo.svg",
    "--html-favicon-url", "https://sinex.dev/favicon.ico",
    "--html-playground-url", "https://play.rust-lang.org/",
    "--default-theme", "ayu",
    "--html-in-header", "doc-header.html",
]
```

### 1.2 Documentation Build Script

Create `scripts/build-docs.sh`:
```bash
#!/usr/bin/env bash

# Generate comprehensive documentation
cargo doc \
    --no-deps \
    --document-private-items \
    --workspace \
    --features doc

# Copy additional assets
cp -r spec/diagram/. target/doc/diagrams/

# Generate search index
cargo doc-index

# Build mdBook for remaining prose docs
mdbook build doc-book/ -d ../target/doc/book/
```

### 1.3 CI Integration

Add to `.github/workflows/docs.yml`:
```yaml
name: Documentation

on:
  push:
    branches: [main]
  pull_request:

jobs:
  build-docs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: dtolnay/rust-toolchain@nightly
      - run: ./scripts/build-docs.sh
      - uses: peaceiris/actions-gh-pages@v3
        if: github.ref == 'refs/heads/main'
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./target/doc
```

## Phase 2: Core Documentation Migration (Week 2-3)

### 2.1 Crate Documentation Template

For each major crate, create comprehensive documentation:

```rust
// Template for crate/sinex-{name}/src/lib.rs

//! # Sinex {Component Name}
//! 
//! {One-line description}
//! 
//! ## Overview
//! 
//! {2-3 paragraph description of purpose and role in system}
//! 
//! ## Architecture
//! 
//! ```text
//! {ASCII diagram showing component relationships}
//! ```
//! 
//! ## Key Concepts
//! 
//! - **{Concept 1}**: {Brief description}
//! - **{Concept 2}**: {Brief description}
//! 
//! ## Quick Start
//! 
//! ```rust
//! use sinex_{name}::{MainType};
//! 
//! // Example usage
//! let instance = MainType::new();
//! instance.do_something()?;
//! ```
//! 
//! ## Implementation Status
//! 
//! | Feature | Status | Progress |
//! |---------|--------|----------|
//! | Core functionality | ✅ Stable | 90% |
//! | Advanced features | 🚧 Beta | 60% |
//! | Future plans | 📋 Planned | 10% |
//! 
//! ## Design Decisions
//! 
//! Major architectural choices:
//! 1. {Decision 1} - {Brief rationale}
//! 2. {Decision 2} - {Brief rationale}
//! 
//! See inline documentation for detailed ADRs.
//! 
//! ## Modules
//! 
//! - [`module1`]: {Brief description}
//! - [`module2`]: {Brief description}

#![doc(
    html_logo_url = "https://sinex.dev/logo.svg",
    html_favicon_url = "https://sinex.dev/favicon.ico"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
```

### 2.2 Module Documentation Structure

For key modules, add comprehensive docs:

```rust
// In module files

//! # {Module Name}
//! 
//! {Brief description of module purpose}
//! 
//! ## Responsibilities
//! 
//! This module handles:
//! - {Responsibility 1}
//! - {Responsibility 2}
//! 
//! ## Architecture
//! 
//! {Explanation of how this module fits in the larger system}
//! 
//! ## Usage Examples
//! 
//! ### Basic Usage
//! ```rust
//! use crate::{module}::{Type};
//! 
//! let instance = Type::new();
//! ```
//! 
//! ### Advanced Patterns
//! ```rust
//! // More complex example
//! ```
```

### 2.3 Type Documentation with TIM Integration

For types that correspond to TIMs:

```rust
/// {Type description}
/// 
/// # Technical Implementation Module
/// 
/// **TIM**: {TIM-Name}  
/// **Maturity**: L{N} - {Status}  
/// **Implementation**: {N}%  
/// **Dependencies**: {List}  
/// **Blocks**: {List}  
/// 
/// ## Overview
/// 
/// {Detailed description from TIM}
/// 
/// ## Design
/// 
/// {Architecture and design decisions}
/// 
/// ## Examples
/// 
/// ```rust
/// // Example code
/// ```
/// 
/// ## See Also
/// 
/// - [`RelatedType`]: {Description}
/// - [Design Doc](https://sinex.dev/tim/{tim-name})
```

## Phase 3: Glossary and Cross-References (Week 4)

### 3.1 Create Glossary Module

```rust
// crate/sinex-core/src/glossary.rs

//! # Sinex Glossary and Terminology
//! 
//! Central reference for all Sinex terms and concepts.
//! 
//! ## Usage
//! 
//! This module provides:
//! - Type definitions for documentation
//! - Searchable terminology
//! - Cross-references to implementations
//! 
//! ## Finding Terms
//! 
//! - Use rustdoc search to find terms
//! - Check the [index](#index) below
//! - Follow cross-references to implementations

/// Glossary index for all terms
pub mod index {
    pub use super::terms::*;
}

/// Term definitions
pub mod terms {
    // Generate documentation for each term
    sinex_glossary! {
        // Format: TermName => "definition" => related_items
        
        Event => "Atomic unit of captured data" => [
            crate::events::Event,
            crate::events::EventBuilder,
            crate::substrate::EventStore,
        ],
        
        Satellite => "Independent service in constellation" => [
            crate::satellite::Satellite,
            crate::satellite::StatefulStreamProcessor,
        ],
        
        // ... more terms
    }
}

/// Macro to generate glossary documentation
#[macro_export]
macro_rules! sinex_glossary {
    (
        $($name:ident => $def:expr => [$($related:path),* $(,)?] ),* $(,)?
    ) => {
        $(
            #[doc = concat!(
                "# ", stringify!($name), "\n\n",
                $def, "\n\n",
                "## Related Items\n\n",
                $(
                    "- [`", stringify!($related), "`](", stringify!($related), ")\n",
                )*
            )]
            pub struct $name;
        )*
    };
}
```

### 3.2 Cross-Reference Automation

Create a build script to verify cross-references:

```rust
// build.rs

use std::fs;
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=src/");
    
    // Verify all cross-references exist
    verify_crossrefs();
    
    // Generate reference index
    generate_reference_index();
}

fn verify_crossrefs() {
    // Parse rustdoc comments for [`Type`] references
    // Verify they resolve to actual types
}

fn generate_reference_index() {
    // Create an index of all documented items
    // Write to generated file
}
```

## Phase 4: ADR Integration (Week 5)

### 4.1 ADR Documentation Pattern

Create a consistent pattern for embedding ADRs:

```rust
/// {Brief description}
/// 
/// # Architectural Decision: {Title}
/// 
/// **ADR**: {Number}  
/// **Status**: {Status}  
/// **Date**: {Date}  
/// 
/// ## Context
/// 
/// {Problem description}
/// 
/// ## Decision
/// 
/// {What was decided}
/// 
/// ## Rationale
/// 
/// {Why this decision}
/// 
/// ## Alternatives
/// 
/// | Option | Pros | Cons |
/// |--------|------|------|
/// | {Opt1} | {P1} | {C1} |
/// 
/// ## Consequences
/// 
/// **Positive**:
/// - {Consequence 1}
/// 
/// **Negative**:
/// - {Consequence 1}
/// 
/// ## Example
/// 
/// ```rust
/// // Implementation example
/// ```
```

### 4.2 ADR Registry

Create a module to track all ADRs:

```rust
// crate/sinex-core/src/decisions.rs

//! # Architectural Decisions
//! 
//! Registry of all architectural decision records (ADRs) in Sinex.
//! 
//! ## Index
//! 
//! | ADR | Title | Status | Location |
//! |-----|-------|--------|----------|
//! | 001 | ULID Primary Keys | Implemented | [`EventId`](crate::types::EventId) |
//! | 002 | Redis Streams | Implemented | [`MessageBus`](crate::bus::MessageBus) |
//! | ... | ... | ... | ... |

/// Generate ADR documentation
macro_rules! document_adr {
    ($number:expr, $title:expr, $module:path) => {
        concat!(
            "# ADR-", $number, ": ", $title, "\n\n",
            "See [", stringify!($module), "](", stringify!($module), ") for implementation and rationale."
        )
    };
}
```

## Phase 5: Diagram Integration (Week 6)

### 5.1 ASCII Diagrams

For simple diagrams, use ASCII art in rustdoc:

```rust
//! ## System Architecture
//! 
//! ```text
//! ┌─────────────┐     ┌─────────────┐
//! │  Satellite  │────▶│   Ingestd   │
//! └─────────────┘     └──────┬──────┘
//!                            │
//!                     ┌──────▼──────┐
//!                     │  PostgreSQL │
//!                     └─────────────┘
//! ```
```

### 5.2 SVG Integration

For complex diagrams, reference external SVGs:

```rust
//! ## Data Flow
//! 
//! ![Data Flow Diagram](https://sinex.dev/diagrams/data_flow.svg)
//! 
//! The diagram above shows:
//! 1. Event capture by satellites
//! 2. Ingestion and validation
//! 3. Storage and indexing
//! 4. Processing by automata
```

### 5.3 Mermaid Support

Consider using a rustdoc plugin for Mermaid:

```rust
//! ```mermaid
//! graph TD
//!     A[Satellite] -->|Events| B[Ingestd]
//!     B --> C[PostgreSQL]
//!     C --> D[Automata]
//!     D -->|Insights| E[User]
//! ```
```

## Phase 6: Testing and Validation (Week 7)

### 6.1 Documentation Tests

Ensure all examples compile:

```rust
#[cfg(doctest)]
mod test_readme {
    macro_rules! doc_comment {
        ($x:expr) => {
            #[doc = $x]
            extern "C" {}
        };
    }
    
    doc_comment!(include_str!("../README.md"));
}
```

### 6.2 Link Validation

Create a test to validate all documentation links:

```rust
#[test]
fn validate_doc_links() {
    let output = std::process::Command::new("cargo")
        .args(&["doc", "--no-deps", "--document-private-items"])
        .output()
        .expect("Failed to run cargo doc");
        
    assert!(output.status.success(), "Documentation build failed");
    
    // Additional link checking logic
}
```

### 6.3 Coverage Reporting

Track documentation coverage:

```bash
#!/usr/bin/env bash

# Check documentation coverage
cargo doc-coverage \
    --min-item-coverage 80 \
    --min-impl-coverage 60 \
    --output-format json > doc-coverage.json

# Generate report
cargo doc-coverage-report
```

## Phase 7: Publishing and Maintenance (Week 8)

### 7.1 Documentation Site Structure

```
docs.sinex.dev/
├── index.html          # Landing page
├── sinex_core/         # Core crate docs
├── sinex_satellite/    # Satellite SDK docs
├── book/               # Prose documentation
├── diagrams/           # Visual assets
└── api/                # API reference
```

### 7.2 Search Configuration

Configure docsrs features:

```toml
# Cargo.toml
[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
```

### 7.3 Maintenance Checklist

Create a documentation maintenance guide:

```markdown
# Documentation Maintenance

## Weekly Tasks
- [ ] Review new code for missing docs
- [ ] Update implementation percentages
- [ ] Check for broken cross-references

## Per Release
- [ ] Update architecture diagrams
- [ ] Review and update examples
- [ ] Regenerate search index
- [ ] Update glossary with new terms

## Quarterly
- [ ] Full documentation audit
- [ ] Update ADR statuses
- [ ] Review and revise vision alignment
```

## Migration Execution Plan

### Week 1: Infrastructure
- Set up rustdoc configuration
- Create build scripts
- Configure CI/CD

### Week 2-3: Core Documentation
- Migrate VISION.md to crate docs
- Convert STAD.md to module docs
- Document core types and traits

### Week 4: Glossary and References
- Create glossary module
- Implement cross-reference system
- Build reference index

### Week 5: ADR Integration
- Embed ADRs at decision points
- Create ADR registry
- Link implementations to decisions

### Week 6: Visual Integration
- Convert diagrams to appropriate formats
- Integrate into documentation
- Create diagram index

### Week 7: Testing
- Implement doc tests
- Validate all links
- Measure coverage

### Week 8: Publishing
- Deploy documentation site
- Configure search
- Create maintenance process

## Success Metrics

- **Coverage**: >80% of public items documented
- **Examples**: All major features have runnable examples
- **Cross-references**: All architectural concepts linked to implementations
- **Search**: Full-text search across all documentation
- **Build time**: Documentation builds in <5 minutes
- **User satisfaction**: Positive feedback on discoverability

## Long-term Benefits

1. **Living Documentation**: Always synchronized with code
2. **Discoverable Architecture**: Decisions visible at implementation points
3. **Testable Examples**: All code samples verified by compiler
4. **Integrated Knowledge**: Glossary, guides, and API docs in one place
5. **Sustainable Process**: Documentation part of development workflow