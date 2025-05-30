# Sinnix Exocortex

A universal data capture and query system for Linux desktop environments, designed to create a searchable memory of all digital interactions.

## Overview

Sinnix Exocortex captures, stores, and makes queryable all system events and user interactions, creating a comprehensive digital memory that can be searched and analyzed. Think of it as a time machine for your desktop - allowing you to answer questions like "What was that website I visited last Tuesday?" or "Which files did I work on when I had that Zoom call?"

## Architecture

### Data Flow
1. **Ingestors** - Specialized collectors for different data sources (Hyprland, browser, filesystem, etc.)
2. **PostgreSQL + TimescaleDB** - Time-series optimized storage with JSONB for flexible schemas
3. **Query Layer** - SQL-based queries with time-aware operations
4. **CLI** - Simple command-line interface for querying and management

### Core Components

- `ingestors/` - Data collection modules
  - `hyprland/` - Window manager event capture
  - (future: browser, filesystem, audio, etc.)
- `schema/` - Database schemas and migrations
- `cli/` - Command-line tools for querying

## Getting Started

```bash
# Enter development environment
nix develop

# Set up database
createdb exocortex
psql exocortex < schema/mvp_schema.sql

# Run Hyprland ingestor
cd ingestors/hyprland
cargo run

# Query recent events
./cli/exo.py query --last 1h
```

## Philosophy

Inspired by projects like Rewind.ai and research on lifelogging, Sinnix Exocortex aims to augment human memory through comprehensive data capture while maintaining local control and privacy. All data stays on your machine, under your control.

## Status

🚧 **Early MVP** - Currently capturing basic Hyprland events. Many more data sources and query capabilities planned.