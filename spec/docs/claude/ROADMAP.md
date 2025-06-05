# Sinex Roadmap

## ✅ Implemented (MVP)

### Database
- [x] Basic PostgreSQL schema with UUID primary keys
- [x] TimescaleDB hypertable for `raw.events`
- [x] Core indexes for common queries
- [x] Simple provenance tracking

### Ingestors
- [x] Hyprland compositor integration
  - Window focus events
  - Workspace change events
  - Basic IPC socket connection
- [x] Skeleton for filesystem watcher
- [x] Skeleton for Kitty terminal

### Infrastructure
- [x] Nix flake development environment
- [x] NixOS module for deployment
- [x] Systemd service management
- [x] Basic CLI query tool (`exo.py`)

## 🚧 Immediate Tasks (Phase 2)

These are concrete, ready-to-implement features:

### Database Migration
- [ ] Deploy Phase 2 schema with proper ULID support (#1)
  - Replace custom ULID implementation with PostgreSQL extension (#4)
  - Add event_type field for better categorization
  - Add host and ingestor_version fields

### Core Reliability
- [ ] Implement Dead Letter Queue for failed events (#3)
- [ ] Add health check endpoints (#8)
- [ ] Add batch insert optimization (#5)

### Monitoring
- [ ] Add Prometheus metrics to ingestors (#7)
- [ ] Create Grafana dashboards

### Terminal Integration
- [ ] Implement Kitty remote control protocol (#6)
- [ ] Add Atuin shell history ingestion
- [ ] Integrate Asciinema for session recording

### Data Quality
- [ ] Add event schema validation
- [ ] Implement agent manifest registry

## 📅 Future Features (Phase 3+)

These depend on having more infrastructure in place:

### Advanced Processing
- [ ] Event processing work queue
- [ ] Correlation ID support (once we have multi-step workflows)
- [ ] Event enrichment agents

### Desktop Integration
- [ ] AT-SPI2 accessibility events
- [ ] Clipboard monitoring
- [ ] PipeWire screen capture

### Browser Integration
- [ ] Firefox/Chrome extension
- [ ] Web page archiving
- [ ] Bookmark synchronization

### Knowledge Management
- [ ] PKM note integration
- [ ] Entity extraction
- [ ] Knowledge graph construction

### Advanced Features
- [ ] Semantic search with embeddings
- [ ] LLM-powered analysis agents
- [ ] Mobile companion app

## 💭 Speculative/Research

These are interesting ideas from the vision doc that need more research:

- CRDT-based collaborative editing
- Federated Exocortex instances
- Brain-computer interface integration
- Augmented reality overlays

## Development Principles

1. **Incremental Progress**: Each phase should deliver working functionality
2. **Data First**: Capture is more important than analysis initially
3. **Local First**: Everything runs on user's machine
4. **Privacy by Design**: User owns and controls all data

## Success Metrics

- Phase 2: Reliable capture of desktop activity with <1% data loss
- Phase 3: Rich semantic queries across all captured data
- Phase 4: Proactive AI assistance based on historical patterns