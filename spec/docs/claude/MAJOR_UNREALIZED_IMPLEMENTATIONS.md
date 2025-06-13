# Major Unrealized Implementations in Sinex Exocortex

**Date**: 2025-01-12  
**Current Implementation**: ~15-20% of vision  
**Purpose**: Document the major unrealized but well-designed components and what implementing them would involve

## 🎯 Overview

The Sinex Exocortex has ambitious, well-architected designs for many components that remain unimplemented. These designs are "solid and obvious" - they have clear architectural patterns, specific technology choices, and detailed specifications in TIMs and ADRs. This document outlines what implementing these major components would involve.

## 1. PKM Integration with Yjs CRDTs (ADR-004) 🔴 Critical

**Current State**: ❌ NOT IMPLEMENTED  
**Design Quality**: ✅ COMPLETE - ADR-004 fully specifies the approach  
**Implementation Effort**: High  
**Value**: Extremely High - Core user-facing feature

### What It Would Involve:

#### Backend Implementation:
1. **Database Schema**:
   ```sql
   -- New tables needed
   CREATE TABLE core.pkm_note_yjs_documents (
       artifact_id ULID PRIMARY KEY REFERENCES core_artifacts(artifact_id),
       yjs_state_vector BYTEA NOT NULL,
       last_update_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
   );
   
   CREATE TABLE core.pkm_note_yjs_deltas (
       delta_id ULID PRIMARY KEY DEFAULT gen_ulid(),
       artifact_id ULID NOT NULL REFERENCES core_artifacts(artifact_id),
       update_blob BYTEA NOT NULL,
       client_id TEXT NOT NULL,
       ts_created TIMESTAMPTZ NOT NULL DEFAULT NOW()
   );
   ```

2. **Rust Yjs Integration**:
   - Add `yrs` crate for Yjs in Rust
   - Create `YjsDocumentManager` service that:
     - Maintains in-memory Yjs documents for active notes
     - Applies incoming update blobs
     - Generates state vectors for client sync
     - Periodically snapshots to Markdown in `core_artifact_contents`

3. **API Endpoints**:
   - `GET /api/pkm/note/{id}/yjs-state` - Get initial state + updates
   - `POST /api/pkm/note/{id}/yjs-update` - Apply client updates
   - WebSocket endpoint for real-time sync (future)

#### Neovim Plugin (`sinnix-nvim`):
1. **Lua Yjs Integration**:
   - Either FFI bindings to Yjs or bridge to Node.js process
   - Local Yjs document management
   - Sync protocol implementation

2. **Buffer Management**:
   - Custom buffer type for PKM notes
   - Intercept text changes, convert to Yjs ops
   - Apply remote updates without disrupting cursor

3. **Save Workflow**:
   - On `:w`, send accumulated Yjs updates to backend
   - Handle conflict resolution transparently

#### Migration Path:
1. Import existing Markdown files as initial Yjs documents
2. One-way sync from DB to filesystem for compatibility
3. Gradual transition from file-based to DB-native

## 2. LLM Integration Framework 🔴 Critical

**Current State**: ❌ NOT IMPLEMENTED  
**Design Quality**: ✅ COMPLETE - Detailed in AgenticEcosystem_Architecture.md  
**Implementation Effort**: High  
**Value**: Extremely High - Enables "intelligent" features

### What It Would Involve:

#### Core LLM Infrastructure:
1. **Ollama Integration**:
   ```nix
   # NixOS module
   services.ollama = {
     enable = true;
     models = ["llama2:7b" "mistral:7b-instruct"];
     acceleration = "cpu"; # or "cuda" if GPU available
   };
   ```

2. **Database Schema**:
   ```sql
   CREATE TABLE core.llm_models (
       model_id ULID PRIMARY KEY,
       model_name TEXT UNIQUE NOT NULL,
       provider TEXT NOT NULL,
       endpoint_url TEXT,
       capabilities JSONB,
       cost_per_token JSONB,
       status TEXT
   );
   
   CREATE TABLE core.prompts (
       prompt_id ULID PRIMARY KEY,
       prompt_name TEXT UNIQUE NOT NULL,
       template_text TEXT NOT NULL,
       input_schema JSONB,
       target_llm_family TEXT,
       version INTEGER,
       performance_metrics JSONB
   );
   ```

3. **LLM Router Service**:
   ```rust
   pub struct LlmRouter {
       local_ollama: OllamaClient,
       model_registry: ModelRegistry,
       prompt_cache: PromptCache,
   }
   
   impl LlmRouter {
       pub async fn route_request(&self, req: LlmRequest) -> LlmResponse {
           // Select model based on:
           // - Prompt requirements
           // - Cost constraints  
           // - Privacy settings
           // - Model availability
       }
   }
   ```

4. **Prompt Management Pipeline**:
   - Git repo with YAML prompt definitions
   - CI/CD to validate and load into DB
   - A/B testing framework for prompt variants

#### Agent-LLM Integration:
1. **Standard Agent Pattern**:
   ```rust
   #[async_trait]
   impl EventProcessor for SummarizationAgent {
       async fn process_event(&mut self, event: &RawEvent) -> Result<()> {
           let content = extract_content(event)?;
           
           let summary = self.llm_router
               .complete(PromptRequest {
                   prompt_name: "summarize_document_v2",
                   variables: json!({ "content": content }),
                   max_tokens: 500,
               })
               .await?;
               
           emit_summary_event(summary)?;
           Ok(())
       }
   }
   ```

2. **Cost Tracking**:
   - Log all LLM calls as events
   - Track tokens, latency, cost per agent
   - Budget enforcement at agent level

## 3. Neovim Plugin (`sinnix-nvim`) 🔴 Critical

**Current State**: ❌ NOT IMPLEMENTED  
**Design Quality**: ⚠️ PARTIAL - High-level design exists, needs detailed spec  
**Implementation Effort**: Very High  
**Value**: Extremely High - Primary power-user interface

### What It Would Involve:

#### Core Plugin Architecture:
1. **LSP Server** (Rust):
   ```rust
   // Exocortex Language Server
   pub struct ExocortexLsp {
       db_pool: PgPool,
       yjs_manager: YjsDocumentManager,
       search_engine: SearchEngine,
   }
   
   impl LanguageServer for ExocortexLsp {
       // Provide completions for links, tags
       // Resolve backlinks  
       // Sync Yjs documents
       // Execute Exocortex commands
   }
   ```

2. **Lua Plugin Structure**:
   ```lua
   -- lua/sinnix-nvim/init.lua
   local M = {}
   
   -- Core modules
   M.telescope = require('sinnix-nvim.telescope')
   M.pkm = require('sinnix-nvim.pkm')  
   M.living_doc = require('sinnix-nvim.living_doc')
   M.commands = require('sinnix-nvim.commands')
   
   -- Setup LSP client
   M.setup = function(opts)
       vim.lsp.start_client({
           name = 'exocortex-lsp',
           cmd = {'exocortex-lsp'},
           -- ... configuration
       })
   end
   ```

3. **Telescope Integration**:
   ```lua
   -- Custom pickers for:
   -- - PKM notes (with fuzzy title/content search)
   -- - Raw events (with time/source filters)
   -- - Entities (from knowledge graph)
   -- - Web archives
   -- - Living Document nodes
   ```

4. **Living Document Interface**:
   - Special buffer type with custom syntax
   - Real-time command parsing (e.g., `/task`, `/insight`)
   - Agent interaction UI (floating windows for suggestions)

5. **PKM Note Editing**:
   - Yjs-backed buffers (as described above)
   - WikiLink completion and navigation
   - Backlink/outlink panels
   - Tag management UI

## 4. Semantic Search & Embeddings 🟡 High Priority

**Current State**: ❌ NOT IMPLEMENTED (pgvector mentioned but unused)  
**Design Quality**: ✅ COMPLETE - ADR-005, ADR-007 specify approach  
**Implementation Effort**: Medium  
**Value**: High - Enables semantic queries

### What It Would Involve:

1. **Enable pgvector**:
   ```sql
   CREATE EXTENSION vector;
   
   -- Add to relevant tables
   ALTER TABLE artifact_embeddings 
   ADD COLUMN embedding vector(384);
   
   CREATE INDEX ON artifact_embeddings 
   USING hnsw (embedding vector_cosine_ops);
   ```

2. **Embedding Generation Agent**:
   ```rust
   pub struct EmbeddingAgent {
       model: SentenceTransformer, // e.g., all-MiniLM-L6-v2
       chunk_size: usize,
   }
   
   impl EmbeddingAgent {
       async fn process_artifact(&self, artifact: &Artifact) {
           let chunks = self.chunk_text(&artifact.content);
           for (idx, chunk) in chunks.enumerate() {
               let embedding = self.model.encode(&chunk);
               self.store_embedding(artifact.id, idx, embedding).await?;
           }
       }
   }
   ```

3. **Hybrid Search Implementation**:
   ```sql
   -- RRF function combining vector and FTS
   CREATE FUNCTION hybrid_search(
       query_text TEXT,
       query_embedding vector,
       limit_n INT
   ) RETURNS TABLE(...) AS $$
       -- Implementation per TIM-HybridSearchPostgreSQL.md
   $$ LANGUAGE SQL;
   ```

## 5. Meta-Observability Stack 🟡 High Priority

**Current State**: ❌ NOT IMPLEMENTED  
**Design Quality**: ✅ COMPLETE - Well-specified in SystemOperations  
**Implementation Effort**: Medium  
**Value**: High - Critical for long-term reliability

### What It Would Involve:

1. **Prometheus + Grafana Setup**:
   ```nix
   services.prometheus = {
     enable = true;
     scrapeConfigs = [{
       job_name = "exocortex";
       static_configs = [{
         targets = [
           "localhost:9090" # exocortex metrics
           "localhost:9187" # postgres_exporter
         ];
       }];
     }];
   };
   
   services.grafana = {
     enable = true;
     provision = {
       datasources = [{ type = "prometheus"; }];
       dashboards = [{ path = ./dashboards; }];
     };
   };
   ```

2. **Application Metrics**:
   ```rust
   // Add to all services
   use prometheus::{Counter, Histogram, Registry};
   
   struct Metrics {
       events_ingested: Counter,
       processing_duration: Histogram,
       errors_total: Counter,
   }
   ```

3. **Log Aggregation**:
   - Promtail to ship journald logs to Loki
   - Agent to ingest Loki queries back into raw.events

## 6. Advanced Ingestors Suite 🟡 High Priority

**Current State**: ⚠️ BASIC - Only simple file/terminal/window  
**Design Quality**: ✅ COMPLETE - Detailed TIMs exist  
**Implementation Effort**: High (many components)  
**Value**: High - More data = more insights

### Browser Extension + Native Host:
1. **Extension** (Manifest V3):
   ```javascript
   // Capture navigation, tabs, bookmarks
   chrome.webNavigation.onCompleted.addListener(details => {
       chrome.runtime.sendNativeMessage('sinex_browser_host', {
           event_type: 'page_loaded',
           url: details.url,
           timestamp: Date.now()
       });
   });
   ```

2. **Native Messaging Host** (Rust):
   ```rust
   async fn handle_browser_message(msg: BrowserMessage) {
       let event = RawEvent {
           source: "browser_extension",
           event_type: msg.event_type,
           payload: serde_json::to_value(msg)?,
       };
       db.insert_event(event).await?;
   }
   ```

### Web Archiving Pipeline:
1. **Orchestration Agent**:
   - Triggered by browser visits or manual requests
   - Runs Trafilatura → SingleFile → Browsertrix pipeline
   - Stores WARC/HTML in git-annex
   - Extracts text for core_artifact_contents

### Audio/Visual Capture:
1. **PipeWire Integration**:
   ```bash
   # Continuous audio capture with VAD
   pw-record --target=alsa_input.usb.mic \
             --format=s16 --rate=16000 \
             | vad-filter \
             | segment-audio \
             > audio-chunks/
   ```

2. **Screen Capture**:
   - Use xdg-desktop-portal for Wayland
   - Intelligent frame differencing
   - Store keyframes + deltas

## 7. Query Capabilities & Interfaces 🟡 High Priority

**Current State**: ❌ NOT IMPLEMENTED beyond basic SQL  
**Design Quality**: ⚠️ PARTIAL - Concepts exist, needs specification  
**Implementation Effort**: Medium  
**Value**: Very High - Makes data accessible

### What It Would Involve:

1. **Query Parser & Planner**:
   ```rust
   pub enum QueryClause {
       Since(DateTime),
       Source(String),
       Type(String),
       Contains(String),
       Near(String, f32), // semantic similarity
       Connected(EntityId, i32), // graph hops
   }
   
   impl QueryPlanner {
       fn plan(clauses: Vec<QueryClause>) -> SqlQuery {
           // Convert high-level query to optimized SQL
           // Potentially multiple queries with RRF
       }
   }
   ```

2. **CLI Query Interface**:
   ```bash
   exo find --since "2 hours ago" \
            --type "code_edited" \
            --near "rust async programming" \
            --connected-to "project:sinex"
   ```

3. **Knowledge Graph Queries**:
   - Implement recursive CTEs for traversal
   - Optional Apache AGE for Cypher queries
   - Path-finding algorithms

## 8. Living Document Implementation 🟠 Medium Priority

**Current State**: ❌ NOT IMPLEMENTED  
**Design Quality**: ⚠️ PARTIAL - Concept clear, implementation details needed  
**Implementation Effort**: High  
**Value**: High - Unique differentiator

### What It Would Involve:

1. **Storage Model**:
   - Tree structure in PostgreSQL
   - Each node is a thought/paragraph/command
   - Temporal ordering + semantic clustering

2. **Stream Processing**:
   - Real-time parsing of input
   - Command detection (`/task`, `/insight`)
   - Context accumulation

3. **Agent Integration**:
   - Agents monitor Living Document stream
   - Suggest refactoring, links, extractions
   - LLM-powered semantic analysis

## 🎬 Implementation Sequencing

Given dependencies and value, recommended sequence:

1. **Phase 1: Foundation** (Prerequisites)
   - Semantic Search (pgvector) - enables many features
   - Basic LLM integration (Ollama + router)
   - Observability stack

2. **Phase 2: Core User Features**
   - PKM with Yjs CRDTs
   - Neovim plugin (basic version)
   - Query interfaces

3. **Phase 3: Intelligence**
   - Full agent ecosystem
   - Advanced LLM features
   - Living Document

4. **Phase 4: Completeness**
   - Advanced ingestors
   - Backup/DR
   - Security hardening

## 🚧 Current Blockers

1. **Unified Collector Transition** (ADR-010): Need to complete this before adding new ingestors
2. **Database Schema Finalization**: Some tables still need migration from concept to implementation
3. **Testing Infrastructure**: Need comprehensive test harness before PKM/Yjs work

## 💡 Quick Wins

Some components could be implemented quickly for immediate value:

1. **pgvector activation** - Just needs extension + migration
2. **Basic Prometheus/Grafana** - Standard NixOS modules
3. **Atuin integration** - Simple database reader
4. **Browser bookmarks ingestor** - Simple extension

## 📊 Effort Estimates

| Component | Dev Time | Complexity | Dependencies |
|-----------|----------|------------|--------------|
| PKM + Yjs | 3-4 weeks | Very High | Database, API framework |
| LLM Framework | 2-3 weeks | High | Ollama, database |
| Neovim Plugin | 4-6 weeks | Very High | LSP, PKM, queries |
| Semantic Search | 1 week | Medium | pgvector |
| Observability | 1 week | Low | NixOS config |
| Browser Extension | 2 weeks | Medium | Native messaging |
| Living Document | 3-4 weeks | High | PKM, LLM, agents |

## 🎯 Conclusion

The Sinex Exocortex has extraordinarily well-thought-out designs for these major components. The architecture is sound, technology choices are made, and implementation patterns are clear. What's needed is systematic execution of these designs, starting with the foundational pieces that enable the more advanced features.

The gap between vision (100%) and implementation (15-20%) represents enormous potential value. Each component builds on the others, creating a synergistic system that truly could serve as a "sentient archive" and cognitive augmentation platform.