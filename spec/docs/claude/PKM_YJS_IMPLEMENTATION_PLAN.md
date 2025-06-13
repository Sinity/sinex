# PKM Yjs Implementation Plan - Technical Deep Dive

**Date**: 2025-01-12  
**Priority**: 🔴 CRITICAL - Core user-facing feature  
**Design Status**: ✅ COMPLETE (ADR-004)  
**Implementation Status**: ❌ NOT STARTED

## Executive Summary

The PKM (Personal Knowledge Management) system with Yjs CRDTs is the most critical unimplemented feature. It would transform Sinex from a passive event capture system into an active knowledge workspace. This document provides a detailed implementation plan.

## 🏗️ Architecture Overview

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Neovim Plugin  │────▶│  Exocortex API   │────▶│   PostgreSQL    │
│   (Yjs Client)  │◀────│  (Yjs Server)    │◀────│  (Yjs Storage)  │
└─────────────────┘     └──────────────────┘     └─────────────────┘
         │                       │                         │
         ├── Yjs Updates ───────┤                         │
         ├── State Vectors ─────┤                         │
         └── Markdown Export ───┴─────────────────────────┘
```

## 📊 Database Schema

```sql
-- Main tables for PKM with Yjs
CREATE SCHEMA IF NOT EXISTS pkm;

-- Yjs document state (one per note)
CREATE TABLE pkm.yjs_documents (
    artifact_id ULID PRIMARY KEY REFERENCES core_artifacts(artifact_id),
    -- Yjs state vector (for sync protocol)
    state_vector BYTEA NOT NULL,
    -- Garbage collection state
    gc_state BYTEA,
    -- Client tracking
    active_clients JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Yjs updates/deltas (append-only)
CREATE TABLE pkm.yjs_updates (
    update_id ULID PRIMARY KEY DEFAULT gen_ulid(),
    artifact_id ULID NOT NULL REFERENCES pkm.yjs_documents(artifact_id),
    -- The actual Yjs update blob
    update_data BYTEA NOT NULL,
    -- Client that created this update
    client_id TEXT NOT NULL,
    -- Vector clock for ordering
    clock INTEGER NOT NULL,
    -- Metadata
    byte_size INTEGER NOT NULL,
    ts_created TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- For efficient querying
    INDEX idx_yjs_updates_artifact_ts (artifact_id, ts_created DESC)
);

-- Markdown snapshots (for querying and non-Yjs clients)
CREATE TABLE pkm.markdown_snapshots (
    snapshot_id ULID PRIMARY KEY DEFAULT gen_ulid(),
    artifact_id ULID NOT NULL REFERENCES core_artifacts(artifact_id),
    -- The rendered markdown
    content_markdown TEXT NOT NULL,
    -- Blake3 hash for deduplication
    content_hash BYTEA NOT NULL,
    -- Snapshot metadata
    update_count INTEGER NOT NULL, -- Number of updates since last snapshot
    ts_created TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Link to artifact_contents for compatibility
    content_id ULID REFERENCES core_artifact_contents(content_id),
    UNIQUE(artifact_id, content_hash)
);

-- Conflict tracking (for awareness)
CREATE TABLE pkm.conflict_markers (
    conflict_id ULID PRIMARY KEY DEFAULT gen_ulid(),
    artifact_id ULID NOT NULL REFERENCES core_artifacts(artifact_id),
    -- Conflicting updates
    update_ids ULID[] NOT NULL,
    -- Resolution status
    status TEXT NOT NULL CHECK (status IN ('active', 'resolved', 'ignored')),
    resolution_metadata JSONB,
    ts_detected TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ts_resolved TIMESTAMPTZ
);
```

## 🦀 Rust Backend Implementation

### Core Yjs Service

```rust
// crate/sinex-pkm/src/yjs_service.rs

use yrs::{Doc, Transact, Update, StateVector};
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub struct YjsDocument {
    doc: Doc,
    artifact_id: Ulid,
    last_snapshot: Instant,
    update_count: u32,
}

pub struct YjsService {
    // In-memory document cache
    documents: Arc<RwLock<HashMap<Ulid, Arc<RwLock<YjsDocument>>>>>,
    db_pool: PgPool,
    snapshot_threshold: u32, // Updates before snapshot
}

impl YjsService {
    pub async fn get_document(&self, artifact_id: Ulid) -> Result<Arc<RwLock<YjsDocument>>> {
        // Check cache first
        {
            let docs = self.documents.read().await;
            if let Some(doc) = docs.get(&artifact_id) {
                return Ok(doc.clone());
            }
        }
        
        // Load from database
        self.load_document(artifact_id).await
    }
    
    async fn load_document(&self, artifact_id: Ulid) -> Result<Arc<RwLock<YjsDocument>>> {
        // Load state vector
        let state = sqlx::query!(
            "SELECT state_vector FROM pkm.yjs_documents WHERE artifact_id = $1",
            artifact_id as Ulid
        )
        .fetch_optional(&self.db_pool)
        .await?;
        
        let doc = Doc::new();
        
        if let Some(state_row) = state {
            // Apply state vector
            let state_vector = StateVector::from_binary(&state_row.state_vector)?;
            
            // Load and apply all updates since state vector
            let updates = sqlx::query!(
                "SELECT update_data FROM pkm.yjs_updates 
                 WHERE artifact_id = $1 
                 ORDER BY clock ASC",
                artifact_id as Ulid
            )
            .fetch_all(&self.db_pool)
            .await?;
            
            for update in updates {
                doc.transact_mut(|txn| {
                    txn.apply_update(Update::from_binary(&update.update_data)?);
                });
            }
        }
        
        let yjs_doc = Arc::new(RwLock::new(YjsDocument {
            doc,
            artifact_id,
            last_snapshot: Instant::now(),
            update_count: 0,
        }));
        
        // Cache it
        self.documents.write().await.insert(artifact_id, yjs_doc.clone());
        
        Ok(yjs_doc)
    }
    
    pub async fn apply_update(
        &self, 
        artifact_id: Ulid, 
        update: Vec<u8>,
        client_id: String
    ) -> Result<()> {
        let doc = self.get_document(artifact_id).await?;
        
        // Apply update
        {
            let mut doc_guard = doc.write().await;
            doc_guard.doc.transact_mut(|txn| {
                txn.apply_update(Update::from_binary(&update)?);
            });
            doc_guard.update_count += 1;
        }
        
        // Store in database
        let clock = self.get_next_clock(artifact_id).await?;
        sqlx::query!(
            "INSERT INTO pkm.yjs_updates 
             (artifact_id, update_data, client_id, clock, byte_size)
             VALUES ($1, $2, $3, $4, $5)",
            artifact_id as Ulid,
            &update,
            client_id,
            clock,
            update.len() as i32
        )
        .execute(&self.db_pool)
        .await?;
        
        // Check if we need a snapshot
        if doc.read().await.update_count >= self.snapshot_threshold {
            self.create_snapshot(artifact_id).await?;
        }
        
        Ok(())
    }
    
    async fn create_snapshot(&self, artifact_id: Ulid) -> Result<()> {
        let doc = self.get_document(artifact_id).await?;
        let doc_guard = doc.read().await;
        
        // Extract markdown from Yjs document
        let text = doc_guard.doc.get_or_insert_text("content");
        let markdown = text.get_string(&doc_guard.doc.transact());
        
        // Store snapshot
        let hash = blake3::hash(markdown.as_bytes());
        
        sqlx::query!(
            "INSERT INTO pkm.markdown_snapshots 
             (artifact_id, content_markdown, content_hash, update_count)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (artifact_id, content_hash) DO NOTHING",
            artifact_id as Ulid,
            markdown,
            hash.as_bytes(),
            doc_guard.update_count as i32
        )
        .execute(&self.db_pool)
        .await?;
        
        // Update state vector
        let state_vector = doc_guard.doc.transact().state_vector();
        sqlx::query!(
            "UPDATE pkm.yjs_documents 
             SET state_vector = $1, updated_at = NOW()
             WHERE artifact_id = $2",
            &state_vector.to_binary(),
            artifact_id as Ulid
        )
        .execute(&self.db_pool)
        .await?;
        
        Ok(())
    }
}
```

### API Endpoints

```rust
// crate/sinex-api/src/pkm.rs

use axum::{Json, extract::{Path, State}};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct YjsStateResponse {
    state_vector: Vec<u8>,
    updates: Vec<Vec<u8>>,
    client_id: String,
}

#[derive(Deserialize)]
struct YjsUpdateRequest {
    update: Vec<u8>,
    client_id: String,
}

pub async fn get_yjs_state(
    Path(artifact_id): Path<Ulid>,
    State(yjs_service): State<Arc<YjsService>>,
) -> Result<Json<YjsStateResponse>> {
    let doc = yjs_service.get_document(artifact_id).await?;
    let state_vector = doc.read().await.doc.transact().state_vector().to_binary();
    
    // Get any updates client might have missed
    let updates = vec![]; // TODO: Based on client's state vector
    
    Ok(Json(YjsStateResponse {
        state_vector,
        updates,
        client_id: Ulid::new().to_string(),
    }))
}

pub async fn apply_yjs_update(
    Path(artifact_id): Path<Ulid>,
    State(yjs_service): State<Arc<YjsService>>,
    Json(req): Json<YjsUpdateRequest>,
) -> Result<StatusCode> {
    yjs_service.apply_update(artifact_id, req.update, req.client_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// Mount in router
pub fn pkm_routes() -> Router {
    Router::new()
        .route("/api/pkm/:id/yjs/state", get(get_yjs_state))
        .route("/api/pkm/:id/yjs/update", post(apply_yjs_update))
}
```

## 🌙 Neovim Plugin Implementation

### Core Yjs Integration

```lua
-- lua/sinnix-nvim/pkm/yjs.lua

local M = {}
local yjs = nil -- Will hold FFI bindings or msgpack-rpc client

-- Initialize Yjs bridge
function M.setup(opts)
    if opts.yjs_mode == "ffi" then
        yjs = require('sinnix-nvim.pkm.yjs_ffi')
    else
        -- Default to subprocess mode
        yjs = require('sinnix-nvim.pkm.yjs_rpc')
    end
    yjs.init(opts)
end

-- Document management
local documents = {}

-- Open a PKM note with Yjs backing
function M.open_note(artifact_id)
    -- Check if already open
    if documents[artifact_id] then
        -- Switch to existing buffer
        vim.api.nvim_set_current_buf(documents[artifact_id].bufnr)
        return
    end
    
    -- Fetch initial state from server
    local state = M.fetch_state(artifact_id)
    
    -- Create Yjs document
    local ydoc = yjs.create_doc()
    if state.state_vector then
        yjs.apply_state_vector(ydoc, state.state_vector)
    end
    for _, update in ipairs(state.updates or {}) do
        yjs.apply_update(ydoc, update)
    end
    
    -- Create buffer
    local bufnr = vim.api.nvim_create_buf(true, false)
    vim.api.nvim_buf_set_name(bufnr, "pkm://" .. artifact_id)
    
    -- Set initial content
    local text = yjs.get_text(ydoc, "content")
    local lines = vim.split(text, "\n")
    vim.api.nvim_buf_set_lines(bufnr, 0, -1, false, lines)
    
    -- Track document
    documents[artifact_id] = {
        bufnr = bufnr,
        ydoc = ydoc,
        client_id = state.client_id,
        pending_updates = {},
    }
    
    -- Set up buffer autocmds
    M.setup_buffer_sync(artifact_id, bufnr)
    
    -- Switch to buffer
    vim.api.nvim_set_current_buf(bufnr)
end

-- Buffer change tracking
function M.setup_buffer_sync(artifact_id, bufnr)
    local doc = documents[artifact_id]
    
    -- Track text changes
    vim.api.nvim_buf_attach(bufnr, false, {
        on_lines = function(_, _, _, first_line, last_line, last_line_new, byte_count)
            -- Get the change
            local lines = vim.api.nvim_buf_get_lines(
                bufnr, first_line, last_line_new, false
            )
            local text = table.concat(lines, "\n")
            
            -- Convert to Yjs operation
            local update = yjs.create_text_update(
                doc.ydoc,
                "content", 
                first_line,
                last_line - first_line,
                text
            )
            
            -- Queue update
            table.insert(doc.pending_updates, update)
            
            -- Debounced sync
            M.schedule_sync(artifact_id)
        end
    })
    
    -- Save hook
    vim.api.nvim_create_autocmd("BufWritePre", {
        buffer = bufnr,
        callback = function()
            M.sync_now(artifact_id)
        end
    })
end

-- Sync with server
function M.sync_now(artifact_id)
    local doc = documents[artifact_id]
    if not doc or #doc.pending_updates == 0 then
        return
    end
    
    -- Merge pending updates
    local merged = yjs.merge_updates(doc.pending_updates)
    doc.pending_updates = {}
    
    -- Send to server
    local ok = M.send_update(artifact_id, merged, doc.client_id)
    if ok then
        vim.notify("PKM note synced", vim.log.levels.INFO)
    else
        vim.notify("PKM sync failed", vim.log.levels.ERROR)
    end
end

-- Server communication
function M.fetch_state(artifact_id)
    local response = vim.fn.system({
        "curl", "-s",
        "http://localhost:8080/api/pkm/" .. artifact_id .. "/yjs/state"
    })
    return vim.json.decode(response)
end

function M.send_update(artifact_id, update, client_id)
    local response = vim.fn.system({
        "curl", "-s", "-X", "POST",
        "-H", "Content-Type: application/json",
        "-d", vim.json.encode({
            update = vim.base64.encode(update),
            client_id = client_id
        }),
        "http://localhost:8080/api/pkm/" .. artifact_id .. "/yjs/update"
    })
    return vim.v.shell_error == 0
end

return M
```

### Telescope Integration

```lua
-- lua/sinnix-nvim/telescope/pkm.lua

local pickers = require "telescope.pickers"
local finders = require "telescope.finders"
local conf = require("telescope.config").values
local actions = require "telescope.actions"
local action_state = require "telescope.actions.state"

local M = {}

function M.find_notes(opts)
    opts = opts or {}
    
    -- Query exocortex for PKM notes
    local notes = vim.fn.systemlist({
        "exo", "pkm", "list", "--format=json"
    })
    local parsed_notes = vim.tbl_map(vim.json.decode, notes)
    
    pickers.new(opts, {
        prompt_title = "PKM Notes",
        finder = finders.new_table {
            results = parsed_notes,
            entry_maker = function(note)
                return {
                    value = note,
                    display = note.title,
                    ordinal = note.title .. " " .. (note.tags or ""),
                    path = note.artifact_id, -- Used for preview
                }
            end,
        },
        sorter = conf.generic_sorter(opts),
        previewer = conf.file_previewer(opts),
        attach_mappings = function(prompt_bufnr, map)
            actions.select_default:replace(function()
                actions.close(prompt_bufnr)
                local selection = action_state.get_selected_entry()
                -- Open with Yjs
                require('sinnix-nvim.pkm.yjs').open_note(
                    selection.value.artifact_id
                )
            end)
            return true
        end,
    }):find()
end

return M
```

## 🚀 Implementation Phases

### Phase 1: Backend Foundation (1 week)
1. Database migrations
2. Basic Yjs service (no optimization)
3. REST API endpoints
4. Unit tests for Yjs operations

### Phase 2: Neovim Plugin Core (1 week)
1. Basic Yjs FFI or RPC bridge
2. Buffer management
3. Simple open/edit/save flow
4. Manual sync command

### Phase 3: Sync & Conflict Resolution (1 week)
1. Auto-sync on changes
2. Conflict detection
3. State vector optimization
4. Snapshot generation

### Phase 4: User Experience (1 week)
1. Telescope pickers
2. Link completion
3. Backlink panels
4. Status indicators

### Phase 5: Production Hardening (1 week)
1. WebSocket for real-time sync
2. Offline support
3. Performance optimization
4. Comprehensive testing

## 🧪 Testing Strategy

### Unit Tests
```rust
#[tokio::test]
async fn test_yjs_apply_update() {
    let service = YjsService::new(test_db_pool()).await;
    let artifact_id = Ulid::new();
    
    // Create document
    service.create_document(artifact_id).await.unwrap();
    
    // Apply update
    let update = create_test_update("Hello, PKM!");
    service.apply_update(artifact_id, update, "test-client".into()).await.unwrap();
    
    // Verify content
    let snapshot = service.get_latest_snapshot(artifact_id).await.unwrap();
    assert_eq!(snapshot.content_markdown, "Hello, PKM!");
}
```

### Integration Tests
```lua
describe("PKM Yjs Integration", function()
    it("syncs changes to server", function()
        -- Open note
        local artifact_id = create_test_note()
        require('sinnix-nvim.pkm.yjs').open_note(artifact_id)
        
        -- Make changes
        vim.api.nvim_buf_set_lines(0, 0, -1, false, {"# Test Note", "Content"})
        
        -- Save
        vim.cmd("write")
        
        -- Verify on server
        local saved = fetch_note_content(artifact_id)
        assert.equals(saved, "# Test Note\nContent")
    end)
end)
```

## 🎨 User Experience Details

### Visual Indicators
```lua
-- Show sync status in statusline
vim.o.statusline = vim.o.statusline .. " %{SinnixPkmStatus()}"

function _G.SinnixPkmStatus()
    local artifact_id = vim.b.sinnix_artifact_id
    if not artifact_id then return "" end
    
    local doc = documents[artifact_id]
    if not doc then return "" end
    
    if #doc.pending_updates > 0 then
        return "⟳ PKM: Syncing..."
    else
        return "✓ PKM: Synced"
    end
end
```

### Commands
```vim
:PkmOpen <artifact_id>     " Open note with Yjs
:PkmSync                   " Force sync current buffer
:PkmCreate <title>         " Create new PKM note
:PkmSearch                 " Telescope picker
:PkmBacklinks             " Show backlinks in split
```

## 🔧 Configuration

```lua
require('sinnix-nvim').setup({
    pkm = {
        yjs_mode = "ffi", -- or "rpc" for subprocess
        api_endpoint = "http://localhost:8080",
        auto_sync = true,
        sync_delay_ms = 1000,
        snapshot_threshold = 100, -- Updates before snapshot
    }
})
```

## 📊 Performance Considerations

1. **Document Cache**: Keep recently used Yjs documents in memory
2. **Batch Updates**: Merge multiple small edits before syncing
3. **Lazy Loading**: Only load document content when accessed
4. **Snapshot Strategy**: Balance between query performance and storage

## 🔮 Future Enhancements

1. **Real-time Collaboration**: WebSocket sync for live multi-cursor editing
2. **Semantic Merge**: Use AST for markdown-aware conflict resolution  
3. **Version Browser**: UI to explore note history
4. **Mobile Sync**: Lightweight mobile app using same Yjs protocol
5. **Plugin Ecosystem**: API for other plugins to integrate

## 🎯 Success Metrics

- Sub-second open time for notes <1MB
- Zero data loss during concurrent edits
- Seamless offline/online transitions
- 90% of edits require no manual conflict resolution

This implementation would transform Sinex from a passive capture system into an active, conflict-free knowledge workspace that maintains the "database as truth" principle while providing an excellent editing experience.