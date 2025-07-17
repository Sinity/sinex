# TIM-NeovimPluginIntegration: Neovim Plugin (Lua) for Exocortex

*   **Relevant ADR:** (N/A directly, implements core PKM/LivingDoc interaction as per ADR-004)
*   **Original UG Context:** Section 15

This TIM details the technical aspects of the `sinnix-nvim` Lua plugin for integrating Neovim with the Exocortex, focusing on PKM features, semantic extraction, and communication patterns.

## 1. Rationale Summary

Neovim is a primary power-user interface for the Exocortex (Vision Doc Part V.1.2). The plugin facilitates deep integration for editing PKM notes (managed via Yjs as per ADR-004), querying Exocortex data, logging meta-events, and leveraging semantic features.

## 2. Core Plugin Architecture (Lua)

*   **Structure:** Standard Neovim plugin structure (e.g., `lua/sinnix_nvim/main.lua`, `lua/sinnix_nvim/pkm.lua`, `lua/sinnix_nvim/lsp.lua`, `lua/sinnix_nvim/telescope.lua`, etc.).
*   **Dependencies:**
    *   `plenary.nvim` (for testing, async tasks, jobs).
    *   `telescope.nvim` (for pickers).
    *   `nvim-treesitter` (for parsing).
    *   (Potentially) `nvim-lspconfig` if using a custom Exocortex LS.
    *   (Potentially) Lua Yjs bindings or an interface to an external Yjs helper process.
*   **Configuration:** Via `require('sinnix_nvim').setup({...})` in user's Neovim config. Options for Exocortex API endpoint/DB connection (if direct), paths, feature flags.

## 3. Communication with Exocortex Backend

### 3.1. Custom Exocortex Language Server (LSP) [UG Sec 15.1, CR4] (Preferred for Rich Semantics)

*   **Mechanism:** A custom Language Server (e.g., Rust/Python) for Exocortex-aware files (Markdown in PKM vault, Living Document buffers). Communicates via LSP (JSON-RPC over stdio).
*   **`sinnix-nvim` acts as LSP Client:** Configured via `nvim-lspconfig`.
*   **Custom LSP Requests/Notifications (Neovim plugin sends, LS handles):**
    *   `$/exocortex/resolveLink`: `params: {uri: DocumentUri, position: Position}`. LS returns resolved target info (`artifact_id`, title, preview).
    *   `$/exocortex/getBacklinks`: `params: {uri: DocumentUri}`. LS returns list of backlinks.
    *   `$/exocortex/getRelatedArtifacts`: `params: {uri: DocumentUri, queryText?: string}`. LS returns semantically similar or contextually related items.
    *   `$/exocortex/logRawEvent`: `params: {source: string, event_type: string, payload: object}`. LS writes event to `core.events`.
    *   `$/exocortex/pkmNoteLoad`: `params: {artifact_id: string}`. LS returns Yjs state vector / initial Markdown for note.
    *   `$/exocortex/pkmNoteSaveYjsUpdates`: `params: {artifact_id: string, yjs_updates_b64: string[]}`. LS persists Yjs updates.
*   **Custom LS Notifications (LS sends, Neovim plugin handles):**
    *   `$/exocortex/agentSuggestion`: `params: {suggestion_id: string, description: string, related_uri?: DocumentUri, actions: [{title: string, command: string}]}`. Plugin displays suggestion (e.g., floating window, virtual text) with actionable buttons.
    *   `$/exocortex/yjsRemoteUpdate`: `params: {artifact_id: string, yjs_update_b64: string}`. Plugin applies remote Yjs update to local document replica.
*   **Client Capabilities:** Plugin registers custom capabilities with LS on initialization.

### 3.2. Msgpack-RPC to Helper Process [UG Sec 15.2, CR4] (Alternative/Complementary)

*   **Mechanism:** Neovim's built-in Msgpack-RPC (`vim.fn.rpcstart`, `vim.fn.rpcrequest`).
*   **Use Cases:**
    *   Similar to LSP for offloading Exocortex queries or processing if a full LS is too heavy for certain tasks.
    *   If a helper process (e.g., Node.js for Yjs) is used for specific functionality.
*   **Optimization:** Debounce frequent calls (e.g., on `CursorMoved`), batch operations.

### 3.3. `exo` CLI Invocation

*   For simpler, non-interactive tasks, the plugin can call the `exo` CLI using `vim.fn.jobstart()` or `plenary.Job`.
*   Parse JSON output from `exo`.
*   Example: `exo pkm tag <current_note_id> add new_tag`.

## 4. PKM Note Editing (Yjs-based, as per ADR-004)

*   **Load Note:**
    1.  User opens PKM note (e.g., via Telescope picker for `core_artifacts`).
    2.  Plugin gets `artifact_id`. Sends `$/exocortex/pkmNoteLoad` to LS/helper.
    3.  LS/helper returns Yjs initial state (e.g., from `core_artifact_contents` snapshot) and subsequent deltas (from `core.pkm_note_yjs_deltas`).
    4.  Plugin initializes/updates its local Yjs document replica for the buffer. Buffer content is populated from Yjs.
*   **Live Edits:**
    1.  `BufChanged`, `TextChanged` Neovim autocmds trigger plugin.
    2.  Plugin calculates diffs from buffer changes.
    3.  Translates diffs into Yjs operations on the local Yjs document.
*   **Save Note (`BufWriteCmd` or custom save command):**
    1.  Plugin gets pending Yjs update blobs from its local Yjs document (changes since last sync with backend).
    2.  Sends these updates via `$/exocortex/pkmNoteSaveYjsUpdates` to LS/helper.
    3.  LS/helper persists updates to `core.pkm_note_yjs_deltas`.
    4.  Backend might asynchronously generate new Markdown snapshot for `core_artifact_contents`.
*   **Remote Updates:** If LS/helper sends `$/exocortex/yjsRemoteUpdate`, plugin applies it to local Yjs doc and updates buffer content (handling cursor position carefully).

## 5. Treesitter Integration for Semantic Extraction [UG Sec 15.3, CR4]

*   **Mechanism:** Use Neovim's built-in Treesitter (`nvim-treesitter`) to parse buffer content.
*   **Custom Queries (`.scm` files in plugin's `queries/markdown/` etc.):**
    *   Wikilinks: `((link_destination) @wikilink (#match? @wikilink "^\\[\\[.*\\]\\]$"))`
    *   Standard Links: `(link_destination) @url`, `(link_text) @link_text`
    *   Exocortex URIs: `(link_destination (_) @exocortex_uri (#match? @exocortex_uri "^(annex_key:|blob_id:|artifact_id:|event_id:|entity_id:)"))`
    *   Tags: `#tag`, `key::value` (may need custom injections or grammar extensions).
    *   Headings: `(atx_heading heading_content: (_) @heading.text)`
*   **Usage in Lua Plugin:**
    ```lua
    -- local ts_utils = require('nvim-treesitter.ts_utils')
    -- local parsers = require('nvim-treesitter.parsers')
    -- local queries = require('nvim-treesitter.query')

    -- function extract_semantic_elements(bufnr)
    //   bufnr = bufnr or vim.api.nvim_get_current_buf()
    //   local lang = parsers.get_buf_lang(bufnr)
    //   if not lang then return {} end
    //   -- Ensure parser for lang is installed

    //   local root = ts_utils.get_root_for_buffer(bufnr)
    //   if not root then return {} end

    //   local elements = { wikilinks = {}, urls = {}, tags = {}, headings = {} }

    //   -- Example for Wikilinks (assuming 'markdown' language and a query named 'wikilinks')
    //   local wikilink_query_str = queries.get_query(lang, "wikilinks") -- From queries/markdown/wikilinks.scm
    //   if wikilink_query_str then
    //     local query = vim.treesitter.query.parse(lang, wikilink_query_str)
    //     for id, node, metadata in query:iter_captures(root, bufnr, 0, -1) do
    //       local capture_name = query.captures[id]
    //       if capture_name == "wikilink.destination" then -- Assuming @wikilink.destination capture
    //         table.insert(elements.wikilinks, vim.treesitter.get_node_text(node, bufnr))
    //       end
    //     end
    //   end
    //   -- Similar logic for other element types using their respective queries
    //   return elements
    // end
    ```
*   **Use Cases:** Populate link resolution for `gf`, provide data for `:ExoLinkNewNote` commands, extract tags for auto-tagging suggestions, build document outline/TOC.

## 6. Concurrency, Caching, Performance [UG Sec 15.4, 15.5, CR4]

*   **Concurrency (Yjs/OT):** The Yjs model (ADR-004) handles concurrent edits to note content. Vector clocks or HLCs might be used in the Yjs update metadata if multi-device sync becomes very fine-grained (currently, backend sequences updates).
*   **Caching (LRU Cache in Lua):**
    *   Cache results from frequent LS/RPC calls (resolved links, backlinks for recently viewed notes, Telescope query results).
    *   Capacity: ~100 items [CR4].
*   **Performance Tuning:**
    *   **Lazy Loading:** Load plugin modules/features on demand.
    *   **Asynchronous Operations:** Use `vim.loop`, `vim.uv`, `vim.rpcrequest`, `vim.defer_fn` for all potentially blocking operations (IPC, complex queries).
    *   **Benchmark Target [CR4]:** Interactive operations < 50-100ms (warm cache).

## 7. Key User Commands and Mappings (Examples)

*   `:ExoFind`: Opens Telescope picker for global Exocortex search.
*   `:ExoPkmNoteNew`: Creates new PKM note (prompts for title, tags; creates `core_artifacts` entry via LS/RPC; opens buffer linked to Yjs doc).
*   `:ExoPkmNoteLink <target_query>`: Links current note to another (prompts to select target if query ambiguous).
*   `:ExoPkmShowBacklinks`: Opens panel with backlinks to current note.
*   `:ExoLogFriction "description"`: Logs a `meta.friction_logged` event.
*   `gf` (go to file/link): Enhanced to use `$/exocortex/resolveLink` for Exocortex-specific links.

