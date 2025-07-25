# ADR-004: PKM Note Content Management & Synchronization Strategy

*   **Status:** Implemented
*   **Date:** 2024-03-11
*   **Implementation Date:** 2025-07-17
*   **Context & Problem Statement:**
    Personal Knowledge Management (PKM) notes, typically Markdown files, are a core data type for the Exocortex. A robust strategy is needed for how their content is stored, versioned, edited, and synchronized, particularly considering user workflows that may involve external editors like Neovim. Key challenges include:
    1.  **Source of Truth:** Should the filesystem (e.g., a directory of Markdown files) or the Exocortex database be the canonical store for note content?
    2.  **Versioning:** How are changes to notes tracked and previous versions maintained?
    3.  **Synchronization:** If both filesystem and database representations exist, how is consistency maintained, and how are conflicts resolved?
    4.  **Editor Integration:** How do users edit notes using preferred tools (like Neovim) in a way that integrates smoothly with the Exocortex?
    5.  **Conflict Resolution:** How are concurrent edits (e.g., from different devices or an agent modifying a note while the user edits it) handled?

*   **Discussed Options:**

    1.  **Filesystem-First with Bi-Directional DB Sync (Traditional PKM Model):**
        *   **Description:** The user's PKM vault (a directory of Markdown files) is the primary source of truth. An Exocortex agent monitors this directory for changes (e.g., using `inotify`). Changes are parsed, and the content/metadata is synced *into* the Exocortex database (`core_artifacts`, `core_artifact_contents`). Conversely, changes made to notes *via Exocortex interfaces* (e.g., an agent modifying a note, or a future Exocortex web editor) would need to be synced *back* to the corresponding Markdown files on the filesystem.
        *   **Pros:**
            *   High compatibility with existing Markdown editors and tools that operate directly on filesystem vaults (Obsidian, Logseq, simple text editors).
            *   Familiar workflow for users accustomed to file-based PKM.
        *   **Cons:**
            *   **Synchronization Complexity:** Bi-directional synchronization is notoriously difficult to implement robustly. Handling concurrent edits, resolving conflicts (requiring 3-way merge logic for text, e.g., Tree-sitter + `diff3` as discussed in UG Sec 13.3), and ensuring atomicity are significant challenges.
            *   **Conflict Resolution Overhead:** Textual merges can be lossy or produce confusing conflict markers for users.
            *   **Database as Secondary Cache:** The database effectively becomes a (potentially stale) cache or index of the filesystem, rather than the authoritative source.
            *   **Performance for Deep Integration:** Frequent re-parsing of files and complex diffing can be resource-intensive for large vaults or frequent small edits.
            *   **Logical Replication Complexity (UG Sec 13.1):** If using advanced DB features for sync (like PowerSync concepts or PG logical replication), this adds another layer of complexity.

    2.  **Database-Native Content with Unidirectional Filesystem Export/View (Exocortex-Centric):**
        *   **Description:** The Exocortex PostgreSQL database (specifically `core_artifacts` and `core_artifact_contents`) is the **canonical source of truth** for all PKM note content and versions.
            *   **Editing Workflow:**
                1.  To edit a note, its latest version is queried from the database and loaded into the editor (e.g., Neovim, via the Exocortex plugin, potentially as a temporary buffer or by writing to a temporary/cache file).
                2.  Upon saving in the editor, the plugin transmits the *full modified content* (or a set of CRDT deltas, see Option 3) back to the Exocortex backend (e.g., via an API call or a message to an agent).
                3.  The backend processes this as a *new version* of the note. A new row is created in `core_artifact_contents` (linked to the original `core_artifacts.artifact_id`). The `core_artifacts.current_content_id` is updated to point to this new version. All previous versions are retained.
            *   **Filesystem as Optional Read-Only View/Export:** The filesystem PKM vault can be maintained as an *export* of the database content, updated periodically or on demand. Edits made directly to these files (outside the Exocortex-aware editor workflow) would either be ignored, flagged as conflicts to be manually reconciled with the DB version, or trigger an "import as new version" process if the user explicitly indicates the file system change is authoritative for that instance.
        *   **Pros:**
            *   **Single Source of Truth:** Database is unambiguously canonical. Simplifies data integrity and consistency.
            *   **Robust Versioning:** Database handles versioning naturally via new rows in `core_artifact_contents`. No complex Git-like history management needed for content itself (though the Exocortex system config is in Git).
            *   **Eliminates Complex Bi-Directional Sync & Textual Merges:** The primary editing flow does not require file-based 3-way merges. Conflicts are handled at the semantic level (e.g., by CRDTs if used, or by simply creating divergent versions if edits are truly concurrent on the same base version without CRDTs).
            *   **Optimized for Exocortex Integration:** Content is already in the DB, readily available for embedding, linking, agent processing, and rich querying without needing to constantly re-parse files.
        *   **Cons:**
            *   **Editor Integration Required:** Relies heavily on a well-integrated editor plugin (e.g., `sinnix-nvim`) to manage the fetch/save cycle with the database. Users cannot simply edit files in *any* text editor and expect seamless Exocortex integration without some intermediary agent.
            *   **Filesystem View Management:** If a synchronized filesystem view is desired for compatibility with other tools, managing its updates from the database adds a one-way sync component.

    3.  **Database-Native Content with CRDTs (Yjs) for Textual Content (Enhancement to Option 2):**
        *   **Description:** Builds upon Option 2. The actual textual content of notes within `core_artifact_contents` (or a related delta table) is managed as a Yjs CRDT document.
        *   **Editing Workflow:**
            1.  Editor plugin fetches the Yjs document state (or initial state + updates) for a note from the Exocortex backend.
            2.  Local edits in the editor are applied as Yjs operations to the local Yjs document replica.
            3.  On save (or periodically), Yjs "update blobs" (binary diffs) representing local changes are sent to the Exocortex backend.
            4.  Backend applies these updates to its canonical Yjs document for the note. These updates can be stored as immutable deltas in a table like `core_artifact_content_deltas` (as discussed in UG Sec 13.2.2), with periodic full snapshots (e.g., rendered Markdown) stored in `core_artifact_contents`.
        *   **Pros (in addition to Option 2's pros):**
            *   **Conflict-Free Merging:** Yjs is designed for robust, conflict-free merging of concurrent textual edits from different sources (e.g., user editing in Neovim while an agent makes an automated change, or future multi-device editing). This virtually eliminates traditional textual merge conflicts for content.
            *   **Efficient Deltas:** Sending compact Yjs update blobs can be more network-efficient than sending full document content on every save, especially for large notes with small changes.
            *   **Real-time Collaboration Potential:** Provides a foundation for future real-time collaborative editing features.
        *   **Cons:**
            *   **Increased Complexity:** Requires integrating Yjs libraries into both the editor plugin (e.g., Lua Yjs bindings or interface to a JS Yjs instance) and the Exocortex backend.
            *   **Binary Blobs:** Yjs updates are binary. Storing and managing these (and their snapshots) requires careful schema design. Rendered Markdown for easy human/SQL queryability would still be derived from these.

*   **Decision:**
    The Exocortex will adopt **Option 3: Database-Native Content with CRDTs (Yjs) for Textual Content**.
    *   The PostgreSQL database (`core_artifacts` and `core_artifact_contents`, supplemented by a delta store like `core_artifact_content_deltas` for Yjs updates) will be the **canonical source of truth** for all PKM note content and its version history.
    *   **Yjs** will be used as the CRDT for managing the textual content of notes.
    *   The primary editing workflow (e.g., in Neovim via `sinnix-nvim`) will involve fetching the Yjs document state, applying local edits as Yjs operations, and sending Yjs update blobs back to the Exocortex backend upon save.
    *   The backend will store these Yjs updates. Periodically, or on demand, a full snapshot of the note's content (e.g., as rendered Markdown) will be generated from the Yjs document state and stored in `core_artifact_contents.content_text` for easy querying, display, and consumption by agents that expect Markdown. `core_artifacts.current_content_id` will point to this snapshot.
    *   **A local filesystem directory of Markdown files (the "PKM vault") will be treated as an optional, read-only export or a carefully managed uni-directional sync target from the database.** Direct edits to these files will *not* automatically propagate back as the primary means of updating canonical content. If edits are made to these files outside the Exocortex-aware editor flow, an agent might detect changes and offer to "import these changes as a new version" or flag them as a conflict against the DB's canonical state. Bi-directional file sync and complex textual merging are explicitly de-prioritized for the core workflow.

*   **Rationale for Decision:**
    1.  **Single Source of Truth & Data Integrity:** Aligns with the Exocortex principle of having a canonical, database-centric store. Eliminates the complexities and potential inconsistencies of bi-directional file sync.
    2.  **Robust Versioning & Conflict Freedom:** Yjs provides strong guarantees for versioning and conflict-free merging of textual content, crucial for long-term data integrity and future multi-device/collaborative scenarios. This is superior to manual or `diff3`-based textual conflict resolution.
    3.  **Optimized for Agentic Integration:** Canonical content and its semantic structure (via Yjs awareness or Markdown snapshots) are readily available in the database for processing by embedding agents, linking agents, LLM agents, etc., without constant filesystem I/O and parsing.
    4.  **Future-Proofing:** CRDTs provide a solid foundation for more advanced collaborative features or distributed Exocortex deployments.
    5.  **User Feedback Alignment (from SADI ADR-004):** This decision directly reflects user preference for a DB-native approach, prioritizing consistency and robust versioning over traditional file-system-first PKM paradigms for the *canonical store*.
    6.  **Reduced Complexity in Core Workflow:** While Yjs integration adds its own complexity, it removes the even greater complexity of robust bi-directional file sync and textual merge conflict resolution from the critical path of note editing.

*   **Consequences:**
    *   Heavy reliance on the Exocortex editor plugin (e.g., `sinnix-nvim`) for a seamless PKM editing experience. This plugin must handle Yjs document management and communication of updates with the backend.
    *   The Exocortex backend needs components to manage Yjs documents/updates and generate Markdown snapshots.
    *   Users accustomed to directly editing Markdown files in arbitrary editors will need to adapt to the Exocortex-mediated workflow for their canonical notes, or use a uni-directional sync-out from the Exocortex DB to their filesystem for consumption by other tools (understanding that changes in those tools won't automatically sync back).
    *   Detailed technical specifications for Yjs update storage (e.g., `core_artifact_content_deltas` table) and snapshotting strategy are required (see `TIM-PKMContentCRDT_Yjs.md`).
    *   UG Sections 13.1 (Bi-Directional Filesystem-Database Sync) and 13.3 (Hybrid Three-Way Merge for Markdown files) become significantly less relevant for the primary PKM workflow and will be refocused on scenarios like initial import of external Markdown or occasional reconciliation with externally modified files, rather than continuous bi-directional sync.

