# TIM-CanonicalEventSchemas: Core Event Payload JSON Schema Examples

*   **Purpose:** Provides JSON Schema definitions for the `payload` of key canonical `raw.events` types used throughout the Exocortex. These schemas are registered in `sinex_schemas.event_payload_schemas`. This TIM expands on original Vision Document Appendix B.
*   **Source:** Derived from original Vision Document Appendix B and payload descriptions across various Vision/UG sections.
*   **Reference:** For schema registry DDL and management, see `TIM-EventSchemaRegistry.md`. For `_provenance` sub-schema, see Section 1 of this TIM.

## 1. Common `_provenance` Sub-Schema

Many event payloads will share a common `_provenance` block for lineage. This can be defined once and referenced.

```json
{
  "$id": "https://sinnix.exocortex.com/schemas/common/provenance_v1.0.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Common Provenance Block",
  "description": "Standard provenance information often included in event payloads.",
  "type": "object",
  "properties": {
    "agent_id_if_generated": { 
      "type": "string", "format": "ulid", 
      "description": "ULID of the agent (from sinex_schemas.agent_manifests.agent_name, if agent name includes version) that generated this event payload." 
    },
    "input_event_ids_ulid": { 
      "type": "array", "items": {"type": "string", "format": "ulid"}, 
      "description": "ULIDs of raw.events that were primary inputs to generating this derived event." 
    },
    "input_artifact_ids_ulid": { 
      "type": "array", "items": {"type": "string", "format": "ulid"}, 
      "description": "ULIDs of core.artifacts that were primary inputs." 
    },
    "workflow_correlation_id_custom": { 
      "type": "string", 
      "description": "Optional custom correlation ID for a specific multi-step workflow that this event is part of. Not universally mandated, used by specific agents/workflows." 
    },
    "triggering_user_action_id": {
      "type": "string", "format": "ulid",
      "description": "Optional ULID of a user-initiated event (e.g., a CLI command event) that ultimately triggered this event's generation."
    }
  },
  "additionalProperties": true 
}
```

## 2. Key Event Payload Schemas

### 2.1. `desktop.hyprland.ipc_ingestor/window_focused` (`v1.0`)
*(Already generated in previous TIM-CanonicalEventSchemas output, repeated here for completeness if this TIM is the sole source)*
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Hyprland Window Focused Event Payload",
  "description": "Payload for an event indicating a new window has gained focus in Hyprland, captured via IPC.",
  "type": "object",
  "properties": {
    "window_id_hex": { "type": "string", "description": "Hyprland's hex address for the window (e.g., '0x123abc'). From activewindowv2." },
    "window_class": { "type": "string", "description": "WM_CLASS instance name of the focused window." },
    "window_initial_class": { "type": ["string", "null"], "description": "WM_CLASS class name of the focused window." },
    "window_title": { "type": "string", "description": "Current title of the focused window." },
    "pid": { "type": "integer", "description": "Process ID of the window owner." },
    "process_name": { "type": ["string", "null"], "description": "Name of the process owning the window (e.g., from /proc/pid/comm)." },
    "executable_path": { "type": ["string", "null"], "description": "Full path to the executable of the window's process (e.g., from /proc/pid/exe)." },
    "workspace_id": { "type": "integer", "description": "Numeric ID of the workspace containing the focused window." },
    "workspace_name": { "type": ["string", "null"], "description": "User-defined name of the workspace, if available." },
    "monitor_id": { "type": "integer", "description": "Numeric ID of the monitor displaying the workspace." },
    "monitor_description": { "type": ["string", "null"], "description": "Description of the monitor (e.g., 'DP-1')." },
    "is_floating": { "type": "boolean", "description": "True if the window is floating." },
    "is_fullscreen": { "type": "boolean", "description": "True if the window is fullscreen." },
    "geometry": {
      "type": "object",
      "properties": { 
        "x": {"type": "integer"}, "y": {"type": "integer"}, 
        "width": {"type": "integer"}, "height": {"type": "integer"} 
      },
      "required": ["x", "y", "width", "height"],
      "description": "Geometry of the focused window relative to its monitor."
    },
    "_provenance": { "$ref": "https://sinnix.exocortex.com/schemas/common/provenance_v1.0.json" }
  },
  "required": ["window_id_hex", "window_class", "window_title", "pid", "workspace_id", "monitor_id", "is_floating", "is_fullscreen", "geometry"]
}
```

### 2.2. `shell.command.executed_atuin` (`v1.0`)
*(Schema for events ingested from Atuin by `ingestor.atuin_db_reader`)*
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Atuin Shell Command Executed",
  "description": "Payload for a command execution event captured by Atuin and ingested into Exocortex.",
  "type": "object",
  "properties": {
    "command_string": { "type": "string", "description": "The full command string executed." },
    "cwd": { "type": "string", "description": "Current working directory when the command was run." },
    "exit_code": { "type": "integer", "description": "Exit status code of the command." },
    "duration_ns": { "type": "integer", "description": "Duration of the command execution in nanoseconds." },
    "atuin_history_id": { "type": "integer", "description": "Atuin's internal database ID for this history entry." },
    "atuin_session_id": { "type": "string", "description": "Atuin's session identifier for this command." },
    "terminal_session_ulid": { 
      "type": ["string", "null"], "format": "ulid", 
      "description": "ULID of the Exocortex terminal session (e.g., from Asciinema wrapper) if correlation is possible."
    },
    "_provenance": { "$ref": "https://sinnix.exocortex.com/schemas/common/provenance_v1.0.json" }
  },
  "required": ["command_string", "cwd", "exit_code", "duration_ns", "atuin_history_id", "atuin_session_id"]
}
```

### 2.3. `terminal.session.ended` (`v1.0`)
*(Schema for event logged when an Asciinema/script session recording is finalized)*
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Terminal Session Ended",
  "description": "Payload for an event indicating a PTY recording session has ended and the recording is stored.",
  "type": "object",
  "properties": {
    "session_id_ulid": { "type": "string", "format": "ulid", "description": "The unique ULID assigned to this terminal session recording." },
    "recording_tool_name": { "type": "string", "enum": ["asciinema", "script"], "description": "Tool used for recording." },
    "duration_seconds": { "type": ["number", "null"], "description": "Total duration of the recorded session in seconds." },
    "end_ts_iso": { "type": "string", "format": "date-time", "description": "Timestamp when the session ended." },
    "recording_blob_annex_key": { "type": "string", "description": "The git-annex key for the stored .cast or typescript file in core.blobs." },
    "recording_content_hash_blake3": { "type": "string", "description": "BLAKE3 hash of the recording file content." },
    "exit_status_shell": { "type": ["integer", "null"], "description": "Exit status of the main shell process that was recorded." },
    "_provenance": { "$ref": "https://sinnix.exocortex.com/schemas/common/provenance_v1.0.json" }
  },
  "required": ["session_id_ulid", "recording_tool_name", "end_ts_iso", "recording_blob_annex_key", "recording_content_hash_blake3"]
}
```

### 2.4. `sinex.pkm.note_version_saved_yjs` (`v1.0`)
*(This event signals a new Yjs delta was persisted, and potentially a new Markdown snapshot was generated for a PKM note - previously listed in Vision Appendix B example)*
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "PKM Note Yjs Version Saved Event Payload",
  "description": "Payload for an event indicating new Yjs deltas for a PKM note were saved, and optionally a new Markdown snapshot was generated.",
  "type": "object",
  "properties": {
    "artifact_id_note": { "type": "string", "format": "ulid", "description": "ULID of the core.artifacts entry for the PKM note." },
    "yjs_delta_ids_ulid": { 
      "type": "array", "items": {"type": "string", "format": "ulid"}, 
      "description": "ULIDs of the new delta(s) stored in core.pkm_note_yjs_deltas for this save operation." 
    },
    "new_snapshot_content_id_ulid": { "type": ["string", "null"], "format": "ulid", "description": "ULID of the new core.artifact_contents entry if a Markdown snapshot was generated, null otherwise." },
    "snapshot_content_hash_blake3": { "type": ["string", "null"], "description": "BLAKE3 hash of the new Markdown snapshot content, if generated." },
    "previous_snapshot_content_id_ulid": { "type": ["string", "null"], "format": "ulid", "description": "ULID of the previous snapshot content, if applicable." },
    "change_originator_actor": { "type": "string", "description": "Actor that initiated the save (e.g., 'user_neovim_plugin_instance_X', 'agent_AutoTagger_v1')." },
    "parsed_title_updated": { "type": ["string", "null"], "description": "New title if parsed from snapshot and changed." },
    "tags_added": { "type": "array", "items": { "type": "string" }, "nullable": true },
    "tags_removed": { "type": "array", "items": { "type": "string" }, "nullable": true },
    "links_added_count": { "type": ["integer", "null"] },
    "links_removed_count": { "type": ["integer", "null"] },
    "_provenance": { "$ref": "https://sinnix.exocortex.com/schemas/common/provenance_v1.0.json" }
  },
  "required": ["artifact_id_note", "yjs_delta_ids_ulid", "change_originator_actor"]
}
```

### 2.5. `user.meta.friction_log/entry_created` (`v1.0`)
*(Schema for manually logged friction - previously listed in Vision Appendix B example)*
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "User Meta Friction Logged Event Payload",
  "description": "Payload for an event where the user manually logged a point of friction.",
  "type": "object",
  "properties": {
    "friction_id_ulid": { "type": "string", "format": "ulid", "description": "Unique ULID for this specific friction entry." },
    "description_text": { "type": "string", "description": "User's free-text description of the friction encountered." },
    "perceived_cause_text": { "type": ["string", "null"], "description": "User's perceived cause or trigger for the friction." },
    "intensity_score": { 
      "type": ["integer", "null"], "minimum": 1, "maximum": 5, 
      "description": "Subjective intensity of the friction (1=minor, 5=major blockage)."
    },
    "affected_project_entity_id": { "type": ["string", "null"], "format": "ulid", "description": "Optional: ULID of a core.entities project this friction relates to." },
    "affected_task_artifact_id": { "type": ["string", "null"], "format": "ulid", "description": "Optional: ULID of a core.artifacts task item this friction relates to." },
    "related_raw_event_ids_ulid": { 
      "type": "array", "items": {"type": "string", "format": "ulid"}, "nullable": true,
      "description": "Optional: ULIDs of other raw.events that provide context to this friction." 
    },
    "resolution_status": { 
      "type": "string", 
      "enum": ["open", "investigating", "workaround_found", "resolved", "wont_fix", "deferred"], 
      "default": "open",
      "description": "Current status of addressing this friction point."
    },
    "resolution_notes_text": { "type": ["string", "null"], "description": "Notes on how the friction was (or might be) resolved." },
    "tags": { "type": "array", "items": { "type": "string" }, "nullable": true, "description": "User-defined tags for categorizing the friction." },
    "logged_via_tool": { "type": ["string", "null"], "description": "Tool used to log this friction (e.g., 'exo_cli', 'neovim_plugin')." },
    "_provenance": { "$ref": "https://sinnix.exocortex.com/schemas/common/provenance_v1.0.json" }
  },
  "required": ["friction_id_ulid", "description_text", "resolution_status"]
}
```

### 2.6. `sinex.agent.llm_api_call` (`v1.0`)
*(Schema for logging LLM API call details - from Vision IV.2.4)*
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "LLM API Call Event Payload",
  "description": "Payload for an event logging details of a call made to an LLM API.",
  "type": "object",
  "properties": {
    "calling_agent_name": { "type": "string", "description": "Name of the Exocortex agent that made the API call." },
    "prompt_id_used_ulid": { "type": ["string", "null"], "format": "ulid", "description": "ULID of the prompt from core.prompts, if a registered prompt was used." },
    "prompt_name_used": { "type": ["string", "null"], "description": "Name of the prompt used (e.g., 'SummarizeWebArticle_Concise_v1.0')." },
    "model_id_used_ulid": { "type": ["string", "null"], "format": "ulid", "description": "ULID of the model from core.llm_models used for this call." },
    "model_name_invoked": { "type": "string", "description": "Actual model name string sent to the API (e.g., 'ollama/mistral:7b', 'gpt-4-turbo')." },
    "provider_name": { "type": "string", "description": "LLM provider (e.g., 'ollama', 'openai', 'anthropic')." },
    "input_tokens_count": { "type": "integer", "description": "Number of input tokens processed by the LLM." },
    "output_tokens_count": { "type": "integer", "description": "Number of output tokens generated by the LLM." },
    "total_tokens_count": { "type": "integer", "description": "Total tokens (input + output)." },
    "calculated_cost_usd": { "type": ["number", "null"], "description": "Estimated cost of this API call in USD, if applicable." },
    "latency_ms": { "type": "integer", "description": "Duration of the API call in milliseconds." },
    "call_status": { "type": "string", "enum": ["success", "error_api", "error_client", "error_timeout"], "description": "Outcome of the API call." },
    "error_details_if_failed": { "type": ["string", "null"], "description": "Error message or code if the call failed." },
    "request_payload_summary": { "type": ["object", "null"], "description": "Partial/summarized request payload (e.g., key parameters, NO PII from prompt text).", "additionalProperties": true },
    "response_payload_summary": { "type": ["object", "null"], "description": "Partial/summarized response payload (e.g., function call info, NO PII from generated text).", "additionalProperties": true },
    "_provenance": { "$ref": "https://sinnix.exocortex.com/schemas/common/provenance_v1.0.json" }
  },
  "required": [
    "calling_agent_name", "model_name_invoked", "provider_name", 
    "input_tokens_count", "output_tokens_count", "total_tokens_count", 
    "latency_ms", "call_status"
  ]
}
```
This TIM provides a central place for defining key event schemas. As new canonical event types are introduced, their schemas should be added here and registered in `sinex_schemas.event_payload_schemas`.

