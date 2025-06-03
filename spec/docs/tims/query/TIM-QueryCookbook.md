# TIM-QueryCookbook: Practical Exocortex Query Examples

*   **Purpose:** Provides a collection of practical query examples (both simplified `exo` syntax and raw SQL) to illustrate how users can retrieve information and insights from their Exocortex.
*   **Source:** Derived from original Vision Document Part V.2.2 and expanded with common use cases.
*   **Dependencies:** Assumes familiarity with Exocortex data model (`raw.events`, `core.artifacts`, `core.entities`, `event_relations`, etc.) and query capabilities (`TIM-HybridSearchPostgreSQL.md`).

## 1. Introduction

This cookbook serves as a starting point for users and developers to understand the types of questions the Exocortex can answer. Queries can range from simple event lookups to complex contextual recall and pattern analysis. The `exo find` and `exo query --eql` commands offer a simplified syntax, while direct SQL provides maximum power.

## 2. Contextual Recall Queries

These queries help retrieve information related to specific past activities or contexts.

### 2.1. Activity around a Specific PKM Note

*   **Goal:** Find browser tabs and terminal commands active around the time a specific PKM note was last edited.
*   **Simplified `exo` (Conceptual EQL):**
    ```bash
    # Assume NOTE_ARTIFACT_ID is known
    # exo query --eql "
    #   LET last_edit_ts = (SELECT last_event_ts_orig FROM core.artifacts WHERE artifact_id = '$NOTE_ARTIFACT_ID');
    #   SELECT * FROM raw.events 
    #   WHERE (source CONTAINS 'browser' OR source CONTAINS 'terminal') 
    #     AND ts_orig BETWEEN ($last_edit_ts - '15 minutes') AND ($last_edit_ts + '15 minutes')
    #   ORDER BY ts_orig;
    # "
    ```
*   **SQL (Illustrative, assumes `domain_web.page_visits` and `domain_terminal.commands` promoted tables):**
    ```sql
    WITH note_context AS (
        SELECT 
            ca.artifact_id, 
            ca.current_title,
            -- Get last modification time of the note's content
            (SELECT MAX(cac.captured_at_ts_orig) 
             FROM core.artifact_contents cac 
             WHERE cac.artifact_id = ca.artifact_id) as last_modified_ts
        FROM core.artifacts ca
        WHERE ca.artifact_id = 'ULID_OF_TARGET_PKM_NOTE' -- Parameter
    )
    -- Browser tabs active (approximated by page visits)
    SELECT 
        'browser_visit' as type, 
        pv.url_visited, 
        pv.page_title,
        pv.ts_orig
    FROM domain_web.page_visits pv, note_context nc
    WHERE pv.ts_orig BETWEEN (nc.last_modified_ts - INTERVAL '15 minutes') AND (nc.last_modified_ts + INTERVAL '15 minutes')
    UNION ALL
    -- Terminal commands
    SELECT 
        'terminal_command' as type,
        tc.command_string,
        tc.cwd,
        tc.ts_orig
    FROM domain_terminal.commands tc, note_context nc -- Assuming domain_terminal.commands table exists
    WHERE tc.ts_orig BETWEEN (nc.last_modified_ts - INTERVAL '15 minutes') AND (nc.last_modified_ts + INTERVAL '15 minutes')
    ORDER BY ts_orig;
    ```

### 2.2. Files Worked On for a Project Recently

*   **Goal:** List files edited or saved in the last 3 days related to "Project Exocortex".
*   **Simplified `exo` (Conceptual EQL):**
    ```bash
    # exo query --eql "
    #   SELECT payload->>'file_path' AS file_path, ts_orig 
    #   FROM raw.events 
    #   WHERE source = 'app.neovim.plugin' AND event_type = 'file_saved' 
    #     AND ts_orig > '3d_ago' 
    #     AND payload->>'file_path' CONTAINS '/project_exocortex/' 
    #   ORDER BY ts_orig DESC;
    # "
    ```
*   **SQL (Querying `raw.events` directly):**
    ```sql
    SELECT 
        payload->>'file_path' AS file_path, 
        ts_orig,
        payload->>'buffer_name' as buffer_name
    FROM raw.events
    WHERE 
        source = 'app.neovim.plugin' AND event_type = 'file_saved'
        AND ts_orig >= (now() - INTERVAL '3 days')
        AND payload->>'file_path' LIKE '%/project_exocortex/%' -- Assumes project files in a dir
    ORDER BY ts_orig DESC;
    ```

## 3. Knowledge Discovery Queries

These queries aim to find connections or related information within the knowledge base.

### 3.1. Find PKM Notes Semantically Similar to Selected Text

*   **Goal:** User selects text in Neovim, find similar PKM notes, excluding obsolete ones.
*   **Mechanism:** Neovim plugin gets selected text, calls `exo embed find-similar-to-text` or an LSP equivalent which uses the hybrid search function.
*   **Simplified `exo` (Illustrative CLI call):**
    ```bash
    # SELECTED_TEXT="The concept of CRDTs for collaborative editing..."
    # exo find --semantic-similar-to-text "$SELECTED_TEXT" \
    #   --type pkm_note \
    #   --tags-none "#obsolete" \
    #   --tags-any "#AI, #distributed_systems" \
    #   --limit 10 --output-format json
    ```
*   **SQL (Core part, uses hybrid search function `hybrid_search_exocortex_artifacts` from `TIM-HybridSearchPostgreSQL.md`):**
    ```sql
    -- Assume p_query_text and p_query_embedding are provided by the application
    -- Also p_target_embedding_model_name, p_fts_limit, p_vector_limit, p_hybrid_limit

    SELECT hs.*
    FROM hybrid_search_exocortex_artifacts(
        p_search_text := 'CRDTs collaborative editing', -- Keywords from selected text
        p_query_embedding := '[...]', -- Embedding vector of selected text
        p_target_embedding_model_name := 'all-MiniLM-L6-v2_local_v1',
        p_filter_artifact_types := ARRAY['pkm_note'],
        p_filter_tags_all := NULL, -- No tags that *all* must be present
        p_filter_tags_any := ARRAY['#AI', '#distributed_systems'],
        p_filter_tags_none := ARRAY['#obsolete'],
        p_fts_limit := 50,
        p_vector_limit := 50,
        p_hybrid_limit := 10
    ) hs
    ORDER BY hs.r_hybrid_score DESC;
    ```

### 3.2. What entities are commonly mentioned with "Project X"?

*   **Goal:** Find persons, organizations, or topics frequently co-occurring in notes/events related to "Project X".
*   **SQL (Using Knowledge Graph tables):**
    ```sql
    WITH project_x_entity AS (
        SELECT entity_id FROM core.entities WHERE canonical_label = 'Project X' AND entity_type = 'project' LIMIT 1
    ),
    artifacts_related_to_project_x AS (
        -- Find artifacts directly linked to Project X (e.g., PKM notes)
        SELECT target_entity_id AS related_artifact_entity_id
        FROM core.entity_relations
        WHERE source_entity_id = (SELECT entity_id FROM project_x_entity) 
          AND relation_type = 'has_artifact_note' -- Assuming such a relation type
        UNION
        -- Or artifacts tagged with Project X (if tags are also entities)
        SELECT at.target_object_id
        FROM artifact_tags at
        JOIN core.tags t ON at.tag_id = t.tag_id
        WHERE t.tag_name = 'project.project_x' AND at.target_object_type = 'core_artifact'
    ),
    entities_mentioned_in_related_artifacts AS (
        SELECT cer.target_entity_id AS mentioned_entity_id, ce_target.canonical_label, ce_target.entity_type
        FROM core.entity_relations cer
        JOIN core.entities ce_source ON cer.source_entity_id = ce_source.entity_id
        JOIN core.entities ce_target ON cer.target_entity_id = ce_target.entity_id
        WHERE cer.source_entity_id IN (SELECT related_artifact_entity_id FROM artifacts_related_to_project_x)
          AND cer.relation_type = 'mentions_entity' -- Assuming artifact entities mention other entities
          AND cer.target_entity_id != (SELECT entity_id FROM project_x_entity) -- Don't count mentions of Project X itself
          AND ce_target.entity_type IN ('person', 'organization', 'topic_tag_object')
    )
    SELECT canonical_label, entity_type, COUNT(*) as mention_frequency
    FROM entities_mentioned_in_related_artifacts
    GROUP BY canonical_label, entity_type
    ORDER BY mention_frequency DESC
    LIMIT 20;
    ```

## 4. Self-Reflection & Pattern Analysis Queries

These queries help analyze personal behavior, productivity, and cognitive patterns.

### 4.1. Daily Count of Friction Events by Cause

*   **Goal:** Track types of friction logged by the user.
*   **Simplified `exo` (Conceptual EQL):**
    ```bash
    # exo query --eql "
    #   SELECT DATE(ts_orig) AS friction_date, payload->>'perceived_cause_text' AS cause, COUNT(*) 
    #   FROM raw.events 
    #   WHERE source = 'user.meta.friction_log' AND event_type = 'entry_created'
    #   GROUP BY friction_date, cause 
    #   ORDER BY friction_date DESC, COUNT(*) DESC;
    # "
    ```
*   **SQL (Querying `raw.events`):**
    ```sql
    SELECT 
        DATE(ts_orig) AS friction_date,
        payload->>'perceived_cause_text' AS cause,
        COUNT(*) as friction_count
    FROM raw.events
    WHERE 
        source = 'user.meta.friction_log' AND event_type = 'entry_created'
        -- AND ts_orig >= (now() - INTERVAL '30 days') -- Optional: filter for recent period
    GROUP BY 1, 2
    ORDER BY friction_date DESC, friction_count DESC;
    ```

### 4.2. Correlate Mood/Energy with Types of Activity

*   **Goal:** Explore if certain activities correlate with reported mood or energy levels.
*   **SQL (Illustrative, requires promoted tables or complex JSON queries on `raw.events`):**
    ```sql
    -- This is highly conceptual and depends on how mood/energy and activities are logged and structured.
    -- Assumes 'subjective.mood_reported' events and 'activity_segment.identified' events.
    WITH mood_segments AS (
        SELECT 
            id AS mood_event_id,
            ts_orig AS mood_ts,
            payload->'mood_values_jsonb'->>'energy_level_1_to_10' AS energy_level -- Example path
        FROM raw.events
        WHERE source = 'subjective.mood_reported' AND event_type = 'report_created'
          AND payload->'mood_values_jsonb'->>'energy_level_1_to_10' IS NOT NULL
    ),
    activity_segments AS (
        SELECT
            id AS activity_event_id,
            ts_orig AS activity_start_ts,
            (payload->>'ts_end_orig_iso')::timestamptz AS activity_end_ts,
            payload->>'segment_type_user_defined' AS activity_type -- e.g., 'project_work_coding', 'browsing_social_media'
        FROM raw.events
        WHERE source = 'sinex.agent.activity_segmenter' AND event_type = 'activity_segment.identified'
    )
    SELECT 
        ms.energy_level,
        acts.activity_type,
        COUNT(DISTINCT acts.activity_event_id) as count_of_activity_segments,
        AVG(EXTRACT(EPOCH FROM (acts.activity_end_ts - acts.activity_start_ts))/60) as avg_duration_minutes
    FROM mood_segments ms
    JOIN activity_segments acts 
      ON acts.activity_start_ts BETWEEN (ms.mood_ts - INTERVAL '2 hours') AND (ms.mood_ts + INTERVAL '2 hours') -- Activity occurred around mood log
    GROUP BY ms.energy_level, acts.activity_type
    ORDER BY ms.energy_level, count_of_activity_segments DESC;
    ```

## 5. Workflow Support Queries

Queries that help manage tasks or ongoing work.

### 5.1. List Open Tasks for a Specific Project

*   **Goal:** See all open tasks related to "Project Exocortex".
*   **Simplified `exo` (Conceptual EQL):**
    ```bash
    # exo find --type task_item --properties '{"status":"open"}' --tags-any "#project_exocortex, project.exocortex"
    ```
*   **SQL (Querying `core.artifacts` and `artifact_tags`):**
    ```sql
    SELECT 
        ca.artifact_id,
        ca.current_title AS task_title,
        ca.properties->>'priority' AS priority,
        ca.properties->>'due_date_iso' AS due_date
    FROM core.artifacts ca
    LEFT JOIN artifact_tags atags ON ca.artifact_id = atags.target_object_id AND atags.target_object_type = 'core_artifact'
    LEFT JOIN core.tags t ON atags.tag_id = t.tag_id
    WHERE 
        ca.artifact_type = 'task_item'
        AND ca.properties->>'status' = 'open'
        AND (
            t.tag_name ILIKE 'project.exocortex%' OR -- Tag indicates project
            EXISTS ( -- Or task entity is linked to project entity
                SELECT 1 FROM core.entity_relations cer
                JOIN core.entities ce_proj ON cer.target_entity_id = ce_proj.entity_id
                WHERE cer.source_entity_id = ca.artifact_id -- Assuming task artifact_id is also an entity_id or linked
                  AND ce_proj.canonical_label = 'Project Exocortex' AND ce_proj.entity_type = 'project'
                  AND cer.relation_type = 'part_of_project' -- Example relation
            )
        )
    GROUP BY ca.artifact_id, ca.current_title, ca.properties -- Group to avoid duplicates if multiple tags match
    ORDER BY ca.properties->>'priority' DESC NULLS LAST, ca.properties->>'due_date_iso' ASC NULLS LAST;
    ```

This cookbook provides a starting point. Users and agents will develop many more sophisticated queries as the Exocortex data grows and new analytical needs emerge.

