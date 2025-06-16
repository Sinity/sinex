# TIM-PostgreSQL-AdvancedFeatures: Triggers, Replication, Graph Storage

*   **Relevant ADRs:** (N/A directly, but supports other features)
*   **Original UG Context:** Section 1.3

This TIM covers specific advanced PostgreSQL features utilized or considered by the Exocortex: triggers, streaming replication protocol (primarily for understanding potential future distribution or CDC), and graph storage capabilities within PostgreSQL.

## 1. Triggers

Triggers are used for automated actions in response to database events (INSERT, UPDATE, DELETE).

### 1.1. Execution Order [CR2]

*   For multiple triggers on the same table, event, and timing (e.g., multiple `BEFORE INSERT FOR EACH ROW`): Fired in alphabetical order by trigger name.
*   General order:
    1.  `BEFORE` row-level triggers.
    2.  `BEFORE` statement-level triggers.
    3.  `INSTEAD OF` row-level triggers (for views).
    4.  The operation itself (INSERT/UPDATE/DELETE).
    5.  `AFTER` row-level triggers.
    6.  `AFTER` statement-level triggers.

### 1.2. Performance Impact [CR2]

*   Row-level triggers execute for each affected row and can add significant overhead if complex or numerous. Use judiciously.
*   Simple audit triggers (logging OLD vs NEW) might add ~0.4% overhead per row operation.
*   Statement-level triggers are generally less expensive as they fire once per statement.

### 1.3. Example Use Case: Populating `promotion_queue` (Conceptual)

A trigger on `raw.events` `AFTER INSERT` could call a router function to populate `sinex_schemas.promotion_queue`. (See `TIM-EventIngestionProcessing.md` for the router function and `TIM-AgentManifestManagement.md` for how it uses manifests).

```sql
CREATE OR REPLACE FUNCTION raw.route_new_event_to_promo_queue_trigger_func()
RETURNS TRIGGER AS $$
BEGIN
    -- Call the main routing function, passing the new event's ID
    PERFORM sinex_router.route_raw_event_to_promotion_queue(NEW.id);
    RETURN NEW; -- Result is ignored for AFTER trigger, but good practice
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_raw_events_after_insert_route_to_promo
AFTER INSERT ON raw.events
FOR EACH ROW
EXECUTE FUNCTION raw.route_new_event_to_promo_queue_trigger_func();
```

## 2. Streaming Replication Protocol (Logical Replication)

While full distributed PostgreSQL instances are not an MVP Exocortex feature, understanding logical replication is useful for potential future Change Data Capture (CDC) needs (e.g., syncing `pgvector` to Milvus, see `TIM-VectorSearchGPUAcceleration.md`).

### 2.1. Mechanism [CR2, SR1]

*   Captures changes (Inserts, Updates, Deletes) from a publisher database and applies them to subscribers.
*   Requires `wal_level = logical` in `postgresql.conf`.
*   Involves `PUBLICATION` on the source and `SUBSCRIPTION` on the target.

### 2.2. Key Message Types [CR2]

Logical replication streams messages like `Begin` (transaction start), `Relation` (schema info), `Insert`, `Update`, `Delete`, `Commit`.

### 2.3. Use for Bi-Directional Sync / Loop Prevention [SR1] (If Exocortex Used PG-to-PG Sync)

*   **Origin Filtering with `pg_replication_origin`:** To prevent replication loops if data were to flow A -> B -> A, transactions applied by a sync agent can be tagged with an origin. Publications/subscriptions can then filter changes based on this origin.
*   `CREATE SUBSCRIPTION ... WITH (origin = none)`: Prevents a subscriber from re-propagating changes it applies if it also acts as a publisher.
*   **For Exocortex:** Primary relevance is for CDC to external systems. For filesystem PKM sync (now de-prioritized by ADR-004), simpler agent logic is used.

## 3. Graph Storage within PostgreSQL

The Exocortex Knowledge Graph is primarily modeled using `core_entities` and `core_entity_relations` tables. PostgreSQL offers ways to query this relational graph data.

### 3.1. Recursive Common Table Expressions (CTEs) [CR4]

SQL-standard way to traverse hierarchical or graph-like data.

*   **Example: Finding all entities reachable from a starting entity within N hops:**
    ```sql
    WITH RECURSIVE reachable_entities (source_entity_id, target_entity_id, relation_type, depth, path_array) AS (
        -- Anchor member: direct relations from the start_entity_id
        SELECT
            cer.source_entity_id,
            cer.target_entity_id,
            cer.relation_type,
            1 AS depth,
            ARRAY[cer.source_entity_id, cer.target_entity_id] AS path_array
        FROM core.entity_relations cer
        WHERE cer.source_entity_id = 'ULID_OF_START_ENTITY' -- Parameterize this

        UNION ALL

        -- Recursive member: relations from previously found target_entity_ids
        SELECT
            cer.source_entity_id, -- This is re.target_entity_id from previous step
            cer.target_entity_id,
            cer.relation_type,
            re.depth + 1,
            re.path_array || cer.target_entity_id -- Append to path
        FROM core.entity_relations cer
        JOIN reachable_entities re ON cer.source_entity_id = re.target_entity_id
        WHERE
            re.depth < 5 -- Max depth limit
            AND NOT (cer.target_entity_id = ANY(re.path_array)) -- Cycle detection
    )
    SELECT DISTINCT target_entity_id, depth, relation_type FROM reachable_entities;
    ```
*   **Optimizations [CR4]:**
    *   **Cycle Detection:** `AND NOT (next_node = ANY(current_path_array))`.
    *   **Depth Limits:** `WHERE depth < MAX_DEPTH`.
    *   **Indexing:** Crucial on `core_entity_relations(source_entity_id, relation_type)` and `core_entity_relations(target_entity_id, relation_type)`. Also index `core_entities(entity_id)`.

### 3.2. Apache AGE (AgensGraph Extension) [CR4, SA4]

Provides OpenCypher query language support on top of PostgreSQL.
*   **ADR Implication:** While powerful, ADRs haven't mandated AGE. It remains an *option* for advanced graph querying if SQL CTEs prove insufficient or too cumbersome for complex graph patterns. If used, it would require its own extension setup and schema synchronization.
*   **Setup (from UG Sec 1.3.3.2):**
    ```sql
    CREATE EXTENSION IF NOT EXISTS age;
    LOAD 'age';
    SET search_path = ag_catalog, "$user", public;
    SELECT ag_catalog.create_graph('sinex_knowledge_graph');
    -- Define vlabels (vertex types) and elabels (edge types)
    SELECT ag_catalog.create_vlabel('sinex_knowledge_graph', 'CoreEntity');
    SELECT ag_catalog.create_elabel('sinex_knowledge_graph', 'RELATED_TO');
    -- ... etc. for all entity_types and relation_types
    ```
*   **Data Synchronization:** If AGE is used, triggers on `core_entities` and `core_entity_relations` would be needed to keep the AGE graph synchronized with the relational representation.
    *   Example trigger from UG Sec 1.3.3.2 (to upsert a vertex in AGE when `core.entities` changes):
        ```sql
        CREATE OR REPLACE FUNCTION core.sync_entity_to_age_upsert()
        RETURNS TRIGGER AS $$
        DECLARE
            cypher_query TEXT;
            params JSONB;
        BEGIN
            params := jsonb_build_object(
                'p_entity_id', NEW.entity_id::text,
                'p_entity_type', NEW.entity_type,
                'p_canonical_label', NEW.canonical_label
                -- ... other properties to sync ...
            );
            cypher_query := format('
                MERGE (v:%I {entity_id: $p_entity_id})
                ON CREATE SET v = $p_all_props_map -- Pass all properties as a map
                ON MATCH SET v += $p_all_props_map -- Update existing properties
                RETURN v
            ', NEW.entity_type); -- Use entity_type as AGE vertex label dynamically
            -- Note: Constructing $p_all_props_map carefully is key.
            -- For specific properties:
            -- MERGE (v:CoreEntity {entity_id: $p_entity_id})
            -- ON CREATE SET v.entity_type = $p_entity_type, v.canonical_label = $p_canonical_label
            -- ON MATCH SET v.entity_type = $p_entity_type, v.canonical_label = $p_canonical_label;

            PERFORM ag_catalog.cypher('sinex_knowledge_graph', cypher_query, params);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        -- Attach this trigger to core.entities AFTER INSERT OR UPDATE.
        -- Similar triggers for DELETE and for core.entity_relations.
        ```
*   **Querying with OpenCypher:**
    ```sql
    SELECT * FROM ag_catalog.cypher('sinex_knowledge_graph', $$
        MATCH (a:CoreEntity {canonical_label: 'NixOS'})-[r:RELATED_TO*1..3]->(b:CoreEntity)
        WHERE b.entity_type = 'PkmNote'
        RETURN a.canonical_label, type(r), b.canonical_label, b.entity_id
    $$) AS (source_label TEXT, relation_type TEXT, target_label TEXT, target_id TEXT);
    ```
*   **Indexing in AGE:** Use Cypher's `CREATE INDEX ON :LabelName(propertyName);`.

