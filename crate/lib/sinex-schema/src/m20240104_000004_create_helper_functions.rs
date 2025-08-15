use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Function to safely archive old events
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.archive_events_older_than(
                    cutoff_date TIMESTAMPTZ,
                    batch_size INTEGER DEFAULT 1000
                ) RETURNS TABLE (archived_count BIGINT, last_archived_id ULID) AS $$
                DECLARE
                    total_archived BIGINT := 0;
                    last_id ULID;
                    batch_count INTEGER;
                BEGIN
                    LOOP
                        -- Archive a batch of events
                        WITH archived AS (
                            DELETE FROM core.events
                            WHERE ts_ingest < cutoff_date
                              AND id IN (
                                  SELECT id 
                                  FROM core.events 
                                  WHERE ts_ingest < cutoff_date 
                                  LIMIT batch_size
                              )
                            RETURNING *
                        ), inserted AS (
                            INSERT INTO audit.archived_events
                            SELECT *, NOW(), 'age_based_archival'
                            FROM archived
                            RETURNING id
                        )
                        SELECT COUNT(*), MAX(id) INTO batch_count, last_id
                        FROM inserted;
                        
                        -- Update totals
                        total_archived := total_archived + COALESCE(batch_count, 0);
                        
                        -- Exit if no more events to archive
                        EXIT WHEN batch_count IS NULL OR batch_count = 0;
                        
                        -- Brief pause to avoid overwhelming the system
                        PERFORM pg_sleep(0.1);
                    END LOOP;
                    
                    RETURN QUERY SELECT total_archived, last_id;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Function to get event lineage
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.get_event_lineage(
                    start_event_id ULID,
                    max_depth INTEGER DEFAULT 10
                ) RETURNS TABLE (
                    level INTEGER,
                    event_id ULID,
                    event_type TEXT,
                    source TEXT,
                    ts_orig TIMESTAMPTZ,
                    parent_event_ids ULID[]
                ) AS $$
                WITH RECURSIVE lineage AS (
                    -- Base case: the starting event
                    SELECT 
                        0 as level,
                        e.id,
                        e.event_type,
                        e.source,
                        e.ts_orig,
                        e.source_event_ids as parent_event_ids
                    FROM core.events e
                    WHERE e.id::uuid = start_event_id::uuid
                    
                    UNION ALL
                    
                    -- Recursive case: parent events
                    SELECT 
                        l.level + 1,
                        e.id,
                        e.event_type,
                        e.source,
                        e.ts_orig,
                        e.source_event_ids as parent_event_ids
                    FROM lineage l
                    JOIN core.events e ON e.id = ANY(l.parent_event_ids)
                    WHERE l.level < max_depth
                      AND l.parent_event_ids IS NOT NULL
                )
                SELECT * FROM lineage ORDER BY level;
                $$ LANGUAGE sql STABLE;
                "#,
            )
            .await?;

        // Function to calculate event statistics
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION metrics.get_event_stats(
                    time_window INTERVAL DEFAULT '24 hours'
                ) RETURNS TABLE (
                    source TEXT,
                    event_type TEXT,
                    event_count BIGINT,
                    first_seen TIMESTAMPTZ,
                    last_seen TIMESTAMPTZ,
                    avg_payload_size_bytes NUMERIC,
                    hosts_count BIGINT
                ) AS $$
                BEGIN
                    RETURN QUERY
                    SELECT 
                        e.source,
                        e.event_type,
                        COUNT(*) as event_count,
                        MIN(e.ts_ingest) as first_seen,
                        MAX(e.ts_ingest) as last_seen,
                        AVG(pg_column_size(e.payload))::NUMERIC as avg_payload_size_bytes,
                        COUNT(DISTINCT e.host) as hosts_count
                    FROM core.events e
                    WHERE e.ts_ingest >= NOW() - time_window
                    GROUP BY e.source, e.event_type
                    ORDER BY event_count DESC;
                END;
                $$ LANGUAGE plpgsql STABLE;
                "#,
            )
            .await?;

        // Function to validate event payload against schema
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION sinex_schemas.validate_event_payload(
                    event_id_param ULID
                ) RETURNS TABLE (
                    is_valid BOOLEAN,
                    validation_errors JSONB,
                    schema_name TEXT,
                    schema_version TEXT
                ) AS $$
                DECLARE
                    event_record RECORD;
                    schema_record RECORD;
                    validation_result BOOLEAN;
                    validation_errors_json JSONB;
                    payload_hash_value TEXT;
                BEGIN
                    -- Get the event
                    SELECT e.*, s.schema_name, s.schema_version, s.schema_content
                    INTO event_record
                    FROM core.events e
                    LEFT JOIN sinex_schemas.event_payload_schemas s ON e.payload_schema_id = s.id
                    WHERE e.id::uuid = event_id_param::uuid;
                    
                    IF NOT FOUND THEN
                        RAISE EXCEPTION 'Event % not found', event_id_param;
                    END IF;
                    
                    IF event_record.schema_content IS NULL THEN
                        RETURN QUERY SELECT NULL::BOOLEAN, 
                                           jsonb_build_object('error', 'No schema associated with event')::JSONB,
                                           NULL::TEXT,
                                           NULL::TEXT;
                        RETURN;
                    END IF;
                    
                    -- Calculate payload hash for cache key
                    payload_hash_value := encode(digest(event_record.payload::text, 'sha256'), 'hex');
                    
                    -- Check cache first
                    SELECT vc.is_valid, vc.validation_errors 
                    INTO validation_result, validation_errors_json
                    FROM sinex_schemas.validation_cache vc
                    WHERE vc.payload_hash = payload_hash_value 
                      AND vc.schema_id = event_record.payload_schema_id;
                    
                    -- If found in cache, return cached result
                    IF FOUND THEN
                        RETURN QUERY SELECT validation_result, 
                                           validation_errors_json,
                                           event_record.schema_name,
                                           event_record.schema_version;
                        RETURN;
                    END IF;
                    
                    -- Validate using pg_jsonschema
                    BEGIN
                        validation_result := json_matches_schema(
                            event_record.schema_content::json,
                            event_record.payload::json
                        );
                        
                        IF validation_result THEN
                            validation_errors_json := NULL;
                        ELSE
                            validation_errors_json := jsonb_build_object(
                                'error', 'Schema validation failed',
                                'schema_id', event_record.payload_schema_id::text
                            );
                        END IF;
                    EXCEPTION WHEN OTHERS THEN
                        validation_result := FALSE;
                        validation_errors_json := jsonb_build_object(
                            'error', 'Validation error',
                            'detail', SQLERRM
                        );
                    END;
                    
                    -- Cache the result
                    INSERT INTO sinex_schemas.validation_cache 
                        (payload_hash, schema_id, is_valid, validation_errors)
                    VALUES 
                        (payload_hash_value, event_record.payload_schema_id, validation_result, validation_errors_json)
                    ON CONFLICT (payload_hash, schema_id) 
                    DO UPDATE SET 
                        is_valid = EXCLUDED.is_valid,
                        validation_errors = EXCLUDED.validation_errors,
                        validated_at = NOW();
                    
                    RETURN QUERY SELECT validation_result, 
                                       validation_errors_json,
                                       event_record.schema_name,
                                       event_record.schema_version;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        // Function to find related events by time window
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.find_related_events(
                    reference_event_id ULID,
                    time_window INTERVAL DEFAULT '1 minute',
                    same_host_only BOOLEAN DEFAULT FALSE
                ) RETURNS TABLE (
                    event_id ULID,
                    event_type TEXT,
                    source TEXT,
                    ts_orig TIMESTAMPTZ,
                    time_diff INTERVAL,
                    relevance_score NUMERIC
                ) AS $$
                DECLARE
                    ref_event RECORD;
                BEGIN
                    -- Get reference event details
                    SELECT e.ts_orig, e.ts_ingest, e.host, e.source
                    INTO ref_event
                    FROM core.events e
                    WHERE e.id::uuid = reference_event_id::uuid;
                    
                    IF NOT FOUND THEN
                        RAISE EXCEPTION 'Reference event % not found', reference_event_id;
                    END IF;
                    
                    RETURN QUERY
                    WITH time_ref AS (
                        SELECT COALESCE(ref_event.ts_orig, ref_event.ts_ingest) as ref_time
                    )
                    SELECT 
                        e.id,
                        e.event_type,
                        e.source,
                        e.ts_orig,
                        COALESCE(e.ts_orig, e.ts_ingest) - time_ref.ref_time as time_diff,
                        CASE 
                            WHEN e.source = ref_event.source THEN 1.0
                            WHEN e.host = ref_event.host THEN 0.8
                            ELSE 0.5
                        END * (1.0 - LEAST(1.0, ABS(EXTRACT(EPOCH FROM (COALESCE(e.ts_orig, e.ts_ingest) - time_ref.ref_time))) / EXTRACT(EPOCH FROM time_window))) as relevance_score
                    FROM core.events e, time_ref
                    WHERE e.id::uuid != reference_event_id::uuid
                      AND COALESCE(e.ts_orig, e.ts_ingest) BETWEEN time_ref.ref_time - time_window AND time_ref.ref_time + time_window
                      AND (NOT same_host_only OR e.host = ref_event.host)
                    ORDER BY relevance_score DESC, ABS(EXTRACT(EPOCH FROM time_diff));
                END;
                $$ LANGUAGE plpgsql STABLE;
                "#,
            )
            .await?;

        // Add function comments
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                COMMENT ON FUNCTION core.archive_events_older_than IS 'Archives events older than specified date to archived_events table';
                COMMENT ON FUNCTION core.get_event_lineage IS 'Traces the lineage of an event through its source_event_ids';
                COMMENT ON FUNCTION metrics.get_event_stats IS 'Calculates statistics for events within a time window';
                COMMENT ON FUNCTION sinex_schemas.validate_event_payload IS 'Validates an event payload against its associated JSON schema';
                COMMENT ON FUNCTION core.find_related_events IS 'Finds events related by time proximity and other factors';
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop all helper functions
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP FUNCTION IF EXISTS core.archive_events_older_than(TIMESTAMPTZ, INTEGER);
                DROP FUNCTION IF EXISTS core.get_event_lineage(ULID, INTEGER);
                DROP FUNCTION IF EXISTS metrics.get_event_stats(INTERVAL);
                DROP FUNCTION IF EXISTS sinex_schemas.validate_event_payload(ULID);
                DROP FUNCTION IF EXISTS core.find_related_events(ULID, INTERVAL, BOOLEAN);
                "#,
            )
            .await?;

        Ok(())
    }
}
