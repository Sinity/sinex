-- Test that validation cache correctly uses payload_hash
DO $$
DECLARE
    test_schema_id ULID;
    test_event_id ULID;
    cache_count INTEGER;
    payload_hash_result TEXT;
    test_payload TEXT := '{"test": "validation_cache_fix"}';
BEGIN
    -- Clean up any existing test data
    DELETE FROM sinex_schemas.validation_cache WHERE EXISTS (
        SELECT 1 FROM sinex_schemas.event_payload_schemas 
        WHERE id = sinex_schemas.validation_cache.schema_id 
        AND schema_name = 'validation_cache_test'
    );
    DELETE FROM sinex_schemas.event_payload_schemas WHERE schema_name = 'validation_cache_test';
    
    -- Create test schema
    INSERT INTO sinex_schemas.event_payload_schemas (id, schema_name, schema_version, schema_content)
    VALUES (gen_ulid(), 'validation_cache_test', '1.0.0', 
            '{"type": "object", "properties": {"test": {"type": "string"}}, "required": ["test"]}')
    RETURNING id INTO test_schema_id;
    
    -- Create test event
    INSERT INTO core.events (id, event_type, source, host, payload, payload_schema_id, source_event_ids)
    VALUES (gen_ulid(), 'test.cache', 'test', 'test-host', test_payload::jsonb, test_schema_id, 
            ARRAY[gen_ulid()]::ULID[])  -- Internal provenance
    RETURNING id INTO test_event_id;
    
    -- Test validation function
    PERFORM sinex_schemas.validate_event_payload(test_event_id);
    
    -- Verify cache entry exists
    SELECT COUNT(*) INTO cache_count
    FROM sinex_schemas.validation_cache 
    WHERE schema_id = test_schema_id;
    
    IF cache_count <> 1 THEN
        RAISE EXCEPTION 'Expected 1 cache entry, found %', cache_count;
    END IF;
    
    -- Verify payload_hash is correct
    SELECT payload_hash INTO payload_hash_result
    FROM sinex_schemas.validation_cache 
    WHERE schema_id = test_schema_id;
    
    IF payload_hash_result <> encode(digest(test_payload, 'sha256'), 'hex') THEN
        RAISE EXCEPTION 'Payload hash mismatch: % vs expected %', 
            payload_hash_result, encode(digest(test_payload, 'sha256'), 'hex');
    END IF;
    
    RAISE NOTICE 'SUCCESS: Validation cache fix verified - uses payload_hash correctly!';
END $$;