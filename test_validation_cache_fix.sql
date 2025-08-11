-- Test script to verify the validation_cache fix
-- This tests that the function uses payload_hash instead of event_id

-- First, let's create a simple test schema 
INSERT INTO sinex_schemas.event_payload_schemas (id, schema_name, schema_version, schema_content)
VALUES (
    gen_ulid(),
    'test_schema',
    '1.0.0',
    '{"type": "object", "properties": {"message": {"type": "string"}}, "required": ["message"]}'
);

-- Get the schema ID
DO $$
DECLARE
    test_schema_id ULID;
    test_event_id ULID;
    validation_cache_count INTEGER;
    payload_hash_test TEXT;
BEGIN
    -- Get the schema ID
    SELECT id INTO test_schema_id 
    FROM sinex_schemas.event_payload_schemas 
    WHERE schema_name = 'test_schema';

    -- Insert a test event with valid payload
    INSERT INTO core.events (
        id, event_type, source, host, payload, payload_schema_id,
        source_event_ids  -- Use internal provenance to satisfy XOR constraint
    ) VALUES (
        gen_ulid(),
        'test.validation',
        'test',
        'test-host',
        '{"message": "hello world"}',
        test_schema_id,
        ARRAY[gen_ulid()]::ULID[]  -- Fake parent event ID
    )
    RETURNING id INTO test_event_id;

    -- Test the validation function
    PERFORM sinex_schemas.validate_event_payload(test_event_id);
    
    -- Check that cache was populated with payload_hash (not event_id)
    SELECT COUNT(*) INTO validation_cache_count
    FROM sinex_schemas.validation_cache
    WHERE schema_id = test_schema_id;
    
    IF validation_cache_count != 1 THEN
        RAISE EXCEPTION 'Expected 1 cache entry, found %', validation_cache_count;
    END IF;
    
    -- Verify the payload_hash was calculated correctly
    SELECT payload_hash INTO payload_hash_test
    FROM sinex_schemas.validation_cache
    WHERE schema_id = test_schema_id;
    
    -- Verify it's a 64-character hex string (SHA256)
    IF LENGTH(payload_hash_test) != 64 THEN
        RAISE EXCEPTION 'Invalid payload hash length: % (expected 64)', LENGTH(payload_hash_test);
    END IF;
    
    -- Verify it matches the expected hash
    IF payload_hash_test != encode(digest('{"message": "hello world"}', 'sha256'), 'hex') THEN
        RAISE EXCEPTION 'Payload hash mismatch: % vs expected %', 
            payload_hash_test, 
            encode(digest('{"message": "hello world"}', 'sha256'), 'hex');
    END IF;
    
    -- Test caching: call validation function again and ensure cache is used
    PERFORM sinex_schemas.validate_event_payload(test_event_id);
    
    -- Cache count should still be 1 (no new entries)
    SELECT COUNT(*) INTO validation_cache_count
    FROM sinex_schemas.validation_cache
    WHERE schema_id = test_schema_id;
    
    IF validation_cache_count != 1 THEN
        RAISE EXCEPTION 'Cache not reused: expected 1 entry, found %', validation_cache_count;
    END IF;

    RAISE NOTICE 'SUCCESS: Validation cache fix is working correctly!';
END $$;