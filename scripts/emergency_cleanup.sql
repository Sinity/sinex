-- Emergency cleanup script for test databases
-- This forcefully terminates connections and drops all test databases

-- First, terminate ALL connections to test databases
SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE datname LIKE 'sinex_test_%' 
  AND datname NOT LIKE '%template%'
  AND pid <> pg_backend_pid();

-- Get list of databases to drop
DO $$
DECLARE
    db_name TEXT;
    counter INT := 0;
BEGIN
    -- Loop through all test databases
    FOR db_name IN 
        SELECT datname 
        FROM pg_database 
        WHERE datname LIKE 'sinex_test_%' 
          AND datname NOT LIKE '%template%'
        ORDER BY datname
    LOOP
        -- Drop each database
        BEGIN
            EXECUTE format('DROP DATABASE IF EXISTS %I', db_name);
            counter := counter + 1;
            IF counter % 100 = 0 THEN
                RAISE NOTICE 'Dropped % databases so far...', counter;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Failed to drop database %: %', db_name, SQLERRM;
        END;
    END LOOP;
    
    RAISE NOTICE 'Total databases dropped: %', counter;
END $$;

-- Verify cleanup
SELECT COUNT(*) as remaining_test_databases
FROM pg_database 
WHERE datname LIKE 'sinex_test_%' 
  AND datname NOT LIKE '%template%';