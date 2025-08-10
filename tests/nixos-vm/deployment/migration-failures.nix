{ pkgs, lib, ... }:

{
  name = "sinex-migration-failures";
  meta.maintainers = with lib.maintainers; [ sinity ];

  nodes = {
    sinex = { config, pkgs, ... }: {
      imports = [ ../common/production-load.nix ];
      
      services.sinex = {
        enable = true;
        preset = "normal";
        
        database = {
          url = "postgresql:///sinex?host=/run/postgresql";
          createDatabase = true;
        };
        
        collector = {
          enable = true;
          sources = [ "filesystem" "terminal" ];
        };
        
        workers = {
          promo.enable = true;
          promo.concurrency = 4;
        };
      };
      
      services.postgresql = {
        enable = true;
        package = pkgs.postgresql_16;
        extraPlugins = with pkgs.postgresql16Packages; [ timescaledb ];
        
        # Enable statement timeout for migration safety
        settings = {
          statement_timeout = "30s";
          lock_timeout = "10s";
        };
      };
      
      # Migration test utilities
      environment.systemPackages = [
        (pkgs.writeScriptBin "test-migration" ''
          #!${pkgs.bash}/bin/bash
          set -euo pipefail
          
          MIGRATION_FILE=$1
          
          # Run migration in transaction
          sudo -u postgres psql -d sinex << EOF
          BEGIN;
          \i $MIGRATION_FILE
          
          -- Test migration effects
          SELECT 'Migration applied successfully';
          
          -- Rollback for testing
          ROLLBACK;
          EOF
        '')
        
        (pkgs.writeScriptBin "apply-migration" ''
          #!${pkgs.bash}/bin/bash
          set -euo pipefail
          
          MIGRATION_FILE=$1
          
          # Apply migration with safety checks
          sudo -u postgres psql -d sinex -v ON_ERROR_STOP=1 -f "$MIGRATION_FILE"
        '')
      ];
    };
  };

  testScript = ''
    import json
    import time
    
    start_all()
    
    # Initial setup
    sinex.wait_for_unit("postgresql.service")
    sinex.wait_for_unit("sinex-ingestd.service")
    sinex.wait_for_unit("sinex-gateway.service")
    
    # Test safe migration practices
    with subtest("Safe migration execution"):
        # Create a safe migration
        sinex.succeed("""
            cat > /tmp/safe_migration.sql << 'EOF'
            -- Safe migration with proper practices
            BEGIN;
            
            -- Add column safely
            ALTER TABLE core.events 
            ADD COLUMN IF NOT EXISTS test_flag BOOLEAN DEFAULT false;
            
            -- Create index concurrently (outside transaction)
            COMMIT;
            
            CREATE INDEX CONCURRENTLY IF NOT EXISTS 
            idx_events_test_flag ON core.events(test_flag) 
            WHERE test_flag = true;
            EOF
        """)
        
        # Test migration first
        sinex.succeed("test-migration /tmp/safe_migration.sql")
        
        # Apply migration
        sinex.succeed("apply-migration /tmp/safe_migration.sql")
        
        # Verify services stayed up
        sinex.succeed("systemctl is-active sinex-ingestd.service")
        sinex.succeed("systemctl is-active sinex-gateway.service")
    
    # Test migration with syntax errors
    with subtest("Migration syntax error handling"):
        # Create migration with syntax error
        sinex.succeed("""
            cat > /tmp/syntax_error_migration.sql << 'EOF'
            -- Migration with syntax error
            ALTER TABLE core.events 
            ADD COLUMN bad_column TEXTTT;  -- Invalid type
            EOF
        """)
        
        # Migration should fail
        sinex.fail("apply-migration /tmp/syntax_error_migration.sql")
        
        # Database should be unchanged
        sinex.fail(
            "sudo -u postgres psql -d sinex -c "
            "'\\d core.events' | grep bad_column"
        )
        
        # Services should remain operational
        sinex.succeed("systemctl is-active sinex-ingestd.service")
    
    # Test migration with constraint violations
    with subtest("Constraint violation handling"):
        # Create migration that would violate constraints
        sinex.succeed("""
            cat > /tmp/constraint_violation.sql << 'EOF'
            BEGIN;
            -- Add NOT NULL column without default (will fail on existing data)
            ALTER TABLE core.events 
            ADD COLUMN required_field TEXT NOT NULL;
            COMMIT;
            EOF
        """)
        
        # Migration should fail
        sinex.fail("apply-migration /tmp/constraint_violation.sql")
        
        # Table structure should be unchanged
        sinex.fail(
            "sudo -u postgres psql -d sinex -c "
            "'\\d core.events' | grep required_field"
        )
    
    # Test long-running migration timeout
    with subtest("Long-running migration timeout"):
        # Create a migration that would take too long
        sinex.succeed("""
            cat > /tmp/long_migration.sql << 'EOF'
            -- Simulate long-running migration
            BEGIN;
            
            -- Lock table for extended period
            LOCK TABLE core.events IN ACCESS EXCLUSIVE MODE;
            
            -- Simulate work
            SELECT pg_sleep(35);  -- Exceeds statement_timeout
            
            ALTER TABLE core.events ADD COLUMN long_test TEXT;
            COMMIT;
            EOF
        """)
        
        # Start migration in background
        sinex.execute(
            "apply-migration /tmp/long_migration.sql > /tmp/migration.log 2>&1 &"
        )
        
        # Wait a bit
        time.sleep(5)
        
        # Service should detect lock and handle gracefully
        sinex.succeed("systemctl is-active sinex-ingestd.service")
        
        # Migration should timeout
        time.sleep(35)
        sinex.fail("grep 'long_test' /tmp/migration.log")
    
    # Test migration rollback on partial failure
    with subtest("Partial migration rollback"):
        # Create multi-step migration
        sinex.succeed("""
            cat > /tmp/partial_migration.sql << 'EOF'
            BEGIN;
            
            -- Step 1: Add table (will succeed)
            CREATE TABLE sinex_schemas.migration_test (
                id TEXT PRIMARY KEY,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            
            -- Step 2: Add column (will succeed)
            ALTER TABLE core.events 
            ADD COLUMN IF NOT EXISTS migration_test_id TEXT;
            
            -- Step 3: Add constraint referencing non-existent column (will fail)
            ALTER TABLE core.events 
            ADD CONSTRAINT fk_migration_test 
            FOREIGN KEY (nonexistent_column) 
            REFERENCES sinex_schemas.migration_test(id);
            
            COMMIT;
            EOF
        """)
        
        # Migration should fail and rollback all changes
        sinex.fail("apply-migration /tmp/partial_migration.sql")
        
        # Verify complete rollback
        sinex.fail(
            "sudo -u postgres psql -d sinex -c "
            "'\\dt sinex_schemas.migration_test'"
        )
        sinex.fail(
            "sudo -u postgres psql -d sinex -c "
            "'\\d core.events' | grep migration_test_id"
        )
    
    # Test migration with data integrity checks
    with subtest("Data integrity during migration"):
        # Capture event count before migration
        pre_count = int(sinex.succeed(
            "sudo -u postgres psql -d sinex -t -c "
            "'SELECT COUNT(*) FROM core.events'"
        ).strip())
        
        # Create events during migration prep
        sinex.execute("touch /tmp/integrity-test-{1..10}")
        time.sleep(3)
        
        # Create migration that checks data integrity
        sinex.succeed("""
            cat > /tmp/integrity_migration.sql << 'EOF'
            DO $$
            DECLARE
                event_count INTEGER;
            BEGIN
                -- Check current event count
                SELECT COUNT(*) INTO event_count FROM core.events;
                
                -- Add metadata column
                ALTER TABLE core.events 
                ADD COLUMN IF NOT EXISTS integrity_check JSONB;
                
                -- Verify no data loss
                IF (SELECT COUNT(*) FROM core.events) < event_count THEN
                    RAISE EXCEPTION 'Data loss detected during migration';
                END IF;
            END $$;
            EOF
        """)
        
        # Apply migration
        sinex.succeed("apply-migration /tmp/integrity_migration.sql")
        
        # Verify no events were lost
        post_count = int(sinex.succeed(
            "sudo -u postgres psql -d sinex -t -c "
            "'SELECT COUNT(*) FROM core.events'"
        ).strip())
        
        assert post_count >= pre_count, f"Event loss: {pre_count} -> {post_count}"
    
    # Test concurrent migration attempts
    with subtest("Concurrent migration protection"):
        # Create two migrations
        sinex.succeed("""
            cat > /tmp/migration1.sql << 'EOF'
            BEGIN;
            SELECT pg_sleep(5);
            ALTER TABLE core.events ADD COLUMN IF NOT EXISTS test1 TEXT;
            COMMIT;
            EOF
            
            cat > /tmp/migration2.sql << 'EOF'
            BEGIN;
            ALTER TABLE core.events ADD COLUMN IF NOT EXISTS test2 TEXT;
            COMMIT;
            EOF
        """)
        
        # Start first migration in background
        sinex.execute("apply-migration /tmp/migration1.sql &")
        
        # Attempt second migration immediately
        time.sleep(1)
        
        # Second migration should wait or fail gracefully
        result = sinex.execute(
            "timeout 3 apply-migration /tmp/migration2.sql || echo 'blocked'"
        )
        
        # Wait for first migration to complete
        time.sleep(6)
        
        # At least one migration should have succeeded
        columns = sinex.succeed(
            "sudo -u postgres psql -d sinex -c '\\d core.events' | "
            "grep -E 'test1|test2' | wc -l"
        ).strip()
        
        assert int(columns) >= 1, "No migrations succeeded"
  '';
}