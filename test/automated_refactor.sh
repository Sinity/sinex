#!/bin/bash
# Automated test refactoring script

# Files to refactor:
# - test/adversarial/boundary_test.rs (6 queries)
# - test/adversarial/chaos_engineering_test.rs (12 queries)
# - test/adversarial/concurrency_test.rs (19 queries)
# - test/common/schema_test_utils.rs (4 queries)
# - test/integration/checkpoint_consistency_test.rs (34 queries)
# - test/integration/data_corruption_detection_test.rs (6 queries)
# - test/integration/database_test.rs (44 queries)
# - test/integration/end_to_end_workflows_test.rs (4 queries)
# - test/integration/failure_modes_test.rs (4 queries)
# - test/integration/import_deduplication_test.rs (4 queries)
# - test/integration/process_event_test.rs (1 queries)
# - test/integration/provenance_tracking_test.rs (2 queries)
# - test/integration/satellite_architecture_test.rs (1 queries)
# - test/integration/search_service_test.rs (2 queries)
# - test/integration/system_integration_test.rs (23 queries)
# - test/integration/ulid_ordering_verification_test.rs (4 queries)
# - test/performance/baseline_performance_test.rs (4 queries)
# - test/performance/bottleneck_identification_test.rs (1 queries)
# - test/performance/checkpoint_performance_test.rs (9 queries)
# - test/performance/concurrent_load_test.rs (2 queries)
# - test/performance/database_performance_test.rs (19 queries)
# - test/performance/memory_usage_test.rs (1 queries)
# - test/performance/performance_test_runner.rs (1 queries)
# - test/performance/resource_exhaustion_test.rs (6 queries)
# - test/performance/throughput_latency_test.rs (11 queries)
# - test/property/schema_property_test.rs (1 queries)
# - test/property/ulid_property_test.rs (10 queries)
# - test/system/external_test.rs (9 queries)
# - test/system/performance_test.rs (10 queries)
# - test/system/reliability_test.rs (34 queries)
# - test/system/stress_test.rs (39 queries)
# - test/unit/database_test.rs (4 queries)

# Add imports to test files
for file in test/**/*.rs; do
  if grep -q "sqlx::query" "$file"; then
    sed -i '1i use crate::common::query_helpers::TestQueries;' "$file"
    sed -i '1i use crate::common::builders::{TestEventBuilder, TestCheckpointBuilder};' "$file"
  fi
done