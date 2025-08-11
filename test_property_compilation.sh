#!/usr/bin/env bash

# Test compilation of new property test files
set -e

echo "Testing compilation of new property test files..."

# Check path sanitization property test
echo "Checking path_sanitization_property_test.rs..."
cargo check --test path_sanitization_property_test || echo "Path sanitization test has issues"

# Check time range property test  
echo "Checking time_range_property_test.rs..."
cargo check --test time_range_property_test || echo "Time range test has issues"

# Check validation invariants property test
echo "Checking validation_invariants_property_test.rs..."
cargo check --test validation_invariants_property_test || echo "Validation invariants test has issues"

echo "Property test compilation check complete!"