#!/usr/bin/env bash

cd /realm/project/sinex/crate/sinex-db/src/repositories

# Find all queries that select from core.events and add the missing columns
# After associated_blob_ids, add the three missing columns

sed -i '/associated_blob_ids$/,/FROM core\.events/ {
    s/associated_blob_ids$/associated_blob_ids,\n                payload_schema_name,\n                payload_schema_version,\n                processor_manifest_id/
}' events.rs

echo "Added missing columns to event queries"