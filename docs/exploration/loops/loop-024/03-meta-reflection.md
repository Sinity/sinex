Meta-reflection
- I focused on ingestd and gateway entrypoints plus Node SDK conversions; I did not trace error propagation inside sinex-services or RPC handlers, which likely map to JSON-RPC responses.
- This pass did not enumerate every SinexError constructor; it highlights high-impact flows and unused variants but may miss niche origins.
- Future work should include runtime error surfacing paths in RPC handlers and any HTTP-facing services for full end-to-end mapping.
