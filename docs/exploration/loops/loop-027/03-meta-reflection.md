Meta-reflection
- This focuses on SelfObserver and heartbeat paths; I did not map all tracing fields or structured logs across every crate.
- I assumed continuous aggregates imply intended metrics coverage; verifying actual data presence would require querying the database.
- I did not trace observability at the RPC/CLI boundary, where error formatting may alter signal quality.
