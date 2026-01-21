Meta-reflection
- This pass relied on keyword searches (std::fs, std::process::Command, ToSocketAddrs) plus manual inspection, so it may miss blocking I/O hidden behind wrappers or third-party crates.
- I focused on production paths and skipped tests/build scripts; if tests are run in async contexts, they may also suffer similar issues.
- I did not measure runtime impact; some blocking calls may be low-frequency and acceptable, but that should be confirmed with tracing or profiling.
- I did not verify call stacks for every sync helper; a follow-up should trace how often the async paths execute in normal operation.
