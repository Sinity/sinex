Meta-reflection
- This focuses on explicit spawn/join patterns and does not cover tasks hidden inside third-party libraries (NATS, sqlx) or systemd units.
- I assumed that dropping channels is a sufficient shutdown signal for some loops, but this should be verified in actual shutdown flows.
- I did not inspect every ingestor/automaton; only the main patterns and a few representative watchers.
