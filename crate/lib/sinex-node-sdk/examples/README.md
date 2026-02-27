# node SDK Examples

This directory contains example implementations of common node patterns.

## Examples

### File-based Ingestor
See `file_ingestor.rs` for an example of ingesting data from files with proper checkpoint management.

### Event-based Automaton
See `event_automaton.rs` for an example of processing events from the stream.

### Custom node
See `custom_node.rs` for a complete example implementing a custom data source.

## Running Examples

```bash
# Run file ingestor example
cargo run --example file_ingestor

# Run event automaton example
cargo run --example event_automaton
```

## Key Patterns

All examples demonstrate:
- Proper checkpoint management
- TimeHorizon handling
- Error handling
- Exploration provider implementation
- CLI integration with `node_entrypoint!` macro