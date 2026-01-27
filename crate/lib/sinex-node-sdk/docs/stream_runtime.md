# Stream Processing Runtime

The `sinex-node-sdk` provides a high-level runtime for implementing both data ingestors and stateful automata.

## The SimpleNode Abstraction

The `SimpleNode` trait is the primary interface for implementing logic that processes events from NATS and optionally emits new events or stores internal state.

### Key Components

- **Input/Output**: Defines the expected event types.
- **State Management**: Automatic persistence of the node's `State` type using NATS KV checkpoints.
- **Context**: Provides access to logging, metrics, and event emission during processing.

## Processing Pipeline

The runtime follows a robust exactly-once (provisional) delivery pattern:

1. **Fetch**: The `JetStreamConsumer` pulls a batch of messages from NATS.
2. **Provisional Handle**: The `SimpleNode` processes the event.
3. **Checkpoint**: The new state and processing offset are atomically saved to NATS KV.
4. **ACK**: The original message is acknowledged in JetStream only after the checkpoint is successful.

## Error Handling

Nodes define their own error policies via the `handle_error` method:

| Policy | Action |
|--------|--------|
| `Retry` | NAK the message, triggering redelivery according to JetStream backoff. |
| `Skip` | ACK the message and move to the next event, optionally logging the failure. |
| `Fail` | Stop the node and signal a critical failure to the orchestrator. |

## Deployment Patterns

- **Ingestors**: Nodes that produce events from external sources (e.g., FS, Terminal).
- **Automata**: Nodes that transform existing events or maintain derived state (e.g., Health, Search).
- **Graceful Shutdown**: The runtime supports cooperative cancellation, ensuring that in-flight events are either completed or NAKed before the process exits.