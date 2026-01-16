# Event Payload Blanket Impl Notes

Blanket implementations for EventPayload trait

This module provides automatic EventPayload implementations for common wrapper types
like Option<T>, Vec<T>, Box<T>, etc. This enables ergonomic handling of optional
and collection payloads while preserving the same source/event_type/version constants.

## IMPORTANT: These do NOT create new event types

These implementations allow EventPayload types to be wrapped in standard containers.
They inherit the source/event_type/version from the inner type and are used to model
payloads with optional or collection structure.

Example: An Option<FileCreated> is used when deserializing events that might have missing
payloads, not to create a new event type. The event still has source="fs-watcher",
event_type="file.created", just with optional payload handling.
