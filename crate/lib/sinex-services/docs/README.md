# Sinex Services Layer

`sinex-services` no longer owns PKM or content orchestration.

- PKM now lives in `crate/lib/sinex-db/src/pkm.rs`.
- Content orchestration now lives in `crate/core/sinex-gateway/src/content_service.rs`.

This crate is awaiting removal once the workspace cleanup lands.
