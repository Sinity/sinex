# Material Rotation

`material_rotation.rs` defines how captured source material is chunked,
rotated, and persisted. It ensures durable storage while keeping hot buffers
bounded.

- Applies rotation policies per sensor family.
- Hands off rotated material to the ingestion pipeline via `material_stream`.
- Coordinates with `sinex-core` repositories for final persistence.
