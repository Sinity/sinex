# Mobile/IoT Sensor Bridge (ESP32)

## Overview
Introduce a small ESP32‑class bridge to capture ambient signals (e.g., BLE beacons, environmental sensors) and forward summarized events to a Sinex node on the local network. This augments desktop capture with passive, mobile context.

## Goals
- Local‑first, consent‑based ambient capture
- Low‑power, intermittent connectivity tolerance
- Privacy‑preserving summaries (no raw continuous streams by default)

## Architecture
- ESP32 firmware captures sensor observations (BLE advertisements, simple environmental readings) and batches lightweight summaries.
- Transport to desktop Sinex node via local Wi‑Fi (HTTP over TLS or MQTT), or tethered serial when offline.
- Desktop ingestor normalizes to `core.events` with explicit `source` and `event_type` families.

## Event Types (examples)
- `mobile.ble.beacon_seen`: beacon_id_hash, rssi, duration_ms, seen_count
- `mobile.env.reading`: sensor_kind (temp|co2|noise), value, unit
- `mobile.device.presence`: device_id_hash, action (arrive|depart), confidence

## Reliability & Ordering
- Use ULIDs at the desktop ingest boundary; ESP32 provides monotonic sequence numbers per batch.
- Optional HLC/clock tags for multi‑device ordering if needed.

## Security & Privacy
- Whitelist capture (explicit sensors only); no raw audio/video.
- TLS with pinned certs for Wi‑Fi transport; signed batches.
- Hash beacon/device identifiers at source; allow opt‑out by SSID/SSID class.

## Implementation Notes
- Keep firmware minimal; avoid storing PII; focus on summaries and counts.
- Provide a simple desktop bridge service that authenticates, validates, and forwards events to ingestd (JetStream).
- Integrate with the existing gateway for monitoring (health, batch lag).

## Roadmap
- P1: BLE beacon summary → desktop ingest
- P2: Environmental sensors (temp/CO₂/noise) → dashboards
- P3: Presence heuristics and simple automations
