# TIM-MobileIoTImplementation_ESP32: Mobile and IoT Implementation (ESP32 Focus)

*   **Relevant ADR:** (N/A directly, enables Vision Doc Part III.2.2.E)
*   **Original UG Context:** Section 27

This TIM details technical considerations for integrating data from mobile devices and IoT sensors into the Exocortex, with a focus on ESP32 as a reference IoT platform.

## 1. Rationale Summary

Extending Exocortex capture beyond the desktop to mobile (location, activity, notifications) and IoT (environmental sensors, presence) provides richer contextual awareness. This requires efficient protocols and robust device-side implementations.

## 2. Protocol Selection for Constrained Environments [UG Sec 27.1, CR4]

*   **MQTT (Message Queuing Telemetry Transport) - Preferred for IoT/Mobile:**
    *   Lightweight binary protocol, low bandwidth, energy efficient (~22% more than HTTP for IoT [CR4]).
    *   Publish/Subscribe model (MQTT broker needed for Exocortex).
    *   QoS Levels: 0 (at most once), 1 (at least once), 2 (exactly once).
    *   Keep-Alive, Persistent Sessions, Last Will & Testament (LWT).
*   **CoAP (Constrained Application Protocol):**
    *   UDP-based, REST-like, for very resource-constrained microcontrollers. Observe for push.
*   **gRPC:**
    *   HTTP/2 + Protobuf. More suitable for higher bandwidth/power devices (e.g., mobile app on Wi-Fi to server). Less ideal for battery-powered IoT.
*   **Exocortex Ingest:** An Exocortex agent (`agent/mqtt_ingestor` or `agent/coap_ingestor`) subscribes to relevant topics/resources on the broker/CoAP server and writes received messages as `raw.events`.

## 3. ESP32 Implementation Details (Reference IoT Platform) [UG Sec 27.2, CR4, CR5]

### 3.1. Development Environment and MQTT Client

*   **PlatformIO with ESP-IDF [CR5]:** Recommended for ESP32 projects.
*   **MQTT Client Libraries:** `esp-mqtt` (from ESP-IDF), PubSubClient.
*   **MQTT Performance [CR4]:** ESP32 can achieve 100-500 msgs/sec (depends on size, QoS, network).
*   **Memory [CR4]:** Minimal Wi-Fi+MQTT app ~40-60KB RAM + ~4KB+ for MQTT buffers/app logic.

### 3.2. Offline Buffering and Store-and-Forward [CR4]

For intermittent network connectivity.
*   **Persistent Storage for Buffering:**
    *   SPIFFS/LittleFS (on-chip SPI flash). Store messages as files or in log.
    *   SQLite on SPIFFS/SD Card (`esp32-sqlite3-persistence`).
    *   External SD Card (via SPI).
*   **In-Memory Queue:** Power-of-2 sized circular buffer in RAM (lock-free for SPSC).
*   **Store-and-Forward Logic:**
    1.  On MQTT publish fail: Enqueue message (with timestamp) to persistent buffer.
    2.  Separate FreeRTOS task periodically checks network.
    3.  If connected, reads from buffer (oldest first), attempts publish.
    4.  On success, remove from buffer. On fail, use exponential backoff with jitter for retries.

### 3.3. Power Management [UG Sec 27.2.3, CR4, CR5]

*   **Deep Sleep Modes:** ESP32 current ~10-150µA.
*   **Wake-up Sources:** RTC Timer, External Interrupt (sensor event), Touch Pad.
*   **Low-Power Workflow:** Wake -> Init Wi-Fi/Sensors -> Read -> Connect MQTT -> Publish -> Disconnect Wi-Fi -> Deep Sleep.
*   **INA219 Power Measurement [CR5]:** High-side current/power sensor IC for profiling/optimizing ESP32 power.

### 3.4. ULID Generation Entropy Sources on ESP32 [UG Sec 27.2.4, CR4]

For 80-bit random part of ULIDs generated on device.
*   **Primary:** Hardware TRNG (True Random Number Generator), best when Wi-Fi/BT radio active.
*   **Fallback (Radios Off):** ADC noise (floating ADC pin), CPU cycle counter jitter. Use to seed a PRNG. Accumulate entropy over time.

### 3.5. Advanced ESP32 Capabilities [CR5]

*   **OTA (Over-the-Air) Updates:** ESP-IDF supports robust OTA firmware updates (dual app slots, rollback).
*   **ESP-NOW Mesh Networking:** Proprietary, connectionless Wi-Fi between ESP devices. For local sensor mesh relaying to a gateway ESP32 (which then uses MQTT to Exocortex).
*   **LoRaWAN Integration:** For long-range, very low-power, low-data-rate communication. ESP32 + LoRaWAN transceiver module -> LoRaWAN Gateway -> LoRaWAN Network Server -> Exocortex ingest endpoint/MQTT bridge.
*   **TinyML Edge Computing:** TensorFlow Lite for Microcontrollers, Edge Impulse, MicroTVM. For local anomaly detection, keyword spotting, simple image classification on ESP32. Send higher-level insights to Exocortex.
*   **Bill of Materials (BOM) Example [CR5]:** Hypothetical ESP32 sensor node ~$36/unit @ 1k units (ESP32, sensors, PCB, enclosure, battery).

