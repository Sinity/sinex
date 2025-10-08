use sinex_core::db::models::Event;
use sinex_core::types::ulid::Ulid;
use sinex_core::JsonValue;
use sinex_satellite_sdk::sensor_guard::{EventProcessor, MaterialConsumer, NotASensor};

struct TestSatellite;

impl EventProcessor for TestSatellite {
    type Guard = NotASensor;
}

impl MaterialConsumer for TestSatellite {
    async fn process_material_slice(
        &self,
        _material_id: Ulid,
        _slice_data: &[u8],
    ) -> Result<Vec<Event<JsonValue>>, Box<dyn std::error::Error>> {
        Ok(vec![])
    }
}

#[test]
fn verify_not_sensor_allows_compilation() {
    let satellite = TestSatellite;
    let _guard = satellite.verify_not_sensor();
}
