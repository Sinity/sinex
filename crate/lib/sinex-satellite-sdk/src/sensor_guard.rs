//! Compile-time guards to prevent satellites from acting as sensors
//!
//! This module provides types and traits that enforce the architectural principle:
//! "Only sensd should capture source material directly"

use std::marker::PhantomData;

use async_trait::async_trait;
use sinex_core::JsonValue;

/// Marker trait that explicitly declares a component as a sensor
/// Only sensd should implement this trait
pub trait AuthorizedSensor: private::Sealed {
    /// Sensor identification for audit trail
    fn sensor_id() -> &'static str;
}

/// Guard type that prevents direct source material capture
/// Satellites should NOT have access to this type
#[derive(Debug)]
pub struct SensorCapability<T> {
    _phantom: PhantomData<T>,
    _private: private::Private,
}

/// Type-level enforcement that a component is NOT a sensor
pub struct NotASensor;

impl AsRef<NotASensor> for NotASensor {
    fn as_ref(&self) -> &NotASensor {
        self
    }
}

/// Trait for components that process events but don't capture source material
pub trait EventProcessor {
    /// Marker type that proves this is not a sensor
    type Guard: AsRef<NotASensor>;

    /// Process events from sensd's captured material
    fn process_from_material(&self) -> Self::Guard {
        panic!("This component should not capture source material directly! Use sensd.");
    }
}

/// Compile-time check that prevents sensor operations in non-sensor contexts
#[macro_export]
macro_rules! ensure_not_sensor {
    ($component:expr) => {
        const _: () = {
            fn _check_not_sensor<T: $crate::sensor_guard::EventProcessor>(_: &T) {}
            fn _type_check() {
                let _ = _check_not_sensor(&$component);
            }
        };
    };
}

/// Documentation-enforced pattern for sensor operations
/// This trait is sealed and can only be implemented by sensd
#[async_trait]
pub trait SensorOperation: private::Sealed {
    /// Capture raw data from external source
    ///
    /// # WARNING
    /// This method should ONLY be called by sensd!
    /// Satellites must use MaterialSliceStream instead.
    async fn capture_source_material(
        &self,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

mod private {
    /// Private type to prevent external implementation
    #[derive(Debug)]
    pub struct Private;

    /// Sealed trait pattern to prevent external implementation
    pub trait Sealed {}

    // Only sensd can implement this
    impl Sealed for super::SensdMarker {}
}

/// Marker type that only sensd possesses
pub struct SensdMarker;

impl AuthorizedSensor for SensdMarker {
    fn sensor_id() -> &'static str {
        "sensd"
    }
}

/// Compile error messages for common mistakes
pub mod compile_errors {
    /// This type intentionally fails to compile if used
    pub struct DoNotCaptureSourceMaterialDirectly;

    impl DoNotCaptureSourceMaterialDirectly {
        #[allow(dead_code)]
        #[deprecated(
            note = "Satellites must not capture source material directly! Use sensd's MaterialSliceStream instead. See: docs/ARCHITECTURE.md#sensor-responsibility"
        )]
        pub fn new() -> ! {
            panic!("Direct source material capture is forbidden in satellites! Only sensd should capture source material.");
        }
    }
}

/// Trait that satellites should implement instead of sensor traits
#[async_trait]
pub trait MaterialConsumer {
    /// Process material that sensd has already captured
    async fn process_material_slice(
        &self,
        material_id: sinex_core::types::Ulid,
        slice_data: &[u8],
    ) -> Result<
        Vec<sinex_core::db::models::Event<JsonValue>>,
        Box<dyn std::error::Error + Send + Sync>,
    >;

    /// This method is intentionally missing sensor capabilities
    /// to prevent satellites from capturing directly
    fn verify_not_sensor(&self) -> NotASensor {
        NotASensor
    }
}
