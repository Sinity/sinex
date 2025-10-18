// SINEX TEST TEMPLATE
//
// Use this snippet when adding module-level tests:
//
// ```rust
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use sinex_test_utils::prelude::*;
//
//     #[sinex_test]
//     async fn test_basic_functionality(ctx: TestContext) -> color_eyre::eyre::Result<()> {
//         let event = ctx
//             .create_test_event(\"your-component\", \"action.performed\", json!({\"key\": \"value\"}))
//             .await?;
//
//         let source_ref = sinex_core::EventSource::from(\"your-component\");
//         let events = ctx
//             .pool
//             .events()
//             .get_by_source(&source_ref, Some(10), None)
//             .await?;
//
//         assert_eq!(events.len(), 1);
//         assert_eq!(events[0].id, event.id);
//         Ok(())
//     }
// }
// ```
