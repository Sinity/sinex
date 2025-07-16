//! Analytics Service Automaton
//!
//! This automaton provides analytics capabilities as a standalone service,
//! extracting the analytics functionality from the gateway monolith.

use async_trait::async_trait;
use serde_json::{json, Value};
use sinex_satellite_sdk::{SatelliteResult, SatelliteError};
use sinex_satellite_sdk::{
    EventFilter, HotlogAutomaton, HotlogAutomatonContext, HotlogAutomatonEvent, ProcessingResult,
};
use sinex_services::AnalyticsService;
use std::sync::Arc;
use tracing::{info, warn};

/// Analytics service automaton that responds to analytics requests
pub struct AnalyticsServiceAutomaton {
    context: Option<HotlogAutomatonContext>,
    service: Option<Arc<AnalyticsService>>,
}

impl AnalyticsServiceAutomaton {
    pub fn new() -> Self {
        Self {
            context: None,
            service: None,
        }
    }

    /// Handle analytics RPC request
    async fn handle_analytics_request(&self, request: Value) -> SatelliteResult<Value> {
        let service = self.service.as_ref().unwrap();
        let method = request.get("method").and_then(|v| v.as_str()).unwrap_or("");
        let params = request.get("params").cloned().unwrap_or(json!({}));

        match method {
            "analytics.event_count_by_source" => {
                self.handle_event_count_by_source(service, params).await
            }
            "analytics.activity_heatmap" => {
                self.handle_activity_heatmap(service, params).await
            }
            _ => Err(SatelliteError::Automaton(format!(
                "Unknown analytics method: {}",
                method
            ))),
        }
    }

    async fn handle_event_count_by_source(
        &self,
        service: &AnalyticsService,
        params: Value,
    ) -> SatelliteResult<Value> {
        use chrono::{Duration, Utc};

        let days_back = params
            .get("days_back")
            .and_then(|v| v.as_i64())
            .unwrap_or(7);

        let end_time = Utc::now();
        let start_time = end_time - Duration::days(days_back);

        let counts = service
            .get_event_count_by_source(Some(start_time), Some(end_time))
            .await
            .map_err(|e| SatelliteError::Automaton(format!("Analytics error: {}", e)))?;

        Ok(json!(counts))
    }

    async fn handle_activity_heatmap(
        &self,
        service: &AnalyticsService,
        params: Value,
    ) -> SatelliteResult<Value> {
        let bucket_size_minutes = params
            .get("bucket_size_minutes")
            .and_then(|v| v.as_i64())
            .unwrap_or(60) as i32;

        let limit = params.get("limit").and_then(|v| v.as_i64()).unwrap_or(100) as i32;

        let heatmap = service
            .activity_heatmap(bucket_size_minutes, limit)
            .await
            .map_err(|e| SatelliteError::Automaton(format!("Analytics error: {}", e)))?;

        Ok(json!(heatmap))
    }
}

impl Default for AnalyticsServiceAutomaton {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HotlogAutomaton for AnalyticsServiceAutomaton {
    fn automaton_name(&self) -> &str {
        "analytics-service"
    }
    
    async fn initialize(&mut self, ctx: HotlogAutomatonContext) -> SatelliteResult<()> {
        info!("Initializing analytics service automaton");
        
        // Initialize the analytics service
        let service = Arc::new(AnalyticsService::new(ctx.db_pool.clone()));
        
        self.service = Some(service);
        self.context = Some(ctx);
        
        info!("Analytics service automaton initialized successfully");
        Ok(())
    }

    fn event_filters(&self) -> Vec<EventFilter> {
        vec![
            // Listen for analytics RPC requests
            EventFilter::new(Some("rpc.analytics".to_string()), Some("request".to_string())),
        ]
    }

    async fn process_event(&mut self, event: HotlogAutomatonEvent) -> SatelliteResult<ProcessingResult> {
        let payload = event.event.payload.clone();
        
        // Handle analytics RPC requests
        if event.event.source == "rpc.analytics" && event.event.event_type == "request" {
            match self.handle_analytics_request(payload.clone()).await {
                Ok(response) => {
                    // Submit response as synthesis event
                    let _ctx = self.context.as_ref().unwrap();
                    
                    let response_event = serde_json::json!({
                        "request_id": payload.get("request_id"),
                        "response": response,
                        "service": "analytics"
                    });

                    // For now, just log the response - synthesis events need to be implemented in gRPC
                    info!("Analytics response: {:?}", response_event);

                    Ok(ProcessingResult::Success { checkpoint_data: None })
                }
                Err(e) => {
                    warn!("Analytics request failed: {}", e);
                    
                    // Submit error response
                    let _ctx = self.context.as_ref().unwrap();
                    let error_response = serde_json::json!({
                        "request_id": payload.get("request_id"),
                        "error": {
                            "code": -32603,
                            "message": format!("Analytics service error: {}", e)
                        },
                        "service": "analytics"
                    });

                    // For now, just log the error response
                    warn!("Analytics error response: {:?}", error_response);

                    Ok(ProcessingResult::Success { checkpoint_data: None })
                }
            }
        } else {
            Ok(ProcessingResult::Skip { reason: "Not an analytics request".to_string() })
        }
    }
}