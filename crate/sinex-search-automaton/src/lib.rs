//! Search Service Automaton
//!
//! This automaton provides search capabilities as a standalone service,
//! extracting the search functionality from the gateway monolith.

use async_trait::async_trait;
use serde_json::{json, Value};
use sinex_satellite_sdk::{SatelliteResult, SatelliteError};
use sinex_satellite_sdk::{
    EventFilter, HotlogAutomaton, HotlogAutomatonContext, HotlogAutomatonEvent, ProcessingResult,
};
use sinex_services::{SearchQuery, SearchService};
use std::sync::Arc;
use tracing::{info, warn};

/// Search service automaton that responds to search requests
pub struct SearchServiceAutomaton {
    context: Option<HotlogAutomatonContext>,
    service: Option<Arc<SearchService>>,
}

impl SearchServiceAutomaton {
    pub fn new() -> Self {
        Self {
            context: None,
            service: None,
        }
    }

    /// Handle search RPC request
    async fn handle_search_request(&self, request: Value) -> SatelliteResult<Value> {
        let service = self.service.as_ref().unwrap();
        let method = request.get("method").and_then(|v| v.as_str()).unwrap_or("");
        let params = request.get("params").cloned().unwrap_or(json!({}));

        match method {
            "search.search_events" => self.handle_search_events(service, params).await,
            _ => Err(SatelliteError::Automaton(format!(
                "Unknown search method: {}",
                method
            ))),
        }
    }

    async fn handle_search_events(
        &self,
        service: &SearchService,
        params: Value,
    ) -> SatelliteResult<Value> {
        let query: SearchQuery = serde_json::from_value(params)
            .map_err(|e| SatelliteError::Automaton(format!("Invalid search query: {}", e)))?;

        let results = service
            .search_events(query)
            .await
            .map_err(|e| SatelliteError::Automaton(format!("Search error: {}", e)))?;

        Ok(json!(results))
    }
}

impl Default for SearchServiceAutomaton {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HotlogAutomaton for SearchServiceAutomaton {
    fn automaton_name(&self) -> &str {
        "search-service"
    }
    
    async fn initialize(&mut self, ctx: HotlogAutomatonContext) -> SatelliteResult<()> {
        info!("Initializing search service automaton");
        
        // Initialize the search service
        let service = Arc::new(SearchService::new(ctx.db_pool.clone()));
        
        self.service = Some(service);
        self.context = Some(ctx);
        
        info!("Search service automaton initialized successfully");
        Ok(())
    }

    fn event_filters(&self) -> Vec<EventFilter> {
        vec![
            // Listen for search RPC requests
            EventFilter::new(Some("rpc.search".to_string()), Some("request".to_string())),
        ]
    }

    async fn process_event(&mut self, event: HotlogAutomatonEvent) -> SatelliteResult<ProcessingResult> {
        let payload = event.event.payload.clone();
        
        // Handle search RPC requests
        if event.event.source == "rpc.search" && event.event.event_type == "request" {
            match self.handle_search_request(payload.clone()).await {
                Ok(response) => {
                    // Submit response as synthesis event
                    let ctx = self.context.as_ref().unwrap();
                    
                    let response_event = serde_json::json!({
                        "request_id": payload.get("request_id"),
                        "response": response,
                        "service": "search"
                    });

                    // For now, just log - synthesis events need to be implemented in gRPC
                    info!("Service response logged");

                    Ok(ProcessingResult::Success { checkpoint_data: None })
                }
                Err(e) => {
                    warn!("Search request failed: {}", e);
                    
                    // Submit error response
                    let ctx = self.context.as_ref().unwrap();
                    let error_response = serde_json::json!({
                        "request_id": payload.get("request_id"),
                        "error": {
                            "code": -32603,
                            "message": format!("Search service error: {}", e)
                        },
                        "service": "search"
                    });

                    // For now, just log - synthesis events need to be implemented in gRPC
                    info!("Service response logged");

                    Ok(ProcessingResult::Success { checkpoint_data: None })
                }
            }
        } else {
            Ok(ProcessingResult::Skip { reason: "Not a search request".to_string() })
        }
    }
}