//! RPC Dispatcher Automaton
//!
//! This automaton replaces the gateway's RPC functionality by receiving
//! RPC requests and dispatching them to appropriate service automata.

use async_trait::async_trait;
use serde_json::{json, Value};
use sinex_events::event_types;
use sinex_satellite_sdk::SatelliteResult;
use sinex_satellite_sdk::{
    EventFilter, HotlogAutomaton, HotlogAutomatonContext, HotlogAutomatonEvent, ProcessingResult,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};
// use sinex_events::constants::{event_types}; // already imported above

/// RPC request dispatcher that routes requests to service automata
pub struct RpcDispatcherAutomaton {
    context: Option<HotlogAutomatonContext>,
    pending_requests: Arc<RwLock<HashMap<String, PendingRequest>>>,
}

#[derive(Clone)]
struct PendingRequest {
    request_id: String,
    _method: String,
    _params: Value,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl RpcDispatcherAutomaton {
    pub fn new() -> Self {
        Self {
            context: None,
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Route RPC request to appropriate service automaton
    async fn route_rpc_request(&self, request: Value) -> SatelliteResult<()> {
        let method = request.get("method").and_then(|v| v.as_str()).unwrap_or("");
        let params = request.get("params").cloned().unwrap_or(json!({}));
        let request_id = request
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        // Store pending request
        let pending_request = PendingRequest {
            request_id: request_id.to_string(),
            _method: method.to_string(),
            _params: params.clone(),
            created_at: chrono::Utc::now(),
        };

        {
            let mut pending = self.pending_requests.write().await;
            pending.insert(request_id.to_string(), pending_request);
        }

        // Determine target service based on method prefix
        let (_service_name, service_method) = match method {
            m if m.starts_with("analytics.") => ("analytics", m),
            m if m.starts_with("pkm.") => ("pkm", m),
            m if m.starts_with("search.") => ("search", m),
            m if m.starts_with("content.") => ("content", m),
            _ => {
                warn!("Unknown RPC method: {}", method);
                return self
                    .send_error_response(
                        request_id,
                        -32601,
                        "Method not found",
                        format!("Unknown method: {}", method),
                    )
                    .await;
            }
        };

        // Route to appropriate service automaton
        let _ctx = self.context.as_ref().unwrap();
        let _service_request = json!({
            "request_id": request_id,
            "method": service_method,
            "params": params
        });

        // For now, just log - synthesis events need to be implemented in gRPC
        info!("Service response logged");

        Ok(())
    }

    /// Send error response for RPC request
    async fn send_error_response(
        &self,
        request_id: &str,
        code: i32,
        message: &str,
        details: String,
    ) -> SatelliteResult<()> {
        let _ctx = self.context.as_ref().unwrap();
        let _error_response = json!({
            "jsonrpc": "2.0",
            "error": {
                "code": code,
                "message": message,
                "data": details
            },
            "id": request_id
        });

        // For now, just log - synthesis events need to be implemented in gRPC
        info!("Service response logged");

        Ok(())
    }

    /// Handle service response and complete RPC request
    async fn handle_service_response(&self, response: Value) -> SatelliteResult<()> {
        let request_id = response
            .get("request_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let service_name = response
            .get("service")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        // Remove from pending requests
        let pending_request = {
            let mut pending = self.pending_requests.write().await;
            pending.remove(request_id)
        };

        if pending_request.is_none() {
            warn!("Received response for unknown request: {}", request_id);
            return Ok(());
        }

        let _ctx = self.context.as_ref().unwrap();
        let _rpc_response = if let Some(error) = response.get("error") {
            // Service returned an error
            json!({
                "jsonrpc": "2.0",
                "error": error,
                "id": request_id
            })
        } else if let Some(result) = response.get("response") {
            // Service returned a successful result
            json!({
                "jsonrpc": "2.0",
                "result": result,
                "id": request_id
            })
        } else {
            // Invalid response format
            json!({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": "Invalid service response format"
                },
                "id": request_id
            })
        };

        // For now, just log - synthesis events need to be implemented in gRPC
        info!("Service response logged");

        info!(
            "Completed RPC request {} for service {}",
            request_id, service_name
        );
        Ok(())
    }

    /// Clean up expired pending requests
    async fn cleanup_expired_requests(&self) -> SatelliteResult<()> {
        let timeout = chrono::Duration::seconds(30);
        let now = chrono::Utc::now();

        let mut pending = self.pending_requests.write().await;
        let expired_keys: Vec<String> = pending
            .iter()
            .filter(|(_, request)| now.signed_duration_since(request.created_at) > timeout)
            .map(|(key, _)| key.clone())
            .collect();

        for key in expired_keys {
            if let Some(request) = pending.remove(&key) {
                warn!("Request {} expired after timeout", request.request_id);

                // Send timeout error response
                drop(pending); // Release lock before async operation
                self.send_error_response(
                    &request.request_id,
                    -32603,
                    "Request timeout",
                    format!("Request {} timed out", request.request_id),
                )
                .await?;

                // Re-acquire lock for next iteration
                pending = self.pending_requests.write().await;
            }
        }

        Ok(())
    }
}

impl Default for RpcDispatcherAutomaton {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HotlogAutomaton for RpcDispatcherAutomaton {
    fn automaton_name(&self) -> &str {
        "rpc-dispatcher"
    }

    async fn initialize(&mut self, ctx: HotlogAutomatonContext) -> SatelliteResult<()> {
        info!("Initializing RPC dispatcher automaton");

        self.context = Some(ctx);

        info!("RPC dispatcher automaton initialized successfully");
        Ok(())
    }

    fn event_filters(&self) -> Vec<EventFilter> {
        vec![
            // Listen for incoming RPC requests
            EventFilter::new(Some(event_types::rpc::GATEWAY_PREFIX.to_string()), Some(event_types::rpc::REQUEST.to_string())),
            // Listen for service responses
            EventFilter::new(
                Some(event_types::rpc::ANALYTICS_PREFIX.to_string()),
                Some(event_types::rpc::RESPONSE.to_string()),
            ),
            EventFilter::new(Some(event_types::rpc::PKM_PREFIX.to_string()), Some(event_types::rpc::RESPONSE.to_string())),
            EventFilter::new(Some(event_types::rpc::SEARCH_PREFIX.to_string()), Some(event_types::rpc::RESPONSE.to_string())),
            EventFilter::new(
                Some(event_types::rpc::CONTENT_PREFIX.to_string()),
                Some(event_types::rpc::RESPONSE.to_string()),
            ),
            // Listen for service errors
            EventFilter::new(Some(event_types::rpc::ANALYTICS_PREFIX.to_string()), Some(event_types::rpc::ERROR.to_string())),
            EventFilter::new(Some(event_types::rpc::PKM_PREFIX.to_string()), Some(event_types::rpc::ERROR.to_string())),
            EventFilter::new(Some(event_types::rpc::SEARCH_PREFIX.to_string()), Some(event_types::rpc::ERROR.to_string())),
            EventFilter::new(Some(event_types::rpc::CONTENT_PREFIX.to_string()), Some(event_types::rpc::ERROR.to_string())),
        ]
    }

    async fn process_event(
        &mut self,
        event: HotlogAutomatonEvent,
    ) -> SatelliteResult<ProcessingResult> {
        let payload = event.event.payload.clone();

        match (event.event.source.as_str(), event.event.event_type.as_str()) {
            ("rpc.gateway", "request") => {
                // Route incoming RPC request
                self.route_rpc_request(payload).await?;
                Ok(ProcessingResult::Success {
                    checkpoint_data: None,
                })
            }
            (source, "response") if source.starts_with("rpc.") => {
                // Handle service response
                self.handle_service_response(payload).await?;
                Ok(ProcessingResult::Success {
                    checkpoint_data: None,
                })
            }
            (source, "error") if source.starts_with("rpc.") => {
                // Handle service error response
                self.handle_service_response(payload).await?;
                Ok(ProcessingResult::Success {
                    checkpoint_data: None,
                })
            }
            _ => {
                // Clean up expired requests periodically
                self.cleanup_expired_requests().await?;
                Ok(ProcessingResult::Skip {
                    reason: "Not an RPC event".to_string(),
                })
            }
        }
    }
}
