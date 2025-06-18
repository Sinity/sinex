# TIM - LLM Resource Orchestration

**Category**: Infrastructure  
**Maturity Level**: L2 - Ready for Implementation  
**Implementation Status**: 25% - Database Foundation and Basic Infrastructure  

## Status Dashboard

### MVP Specification
- [ ] Ollama integration and model management (0%)
- [ ] LLM request queuing and prioritization (0%)
- [ ] Resource usage monitoring and limits (0%)
- [ ] Model registry and capability tracking (20% - tables exist)
- [ ] Basic prompt template system (15% - tables exist)

### Enhanced Features  
- [ ] Dynamic model loading/unloading (0%)
- [ ] Multi-model request routing (0%)
- [ ] Cost optimization and budget management (0%)
- [ ] Performance monitoring and caching (0%)
- [ ] Federated LLM service coordination (0%)

### Implementation Checklist
- [ ] Install and configure Ollama service
- [ ] Create `LLMOrchestrationWorker` in sinex-worker
- [ ] Implement model registry population and updates
- [ ] Add request queuing and load balancing
- [ ] Create resource monitoring and limits
- [ ] Implement prompt template rendering system
- [ ] Add cost tracking and budget controls
- [ ] Create health monitoring for LLM services
- [ ] Implement model performance benchmarking
- [ ] Add horizontal scaling capabilities

## Overview

LLM Resource Orchestration manages local and remote Language Learning Models to support AI-powered features in Sinex. This includes model lifecycle management, request routing, resource optimization, and cost control across multiple LLM providers and local deployments.

## Current Implementation Status

**Verification against codebase:**
- ✅ **Database Infrastructure**: Complete LLM tables exist (`core.llm_models`, `core.prompts`, `core.prompt_executions`)
- ✅ **AI Content Storage**: `core.ai_generated_content` table for results
- ✅ **Embedding Infrastructure**: Vector embeddings tables and indexes
- ✅ **Worker Infrastructure**: Worker pattern and queue system exists
- ❌ **Ollama Integration**: No Ollama-specific integration found
- ❌ **LLM Worker**: No LLM orchestration worker implementation
- ❌ **Model Management**: No model lifecycle management code
- ❌ **Request Routing**: No LLM request routing or load balancing

## Motivation

Effective LLM orchestration enables:
- Cost-effective AI processing with mixed local/remote models
- Optimized resource utilization across available hardware
- Intelligent request routing based on model capabilities
- Scalable AI processing for large event volumes
- Robust fallback mechanisms for high availability

## Technical Requirements

### Core Components

1. **LLMOrchestrationWorker**
   - Manage Ollama and other LLM service connections
   - Route requests to appropriate models based on capabilities
   - Monitor resource usage and enforce limits
   - Handle model loading/unloading for resource optimization

2. **Model Registry Manager**
   - Track available models and their capabilities
   - Update model status and performance metrics
   - Manage model versioning and deprecation
   - Coordinate model deployment across multiple nodes

3. **Request Queue Manager**
   - Prioritize LLM requests based on urgency and type
   - Implement load balancing across available models
   - Handle request timeouts and retries
   - Provide request status tracking and monitoring

### Integration Points

- **Event Processing Pipeline**: AI analysis of captured events
- **Entity Resolution**: LLM-powered entity extraction and classification
- **Audio Transcription**: Post-processing and summarization
- **Knowledge Management**: Document analysis and summarization
- **Query Interface**: Natural language query processing

## Implementation Architecture

### Orchestration Worker Structure
```rust
pub struct LLMOrchestrationWorker {
    pool: PgPool,
    ollama_client: OllamaClient,
    openai_client: Option<OpenAIClient>,
    anthropic_client: Option<AnthropicClient>,
    model_registry: ModelRegistry,
    request_queue: RequestQueue,
    resource_monitor: ResourceMonitor,
}

#[async_trait]
impl LLMOrchestrationWorker {
    pub async fn route_request(&self, request: LLMRequest) -> Result<LLMResponse>;
    pub async fn update_model_registry(&self) -> Result<()>;
    pub async fn monitor_resource_usage(&self) -> Result<ResourceMetrics>;
    pub async fn claim_and_process_llm_job(&self) -> Result<bool>;
}
```

### Model Management
```rust
#[derive(Serialize, Deserialize, Debug)]
pub struct ModelCapabilities {
    pub chat: bool,
    pub completion: bool,
    pub embeddings: bool,
    pub vision: bool,
    pub function_calling: bool,
    pub context_window: u32,
    pub max_output_tokens: u32,
}

#[derive(Serialize, Deserialize)]
pub struct LLMRequest {
    pub id: Ulid,
    pub request_type: RequestType,
    pub priority: Priority,
    pub model_requirements: ModelRequirements,
    pub prompt_template_id: Option<Ulid>,
    pub variables: HashMap<String, Value>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f64>,
    pub timeout_seconds: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub enum RequestType {
    ChatCompletion,
    TextCompletion,
    Embedding,
    Summarization,
    EntityExtraction,
    Classification,
    Translation,
}

#[derive(Serialize, Deserialize)]
pub enum Priority {
    Critical,    // Real-time user requests
    High,        // Interactive features
    Normal,      // Background processing
    Low,         // Batch analysis
    Bulk,        // Large-scale historical processing
}
```

### Ollama Integration
```rust
pub struct OllamaClient {
    base_url: Url,
    http_client: reqwest::Client,
    available_models: Arc<RwLock<Vec<OllamaModel>>>,
}

impl OllamaClient {
    pub async fn list_models(&self) -> Result<Vec<OllamaModel>>;
    pub async fn pull_model(&self, model_name: &str) -> Result<()>;
    pub async fn generate(&self, request: GenerateRequest) -> Result<GenerateResponse>;
    pub async fn embed(&self, request: EmbedRequest) -> Result<EmbedResponse>;
    pub async fn show_model_info(&self, model_name: &str) -> Result<ModelInfo>;
}

#[derive(Serialize, Deserialize)]
pub struct OllamaModel {
    pub name: String,
    pub size: u64,
    pub digest: String,
    pub modified_at: OffsetDateTime,
    pub details: ModelDetails,
}
```

## Configuration

### Basic Configuration
```toml
[llm_orchestration]
enabled = true
default_timeout_seconds = 300
max_concurrent_requests = 10
request_queue_size = 1000

[llm_orchestration.ollama]
enabled = true
base_url = "http://localhost:11434"
health_check_interval_seconds = 60
auto_pull_models = false
preferred_models = ["llama3.2:3b", "phi3:mini", "nomic-embed-text"]

[llm_orchestration.openai]
enabled = false
api_key_env_var = "OPENAI_API_KEY"
default_model = "gpt-4o-mini"
max_requests_per_minute = 60

[llm_orchestration.anthropic]
enabled = false
api_key_env_var = "ANTHROPIC_API_KEY"
default_model = "claude-3-haiku-20240307"
max_requests_per_minute = 100

[llm_orchestration.resource_limits]
max_memory_mb = 8192
max_gpu_memory_mb = 4096
max_cpu_percent = 80
queue_size_warning_threshold = 500

[llm_orchestration.routing]
prefer_local_models = true
fallback_to_remote = true
cost_optimization_enabled = true
performance_optimization_enabled = true
```

### Model-Specific Configuration
```toml
[llm_orchestration.model_configs]

[llm_orchestration.model_configs."llama3.2:3b"]
preferred_for = ["chat", "general_completion"]
max_context_length = 8192
typical_response_time_ms = 2000
resource_weight = 3

[llm_orchestration.model_configs."phi3:mini"]
preferred_for = ["quick_completion", "code_generation"]
max_context_length = 4096
typical_response_time_ms = 800
resource_weight = 1

[llm_orchestration.model_configs."nomic-embed-text"]
preferred_for = ["embeddings"]
embedding_dimensions = 768
typical_response_time_ms = 500
resource_weight = 1
```

## Database Schema Extensions

### LLM Request Queue
```sql
CREATE TABLE IF NOT EXISTS core.llm_requests (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    request_type TEXT NOT NULL,
    priority TEXT NOT NULL DEFAULT 'normal' 
        CHECK (priority IN ('critical', 'high', 'normal', 'low', 'bulk')),
    status TEXT NOT NULL DEFAULT 'queued' 
        CHECK (status IN ('queued', 'processing', 'completed', 'failed', 'timeout')),
    
    -- Request details
    prompt_template_id ulid REFERENCES core.prompts(id),
    rendered_prompt TEXT,
    variables JSONB NOT NULL DEFAULT '{}',
    model_requirements JSONB NOT NULL DEFAULT '{}',
    
    -- Routing and execution
    assigned_model_id ulid REFERENCES core.llm_models(id),
    assigned_provider TEXT,
    execution_node TEXT,
    
    -- Timing and performance
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processing_started_at TIMESTAMPTZ,
    processing_completed_at TIMESTAMPTZ,
    timeout_at TIMESTAMPTZ,
    
    -- Results and metrics
    response_text TEXT,
    response_metadata JSONB,
    tokens_input INTEGER,
    tokens_output INTEGER,
    cost_estimate DECIMAL(10, 6),
    processing_time_ms INTEGER,
    error_message TEXT,
    
    -- Queue management
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    priority_score FLOAT NOT NULL DEFAULT 0
);

-- Resource usage tracking
CREATE TABLE IF NOT EXISTS core.llm_resource_usage (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    provider TEXT NOT NULL,
    model_name TEXT NOT NULL,
    execution_node TEXT NOT NULL,
    
    -- Resource metrics
    cpu_percent FLOAT,
    memory_mb INTEGER,
    gpu_memory_mb INTEGER,
    requests_per_minute INTEGER,
    average_response_time_ms INTEGER,
    
    -- Cost metrics
    tokens_processed INTEGER,
    estimated_cost DECIMAL(10, 6),
    
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT unique_provider_model_node_time 
        UNIQUE(provider, model_name, execution_node, recorded_at)
);
```

### Enhanced Model Registry
```sql
-- Extend existing core.llm_models table
ALTER TABLE core.llm_models ADD COLUMN IF NOT EXISTS provider_config JSONB DEFAULT '{}';
ALTER TABLE core.llm_models ADD COLUMN IF NOT EXISTS performance_metrics JSONB DEFAULT '{}';
ALTER TABLE core.llm_models ADD COLUMN IF NOT EXISTS resource_requirements JSONB DEFAULT '{}';
ALTER TABLE core.llm_models ADD COLUMN IF NOT EXISTS health_status TEXT DEFAULT 'unknown' 
    CHECK (health_status IN ('healthy', 'degraded', 'unhealthy', 'offline', 'unknown'));
ALTER TABLE core.llm_models ADD COLUMN IF NOT EXISTS last_health_check TIMESTAMPTZ;
```

## Ollama Integration Strategy

### Service Management
```rust
pub struct OllamaManager {
    pub async fn ensure_service_running(&self) -> Result<()>;
    pub async fn list_available_models(&self) -> Result<Vec<String>>;
    pub async fn pull_model_if_needed(&self, model: &str) -> Result<()>;
    pub async fn get_model_info(&self, model: &str) -> Result<ModelInfo>;
    pub async fn health_check(&self) -> Result<HealthStatus>;
}
```

### Model Lifecycle
```rust
impl OllamaManager {
    // Automatic model management
    pub async fn load_model(&self, model: &str) -> Result<()>;
    pub async fn unload_model(&self, model: &str) -> Result<()>;
    pub async fn warm_up_model(&self, model: &str) -> Result<()>;
    
    // Resource optimization
    pub async fn get_memory_usage(&self) -> Result<MemoryInfo>;
    pub async fn free_unused_models(&self) -> Result<Vec<String>>;
    pub async fn optimize_model_loading(&self) -> Result<()>;
}
```

## Request Routing Strategy

### Capability-Based Routing
```rust
pub struct RequestRouter {
    pub fn find_suitable_models(
        &self,
        requirements: &ModelRequirements,
    ) -> Vec<(ModelId, f64)>; // (model_id, suitability_score)
    
    pub fn select_optimal_model(
        &self,
        candidates: &[(ModelId, f64)],
        current_load: &LoadMetrics,
    ) -> Option<ModelId>;
}
```

### Load Balancing
- Round-robin for models with equal capabilities
- Weighted routing based on model performance
- Queue length consideration for load distribution
- Failure detection and automatic failover

### Cost Optimization
- Prefer local models when performance is adequate
- Route expensive requests to cost-effective providers
- Implement request batching where possible
- Cache results for repeated queries

## Privacy and Security

### Data Protection
- Local model processing keeps data on-premises
- Configurable data retention for request/response logs
- Encryption for sensitive prompt content
- Access controls for different model tiers

### Model Security
- Validate model checksums and signatures
- Isolate model execution environments
- Monitor for unusual model behavior or outputs
- Implement rate limiting and abuse detection

## Performance Considerations

### Throughput Optimization
- Concurrent request processing within resource limits
- Request batching for compatible operations
- Connection pooling for HTTP-based providers
- Asynchronous processing with proper backpressure

### Latency Management
- Model warm-up strategies for faster first responses
- Connection keep-alive for repeated requests
- Geographic routing for remote providers
- Caching for repeated prompt patterns

### Resource Management
- Dynamic scaling based on queue length and resource usage
- Memory-based model eviction when limits approached
- CPU and GPU monitoring with throttling
- Graceful degradation under resource pressure

## Testing Strategy

### Unit Tests
- Model capability matching algorithms
- Request routing logic and fallback mechanisms
- Resource monitoring and limit enforcement
- Configuration validation and error handling

### Integration Tests
- Ollama service integration and model management
- End-to-end request processing pipeline
- Database persistence and retrieval operations
- Health monitoring and failure recovery

### System Tests
- Large-scale concurrent request processing
- Resource limit enforcement under load
- Model switching and optimization scenarios
- Cost tracking accuracy and budget enforcement

## Success Metrics

### Performance Metrics
- <5 second response time for 95% of requests
- >99.5% successful request completion rate
- <10% resource overhead for orchestration layer
- <1 minute model loading time for cached models

### Cost Metrics
- 50% cost reduction through local model preference
- Accurate cost tracking within 5% of actual usage
- Budget limit enforcement with zero overruns
- ROI tracking for model infrastructure investment

### Reliability Metrics
- >99.9% orchestration service uptime
- <30 second recovery time from model failures
- Zero data loss in request processing pipeline
- Consistent performance under varying load patterns

## Dependencies

### System Requirements
- **Ollama**: Local LLM serving platform
- **CUDA/ROCm**: GPU acceleration for local models (optional)
- **Docker**: Containerized model deployment (optional)
- **Sufficient Hardware**: RAM and GPU memory for model hosting

### Rust Crates
- `reqwest` - HTTP client for API communication
- `tokio-tungstenite` - WebSocket support for streaming
- `serde_json` - JSON serialization for API protocols
- `uuid` - Request tracking and correlation
- `prometheus` - Metrics collection and monitoring

### External Services
- **Ollama API**: Local model serving
- **OpenAI API**: Remote model access (optional)
- **Anthropic API**: Remote model access (optional)
- **Monitoring Stack**: Prometheus, Grafana for observability

## Future Enhancements

### Advanced Features
- Multi-node model distribution and federation
- Automatic model benchmarking and performance tuning
- Custom model fine-tuning pipeline integration
- Intelligent request caching and result reuse

### Integration Opportunities
- Voice interaction with speech-to-text integration
- Real-time event processing with streaming models
- Cross-modal processing combining text, audio, and visual inputs
- Federated learning across multiple Sinex deployments

### Optimization Strategies
- Model quantization for reduced resource usage
- Speculative execution for improved latency
- Adaptive batching based on model characteristics
- Edge deployment for ultra-low latency processing

## References

- [Ollama Documentation](https://ollama.ai/docs)
- [LLM Serving Best Practices](https://arxiv.org/abs/2312.07104)
- [Model Orchestration Patterns](https://www.usenix.org/conference/osdi22/presentation/yu)
- [Cost Optimization for LLM Services](https://arxiv.org/abs/2401.06009)