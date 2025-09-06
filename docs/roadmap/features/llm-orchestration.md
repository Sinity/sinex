# LLM Resource Management & Orchestration

**Status**: Designed, not implemented
**Implementation**: 0% (Design complete, core infrastructure needed)
**Priority**: High
**Dependencies**: Ollama service, LLM models, worker infrastructure, prompt management, API integration
**Blocks**: AI-powered analysis, content generation, intelligent automation, agentic workflows

## Overview

This feature provides comprehensive LLM orchestration capabilities, including local model management with Ollama, sophisticated prompt versioning and testing, intelligent request routing, and integration with agentic frameworks like DSPy and LangGraph for building complex AI workflows.

## Technical Specification

### Local LLM Management with Ollama

**NixOS Configuration**:
```nix
services.ollama = {
  enable = true;
  acceleration = "cuda"; # Or "rocm" for AMD, "cpu" for CPU-only
  # Pre-pull models on service start
  models = [
    "mistral:7b-instruct-q4_K_M"
    "llama2:13b-chat-q5_0"
  ];
};
```

**Resource Requirements**:
- RAM: 8GB minimum (16-32GB recommended)
- VRAM (GPU): Model must fit (7B Q4 model ~6-8GB)
- Storage: ~5-20GB per model

**API Endpoint**: `http://localhost:11434` for `/api/generate`, `/api/chat`

### Prompt Registry Architecture

Central system for managing, versioning, and testing prompts.

#### Database Schema

```sql
CREATE TABLE IF NOT EXISTS core.prompts (
    prompt_id               ULID PRIMARY KEY DEFAULT gen_ulid(),
    prompt_name             TEXT NOT NULL,
    version                 TEXT NOT NULL, -- Semantic versioning
    prompt_template_content TEXT NOT NULL, -- With placeholders
    
    -- Input schema for variables
    variables_input_schema  JSONB NULLABLE,
    
    description             TEXT,
    category                TEXT NULLABLE,
    tags                    TEXT[],
    
    -- LLM targeting
    target_llm_family       TEXT NULLABLE,
    model_preferences       JSONB NULLABLE,
    default_parameters      JSONB NULLABLE,
    
    -- Status and metrics
    status                  TEXT NOT NULL DEFAULT 'experimental',
    metrics_summary         JSONB NULLABLE,
    
    author                  TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    CONSTRAINT uq_core_prompts_name_version UNIQUE (prompt_name, version)
);
```

#### Git-Based Version Control

- Store prompts as YAML files: `/prompts/<category>/<name>/v<version>.yaml`
- CI/CD pipeline syncs to database
- Database is runtime source of truth

#### A/B Testing Framework

Key methods:
- `create_test()`: Initialize A/B test with control and variants
- `get_prompt_for_request()`: Select prompt based on traffic split
- `record_result()`: Log test outcomes
- `analyze_test()`: Statistical analysis with significance testing

#### Canary Deployment

Progressive rollout system:
- `deploy_canary()`: Start with small traffic percentage
- `monitor_and_adjust_canary()`: Auto-ramp or rollback based on metrics
- `promote_canary()`: Make canary the new stable version

### LLM Router Architecture

Intelligent request routing based on:
- Prompt requirements and model capabilities
- Cost optimization (local vs. cloud)
- Load balancing and availability
- Privacy tier requirements

#### LLM Models Registry

```sql
CREATE TABLE IF NOT EXISTS core.llm_models (
    model_id                ULID PRIMARY KEY DEFAULT gen_ulid(),
    model_name_unique       TEXT UNIQUE NOT NULL,
    provider                TEXT NOT NULL,
    api_endpoint_url        TEXT NULLABLE,
    
    capabilities            JSONB NOT NULL,
    -- {max_tokens, function_calling, modalities, etc.}
    
    cost_per_million_input_tokens_usd   NUMERIC(10, 4),
    cost_per_million_output_tokens_usd  NUMERIC(10, 4),
    
    access_tier             TEXT NOT NULL DEFAULT 'general',
    status                  TEXT NOT NULL DEFAULT 'active',
    rate_limits_info        JSONB NULLABLE
);
```

### DSPy/LangGraph Integration

#### State Persistence

**LangGraph Checkpointers**:
- SQLite for development
- PostgreSQL for production: `langgraph_checkpoints` table
- Ephemeral state store (NATS JetStream)

**DSPy Program Storage**:
- Serialized programs in git-annex blobs
- Optimization traces for analysis
- Version tracking with prompts

#### Debugging and Visualization

- State history retrieval for any thread
- Execution flow visualization with D3.js
- Integration with LangSmith for tracing
- OpenTelemetry spans for distributed tracing

## Implementation Architecture

### Component Hierarchy

1. **LLM Service Layer**:
   - Ollama management
   - External API clients
   - Model health monitoring

2. **Prompt Management Layer**:
   - Registry operations
   - Version control sync
   - Testing frameworks

3. **Router Layer**:
   - Request analysis
   - Model selection
   - Load balancing
   - Fallback handling

4. **Orchestration Layer**:
   - LangGraph runtime
   - DSPy program execution
   - State management

### Event Integration

```json
{
  "source": "agent.llm_router",
  "event_type": "llm.request.completed",
  "payload": {
    "prompt_id": "ULID",
    "model_used": "ollama/mistral:7b",
    "input_tokens": 150,
    "output_tokens": 200,
    "latency_ms": 850,
    "cost_usd": 0.0012,
    "thread_id": "conversation_123"
  }
}
```

## Implementation Plan

### Phase 1: Core Infrastructure
- [ ] Install and configure Ollama
- [ ] Create prompt registry tables
- [ ] Basic prompt CRUD operations
- [ ] Simple LLM client implementation

### Phase 2: Routing and Management
- [ ] LLM models registry
- [ ] Basic routing logic
- [ ] Model capability matching
- [ ] Fallback handling

### Phase 3: Testing and Deployment
- [ ] A/B testing framework
- [ ] Canary deployment system
- [ ] Metrics collection
- [ ] Statistical analysis

### Phase 4: Advanced Orchestration
- [ ] LangGraph integration
- [ ] DSPy framework support
- [ ] State persistence
- [ ] Visualization tools

### Phase 5: Production Features
- [ ] Cost optimization
- [ ] Rate limiting
- [ ] Multi-tenancy
- [ ] Advanced monitoring

## Performance Considerations

### Local Model Optimization
- Model quantization (Q4, Q5 formats)
- GPU acceleration when available
- Batching for throughput
- Model caching strategies

### Distributed Processing
- Request queueing with NATS JetStream
- Worker pool scaling
- Geographic distribution
- Edge deployment options

## Security and Privacy

### Data Protection
- Local-first processing
- Encrypted API credentials
- Request/response sanitization
- PII detection and masking

### Access Control
- Per-model permissions
- Usage quotas
- Audit logging
- Cost controls

## Future Enhancements

- **Multi-Modal Support**: Vision, audio, video processing
- **Fine-Tuning Pipeline**: Custom model training on user data
- **Federated Learning**: Privacy-preserving model improvements
- **Agent Marketplace**: Share and discover LangGraph agents
- **Real-time Collaboration**: Multi-user agent interactions
- **Adaptive Routing**: ML-based optimal model selection
