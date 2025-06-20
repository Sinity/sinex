# TIM-LLMResourceOrchestration: LLM Resource Management & Orchestration

## Status Dashboard
**Maturity Level**: L2 - Ready for Implementation
**Implementation**: 25% (Database schema and worker infrastructure exists, Ollama integration needed)
**Dependencies**: Ollama service, LLM models, worker infrastructure, prompt management, API integration
**Blocks**: AI-powered analysis, content generation, intelligent automation, agentic workflows

## MVP Specification
- Ollama installation and local model management
- Basic LLM request routing and load balancing
- Simple prompt versioning and management
- Integration with worker infrastructure
- Request/response logging and monitoring

## Enhanced Features
- Advanced model selection and optimization
- Sophisticated prompt engineering and testing
- Multi-model ensemble and fallback strategies
- Cost optimization and resource management
- Integration with external LLM APIs
- Agentic workflow orchestration with DSPy/LangGraph

## Implementation Checklist
- [ ] Ollama service installation and configuration
- [ ] LLM model download and management
- [ ] Request routing and orchestration worker
- [ ] Prompt versioning and template system
- [ ] Integration with existing worker infrastructure
- [ ] Request/response logging and analytics
- [ ] Performance monitoring and optimization
- [ ] External API integration and fallbacks
- [ ] Advanced agentic workflow support

*   **Relevant ADR:** (N/A directly, defines core LLM infrastructure)
*   **Original UG Context:** Section 26
*   **Vision Document Reference:** Part IV.2

This TIM details the architecture for managing local and remote Large Language Models (LLMs), prompt versioning and testing, routing LLM requests, and integrating frameworks like DSPy/LangGraph for building LLM-powered agentic flows in the Exocortex.

## 1. Local LLM Management with Ollama [UG Sec 26.1, SR1]

*   **Tool:** Ollama for running open-source LLMs locally (Llama, Mistral, Gemma, etc.).
*   **NixOS Setup:**
    ```nix
    # services.ollama = {
    //   enable = true;
    //   # For GPU acceleration (NVIDIA CUDA example):
    //   acceleration = "cuda"; # Or "rocm" for AMD, or "cpu"
    //   # Ensure hardware.nvidia settings are correct for CUDA if using GPU.
    //   # Or for Intel iGPU with OpenCL/SYCL via llama.cpp backend in Ollama:
    //   # acceleration = "gpu"; # Generic, Ollama tries to auto-detect suitable backend
    //   # extraArgs = [ "--verbose" ]; # For debugging
    //   # models = [ # Pre-pull models on service start (optional)
    //   #   "mistral:7b-instruct-q4_K_M"
    //   #   "llama2:13b-chat-q5_0"
    //   # ];
    // };
    ```
*   **Configuration:**
    *   `OLLAMA_MAX_LOADED_MODELS` (env var): Control how many models kept in VRAM/RAM.
    *   Model Storage: `~/.ollama/models` or system path.
*   **Resource Needs [SR1]:**
    *   RAM: 8GB+ (16-32GB better). 7B Q4_K_M model ~5-6GB RAM for CPU.
    *   VRAM (GPU): Model must fit. 7B Q4_K_M ~6-8GB VRAM.
*   **Ollama API:** Exposes HTTP endpoint (default `http://localhost:11434`) for `/api/generate`, `/api/chat`, etc.

## 2. Prompt Registry Architecture [UG Sec 26.2]

Central system for managing, versioning, testing, deploying prompts.

### 2.1. Database Schema (`core.prompts`) [UG Sec 26.2.1, SA4]

Stores prompt templates, versions, metadata.
```sql
CREATE TABLE IF NOT EXISTS core.prompts (
    prompt_id               ULID PRIMARY KEY DEFAULT gen_ulid(), -- pgx_ulid
    prompt_name             TEXT NOT NULL, -- User-defined, memorable name (e.g., "SummarizeWebArticle")
    version                 TEXT NOT NULL, -- Semantic version (e.g., "1.0.0", "1.1.0-beta")
    prompt_template_content TEXT NOT NULL, -- Actual prompt text with placeholders (e.g., "{user_query}", "{{context_text}}")
    
    -- Describes the input variables this prompt template expects
    variables_input_schema JSONB NULLABLE, 
    -- Example: {"type": "object", "properties": {"user_query": {"type": "string"}, "context_text": {"type": "string"}}, "required": ["user_query"]}
    -- Could also be a ULID FK to sinex_schemas.event_payload_schemas if schemas are reused.

    description             TEXT,
    category                TEXT NULLABLE, -- e.g., "summarization", "extraction", "generation", "classification"
    tags                    TEXT[],

    -- LLM Targeting & Parameters
    target_llm_family       TEXT NULLABLE, -- e.g., "general_instruct", "function_calling", "code_generation"
    model_preferences       JSONB NULLABLE, -- {"preferred_models": ["ollama/mistral", "openai/gpt-3.5-turbo"], "fallback_models": ["ollama/llama2"]}
    default_parameters      JSONB NULLABLE, -- Default LLM call params for this prompt (temperature, max_tokens, etc.)
    
    -- Operational & Metrics
    status                  TEXT NOT NULL DEFAULT 'experimental', -- 'active', 'experimental', 'deprecated', 'archived'
    metrics_summary         JSONB NULLABLE, -- Aggregated test/prod performance: {avg_latency_ms, avg_tokens, success_rate, last_tested_at}
    
    author                  TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_core_prompts_name_version UNIQUE (prompt_name, version)
);
COMMENT ON TABLE core.prompts IS 'Registry for versioned LLM prompt templates.';
CREATE INDEX IF NOT EXISTS idx_core_prompts_name_status ON core.prompts (prompt_name, status);
CREATE INDEX IF NOT EXISTS idx_core_prompts_category_tags ON core.prompts USING GIN (category, tags) WHERE category IS NOT NULL AND tags IS NOT NULL;

-- Trigger for updated_at
CREATE OR REPLACE FUNCTION core.set_updated_at_trigger_func()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_core_prompts_set_updated_at
BEFORE UPDATE ON core.prompts
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at_trigger_func();
```

### 2.2. Git-Based Version Control for Prompts (Source) [UG Sec 26.2.2, CR5]

*   Store prompt definitions as structured YAML files in a Git repo (e.g., `/prompts/<category>/<prompt_name>/v<version>.yaml`).
*   YAML schema for these files (see UG Sec 26.2.3 for example).
*   CI/CD pipeline or sync agent (`PromptRegistry` Python class concept from CR5) validates YAMLs and loads/updates `core.prompts` table. DB is runtime source of truth.

### 2.3. A/B Testing Framework [UG Sec 26.2.4, CR5]

Python `PromptABTestFramework` class (conceptual from CR5):
*   `create_test(test_name, control_prompt_id, variant_prompt_ids, traffic_split)`: Initializes test, stores config.
*   `get_prompt_for_request(test_name, request_id)`: Selects prompt based on traffic split. Returns `(group_name, chosen_prompt_id)`.
*   `record_result(test_name, group_name, prompt_id_used, success_bool, metrics_dict)`: Logs outcome.
*   `analyze_test(test_name, significance_level)`: Performs statistical analysis (Chi-square for success rates, t-test/Mann-Whitney for continuous metrics). Calculates p-values, confidence intervals (e.g., Wilson score for binomial proportions). Returns summary.
*   Results stored in a dedicated DB table (e.g., `core.prompt_ab_test_results`).

### 2.4. Canary Deployment for Prompts [UG Sec 26.2.5, CR5]

Python `PromptCanaryDeployment` class (conceptual from CR5):
*   `deploy_canary(prompt_name, new_prompt_version, initial_canary_percentage, ramp_up_schedule)`: Starts canary.
*   `get_prompt_version_for_request(prompt_name_canaried, request_id)`: Routes traffic to stable or canary version.
*   `record_canary_metric(prompt_name_canaried, version_type_served, metrics_dict)`: Logs performance.
*   `monitor_and_adjust_canary(prompt_name_canaried)`: Periodically compares metrics. Auto-ramps up traffic if canary is good, or alerts/rolls back if bad.
*   `promote_canary(prompt_name_canaried)`: Makes canary new stable version. Updates `core.prompts.status`.
*   `rollback_canary(prompt_name_canaried)`: Reverts to original stable. Updates status.
*   State stored in DB (e.g., `core.prompt_canary_deployments`).

## 3. LLM Router Architecture [UG Sec 26.3, OR3]

Centralized service/library for routing LLM inference requests.

*   **Interface:** Internal API `llm_router.execute_prompt(prompt_name, version, input_variables, user_context_or_constraints)`.
*   **Routing Logic:**
    *   Based on `prompt_name`/`category` from `core.prompts`.
    *   `target_llm_family` and `model_preferences` in `core.prompts`.
    *   Model capabilities from `core_llm_models` (context window, function calling, modality).
    *   Cost optimization, privacy tiers (local vs. cloud).
    *   Load balancing, model health/availability.
    *   (Advanced) ML-based router predicting best LLM for prompt.
*   **Configuration (`core_llm_models` DB Table - Vision Doc Part IV.2.2):**
    ```sql
    CREATE TABLE IF NOT EXISTS core.llm_models (
        model_id                ULID PRIMARY KEY DEFAULT gen_ulid(),
        model_name_unique       TEXT UNIQUE NOT NULL, -- e.g., "ollama/mistral:7b-instruct-q5_K_M", "openai/gpt-4-turbo-2024-04-09"
        provider                TEXT NOT NULL, -- "ollama", "openai", "anthropic", "google_vertexai"
        api_endpoint_url        TEXT NULLABLE, -- For remote APIs
        -- api_key_secret_ref will be handled by the router/client by looking up appropriate secret based on provider/model_name
        -- For Ollama, endpoint might be inferred if running locally.
        capabilities            JSONB NOT NULL,
        -- e.g., {"max_input_tokens": 8192, "max_output_tokens": 2048, "supports_function_calling": true, "supports_json_mode": true, "modalities": ["text"], "output_formats": ["text", "json"]}
        cost_per_million_input_tokens_usd   NUMERIC(10, 4) NULLABLE,
        cost_per_million_output_tokens_usd  NUMERIC(10, 4) NULLABLE,
        cost_per_image_input_usd            NUMERIC(10, 4) NULLABLE, -- For multimodal
        access_tier             TEXT NOT NULL DEFAULT 'general', -- "local_fast", "local_quality", "cloud_fast", "cloud_quality", "experimental"
        status                  TEXT NOT NULL DEFAULT 'active', -- 'active', 'maintenance', 'degraded', 'restricted_use'
        notes                   TEXT,
        rate_limits_info        JSONB NULLABLE, -- {"requests_per_minute": 100, "tokens_per_minute": 60000}
        created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_llm_models_provider_tier_status ON core.llm_models (provider, access_tier, status);
    -- Add updated_at trigger similar to core.prompts
    CREATE TRIGGER trg_core_llm_models_set_updated_at
    BEFORE UPDATE ON core.llm_models
    FOR EACH ROW EXECUTE FUNCTION core.set_updated_at_trigger_func();
    ```
*   **Fallback & Retry:** Router attempts retries on primary model, then fallbacks from `core.prompts.model_preferences` or `core_llm_models` (e.g., another model in same `access_tier` or `target_llm_family`).
*   **Monitoring:** Logs routing decisions, latencies. `sinex.agent.llm_api_call` events logged by LLM client component provide detailed call metrics.

## 4. DSPy/LangGraph Integration [UG Sec 26.4, CR5, SA4]

Frameworks for building complex, stateful, multi-agent LLM applications.

### 4.1. Persistence for LangGraph States & DSPy Programs

*   **LangGraph Checkpointers [CR5, SA4]:**
    *   `LangGraph().compile(checkpointer=...)` saves `AgentState` after node executions.
    *   Backends:
        *   SQLite: `langgraph.checkpoint.sqlite.SqliteSaver.from_conn_string("my_langgraph_checkpoints.sqlite")`.
        *   PostgreSQL: Custom `AsyncCheckpointSaver` using `sqlx`/`asyncpg` to store in `langgraph_checkpoints` table (columns: `thread_id TEXT PK`, `checkpoint_id TEXT PK` (or use composite PK), `parent_checkpoint_id TEXT NULLABLE`, `serialized_graph_state JSONB`, `ts TIMESTAMPTZ`).
        *   Redis: `langgraph.checkpoint.redis.RedisSaver.from_url("redis://...")`.
    *   `thread_id`: Unique ID for each independent execution flow. Passed in `config={"configurable": {"thread_id": "..."}}`.
*   **DSPy Program Serialization [CR5]:**
    *   Save optimized DSPy programs: `optimized_program.save("path/to/program.json")`.
    *   Load: `MyDSPyProgram().load("path/to/program.json")`.
    *   Store JSONs as `core_blobs` (annexed), referenced by `core_prompts` or agent manifests.
    *   Save DSPy optimization traces (prompts tried, metrics) as JSON blobs.
*   **Hybrid State Persistence [CR5]:** Redis for fast/ephemeral LangGraph checkpoints/DSPy cache, PostgreSQL for durable storage.

### 4.2. Debugging and Visualization

*   **LangGraph State History:** `compiled_graph.get_state_history(config=thread_config)` retrieves all saved state snapshots for a `thread_id`.
*   **LangSmith:** Hosted tracing/debugging UI for LangChain/LangGraph.
*   **Custom Visualization [CR5 - D3.js]:** Export LangGraph structure (`graph.get_graph().draw_mermaid_png()`). Use checkpoint data (node executed, state diffs) to dynamically visualize execution flow with D3.js, Cytoscape.js.
*   **Logging:** Standard Python `logging` within LangGraph nodes / DSPy modules.
*   **DSPy Tracing:** `dspy.settings.configure(trace=[])` logs prompt generation, LLM calls, metrics.

### 4.3. Optimization and Operational Considerations [CR5]

*   **Memory for Long Contexts in State:** Implement state summarization/truncation for long-running LangGraph flows if `AgentState` grows too large.
*   **Retry Strategies for Fallible Nodes:** Use LangGraph conditional edges to route to retry logic (with exponential backoff in state) or error handlers. Use circuit breakers (`pybreaker`) for external API calls.
*   **Cost Tracking for LLM Nodes:** Instrument nodes/modules to log token usage and cost via `sinex.agent.llm_api_call` events or Prometheus metrics. Accumulate total cost in `AgentState`.
*   **OpenTelemetry Integration:** Instrument LangGraph node executions and DSPy `forward` calls as OTel spans for distributed tracing (see `TIM-ObservabilityStackSetup.md`).

