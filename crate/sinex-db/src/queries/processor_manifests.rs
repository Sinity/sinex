//! Processor manifest query registry for managing processor registrations
//!
//! This module provides all database queries related to processor manifests,
//! including automatons, ingestors, and other processor types.

use crate::query_builder::{QueryBuilder, QueryParam};
use chrono::{DateTime, Utc};

/// Processor manifest query registry
pub struct ProcessorManifestQueries;

impl ProcessorManifestQueries {
    /// Insert a new processor manifest
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn insert_manifest(
        processor_name: String,
        processor_type: String,
        processor_version: String,
        hostname: String,
    ) -> QueryBuilder {
        QueryBuilder::insert("core.processor_manifests")
            .columns(&[
                "processor_name",
                "processor_type",
                "processor_version",
                "hostname",
            ])
            .values(&[
                QueryParam::String(processor_name),
                QueryParam::String(processor_type),
                QueryParam::String(processor_version),
                QueryParam::String(hostname),
            ])
    }

    /// Insert a complete processor manifest with all fields
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    #[allow(clippy::too_many_arguments)]
    pub fn insert_complete_manifest(
        processor_name: String,
        processor_type: String,
        description: Option<String>,
        version: Option<String>,
        processor_version: String,
        hostname: String,
        status: Option<String>,
        config_template_json: Option<serde_json::Value>,
        produces_event_types: Option<serde_json::Value>,
        consumes_event_types: Option<serde_json::Value>,
        required_capabilities: Option<serde_json::Value>,
        llm_dependencies: Option<serde_json::Value>,
        repo_url: Option<String>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::insert("core.processor_manifests")
            .columns(&[
                "processor_name",
                "processor_type",
                "processor_version",
                "hostname",
            ])
            .values(&[
                QueryParam::String(processor_name),
                QueryParam::String(processor_type),
                QueryParam::String(processor_version),
                QueryParam::String(hostname),
            ]);

        // Add optional fields
        if let Some(desc) = description {
            builder = builder
                .columns(&["description"])
                .values(&[QueryParam::String(desc)]);
        }

        if let Some(ver) = version {
            builder = builder
                .columns(&["version"])
                .values(&[QueryParam::String(ver)]);
        }

        if let Some(stat) = status {
            builder = builder
                .columns(&["status"])
                .values(&[QueryParam::String(stat)]);
        }

        if let Some(config) = config_template_json {
            builder = builder
                .columns(&["config_template_json"])
                .values(&[QueryParam::Json(config)]);
        }

        if let Some(produces) = produces_event_types {
            builder = builder
                .columns(&["produces_event_types"])
                .values(&[QueryParam::Json(produces)]);
        }

        if let Some(consumes) = consumes_event_types {
            builder = builder
                .columns(&["consumes_event_types"])
                .values(&[QueryParam::Json(consumes)]);
        }

        if let Some(capabilities) = required_capabilities {
            builder = builder
                .columns(&["required_capabilities"])
                .values(&[QueryParam::Json(capabilities)]);
        }

        if let Some(llm_deps) = llm_dependencies {
            builder = builder
                .columns(&["llm_dependencies"])
                .values(&[QueryParam::Json(llm_deps)]);
        }

        if let Some(url) = repo_url {
            builder = builder
                .columns(&["repo_url"])
                .values(&[QueryParam::String(url)]);
        }

        builder
    }

    /// Update processor manifest status
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_status(
        processor_name: String,
        processor_type: String,
        status: String,
    ) -> QueryBuilder {
        QueryBuilder::update("core.processor_manifests")
            .set("status", QueryParam::String(status))
            .where_eq("processor_name", QueryParam::String(processor_name))
            .where_eq("processor_type", QueryParam::String(processor_type))
    }

    /// Update processor manifest with heartbeat and optional error info
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_heartbeat(
        processor_name: String,
        processor_type: String,
        status: String,
        heartbeat_ts: DateTime<Utc>,
        description: Option<String>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::update("core.processor_manifests")
            .set("status", QueryParam::String(status))
            .set("last_heartbeat_ts", QueryParam::Timestamp(heartbeat_ts))
            .where_eq("processor_name", QueryParam::String(processor_name))
            .where_eq("processor_type", QueryParam::String(processor_type));

        if let Some(desc) = description {
            builder = builder.set("description", QueryParam::String(desc));
        }

        builder
    }

    /// Update processor manifest version and description
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_version_and_description(
        processor_name: String,
        processor_type: String,
        version: String,
        description: Option<String>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::update("core.processor_manifests")
            .set("version", QueryParam::String(version))
            .where_eq("processor_name", QueryParam::String(processor_name))
            .where_eq("processor_type", QueryParam::String(processor_type));

        if let Some(desc) = description {
            builder = builder.set("description", QueryParam::String(desc));
        }

        builder
    }

    /// Update complete processor manifest
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn update_complete_manifest(
        processor_name: String,
        processor_type: String,
        version: String,
        status: String,
        config_template_json: Option<serde_json::Value>,
        produces_event_types: Option<serde_json::Value>,
        consumes_event_types: Option<serde_json::Value>,
        runtime_metadata: Option<serde_json::Value>,
    ) -> QueryBuilder {
        let mut builder = QueryBuilder::update("core.processor_manifests")
            .set("version", QueryParam::String(version))
            .set("status", QueryParam::String(status))
            .where_eq("processor_name", QueryParam::String(processor_name))
            .where_eq("processor_type", QueryParam::String(processor_type));

        if let Some(config) = config_template_json {
            builder = builder.set("config_template_json", QueryParam::Json(config));
        }

        if let Some(produces) = produces_event_types {
            builder = builder.set("produces_event_types", QueryParam::Json(produces));
        }

        if let Some(consumes) = consumes_event_types {
            builder = builder.set("consumes_event_types", QueryParam::Json(consumes));
        }

        if let Some(metadata) = runtime_metadata {
            builder = builder.set("runtime_metadata", QueryParam::Json(metadata));
        }

        builder
    }

    /// Delete a processor manifest
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn delete_manifest(processor_name: String) -> QueryBuilder {
        QueryBuilder::delete("core.processor_manifests")
            .where_eq("processor_name", QueryParam::String(processor_name))
    }

    /// Delete a processor manifest by name and type
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.execute(pool)`
    pub fn delete_manifest_by_type(processor_name: String, processor_type: String) -> QueryBuilder {
        QueryBuilder::delete("core.processor_manifests")
            .where_eq("processor_name", QueryParam::String(processor_name))
            .where_eq("processor_type", QueryParam::String(processor_type))
    }

    /// Get processor manifest by name
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<ProcessorManifestRecord>(pool)`
    pub fn get_manifest_by_name(processor_name: String) -> QueryBuilder {
        QueryBuilder::select("core.processor_manifests")
            .columns(&[
                "processor_name",
                "processor_type",
                "description",
                "version",
                "processor_version",
                "hostname",
                "status",
                "config_template_json",
                "produces_event_types",
                "consumes_event_types",
                "required_capabilities",
                "llm_dependencies",
                "repo_url",
                "created_at",
                "updated_at",
            ])
            .where_eq("processor_name", QueryParam::String(processor_name))
    }

    /// Get processor manifest by name and type
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<ProcessorManifestRecord>(pool)`
    pub fn get_manifest_by_name_and_type(
        processor_name: String,
        processor_type: String,
    ) -> QueryBuilder {
        QueryBuilder::select("core.processor_manifests")
            .columns(&[
                "processor_name",
                "processor_type",
                "description",
                "version",
                "processor_version",
                "hostname",
                "status",
                "config_template_json",
                "produces_event_types",
                "consumes_event_types",
                "required_capabilities",
                "llm_dependencies",
                "repo_url",
                "created_at",
                "updated_at",
            ])
            .where_eq("processor_name", QueryParam::String(processor_name))
            .where_eq("processor_type", QueryParam::String(processor_type))
    }

    /// Get all processor manifests of a specific type
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ProcessorManifestRecord>(pool)`
    pub fn get_manifests_by_type(processor_type: String) -> QueryBuilder {
        QueryBuilder::select("core.processor_manifests")
            .columns(&[
                "processor_name",
                "processor_type",
                "description",
                "version",
                "processor_version",
                "hostname",
                "status",
                "config_template_json",
                "produces_event_types",
                "consumes_event_types",
                "required_capabilities",
                "llm_dependencies",
                "repo_url",
                "created_at",
                "updated_at",
            ])
            .where_eq("processor_type", QueryParam::String(processor_type))
            .order_by("processor_name", "ASC")
    }

    /// Get expected automaton names
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<(String,)>(pool)`
    pub fn get_expected_automatons() -> QueryBuilder {
        QueryBuilder::select("core.processor_manifests")
            .columns(&["DISTINCT processor_name"])
            .where_eq("processor_type", QueryParam::String("automaton".to_string()))
    }

    /// Get processor manifests by status
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ProcessorManifestRecord>(pool)`
    pub fn get_manifests_by_status(status: String) -> QueryBuilder {
        QueryBuilder::select("core.processor_manifests")
            .columns(&[
                "processor_name",
                "processor_type",
                "description",
                "version",
                "processor_version",
                "hostname",
                "status",
                "config_template_json",
                "produces_event_types",
                "consumes_event_types",
                "required_capabilities",
                "llm_dependencies",
                "repo_url",
                "created_at",
                "updated_at",
            ])
            .where_eq("status", QueryParam::String(status))
            .order_by("last_heartbeat_ts", "DESC")
    }

    /// Get processor manifests that consume specific event types
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_all::<ProcessorManifestRecord>(pool)`
    pub fn get_manifests_by_consumed_event_type(event_type: String) -> QueryBuilder {
        QueryBuilder::select("core.processor_manifests")
            .columns(&[
                "processor_name",
                "processor_type",
                "description",
                "version",
                "processor_version",
                "hostname",
                "status",
                "config_template_json",
                "produces_event_types",
                "consumes_event_types",
                "required_capabilities",
                "llm_dependencies",
                "repo_url",
                "created_at",
                "updated_at",
            ])
            .where_op("consumes_event_types", "@>", QueryParam::Json(serde_json::json!([event_type])))
    }

    /// Check if a processor manifest exists
    ///
    /// # Returns
    /// QueryBuilder that can be executed with `.fetch_optional::<(i32,)>(pool)`
    pub fn manifest_exists(processor_name: String, processor_type: String) -> QueryBuilder {
        QueryBuilder::select("core.processor_manifests")
            .columns(&["1"])
            .where_eq("processor_name", QueryParam::String(processor_name))
            .where_eq("processor_type", QueryParam::String(processor_type))
            .limit(1)
    }
}