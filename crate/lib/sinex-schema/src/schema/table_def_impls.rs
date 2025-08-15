//! TableDef trait implementations for schema types

use crate::schema::{
    core_events::Events, entities::Entities, processors::OperationsLog,
    source_materials::SourceMaterials, TableDef,
};

impl TableDef for Events {
    fn table_name() -> &'static str {
        "events"
    }

    fn schema_name() -> &'static str {
        "core"
    }

    fn primary_key() -> &'static str {
        "id"
    }
}

impl TableDef for Entities {
    fn table_name() -> &'static str {
        "entities"
    }

    fn schema_name() -> &'static str {
        "core"
    }

    fn primary_key() -> &'static str {
        "id"
    }
}

impl TableDef for SourceMaterials {
    fn table_name() -> &'static str {
        "source_material_registry"
    }

    fn schema_name() -> &'static str {
        "raw"
    }

    fn primary_key() -> &'static str {
        "id"
    }
}

impl TableDef for OperationsLog {
    fn table_name() -> &'static str {
        "operations_log"
    }

    fn schema_name() -> &'static str {
        "core"
    }

    fn primary_key() -> &'static str {
        "operation_id"
    }
}
