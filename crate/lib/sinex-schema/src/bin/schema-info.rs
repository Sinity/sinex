//! CLI tool for querying Sinex schema registry information.
//!
//! This tool provides commands for shell scripts and CI infrastructure to query
//! information about database schemas without hardcoding lists.
//!
//! # Usage
//!
//! ```bash
//! # List all schema names (one per line)
//! cargo run --bin schema-info -- list-schemas
//!
//! # List schemas requiring grants
//! cargo run --bin schema-info -- list-grantable-schemas
//!
//! # Show detailed schema information
//! cargo run --bin schema-info -- describe-schemas
//! ```

use sinex_schema::schema_registry::{schema_names, schemas_requiring_grants, SINEX_SCHEMAS};
use std::env;
use std::process;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <command>", args[0]);
        eprintln!();
        eprintln!("Commands:");
        eprintln!("  list-schemas              List all schema names");
        eprintln!("  list-grantable-schemas    List schemas requiring grants");
        eprintln!("  describe-schemas          Show detailed schema information");
        process::exit(1);
    }

    match args[1].as_str() {
        "list-schemas" => {
            for name in schema_names() {
                println!("{}", name);
            }
        }
        "list-grantable-schemas" => {
            for schema in schemas_requiring_grants() {
                println!("{}", schema.name);
            }
        }
        "describe-schemas" => {
            for schema in SINEX_SCHEMAS {
                println!("{:20} - {}", schema.name, schema.description);
            }
        }
        unknown => {
            eprintln!("Unknown command: {}", unknown);
            process::exit(1);
        }
    }
}
