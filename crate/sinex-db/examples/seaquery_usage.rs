//! Example showing how to use SeaQuery with our schema definitions

use sea_query::{Alias, Expr, PostgresQueryBuilder, Query};
use sinex_db::schema::*;

fn main() {
    println!("=== SeaQuery Schema Usage Examples ===\n");

    // Example 1: Create a simple SELECT query
    let query = Query::select()
        .from((Alias::new(Events::SCHEMA), Alias::new(Events::TABLE)))
        .columns([
            Alias::new(Events::EVENT_ID),
            Alias::new(Events::SOURCE),
            Alias::new(Events::EVENT_TYPE),
            Alias::new(Events::TS_ORIG),
        ])
        .and_where(Expr::col(Alias::new(Events::SOURCE)).eq("filesystem"))
        .order_by(Alias::new(Events::TS_ORIG), sea_query::Order::Desc)
        .limit(10)
        .build(PostgresQueryBuilder);

    println!("1. Select recent filesystem events:");
    println!("{}\n", query);

    // Example 2: Create an INSERT query
    let insert = Query::insert()
        .into_table((
            Alias::new(Checkpoints::SCHEMA),
            Alias::new(Checkpoints::TABLE),
        ))
        .columns([
            Alias::new(Checkpoints::CHECKPOINT_ID),
            Alias::new(Checkpoints::SATELLITE_ID),
            Alias::new(Checkpoints::STATE_TYPE),
            Alias::new(Checkpoints::STATE_DATA),
        ])
        .values_panic([
            "$1".into(),
            "fs-watcher".into(),
            "scan_state".into(),
            "$2".into(),
        ])
        .build(PostgresQueryBuilder);

    println!("2. Insert a new checkpoint:");
    println!("{}\n", insert);

    // Example 3: Create a JOIN query between entities and relations
    let join_query = Query::select()
        .from((Alias::new(Entities::SCHEMA), Alias::new(Entities::TABLE)))
        .columns([
            (Alias::new(Entities::TABLE), Alias::new(Entities::NAME)),
            (
                Alias::new(EntityRelations::TABLE),
                Alias::new(EntityRelations::RELATION_TYPE),
            ),
        ])
        .inner_join(
            (
                Alias::new(EntityRelations::SCHEMA),
                Alias::new(EntityRelations::TABLE),
            ),
            Expr::col((Alias::new(Entities::TABLE), Alias::new(Entities::ENTITY_ID))).equals((
                Alias::new(EntityRelations::TABLE),
                Alias::new(EntityRelations::FROM_ENTITY_ID),
            )),
        )
        .and_where(
            Expr::col((
                Alias::new(Entities::TABLE),
                Alias::new(Entities::ENTITY_TYPE),
            ))
            .eq("person"),
        )
        .build(PostgresQueryBuilder);

    println!("3. Join entities with their relations:");
    println!("{}\n", join_query);

    // Example 4: Create an UPDATE query with JSON operations
    let update_query = Query::update()
        .table((Alias::new(Events::SCHEMA), Alias::new(Events::TABLE)))
        .value(
            Alias::new(Events::PAYLOAD),
            Expr::cust("payload || $1::jsonb"),
        )
        .and_where(Expr::col(Alias::new(Events::EVENT_ID)).eq("$2"))
        .build(PostgresQueryBuilder);

    println!("4. Update event payload with JSON merge:");
    println!("{}\n", update_query);

    // Example 5: Create a complex filtering query
    let complex_query = Query::select()
        .from((Alias::new(Events::SCHEMA), Alias::new(Events::TABLE)))
        .column(Alias::new(Events::EVENT_ID))
        .and_where(Expr::col(Alias::new(Events::SOURCE)).is_in([
            "filesystem",
            "terminal",
            "desktop",
        ]))
        .and_where(Expr::col(Alias::new(Events::TS_ORIG)).between("2024-01-01", "2024-12-31"))
        .and_where(Expr::cust("payload->>'action' = 'create'"))
        .build(PostgresQueryBuilder);

    println!("5. Complex filtering with JSON field access:");
    println!("{}\n", complex_query);

    // Show the generated migration SQL
    println!("=== Generated Migration SQL ===\n");
    let events_table = Events::create_table();
    println!("Events table creation:");
    println!("{}", events_table);
}
