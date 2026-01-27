use std::collections::BTreeMap;

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    println!("== Pool Stats ==");
    let stats = sinex_test_utils::get_pool_stats_async().await;
    println!(
        "total_acquisitions={}, avg_wait_ms={}, total_conns={}, idle_conns={}, cleanup_failures={}, template_recreations={}",
        stats.total_acquisitions,
        stats.average_wait_time_ms,
        stats.total_connections,
        stats.idle_connections,
        stats.cleanup_failures,
        stats.template_recreations
    );

    println!("\n== Slot Health ==");
    for slot in sinex_test_utils::get_slot_stats() {
        println!(
            "- {name}: conns={total}/{idle}, last_clean={clean:?}, result={result:?}",
            name = slot.name,
            total = slot.total_connections,
            idle = slot.idle_connections,
            clean = slot.last_clean_time,
            result = slot.last_clean_result,
        );
        if let Some(residuals) = slot.residuals.as_ref() {
            if !residuals.is_empty() {
                println!("  residuals: {:?}", residuals);
            }
        }
    }

    if std::env::var("DATABASE_URL").is_err() {
        println!("DATABASE_URL not set; skipping database checks.");
        return Ok(());
    }

    let pool = xtask::sandbox::db_common::test_db_pool().await;

    println!("\n== Session State ==");
    if let Err(e) = sinex_test_utils::ensure_default_session_state(&pool).await {
        eprintln!("Failed to normalize session state: {e}");
    } else {
        println!("Session replication role, row_security, and triggers are normalized.");
    }

    println!("\n== Table Row Counts ==");
    match xtask::sandbox::db_common::get_row_counts(&pool).await {
        Ok(counts) => {
            let mut sorted: BTreeMap<_, _> = counts.into_iter().collect();
            for (table, count) in sorted.iter_mut() {
                println!("{table:<40} {count}");
            }
        }
        Err(e) => eprintln!("Failed to gather row counts: {e}"),
    }

    Ok(())
}
