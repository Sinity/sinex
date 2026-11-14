use super::*;
use crate::admission::{PARENT_PAGE, parent_pages};

fn child(parents: Vec<EventId>) -> Event {
    let mut event = imported("synthetic-children", "derived content", record());
    event.origin = Origin::Derived { parents };
    event
}
async fn parents(conn: &mut crate::AdmissionContext, n: usize) -> Vec<EventId> {
    let mut events = (0..n)
        .map(|i| imported("synthetic-parents", &format!("parent-{i}"), record()))
        .collect::<Vec<_>>();
    events
        .iter_mut()
        .max_by_key(|event| event.id)
        .unwrap()
        .class = Class::Guarded;
    let ids = events.iter().map(|event| event.id).collect();
    admit_one(conn, &batch(RunId::new(), events)).await;
    ids
}
#[devkit::test]
async fn parent_authority_deduplicates_before_query_pages_and_preserves_parent_lists(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = parents(&mut conn, PARENT_PAGE * 2 + 1).await;
    let offered = ids
        .iter()
        .rev()
        .copied()
        .cycle()
        .take(ids.len() * 8)
        .collect::<Vec<_>>();
    let event = child(offered.clone());
    let original = event.origin.clone();
    let pages = parent_pages(std::slice::from_ref(&event)).collect::<Vec<_>>();
    assert_eq!(
        pages.iter().map(Vec::len).collect::<Vec<_>>(),
        [PARENT_PAGE, PARENT_PAGE, 1]
    );
    let unique = pages.into_iter().flatten().collect::<Vec<_>>();
    assert!(unique.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(unique.len(), ids.len());
    assert_eq!(event.origin, original);
    let id = event.id;
    admit_one(&mut conn, &batch(RunId::new(), vec![event])).await;
    let (stored, class): (Vec<Uuid>, String) =
        sqlx::query_as("SELECT parents,class FROM core.events WHERE id=$1")
            .bind(id.0)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert_eq!(stored, offered.iter().map(|id| id.0).collect::<Vec<_>>());
    assert_eq!(class, "guarded");
}
#[devkit::test]
async fn late_parent_refusal_rolls_back_and_retry_keeps_all_authority(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = parents(&mut conn, PARENT_PAGE * 2 + 1).await;
    let mut ordered = ids.clone();
    ordered.sort();
    let late = *ordered.last().unwrap();
    let good = child(ids.clone());
    let missing = EventId(Uuid::parse_str("01ffffff-ffff-7000-8000-000000000001").unwrap());
    let mut absent = good.clone();
    absent.id = EventId::new();
    if let Origin::Derived { parents } = &mut absent.origin {
        parents.push(missing);
    }
    let before = crate::query::current_seq(&mut conn).await.unwrap();
    let request = batch(RunId::new(), vec![absent.clone()]);
    let mut tx = conn.begin().await.unwrap();
    let error = tx.admit(&request, &NoDerivations).await.unwrap_err();
    assert!(
        error.to_string().contains("not a committed reading"),
        "{error}"
    );
    tx.rollback().await.unwrap();
    assert_eq!(crate::query::current_seq(&mut conn).await.unwrap(), before);
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM core.events WHERE id=$1)")
            .bind(absent.id.0)
            .fetch_one(&mut *conn)
            .await
            .unwrap()
    );
    let mut tx = conn.begin().await.unwrap();
    crate::admission::remove_live(&mut tx, &[late.0])
        .await
        .unwrap();
    let error = tx
        .admit(&batch(RunId::new(), vec![good.clone()]), &NoDerivations)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not live"), "{error}");
    tx.rollback().await.unwrap();
    assert_eq!(crate::query::current_seq(&mut conn).await.unwrap(), before);
    // The failed transaction restores membership as well as all event writes.
    admit_one(&mut conn, &batch(RunId::new(), vec![good.clone()])).await;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT class FROM core.events WHERE id=$1")
            .bind(good.id.0)
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        "guarded"
    );
}

#[devkit::test]
async fn compressed_parent_planning_uses_bounded_arrays_after_duplicate_elimination(
    db: devkit::pg::TestDatabase,
) {
    use sqlx::Executor;
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    // Check the production SQL's bindings on another connection. This macro
    // must not put a persistent statement into the measured connection cache.
    let mut checked = PgConnection::connect(&db.url(None)).await.unwrap();
    sqlx::query_file!("src/admission/parent_authority.sql", &Vec::<Uuid>::new())
        .fetch_all(&mut checked)
        .await
        .unwrap();
    checked.close().await.unwrap();
    sqlx::query("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class)
      SELECT ('01800000-0000-7000-8000-'||lpad(to_hex(n),12,'0'))::uuid,'command.imported','synthetic-parent-plan',uuidv7(),uuidv7(),n,'unknown',1,
      jsonb_build_object('command','synthetic'),1,decode(repeat('01',32),'hex'),'open' FROM generate_series(1,8192)n")
      .execute(&mut *conn).await.unwrap();
    conn.execute("SELECT compress_chunk(c) FROM show_chunks('core.events') c")
        .await
        .unwrap();
    conn.execute("ANALYZE core.events").await.unwrap();
    conn.execute("SET plan_cache_mode=auto; SET max_parallel_workers_per_gather=0")
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SHOW plan_cache_mode")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        "auto"
    );
    let selected = (1..=16)
        .map(|n| Uuid::parse_str(&format!("01800000-0000-7000-8000-{n:012x}")).unwrap())
        .collect::<Vec<_>>();
    let repeated = selected
        .iter()
        .copied()
        .cycle()
        .take(4096)
        .collect::<Vec<_>>();
    async fn planning(conn: &mut PgConnection, ids: &[Uuid]) -> Value {
        let sql = format!(
            "EXPLAIN (MEMORY,FORMAT JSON) {}",
            include_str!("../../parent_authority.sql")
        );
        sqlx::query_scalar::<_, Value>(&sql)
            .bind(ids)
            .persistent(false)
            .fetch_one(conn)
            .await
            .unwrap()
    }
    let old = planning(&mut conn, &repeated).await;
    let new = planning(&mut conn, &selected).await;
    fn memory(plan: &Value) -> u64 {
        plan[0]["Planning"]["Memory Used"]
            .as_u64()
            .expect("PostgreSQL planner memory measurement")
    }
    let old_memory = memory(&old);
    let new_memory = memory(&new);
    eprintln!(
        "compressed parent planner memory kB: duplicate4096={old_memory}, unique16={new_memory}"
    );
    assert!(
        old_memory > new_memory * 2,
        "actual planner allocation must discriminate duplicate expansion: old={old_memory} new={new_memory}"
    );
    let unique = (1..=1024)
        .map(|n| Uuid::parse_str(&format!("01800000-0000-7000-8000-{n:012x}")).unwrap())
        .collect::<Vec<_>>();
    let unique_old = memory(&planning(&mut conn, &unique).await);
    let all = child(unique.iter().copied().map(EventId).collect());
    let pages = parent_pages(std::slice::from_ref(&all)).collect::<Vec<_>>();
    assert_eq!(pages.len(), 4);
    let mut page_peak = 0;
    let mut received = Vec::new();
    for page in &pages {
        assert!(page.len() <= PARENT_PAGE);
        page_peak = page_peak.max(memory(&planning(&mut conn, page).await));
        received.extend(
            sqlx::query_as::<_, crate::admission::ParentAuthority>(include_str!(
                "../../parent_authority.sql"
            ))
            .bind(page)
            .persistent(false)
            .fetch_all(&mut *conn)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.id),
        );
    }
    received.sort();
    assert_eq!(received, unique);
    eprintln!(
        "compressed unique parent planner memory kB: full1024={unique_old}, max256page={page_peak}"
    );
    assert!(
        unique_old > page_peak * 2,
        "actual unique-parent planner allocation must shrink per query: old={unique_old} peak={page_peak}"
    );
    // Execute the exact new path repeatedly, beyond PostgreSQL's usual generic
    // plan transition, and keep the hard per-query transfer bound executable.
    let event = child(
        selected
            .iter()
            .copied()
            .map(EventId)
            .cycle()
            .take(4096)
            .collect(),
    );
    for _ in 0..8 {
        let pages = parent_pages(std::slice::from_ref(&event)).collect::<Vec<_>>();
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0], selected);
        let rows = sqlx::query_as::<_, crate::admission::ParentAuthority>(include_str!(
            "../../parent_authority.sql"
        ))
        .bind(&pages[0])
        .persistent(false)
        .fetch_all(&mut *conn)
        .await
        .unwrap();
        assert_eq!(rows.len(), 16);
        assert!(rows.iter().all(|row| !row.live));
    }
}
