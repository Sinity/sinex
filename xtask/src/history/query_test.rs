// Inline because these regressions exercise the private query execution path directly.
use super::*;
use crate::sandbox::prelude::*;
use rusqlite::params;
use tempfile::tempdir;

#[sinex_test]
async fn test_run_invocation_query_surfaces_invalid_started_at() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-query-invalid-started-at.db");
    let db = HistoryDb::open(&db_path)?;

    let id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.1)?;
    db.conn.execute(
        "UPDATE invocations SET started_at = ?1 WHERE id = ?2",
        params!["bad-query-started-at", id],
    )?;

    let error = InvocationQuery::new()
        .command("check")
        .run(&db)
        .expect_err("invalid started_at should surface from invocation queries");
    assert!(format!("{error:#}").contains("invalid invocation started_at"));
    Ok(())
}

#[sinex_test]
async fn test_run_invocation_query_surfaces_invalid_finished_at() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-query-invalid-finished-at.db");
    let db = HistoryDb::open(&db_path)?;

    let id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.1)?;
    db.conn.execute(
        "UPDATE invocations SET finished_at = ?1 WHERE id = ?2",
        params!["bad-query-finished-at", id],
    )?;

    let error = InvocationQuery::new()
        .command("check")
        .run(&db)
        .expect_err("invalid finished_at should surface from invocation queries");
    assert!(format!("{error:#}").contains("invalid invocation finished_at"));
    Ok(())
}

#[sinex_test]
async fn test_invocation_query_supports_exact_and_bounded_scopes() -> TestResult<()> {
    let db = HistoryDb::open_in_memory()?;

    let oldest = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(oldest, InvocationStatus::Success, Some(0), 0.1)?;
    let middle = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(middle, InvocationStatus::Success, Some(0), 0.2)?;
    let newest = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(newest, InvocationStatus::Failed, Some(1), 0.3)?;

    let exact = InvocationQuery::new().for_invocation(middle).run(&db)?;
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].id, middle);

    let after_middle = InvocationQuery::new().after_invocation(middle).run(&db)?;
    assert_eq!(
        after_middle.iter().map(|inv| inv.id).collect::<Vec<_>>(),
        vec![newest]
    );

    let before_newest = InvocationQuery::new().before_invocation(newest).run(&db)?;
    assert_eq!(
        before_newest.iter().map(|inv| inv.id).collect::<Vec<_>>(),
        vec![middle, oldest]
    );

    Ok(())
}

#[sinex_test]
async fn test_invocation_query_offset_and_sort_controls() -> TestResult<()> {
    let db = HistoryDb::open_in_memory()?;

    let fast = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(fast, InvocationStatus::Success, Some(0), 0.1)?;
    let medium = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(medium, InvocationStatus::Success, Some(0), 0.5)?;
    let slow_fail = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(slow_fail, InvocationStatus::Failed, Some(1), 1.5)?;

    let duration_sorted = InvocationQuery::new().sort_duration().run(&db)?;
    assert_eq!(
        duration_sorted.iter().map(|inv| inv.id).collect::<Vec<_>>(),
        vec![slow_fail, medium, fast]
    );

    let paged = InvocationQuery::new()
        .limit(1)
        .offset(1)
        .sort_started()
        .run(&db)?;
    assert_eq!(paged.len(), 1);
    assert_eq!(paged[0].id, medium);

    Ok(())
}

#[sinex_test]
async fn test_invocation_query_filters_zombie_cancellations_by_default() -> TestResult<()> {
    let db = HistoryDb::open_in_memory()?;

    let success = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(success, InvocationStatus::Success, Some(0), 0.1)?;

    let stale = db.start_invocation("check", None, None, None)?;
    db.finish_invocation_cancelled(stale, None, 0.2, "stale_pid", "open_time_sweep")?;

    let null_reason = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(null_reason, InvocationStatus::Cancelled, None, 0.3)?;

    let user_cancel = db.start_invocation("check", None, None, None)?;
    db.finish_invocation_cancelled(user_cancel, None, 0.4, "user_cancel", "user")?;

    let visible_ids = InvocationQuery::new()
        .command("check")
        .run(&db)?
        .into_iter()
        .map(|invocation| invocation.id)
        .collect::<Vec<_>>();
    assert_eq!(visible_ids, vec![user_cancel, success]);
    assert_eq!(InvocationQuery::new().command("check").count(&db)?, 2);

    let with_zombies = InvocationQuery::new()
        .command("check")
        .include_zombies()
        .run(&db)?
        .into_iter()
        .map(|invocation| invocation.id)
        .collect::<Vec<_>>();
    assert_eq!(with_zombies, vec![user_cancel, null_reason, stale, success]);

    let exact_zombie = InvocationQuery::new().for_invocation(stale).run(&db)?;
    assert_eq!(exact_zombie.len(), 1);
    assert_eq!(exact_zombie[0].id, stale);

    Ok(())
}
