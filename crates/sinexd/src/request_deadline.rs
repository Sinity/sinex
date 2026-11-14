//! The accepted request's read budget, ending when a mutation takes ownership.
use crate::{Error, Result};
use command_surface::{Failure, FailureKind};
use sqlx::{Connection, PgConnection, PgPool};
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

tokio::task_local! { static CURRENT: Arc<Budget>; }

struct Budget {
    start: Instant,
    duration: Option<Duration>,
    admitted: AtomicBool,
    cancelled: AtomicBool,
    cleanup: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
impl Budget {
    fn remaining(&self) -> Option<Duration> {
        if self.admitted.load(Ordering::Acquire) {
            None
        } else {
            self.duration
                .map(|duration| duration.saturating_sub(self.start.elapsed()))
        }
    }
    fn check(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Acquire)
            || self.remaining().is_some_and(|left| left.is_zero())
        {
            Err(timeout())
        } else {
            Ok(())
        }
    }
}
fn current() -> Option<Arc<Budget>> {
    CURRENT.try_with(Arc::clone).ok()
}
pub(crate) fn timeout() -> Error {
    let mut failure = Failure::new(FailureKind::Timeout, "the request's read deadline passed")
        .with_code(command_surface::DEADLINE_EXCEEDED)
        .with_hint("narrow the query, raise --deadline MS, or use --deadline none");
    failure.retryable = true;
    failure.into()
}

/// Queue reservation is still read work. Once the writer takes the job, the
/// caller waits for its actual result even if the read budget then expires.
pub(crate) fn admit() -> Result<()> {
    if let Some(budget) = current() {
        budget.check()?;
        budget.admitted.store(true, Ordering::Release);
    }
    Ok(())
}

/// Stop the server statement timer before transferring an owned mutation.
/// The outer read budget still applies until the writer accepts its job.
pub(crate) async fn handoff_read(conn: &mut PgConnection) -> Result<()> {
    if let Some(budget) = current() {
        budget.check()?;
        sinex_store::query::set_request_deadline(conn, None).await?;
        budget.check()?;
    }
    Ok(())
}

/// Some authenticated mutations first persist a request run or operation on
/// their held connection. End the read phase before that first durable write.
pub(crate) async fn finish_read(conn: &mut PgConnection) -> Result<()> {
    if let Some(budget) = current() {
        budget.check()?;
        sinex_store::query::set_request_deadline(conn, None).await?;
        admit()?;
    }
    Ok(())
}

struct InFlight<F: Future + Send + 'static>
where
    F::Output: Send + 'static,
{
    future: Option<Pin<Box<F>>>,
    budget: Arc<Budget>,
    done: bool,
}
impl<F: Future + Send + 'static> Future for InFlight<F>
where
    F::Output: Send + 'static,
{
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self
            .future
            .as_mut()
            .expect("running request")
            .as_mut()
            .poll(cx);
        if result.is_ready() {
            self.done = true;
        }
        result
    }
}
impl<F: Future + Send + 'static> Drop for InFlight<F>
where
    F::Output: Send + 'static,
{
    fn drop(&mut self) {
        if !self.done {
            if self.budget.admitted.load(Ordering::Acquire) {
                // Losing an HTTP reader cannot revoke a mutation already owned
                // by the writer. Its remaining finalization still completes.
                if let Some(future) = self.future.take() {
                    tokio::spawn(future);
                }
            } else {
                self.budget.cancelled.store(true, Ordering::Release);
                self.future.take();
            }
        }
    }
}

pub(crate) async fn run<T: Send + 'static>(
    duration: Option<Duration>,
    operation: impl Future<Output = Result<T>> + Send + 'static,
) -> Result<T> {
    let budget = Arc::new(Budget {
        start: Instant::now(),
        duration,
        admitted: AtomicBool::new(false),
        cancelled: AtomicBool::new(false),
        cleanup: Mutex::new(Vec::new()),
    });
    let mut request = InFlight {
        future: Some(Box::pin(CURRENT.scope(budget.clone(), operation))),
        budget: budget.clone(),
        done: false,
    };
    let result = loop {
        let Some(left) = budget.remaining() else {
            break (&mut request).await;
        };
        if left.is_zero() {
            break Err(timeout());
        }
        // Very large caller budgets remain valid without overflowing Instant
        // or introducing PostgreSQL's signed-millisecond ceiling as a cap.
        tokio::select! {
            result = &mut request => break result,
            _ = tokio::time::sleep(left.min(Duration::from_secs(86_400))) => {
                if budget.remaining().is_some_and(|left| left.is_zero()) { break Err(timeout()) }
            }
        }
    };
    drop(request);
    let cleanup = std::mem::take(&mut *budget.cleanup.lock().expect("request cleanup"));
    for task in cleanup {
        let _ = task.await;
    }
    result
}

enum ConnectionKind {
    Pooled(sqlx::pool::PoolConnection<sqlx::Postgres>),
    Detached(PgConnection),
}
/// A cancelled read closes only its own backend. Successful reads keep normal
/// pool reuse; SQL continuation cursors keep their existing detached lifetime.
pub(crate) struct ReadConnection {
    inner: Option<ConnectionKind>,
    pool: PgPool,
    pid: Option<i32>,
    budget: Option<Arc<Budget>>,
}
impl Deref for ReadConnection {
    type Target = PgConnection;
    fn deref(&self) -> &PgConnection {
        match self.inner.as_ref().expect("read connection") {
            ConnectionKind::Pooled(conn) => conn,
            ConnectionKind::Detached(conn) => conn,
        }
    }
}
impl DerefMut for ReadConnection {
    fn deref_mut(&mut self) -> &mut PgConnection {
        match self.inner.as_mut().expect("read connection") {
            ConnectionKind::Pooled(conn) => conn,
            ConnectionKind::Detached(conn) => conn,
        }
    }
}
impl ReadConnection {
    pub(crate) fn detach(mut self) -> Self {
        self.inner = self.inner.take().map(|inner| match inner {
            ConnectionKind::Pooled(conn) => ConnectionKind::Detached(conn.detach()),
            other => other,
        });
        self
    }
    pub(crate) fn into_detached(mut self) -> PgConnection {
        match self.inner.take().expect("read connection") {
            ConnectionKind::Pooled(conn) => conn.detach(),
            ConnectionKind::Detached(conn) => conn,
        }
    }
    pub(crate) async fn prepare(&mut self) -> Result<()> {
        self.budget = current();
        if let Some(budget) = &self.budget {
            budget.check()?;
        }
        let remaining = self.budget.as_ref().and_then(|budget| budget.remaining());
        sinex_store::query::set_request_deadline(self, remaining).await?;
        Ok(())
    }
}
impl Drop for ReadConnection {
    fn drop(&mut self) {
        if let Some(budget) = &self.budget
            && budget.cancelled.load(Ordering::Acquire)
        {
            let Some(inner) = self.inner.take() else {
                return;
            };
            let conn = match inner {
                ConnectionKind::Pooled(conn) => conn.detach(),
                ConnectionKind::Detached(conn) => conn,
            };
            let pool = self.pool.clone();
            let pid = self.pid;
            let cleanup = tokio::spawn(async move {
                let _ = tokio::time::timeout(Duration::from_millis(100), async move {
                    if let Some(pid) = pid
                        && let Ok(mut cancel) = pool.acquire().await
                    {
                        // The target remains owned and detached until cancellation
                        // completes, so its PID cannot name a reused pool request.
                        let _ = sqlx::query("SELECT pg_cancel_backend($1)")
                            .bind(pid)
                            .execute(&mut *cancel)
                            .await;
                    }
                    let _ = conn.close().await;
                })
                .await;
            });
            budget
                .cleanup
                .lock()
                .expect("request cleanup")
                .push(cleanup);
        } else if let Some(budget) = &self.budget
            && matches!(self.inner, Some(ConnectionKind::Pooled(_)))
        {
            let Some(ConnectionKind::Pooled(mut conn)) = self.inner.take() else {
                unreachable!()
            };
            let cleanup = tokio::spawn(async move {
                let reset = tokio::time::timeout(Duration::from_millis(100), async {
                    sqlx::query("RESET statement_timeout")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("RESET sinex.request_deadline")
                        .execute(&mut *conn)
                        .await?;
                    Ok::<_, sqlx::Error>(())
                })
                .await;
                if !matches!(reset, Ok(Ok(_))) {
                    conn.close_on_drop();
                }
            });
            budget
                .cleanup
                .lock()
                .expect("request cleanup")
                .push(cleanup);
        }
    }
}

pub(crate) async fn acquire(pool: &PgPool) -> Result<ReadConnection> {
    let budget = current();
    if let Some(budget) = &budget {
        budget.check()?;
    }
    let mut conn = ReadConnection {
        inner: Some(ConnectionKind::Pooled(pool.acquire().await?)),
        pool: pool.clone(),
        pid: None,
        budget,
    };
    conn.prepare().await?;
    conn.pid = Some(
        sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *conn)
            .await?,
    );
    Ok(conn)
}
