//! Parser job execution loop for the source-worker host.

use sinex_db::DbPool;
use sinex_db::DbPoolExt;
use sinex_primitives::Uuid;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Clone)]
pub enum JobOutcome {
    Completed,
    CompletedWithCaveats { reason: String },
    FailedRetryable { error: String },
    FailedPermanent { error: String },
}

#[derive(Debug, Clone, Default)]
pub struct JobRunStats {
    pub jobs_claimed: u64,
    pub jobs_completed: u64,
    pub jobs_failed_retryable: u64,
    pub jobs_failed_permanent: u64,
}

/// Handler trait for parser-specific logic.
/// Returns `Pin<Box<dyn Future>>` for dyn-compatibility with `&dyn JobHandler`.
pub trait JobHandler: Send + Sync {
    fn process_job(
        &self,
        material_id: Uuid,
        parser_version: &str,
    ) -> Pin<Box<dyn Future<Output = Result<JobOutcome, String>> + Send + '_>>;
}

pub struct ParserJobRunner {
    pool: DbPool,
    lease_owner: String,
    lease_ttl_seconds: i32,
    poll_interval: Duration,
    stats: JobRunStats,
}

impl ParserJobRunner {
    pub fn new(
        pool: DbPool,
        lease_owner: String,
        lease_ttl_seconds: i32,
        poll_interval: Duration,
    ) -> Self {
        Self {
            pool,
            lease_owner,
            lease_ttl_seconds,
            poll_interval,
            stats: JobRunStats::default(),
        }
    }

    pub fn stats(&self) -> &JobRunStats {
        &self.stats
    }

    pub async fn run_once(&mut self, handler: &dyn JobHandler) -> Result<(), String> {
        loop {
            let claimed = self
                .pool
                .parser_jobs()
                .claim_next_job(&self.lease_owner, self.lease_ttl_seconds)
                .await
                .map_err(|e| format!("claim_next_job: {e}"))?;

            let Some(job) = claimed else {
                break;
            };

            self.stats.jobs_claimed += 1;

            match handler
                .process_job(job.source_material_id, &job.parser_version)
                .await
            {
                Ok(JobOutcome::Completed) => {
                    self.pool.parser_jobs().complete_job(job.id).await.map_err(|e| format!("complete_job: {e}"))?;
                    self.stats.jobs_completed += 1;
                }
                Ok(JobOutcome::CompletedWithCaveats { reason }) => {
                    self.pool.parser_jobs().complete_job_with_caveats(job.id, Some(&reason)).await.map_err(|e| format!("complete_job_with_caveats: {e}"))?;
                    self.stats.jobs_completed += 1;
                }
                Ok(JobOutcome::FailedRetryable { error }) => {
                    self.pool.parser_jobs().fail_job(job.id, "retryable", &error, None).await.map_err(|e| format!("fail_job: {e}"))?;
                    self.stats.jobs_failed_retryable += 1;
                }
                Ok(JobOutcome::FailedPermanent { error }) => {
                    self.pool.parser_jobs().fail_job(job.id, "permanent", &error, None).await.map_err(|e| format!("fail_job: {e}"))?;
                    self.stats.jobs_failed_permanent += 1;
                }
                Err(error) => {
                    self.pool.parser_jobs().fail_job(job.id, "permanent", &error, None).await.map_err(|e| format!("fail_job after handler error: {e}"))?;
                    self.stats.jobs_failed_permanent += 1;
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    pub async fn run_continuous(
        &mut self,
        handler: &dyn JobHandler,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            if *shutdown_rx.borrow() {
                break;
            }
            match self.run_once(handler).await {
                Ok(()) | Err(_) => {
                    sleep(self.poll_interval).await;
                }
            }
        }
    }
}
