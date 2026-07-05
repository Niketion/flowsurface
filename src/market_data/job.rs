//! Fetch job lifecycle management.
//!
//! `FetchJob` tracks the state of a single market data fetch operation,
//! from pending through in-progress to completion or failure.

use super::key::MarketDataKey;
use super::range::MarketDataRange;
use super::requirement::ConsumerId;
use exchange::UnixMs;
use uuid::Uuid;

/// Unique identifier for a fetch job.
pub type FetchJobId = Uuid;

/// The status of a fetch job.
#[derive(Debug, Clone, PartialEq)]
pub enum FetchJobStatus {
    /// Job is queued but not yet started
    Pending,
    /// Job is actively fetching data
    InProgress,
    /// Job completed successfully with record count
    Completed { records: usize },
    /// Job failed with error and optional retry time
    Failed {
        error: String,
        retry_at: Option<u64>,
    },
    /// Job was cancelled (e.g., superseded by newer request)
    Cancelled,
}

impl FetchJobStatus {
    /// Short label for logging.
    pub fn label(&self) -> &'static str {
        match self {
            FetchJobStatus::Pending => "Pending",
            FetchJobStatus::InProgress => "InProgress",
            FetchJobStatus::Completed { .. } => "Completed",
            FetchJobStatus::Failed { .. } => "Failed",
            FetchJobStatus::Cancelled => "Cancelled",
        }
    }

    /// Check if this status is terminal (no more transitions expected).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            FetchJobStatus::Completed { .. }
                | FetchJobStatus::Failed { .. }
                | FetchJobStatus::Cancelled
        )
    }
}

/// Progress information for a fetch job.
#[derive(Debug, Clone, Default)]
pub struct FetchJobProgress {
    /// Number of records fetched so far
    pub records_fetched: usize,
    /// Latest timestamp received (for streaming/paginated fetches)
    pub until_time: Option<UnixMs>,
    /// Bytes downloaded (if available)
    pub bytes_downloaded: usize,
}

impl FetchJobProgress {
    /// Log-format representation.
    pub fn log_format(&self) -> String {
        format!(
            "records={} until={} bytes={}",
            self.records_fetched,
            self.until_time.map_or("-".to_string(), |t| {
                crate::connector::fetcher::format_time_short(t)
            }),
            self.bytes_downloaded
        )
    }
}

/// A single market data fetch job.
///
/// Tracks the key, range, consumers, status, and progress of a fetch.
#[derive(Debug, Clone)]
pub struct FetchJob {
    /// Unique job identifier
    pub id: FetchJobId,
    /// The market data key
    pub key: MarketDataKey,
    /// The time range to fetch
    pub range: MarketDataRange,
    /// Consumers that need this data
    pub consumers: Vec<ConsumerId>,
    /// Current status
    pub status: FetchJobStatus,
    /// Current progress
    pub progress: FetchJobProgress,
    /// When the job was created
    pub created_at: u64,
    /// Number of retry attempts
    pub attempts: u32,
}

impl FetchJob {
    /// Create a new pending fetch job.
    pub fn new(key: MarketDataKey, range: MarketDataRange, consumers: Vec<ConsumerId>) -> Self {
        Self {
            id: FetchJobId::new_v4(),
            key,
            range,
            consumers,
            status: FetchJobStatus::Pending,
            progress: FetchJobProgress::default(),
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            attempts: 0,
        }
    }

    /// Mark the job as in-progress.
    pub fn start(&mut self) {
        self.status = FetchJobStatus::InProgress;
        log::info!(
            target: "marketdata",
            "MARKETDATA FetchStart | job={} key={} range={} consumers={}",
            short_id(self.id),
            self.key.display_key(),
            self.range.format_display(),
            self.consumer_names()
        );
    }

    /// Update progress.
    pub fn update_progress(&mut self, progress: FetchJobProgress) {
        log::debug!(
            target: "marketdata",
            "MARKETDATA FetchProgress | job={} {}",
            short_id(self.id),
            progress.log_format()
        );
        self.progress = progress;
    }

    /// Mark the job as completed.
    pub fn complete(&mut self, records: usize) {
        self.status = FetchJobStatus::Completed { records };
        log::info!(
            target: "marketdata",
            "MARKETDATA FetchComplete | job={} key={} range={} records={}",
            short_id(self.id),
            self.key.display_key(),
            self.range.format_display(),
            records
        );
    }

    /// Mark the job as failed.
    pub fn fail(&mut self, error: String) {
        let retry_at = self.compute_retry_time();
        log::warn!(
            target: "marketdata",
            "MARKETDATA FetchFailed | job={} key={} range={} error={} retry_after={}ms",
            short_id(self.id),
            self.key.display_key(),
            self.range.format_display(),
            error,
            retry_at.unwrap_or(0)
        );
        self.status = FetchJobStatus::Failed { error, retry_at };
        self.attempts += 1;
    }

    /// Cancel the job.
    pub fn cancel(&mut self) {
        log::info!(
            target: "marketdata",
            "MARKETDATA FetchCancelled | job={} key={} range={}",
            short_id(self.id),
            self.key.display_key(),
            self.range.format_display()
        );
        self.status = FetchJobStatus::Cancelled;
    }

    /// Check if this job can be retried.
    pub fn can_retry(&self) -> bool {
        if let FetchJobStatus::Failed { retry_at, .. } = &self.status {
            retry_at.is_some_and(|at| chrono::Utc::now().timestamp_millis() as u64 >= at)
        } else {
            false
        }
    }

    /// Compute retry time with exponential backoff.
    fn compute_retry_time(&self) -> Option<u64> {
        let base_delay_ms = 2_500u64;
        let exp = self.attempts.saturating_sub(1).min(5);
        let delay = base_delay_ms.saturating_mul(1u64 << exp);
        Some(chrono::Utc::now().timestamp_millis() as u64 + delay)
    }

    /// Get consumer names for logging.
    pub fn consumer_names(&self) -> String {
        self.consumers
            .iter()
            .map(|c| c.feature.short_name().to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Log-format representation.
    pub fn log_format(&self) -> String {
        format!(
            "job={} key={} range={} status={} consumers={} progress={}",
            short_id(self.id),
            self.key.display_key(),
            self.range.format_display(),
            self.status.label(),
            self.consumer_names(),
            self.progress.log_format()
        )
    }
}

/// Shorten a UUID to first 8 characters for logging.
pub fn short_id(id: Uuid) -> String {
    let s = id.to_string();
    s[..8.min(s.len())].to_string()
}

/// Deduplication check for fetch jobs.
///
/// Two jobs are considered duplicates if they have the same key
/// and overlapping ranges.
#[allow(dead_code)] // SVP readiness — job merging may be needed for concurrent consumers
pub fn jobs_overlap(a: &FetchJob, b: &FetchJob) -> bool {
    a.key == b.key && a.range.overlaps(&b.range)
}

/// Compute the merged range of two overlapping jobs.
#[allow(dead_code)] // SVP readiness — job merging may be needed for concurrent consumers
pub fn merge_job_ranges(a: &FetchJob, b: &FetchJob) -> Option<super::range::MarketDataRange> {
    a.range.merge(&b.range)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::key::{MarketKind, Symbol, Venue};

    fn make_trade_key() -> MarketDataKey {
        MarketDataKey::trades(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
        )
    }

    #[test]
    fn test_job_lifecycle() {
        let key = make_trade_key();
        let range = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();
        let consumer =
            ConsumerId::global(super::super::requirement::ConsumerFeature::VolumeBubbles);

        let mut job = FetchJob::new(key, range, vec![consumer]);
        assert_eq!(job.status, FetchJobStatus::Pending);

        job.start();
        assert_eq!(job.status, FetchJobStatus::InProgress);

        job.complete(500);
        assert!(matches!(
            job.status,
            FetchJobStatus::Completed { records: 500 }
        ));
    }

    #[test]
    fn test_job_failure_and_retry() {
        let key = make_trade_key();
        let range = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();

        let mut job = FetchJob::new(key, range, vec![]);
        job.start();
        job.fail("timeout".to_string());

        assert!(!job.can_retry()); // too early
        assert_eq!(job.attempts, 1);
    }

    #[test]
    fn test_jobs_overlap() {
        let key = make_trade_key();
        let range1 = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();
        let range2 = MarketDataRange::new(UnixMs::new(150), UnixMs::new(250)).unwrap();
        let range3 = MarketDataRange::new(UnixMs::new(300), UnixMs::new(400)).unwrap();

        let job1 = FetchJob::new(key.clone(), range1, vec![]);
        let job2 = FetchJob::new(key.clone(), range2, vec![]);
        let job3 = FetchJob::new(key, range3, vec![]);

        assert!(jobs_overlap(&job1, &job2));
        assert!(!jobs_overlap(&job1, &job3));
    }
}
