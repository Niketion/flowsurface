//! Progress tracking for market data operations.
//!
//! `MarketDataProgressSnapshot` provides a snapshot of current fetch progress
//! for UI display. Shows active jobs, cached data, and loading status.

use super::job::FetchJob;
use super::requirement::ConsumerFeature;
use exchange::UnixMs;

/// A snapshot of current market data progress for UI display.
#[derive(Debug, Clone)]
pub struct MarketDataProgressSnapshot {
    /// Active fetch jobs
    pub active_jobs: Vec<FetchJob>,
    /// Total number of records fetched in this session
    pub total_fetched_records: usize,
    /// Total number of records served from cache
    pub total_cached_records: usize,
    /// The furthest timestamp that has been covered
    pub covered_until: Option<UnixMs>,
    /// Human-readable status message
    pub message: String,
    /// Features that are currently loading
    pub loading_features: Vec<ConsumerFeature>,
    /// Features that are ready
    pub ready_features: Vec<ConsumerFeature>,
}

impl MarketDataProgressSnapshot {
    /// Create a new empty snapshot.
    pub fn empty() -> Self {
        Self {
            active_jobs: Vec::new(),
            total_fetched_records: 0,
            total_cached_records: 0,
            covered_until: None,
            message: "Ready".to_string(),
            loading_features: Vec::new(),
            ready_features: Vec::new(),
        }
    }

    /// Check if any data is currently loading.
    pub fn is_loading(&self) -> bool {
        !self.active_jobs.is_empty()
    }

    /// Get the number of active jobs.
    pub fn active_job_count(&self) -> usize {
        self.active_jobs.len()
    }

    /// Get the progress percentage (0.0 to 1.0) if determinable.
    pub fn progress_percentage(&self) -> Option<f32> {
        // Simple heuristic: if we have records, we're making progress
        if self.total_fetched_records > 0 {
            Some(0.5) // Indeterminate but non-zero
        } else {
            None
        }
    }

    /// Format as a UI-friendly string.
    pub fn format_ui(&self) -> String {
        if !self.is_loading() {
            return self.message.clone();
        }

        let jobs_summary = self
            .active_jobs
            .iter()
            .map(|job| {
                format!(
                    "{}: {} ({})",
                    job.key.display_key(),
                    job.range.format_display(),
                    job.progress.log_format()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        if jobs_summary.is_empty() {
            self.message.clone()
        } else {
            format!("Loading market data:\n{}", jobs_summary)
        }
    }

    /// Format as a compact single-line status.
    pub fn format_status_line(&self) -> String {
        if !self.is_loading() {
            return format!(
                "Ready | cached={} covered={}",
                self.total_cached_records,
                self.covered_until.map_or("-".to_string(), |t| {
                    crate::connector::fetcher::format_time_short(t)
                })
            );
        }

        let fetching = self
            .active_jobs
            .iter()
            .map(|j| j.key.display_key())
            .collect::<Vec<_>>()
            .join(",");

        format!(
            "Fetching: {} | fetched={} cached={}",
            fetching, self.total_fetched_records, self.total_cached_records
        )
    }
}

impl Default for MarketDataProgressSnapshot {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::key::{MarketDataKey, MarketKind, Symbol, Venue};
    use exchange::UnixMs;

    #[test]
    fn test_empty_snapshot() {
        let snapshot = MarketDataProgressSnapshot::empty();
        assert!(!snapshot.is_loading());
        assert_eq!(snapshot.active_job_count(), 0);
        assert_eq!(snapshot.message, "Ready");
    }

    #[test]
    fn test_snapshot_with_jobs() {
        let key = MarketDataKey::trades(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
        );
        let range =
            super::super::range::MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();

        let mut job = FetchJob::new(key, range, vec![]);
        job.start();

        let mut snapshot = MarketDataProgressSnapshot::empty();
        snapshot.active_jobs.push(job);

        assert!(snapshot.is_loading());
        assert_eq!(snapshot.active_job_count(), 1);
    }
}
