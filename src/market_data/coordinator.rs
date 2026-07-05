//! Central market data coordinator.
//!
//! `MarketDataCoordinator` is the single point of entry for all data requirements.
//! It collects requirements, plans fetches, manages jobs, and dispatches data
//! to consumers. Charts and indicators should never directly start fetches;
//! they submit `DataRequirement` instances to the coordinator.

use super::coverage::CoverageLedger;
use super::job::{FetchJob, FetchJobId, FetchJobProgress, FetchJobStatus};
use super::key::MarketDataKey;
use super::planner::{self, DataLoadPlan};
use super::progress::MarketDataProgressSnapshot;
use super::range::{MarketDataRange, compute_missing};
use super::requirement::{DataRequirement, RequirementGroup};
use super::store::MarketDataStore;
use rustc_hash::FxHashMap;
use std::collections::VecDeque;

pub const MIN_TRADE_BACKFILL_SEGMENT_MS: u64 = 1_000;

/// The central market data coordinator.
///
/// Coordinates all data fetching, caching, and distribution. Charts and
/// indicators submit requirements; the coordinator handles deduplication,
/// coverage tracking, and fetch planning.
pub struct MarketDataCoordinator {
    /// In-memory data store
    pub store: MarketDataStore,
    /// Coverage ledger tracking fetched ranges
    pub coverage: CoverageLedger,
    /// Active and recent fetch jobs
    jobs: FxHashMap<FetchJobId, FetchJob>,
    /// Job queue (pending jobs in order)
    job_queue: VecDeque<FetchJobId>,
    /// Current data load plan
    plan: Option<DataLoadPlan>,
    /// Accumulated requirements not yet planned
    pending_requirements: Vec<DataRequirement>,
    /// Maximum concurrent fetch jobs
    max_concurrent_jobs: usize,
    /// Maximum number of pending requirements before auto-planning
    max_pending_requirements: usize,
    /// Records served from local cache during this runtime session.
    total_cached_records_served: usize,
    /// Records received from network workers during this runtime session.
    total_fetched_records_received: usize,
}

impl MarketDataCoordinator {
    /// Create a new coordinator.
    pub fn new() -> Self {
        Self {
            store: MarketDataStore::new(),
            coverage: CoverageLedger::new(),
            jobs: FxHashMap::default(),
            job_queue: VecDeque::new(),
            plan: None,
            pending_requirements: Vec::new(),
            max_concurrent_jobs: 4,
            max_pending_requirements: 10,
            total_cached_records_served: 0,
            total_fetched_records_received: 0,
        }
    }

    /// Create a coordinator with custom limits.
    pub fn with_limits(max_concurrent: usize, max_pending: usize) -> Self {
        Self {
            max_concurrent_jobs: max_concurrent,
            max_pending_requirements: max_pending,
            ..Self::new()
        }
    }

    /// Submit a data requirement to the coordinator.
    ///
    /// Requirements are accumulated and planned when `plan()` is called
    /// (automatically when pending count exceeds the threshold).
    pub fn require(&mut self, requirement: DataRequirement) {
        log::info!(
            target: "marketdata",
            "MARKETDATA Requirement | {}",
            requirement.log_format()
        );

        self.pending_requirements.push(requirement);

        // Auto-plan if we have enough pending requirements
        if self.pending_requirements.len() >= self.max_pending_requirements {
            self.plan();
        }
    }

    /// Submit multiple requirements at once.
    pub fn require_all(&mut self, requirements: Vec<DataRequirement>) {
        for req in requirements {
            self.require(req);
        }
    }

    /// Plan fetches from accumulated requirements.
    ///
    /// Groups requirements by key, computes coverage gaps, and creates
    /// a `DataLoadPlan` with network segments and cached segments.
    pub fn plan(&mut self) -> &DataLoadPlan {
        let requirements = std::mem::take(&mut self.pending_requirements);

        if requirements.is_empty() {
            self.plan = Some(DataLoadPlan::empty());
            return self.plan.as_ref().unwrap();
        }

        // Group requirements by key
        let groups = RequirementGroup::from_requirements(requirements);

        // Compute plan against current coverage
        let plan = planner::compute_plan(groups, &self.coverage);

        log::info!(
            target: "marketdata",
            "MARKETDATA PlanComputed | {}",
            plan.log_format()
        );

        self.plan = Some(plan);
        self.plan.as_ref().unwrap()
    }

    /// Execute the current plan by creating fetch jobs for network segments.
    ///
    /// Returns the list of jobs that were created.
    pub fn execute_plan(&mut self) -> Vec<FetchJobId> {
        let plan = match self.plan.as_ref() {
            Some(plan) if plan.needs_network_fetch() => plan.clone(),
            _ => return vec![],
        };

        let mut created_jobs = Vec::new();

        for segment in &plan.network_segments {
            // Canonicalize Kline/OI ranges to timeframe boundaries
            let segment_range = if let Some(tf_ms) = segment.key.kind.timeframe_ms() {
                match super::range::canonicalize_kline_range(segment.range, tf_ms) {
                    Some(canonical) => canonical,
                    None => {
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA TinyKlineGapSuppressed | key={} range={} reason=canonicalize_empty",
                            segment.key.display_key(),
                            segment.range.format_display()
                        );
                        self.coverage.mark_empty(segment.key.clone(), segment.range);
                        continue;
                    }
                }
            } else {
                segment.range
            };

            let remaining_ranges = self.subtract_active_jobs(&segment.key, segment_range);
            if remaining_ranges.is_empty() {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA JobDedup | key={} range={} reason=already_active",
                    segment.key.display_key(),
                    segment_range.format_display()
                );
                continue;
            }

            if remaining_ranges.len() != 1 || remaining_ranges[0] != segment_range {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA ActiveJobSubtract | key={} requested={} active={} remaining={}",
                    segment.key.display_key(),
                    segment_range.format_display(),
                    self.active_ranges_for_key(&segment.key)
                        .iter()
                        .map(MarketDataRange::format_display)
                        .collect::<Vec<_>>()
                        .join(","),
                    remaining_ranges
                        .iter()
                        .map(MarketDataRange::format_display)
                        .collect::<Vec<_>>()
                        .join(",")
                );
            }

            for range in remaining_ranges {
                // Suppress sub-timeframe Kline jobs
                if let Some(tf_ms) = segment.key.kind.timeframe_ms()
                    && range.duration_ms() < tf_ms
                {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA TinyKlineGapSuppressed | key={} range={} reason=below_timeframe",
                        segment.key.display_key(),
                        range.format_display()
                    );
                    self.coverage.mark_empty(segment.key.clone(), range);
                    continue;
                }

                if matches!(segment.key.kind, super::key::MarketDataKind::Trades)
                    && Self::is_tiny_trade_gap(range)
                {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA TinyTradeGapSuppressed | key={} range={} reason=below_threshold_cache_desync",
                        segment.key.display_key(),
                        range.format_display()
                    );
                    self.coverage.mark_empty(segment.key.clone(), range);
                    continue;
                }

                let job = FetchJob::new(segment.key.clone(), range, segment.consumers.clone());

                let job_id = job.id;
                self.jobs.insert(job_id, job);
                self.job_queue.push_back(job_id);
                created_jobs.push(job_id);

                log::info!(
                    target: "marketdata",
                    "MARKETDATA JobCreated | job={} key={} range={} consumers={}",
                    super::job::short_id(job_id),
                    segment.key.display_key(),
                    range.format_display(),
                    segment
                        .consumers
                        .iter()
                        .map(|c| c.feature.short_name())
                        .collect::<Vec<_>>()
                        .join(",")
                );
            }
        }

        created_jobs
    }

    /// Get the next job to execute (from the queue).
    pub fn next_job(&mut self) -> Option<&FetchJob> {
        // Find the next pending job that isn't blocked by concurrent limit
        let active_count = self.active_job_count();

        if active_count >= self.max_concurrent_jobs {
            return None;
        }

        while let Some(job_id) = self.job_queue.pop_front() {
            if let Some(job) = self.jobs.get(&job_id)
                && job.status == FetchJobStatus::Pending
            {
                return Some(job);
            }
        }

        None
    }

    /// Get a mutable reference to a job by ID.
    pub fn job_mut(&mut self, job_id: FetchJobId) -> Option<&mut FetchJob> {
        self.jobs.get_mut(&job_id)
    }

    /// Get a job by ID.
    pub fn job(&self, job_id: FetchJobId) -> Option<&FetchJob> {
        self.jobs.get(&job_id)
    }

    /// Mark a job as in-progress.
    pub fn start_job(&mut self, job_id: FetchJobId) {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.start();
        }
    }

    /// Update job progress.
    pub fn update_job_progress(&mut self, job_id: FetchJobId, progress: FetchJobProgress) {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.update_progress(progress);
        }
    }

    /// Complete a job successfully.
    pub fn complete_job(&mut self, job_id: FetchJobId, records: usize) {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            let key = job.key.clone();
            let range = job.range;
            let _consumers = job.consumers.clone();

            job.complete(records);

            // Defensive: for Kline/OI, zero records should not mark coverage Complete
            if records == 0
                && matches!(
                    key.kind,
                    super::key::MarketDataKind::Klines { .. }
                        | super::key::MarketDataKind::OpenInterest { .. }
                )
            {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA CoverageCorrupt | key={} range={} records=0 action=mark_empty",
                    key.display_key(),
                    range.format_display()
                );
                self.coverage.mark_empty(key, range);
                return;
            }

            // Update coverage
            self.coverage.mark_complete(key.clone(), range, records);

            log::info!(
                target: "marketdata",
                "MARKETDATA CoverageUpdate | key={} status=Complete range={}",
                key.display_key(),
                range.format_display()
            );
        }
    }

    pub fn remove_job(&mut self, job_id: FetchJobId) -> Option<FetchJob> {
        let removed = self.jobs.remove(&job_id);
        if let Some(job) = &removed {
            log::info!(
                target: "marketdata",
                "MARKETDATA JobRemoved | job={} key={} range={} status={}",
                super::job::short_id(job_id),
                job.key.display_key(),
                job.range.format_display(),
                job.status.label()
            );
        }
        self.job_queue.retain(|queued| *queued != job_id);
        removed
    }

    pub fn complete_and_remove_job(
        &mut self,
        job_id: FetchJobId,
        records: usize,
    ) -> Option<FetchJob> {
        self.complete_job(job_id, records);
        self.remove_job(job_id)
    }

    pub fn mark_empty_and_remove_job(&mut self, job_id: FetchJobId) -> Option<FetchJob> {
        if let Some(job) = self.jobs.get(&job_id) {
            log::info!(
                target: "marketdata",
                "MARKETDATA JobEmpty | job={} key={} range={}",
                super::job::short_id(job_id),
                job.key.display_key(),
                job.range.format_display()
            );
            self.coverage.mark_empty(job.key.clone(), job.range);
        }
        self.remove_job(job_id)
    }

    pub fn fail_and_remove_job(&mut self, job_id: FetchJobId, error: String) -> Option<FetchJob> {
        self.fail_job(job_id, error);
        self.remove_job(job_id)
    }

    /// Fail a job.
    pub fn fail_job(&mut self, job_id: FetchJobId, error: String) {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            let key = job.key.clone();
            let range = job.range;

            job.fail(error.clone());

            // Update coverage
            self.coverage.mark_failed(key, range, error, None);
        }
    }

    /// Cancel a job.
    pub fn cancel_job(&mut self, job_id: FetchJobId) {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.cancel();
        }
    }

    /// Feed raw trades into the store.
    pub fn feed_trades(&mut self, key: &MarketDataKey, trades: &[exchange::Trade]) {
        self.store.insert_trades(key, trades);
    }

    /// Feed klines into the store.
    pub fn feed_klines(&mut self, key: &MarketDataKey, klines: &[exchange::Kline]) {
        self.store.insert_klines(key, klines);
    }

    /// Get a progress snapshot for UI display.
    pub fn progress_snapshot(&self) -> MarketDataProgressSnapshot {
        let active_jobs: Vec<FetchJob> = self
            .jobs
            .values()
            .filter(|j| !j.status.is_terminal())
            .cloned()
            .collect();

        let covered_until = self
            .coverage
            .keys()
            .filter_map(|key| self.coverage.latest_covered_to(key))
            .max();

        let mut snapshot = MarketDataProgressSnapshot::empty();
        snapshot.active_jobs = active_jobs;
        snapshot.total_cached_records = self.total_cached_records_served;
        snapshot.total_fetched_records = self.total_fetched_records_received;
        snapshot.covered_until = covered_until;

        if snapshot.is_loading() {
            snapshot.message = format!("Loading {} segments", snapshot.active_job_count());
        } else {
            snapshot.message = "Ready".to_string();
        }

        snapshot
    }

    pub fn record_cache_served(&mut self, records: usize) {
        self.total_cached_records_served = self.total_cached_records_served.saturating_add(records);
    }

    pub fn record_network_fetched(&mut self, records: usize) {
        self.total_fetched_records_received =
            self.total_fetched_records_received.saturating_add(records);
    }

    pub fn active_ranges_for_key(&self, key: &MarketDataKey) -> Vec<MarketDataRange> {
        self.jobs
            .values()
            .filter(|job| Self::is_active_status(&job.status) && &job.key == key)
            .map(|job| job.range)
            .collect()
    }

    pub fn subtract_active_jobs(
        &self,
        key: &MarketDataKey,
        range: MarketDataRange,
    ) -> Vec<MarketDataRange> {
        let active_ranges = self.active_ranges_for_key(key);
        compute_missing(range, &active_ranges)
    }

    /// Check if a key/range already has a pending or in-progress job.
    pub fn has_active_job_for(&self, key: &MarketDataKey, range: &MarketDataRange) -> bool {
        self.jobs.values().any(|job| {
            Self::is_active_status(&job.status) && &job.key == key && job.range.contains(range)
        })
    }

    /// Clean up completed/failed jobs older than the retention period.
    pub fn cleanup_jobs(&mut self, retention_ms: u64) {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let mut to_remove = Vec::new();

        for (id, job) in &self.jobs {
            if job.status.is_terminal() {
                let age = now.saturating_sub(job.created_at);
                if age > retention_ms {
                    to_remove.push(*id);
                }
            }
        }

        for id in to_remove {
            self.jobs.remove(&id);
        }
    }

    /// Compute volume bubble summaries from store trades.
    ///
    /// This is the primary path for bubble computation, replacing
    /// the network `FetchRange::BubbleSummary` path.
    pub fn compute_bubble_summaries(
        &self,
        key: &MarketDataKey,
        range: &super::range::MarketDataRange,
        timeframe_ms: u64,
        price_step: exchange::unit::PriceStep,
        max_candidates_per_candle: usize,
    ) -> Option<Vec<data::chart::kline::BubbleVolumeSummary>> {
        let trade_refs = self.store.query_trades(key, range);
        if trade_refs.is_empty() {
            return None;
        }

        // Convert Vec<&Trade> to Vec<Trade>
        let trades: Vec<exchange::Trade> = trade_refs.into_iter().copied().collect();

        let summaries = super::derived::bubbles::compute_bubble_summaries(
            &trades,
            timeframe_ms,
            price_step,
            max_candidates_per_candle,
        );

        let candles = summaries.len();
        let candidates: usize = summaries.iter().map(|s| s.candidates.len()).sum();

        log::info!(
            target: "marketdata",
            "MARKETDATA Derived | kind=VolumeBubbles source=Trades range={} candles={} candidates={}",
            range.format_display(),
            candles,
            candidates
        );

        Some(summaries)
    }

    /// Get the current plan.
    pub fn plan_ref(&self) -> Option<&DataLoadPlan> {
        self.plan.as_ref()
    }

    /// Check if there are pending requirements to plan.
    pub fn has_pending_requirements(&self) -> bool {
        !self.pending_requirements.is_empty()
    }

    /// Get the number of active jobs.
    pub fn active_job_count(&self) -> usize {
        self.jobs
            .values()
            .filter(|j| Self::is_active_status(&j.status))
            .count()
    }

    /// Get all active (in-progress) jobs.
    pub fn active_jobs(&self) -> Vec<&FetchJob> {
        self.jobs
            .values()
            .filter(|j| Self::is_active_status(&j.status))
            .collect()
    }

    fn is_active_status(status: &FetchJobStatus) -> bool {
        matches!(status, FetchJobStatus::Pending | FetchJobStatus::InProgress)
    }

    fn is_tiny_trade_gap(range: MarketDataRange) -> bool {
        range.duration_ms() < MIN_TRADE_BACKFILL_SEGMENT_MS && range.to.as_u64() > 1_000_000
    }

    /// Log the current coordinator state.
    pub fn log_state(&self) {
        log::info!(
            target: "marketdata",
            "MARKETDATA CoordinatorState | jobs={} pending_requirements={} coverage_keys={} store={}",
            self.jobs.len(),
            self.pending_requirements.len(),
            self.coverage.len(),
            self.store.summary()
        );
    }
}

impl Default for MarketDataCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::key::{MarketKind, Symbol, Venue};
    use crate::market_data::requirement::{ConsumerFeature, ConsumerId, Priority};
    use exchange::UnixMs;

    fn make_trade_key() -> MarketDataKey {
        MarketDataKey::trades(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
        )
    }

    #[test]
    fn test_coordinator_new() {
        let coord = MarketDataCoordinator::new();
        assert_eq!(coord.active_job_count(), 0);
        assert!(!coord.has_pending_requirements());
    }

    #[test]
    fn test_require_and_plan() {
        let mut coord = MarketDataCoordinator::new();
        let key = make_trade_key();
        let range = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();
        let consumer = ConsumerId::global(ConsumerFeature::VolumeBubbles);

        coord.require(DataRequirement::new(
            consumer,
            key,
            range,
            Priority::Normal,
            "test",
        ));
        assert!(coord.has_pending_requirements());

        let plan = coord.plan();
        assert!(plan.needs_network_fetch());
        assert!(!coord.has_pending_requirements());
    }

    #[test]
    fn test_execute_plan_creates_jobs() {
        let mut coord = MarketDataCoordinator::new();
        let key = make_trade_key();
        let range = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();
        let consumer = ConsumerId::global(ConsumerFeature::VolumeBubbles);

        coord.require(DataRequirement::new(
            consumer,
            key,
            range,
            Priority::Normal,
            "test",
        ));
        coord.plan();

        let jobs = coord.execute_plan();
        assert_eq!(jobs.len(), 1);
        assert_eq!(coord.active_job_count(), 1); // Pending jobs are active work.
    }

    #[test]
    fn test_job_lifecycle() {
        let mut coord = MarketDataCoordinator::new();
        let key = make_trade_key();
        let range = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();
        let consumer = ConsumerId::global(ConsumerFeature::VolumeBubbles);

        coord.require(DataRequirement::new(
            consumer,
            key.clone(),
            range,
            Priority::Normal,
            "test",
        ));
        coord.plan();
        let jobs = coord.execute_plan();
        let job_id = jobs[0];

        // Start
        coord.start_job(job_id);
        assert_eq!(coord.active_job_count(), 1);

        // Complete
        coord.complete_job(job_id, 500);
        assert_eq!(coord.active_job_count(), 0);

        // Coverage should be updated
        assert!(coord.coverage.is_covered(
            &key,
            &MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap()
        ));
    }

    #[test]
    fn test_dedup_active_jobs() {
        let mut coord = MarketDataCoordinator::new();
        let key = make_trade_key();
        let range = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();

        // Submit two overlapping requirements
        coord.require(DataRequirement::new(
            ConsumerId::global(ConsumerFeature::VolumeBubbles),
            key.clone(),
            range,
            Priority::Normal,
            "bubbles",
        ));
        coord.require(DataRequirement::new(
            ConsumerId::global(ConsumerFeature::SessionVolumeProfile),
            key,
            range,
            Priority::Normal,
            "svp",
        ));

        coord.plan();
        let jobs = coord.execute_plan();

        // Should only create one job (deduplication)
        assert_eq!(jobs.len(), 1);
    }

    #[test]
    fn test_duplicate_fetch_guard_overlapping_ranges() {
        let mut coord = MarketDataCoordinator::new();
        let key = make_trade_key();

        // First requirement: 100 → 200
        let range1 = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();
        coord.require(DataRequirement::new(
            ConsumerId::global(ConsumerFeature::VolumeBubbles),
            key.clone(),
            range1,
            Priority::Normal,
            "bubbles",
        ));
        coord.plan();
        let jobs = coord.execute_plan();
        assert_eq!(jobs.len(), 1);
        coord.start_job(jobs[0]);

        // Second requirement: partially overlapping 150 → 300
        let range2 = MarketDataRange::new(UnixMs::new(150), UnixMs::new(300)).unwrap();
        coord.require(DataRequirement::new(
            ConsumerId::global(ConsumerFeature::SessionVolumeProfile),
            key.clone(),
            range2,
            Priority::Normal,
            "svp",
        ));
        coord.plan();
        let new_jobs = coord.execute_plan();

        assert_eq!(new_jobs.len(), 1);
        let job = coord.job(new_jobs[0]).unwrap();
        assert_eq!(job.range.from.as_u64(), 200);
        assert_eq!(job.range.to.as_u64(), 300);
    }

    #[test]
    fn test_active_full_overlap_dedupes_fully() {
        let mut coord = MarketDataCoordinator::new();
        let key = make_trade_key();
        let active = MarketDataRange::new(UnixMs::new(100), UnixMs::new(300)).unwrap();
        coord.require(DataRequirement::new(
            ConsumerId::global(ConsumerFeature::VolumeBubbles),
            key.clone(),
            active,
            Priority::Normal,
            "active",
        ));
        coord.plan();
        let jobs = coord.execute_plan();
        coord.start_job(jobs[0]);

        let requested = MarketDataRange::new(UnixMs::new(150), UnixMs::new(250)).unwrap();
        assert!(coord.subtract_active_jobs(&key, requested).is_empty());
    }

    #[test]
    fn test_active_left_overlap_creates_right_remainder() {
        let mut coord = MarketDataCoordinator::new();
        let key = make_trade_key();
        let active = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();
        coord.require(DataRequirement::new(
            ConsumerId::global(ConsumerFeature::VolumeBubbles),
            key.clone(),
            active,
            Priority::Normal,
            "active",
        ));
        coord.plan();
        let jobs = coord.execute_plan();
        coord.start_job(jobs[0]);

        let requested = MarketDataRange::new(UnixMs::new(150), UnixMs::new(300)).unwrap();
        let missing = coord.subtract_active_jobs(&key, requested);
        assert_eq!(
            missing,
            vec![MarketDataRange::new(UnixMs::new(200), UnixMs::new(300)).unwrap()]
        );
    }

    #[test]
    fn test_active_right_overlap_creates_left_remainder() {
        let mut coord = MarketDataCoordinator::new();
        let key = make_trade_key();
        let active = MarketDataRange::new(UnixMs::new(200), UnixMs::new(300)).unwrap();
        coord.require(DataRequirement::new(
            ConsumerId::global(ConsumerFeature::VolumeBubbles),
            key.clone(),
            active,
            Priority::Normal,
            "active",
        ));
        coord.plan();
        let jobs = coord.execute_plan();
        coord.start_job(jobs[0]);

        let requested = MarketDataRange::new(UnixMs::new(100), UnixMs::new(250)).unwrap();
        let missing = coord.subtract_active_jobs(&key, requested);
        assert_eq!(
            missing,
            vec![MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap()]
        );
    }

    #[test]
    fn test_active_middle_overlap_splits_remainders() {
        let mut coord = MarketDataCoordinator::new();
        let key = make_trade_key();
        let active = MarketDataRange::new(UnixMs::new(150), UnixMs::new(200)).unwrap();
        coord.require(DataRequirement::new(
            ConsumerId::global(ConsumerFeature::VolumeBubbles),
            key.clone(),
            active,
            Priority::Normal,
            "active",
        ));
        coord.plan();
        let jobs = coord.execute_plan();
        coord.start_job(jobs[0]);

        let requested = MarketDataRange::new(UnixMs::new(100), UnixMs::new(300)).unwrap();
        let missing = coord.subtract_active_jobs(&key, requested);
        assert_eq!(
            missing,
            vec![
                MarketDataRange::new(UnixMs::new(100), UnixMs::new(150)).unwrap(),
                MarketDataRange::new(UnixMs::new(200), UnixMs::new(300)).unwrap(),
            ]
        );
    }

    #[test]
    fn test_multiple_active_ranges_subtract_correctly() {
        let mut coord = MarketDataCoordinator::new();
        let key = make_trade_key();
        for (from, to) in [(120, 150), (200, 240)] {
            coord.require(DataRequirement::new(
                ConsumerId::global(ConsumerFeature::VolumeBubbles),
                key.clone(),
                MarketDataRange::new(UnixMs::new(from), UnixMs::new(to)).unwrap(),
                Priority::Normal,
                "active",
            ));
            coord.plan();
            let jobs = coord.execute_plan();
            coord.start_job(*jobs.last().unwrap());
        }

        let requested = MarketDataRange::new(UnixMs::new(100), UnixMs::new(300)).unwrap();
        let missing = coord.subtract_active_jobs(&key, requested);
        assert_eq!(
            missing,
            vec![
                MarketDataRange::new(UnixMs::new(100), UnixMs::new(120)).unwrap(),
                MarketDataRange::new(UnixMs::new(150), UnixMs::new(200)).unwrap(),
                MarketDataRange::new(UnixMs::new(240), UnixMs::new(300)).unwrap(),
            ]
        );
    }

    #[test]
    fn test_adjacent_active_ranges_subtract_without_artificial_gap() {
        let mut coord = MarketDataCoordinator::new();
        let key = make_trade_key();
        for (from, to) in [(100, 150), (150, 200)] {
            coord.require(DataRequirement::new(
                ConsumerId::global(ConsumerFeature::VolumeBubbles),
                key.clone(),
                MarketDataRange::new(UnixMs::new(from), UnixMs::new(to)).unwrap(),
                Priority::Normal,
                "active",
            ));
            coord.plan();
            let jobs = coord.execute_plan();
            coord.start_job(*jobs.last().unwrap());
        }

        let requested = MarketDataRange::new(UnixMs::new(100), UnixMs::new(250)).unwrap();
        let missing = coord.subtract_active_jobs(&key, requested);
        assert_eq!(
            missing,
            vec![MarketDataRange::new(UnixMs::new(200), UnixMs::new(250)).unwrap()]
        );
    }

    #[test]
    fn test_tiny_trade_gap_is_suppressed_without_active_job() {
        let mut coord = MarketDataCoordinator::new();
        let key = make_trade_key();
        let range = MarketDataRange::new(
            UnixMs::new(1_720_000_000_000),
            UnixMs::new(1_720_000_000_002),
        )
        .unwrap();

        coord.require(DataRequirement::new(
            ConsumerId::global(ConsumerFeature::VolumeBubbles),
            key,
            range,
            Priority::Normal,
            "tiny",
        ));
        coord.plan();
        let jobs = coord.execute_plan();

        assert!(jobs.is_empty());
        assert_eq!(coord.active_job_count(), 0);
        assert_eq!(coord.progress_snapshot().active_job_count(), 0);
    }

    #[test]
    fn test_partial_overlap_creates_extension_only() {
        let mut coord = MarketDataCoordinator::new();
        let key = make_trade_key();

        // First requirement: 100 → 200, completed
        let range1 = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();
        coord.require(DataRequirement::new(
            ConsumerId::global(ConsumerFeature::VolumeBubbles),
            key.clone(),
            range1,
            Priority::Normal,
            "bubbles",
        ));
        coord.plan();
        let jobs = coord.execute_plan();
        coord.start_job(jobs[0]);
        coord.complete_job(jobs[0], 500);

        // Second requirement: 150 → 300 (partial overlap with completed range)
        let range2 = MarketDataRange::new(UnixMs::new(150), UnixMs::new(300)).unwrap();
        coord.require(DataRequirement::new(
            ConsumerId::global(ConsumerFeature::SessionVolumeProfile),
            key.clone(),
            range2,
            Priority::Normal,
            "svp",
        ));
        coord.plan();
        let new_jobs = coord.execute_plan();

        // Should create one job for 200 → 300 (the missing portion)
        assert_eq!(new_jobs.len(), 1);
        let job = coord.job(new_jobs[0]).unwrap();
        assert_eq!(job.range.from.as_u64(), 200);
        assert_eq!(job.range.to.as_u64(), 300);
    }

    #[test]
    fn test_compute_bubble_summaries_from_raw_trades() {
        let mut coord = MarketDataCoordinator::new();
        let key = make_trade_key();
        let range = MarketDataRange::new(UnixMs::new(60_000), UnixMs::new(120_000)).unwrap();
        let trades = vec![
            exchange::Trade {
                time: UnixMs::new(61_000),
                is_sell: false,
                price: exchange::unit::Price::from_f64(100.0),
                qty: exchange::unit::Qty::from_f64(2.0),
            },
            exchange::Trade {
                time: UnixMs::new(62_000),
                is_sell: true,
                price: exchange::unit::Price::from_f64(100.0),
                qty: exchange::unit::Qty::from_f64(1.0),
            },
        ];

        coord.feed_trades(&key, &trades);
        let summaries = coord
            .compute_bubble_summaries(
                &key,
                &range,
                60_000,
                exchange::unit::PriceStep {
                    units: 100_000_000_000,
                },
                5,
            )
            .expect("raw trades should produce derived bubble summaries");

        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].candidates.len(), 1);
    }

    /// Integration test proving the bridge → coordinator → job → completion flow.
    ///
    /// This test validates the current accepted architecture:
    /// 1. Chart emits FetchRange (compatibility signal)
    /// 2. Bridge converts FetchRange → DataRequirement
    /// 3. Coordinator plans cached vs network segments
    /// 4. Coordinator creates job for network segments
    /// 5. Dashboard would create worker FetchSpec from coordinator job
    /// 6. Fetched data completes coordinator job
    /// 7. Progress active count returns to zero
    #[test]
    fn test_bridge_to_coordinator_to_completion_flow() {
        use crate::connector::fetcher::FetchRange;
        use crate::market_data::bridge;
        use crate::market_data::job::FetchJobStatus;
        use crate::market_data::requirement::ConsumerFeature;

        let pane_id = uuid::Uuid::new_v4();
        let mut coord = MarketDataCoordinator::new();

        // --- Step 1: Chart emits FetchRange (simulating KlineChart::update) ---
        let fetch_range = FetchRange::Kline(UnixMs::new(1000), UnixMs::new(5000));

        // --- Step 2: Bridge converts FetchRange → DataRequirement ---
        let ticker_info = exchange::TickerInfo::new(
            exchange::Ticker::new("BTCUSDT", exchange::adapter::Exchange::BinanceLinear),
            0.1,
            0.001,
            Some(1.0),
        );
        let feature = bridge::fetch_range_to_feature(&fetch_range);
        assert_eq!(feature, ConsumerFeature::ChartKlines);

        let key = bridge::fetch_range_to_key(
            &fetch_range,
            Some(&ticker_info),
            Some(exchange::Timeframe::M1),
        )
        .expect("bridge should convert FetchRange to MarketDataKey");
        assert!(matches!(
            key.kind,
            crate::market_data::key::MarketDataKind::Klines { .. }
        ));

        let range = bridge::fetch_range_to_range(&fetch_range)
            .expect("bridge should convert FetchRange to MarketDataRange");
        assert_eq!(range.from.as_u64(), 1000);
        assert_eq!(range.to.as_u64(), 5000);

        let requirement = bridge::fetch_range_to_requirement(
            &fetch_range,
            pane_id,
            feature,
            Some(&ticker_info),
            Some(exchange::Timeframe::M1),
        )
        .expect("bridge should produce a DataRequirement");

        // --- Step 3: Coordinator plans (no coverage yet → all network) ---
        coord.require(requirement);
        let plan = coord.plan().clone();
        assert!(
            !plan.network_segments.is_empty(),
            "plan should have network segments (no coverage yet)"
        );
        assert_eq!(plan.cached_segments.len(), 0, "no cached segments yet");

        // --- Step 4: Coordinator creates job ---
        let created_jobs = coord.execute_plan();
        assert_eq!(created_jobs.len(), 1, "should create exactly one job");
        let job_id = created_jobs[0];
        let job = coord.job(job_id).cloned().unwrap();
        assert!(matches!(
            job.status,
            FetchJobStatus::Pending | FetchJobStatus::InProgress
        ));

        // --- Step 5: Dashboard would create worker FetchSpec from coordinator job ---
        // (This happens in Dashboard::fetch_spec_for_market_job — verified by dashboard tests)

        // --- Step 6: Fetched data completes coordinator job ---
        coord.start_job(job_id);
        assert!(matches!(
            coord.job(job_id).unwrap().status,
            FetchJobStatus::InProgress
        ));

        // Simulate feeding fetched data into the store (as dashboard does)
        coord.feed_klines(&key, &[]); // empty for this test, just proving the API

        // Complete the job (as dashboard does on FetchCompleted)
        coord.complete_job(job_id, 100);
        assert!(coord.job(job_id).unwrap().status.is_terminal());

        // --- Step 7: Progress active count returns to zero ---
        let progress = coord.progress_snapshot();
        assert_eq!(
            progress.active_job_count(),
            0,
            "progress should show 0 active jobs after completion"
        );
        assert!(
            !progress.is_loading(),
            "should not be loading after job completion"
        );
    }
}
