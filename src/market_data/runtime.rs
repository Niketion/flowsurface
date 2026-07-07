//! Runtime owner for market-data orchestration state.
//!
//! This is the dashboard-facing runtime layer that owns coordinator/cache
//! state plus pending consumer and worker/job mappings. Phase 1 keeps the
//! existing dashboard integration methods in place while moving ownership
//! of the market-data runtime state out of `Dashboard`.

use std::{collections::HashMap, time::Instant};

use exchange::unit::PriceStep;
use exchange::{Kline, OpenInterest, TickerInfo, Trade, UnixMs, adapter::StreamKind};
use uuid::Uuid;

use super::{
    bridge,
    cache::LocalMarketCache,
    chart_need::ChartDataNeed,
    coordinator::{MIN_TRADE_BACKFILL_SEGMENT_MS, MarketDataCoordinator},
    job::{FetchJob, FetchJobId},
    key::{MarketDataKey, MarketDataKind},
    live::LiveDataAdapter,
    range::{MarketDataRange, compute_missing},
    requirement::ConsumerFeature,
};
use crate::connector::fetcher;

const COVERAGE_SAVE_INTERVAL_SECS: u64 = 30;

/// Logical segment status for accurate logging/debug reporting.
#[derive(Debug, Clone, PartialEq)]
pub struct ConsumerSegmentStatus {
    pub completed_logical: usize,
    pub total_logical: usize,
    pub missing: Vec<MarketDataRange>,
    pub coverage_complete: bool,
}

#[derive(Debug, Clone)]
pub struct PendingMarketDataConsumer {
    pub pane_id: Uuid,
    pub req_id: Uuid,
    pub fetch: fetcher::FetchRange,
    pub stream: Option<StreamKind>,
    pub key: MarketDataKey,
    pub range: MarketDataRange,
    pub feature: ConsumerFeature,
    pub bubble_config: Option<BubbleConsumerConfig>,
    pub chart_generation: u64,
    pub has_partial_updates: bool,
    pub completed: bool,
    pub required_segments: Vec<MarketDataRange>,
    pub completed_segments: Vec<MarketDataRange>,
    pub failed_segments: Vec<MarketDataRange>,
    pub delivered_segments: Vec<MarketDataRange>,
}

#[derive(Debug, Clone)]
pub struct DashboardFetchRoute {
    pub pane_id: Uuid,
    pub ready_streams: Vec<StreamKind>,
    pub chart_generation: u64,
    pub reqs: Vec<fetcher::FetchSpec>,
}

#[derive(Debug, Clone)]
pub struct DashboardChartNeedRoute {
    pub pane_id: Uuid,
    pub ready_streams: Vec<StreamKind>,
    pub chart_generation: u64,
    pub needs: Vec<ChartDataNeed>,
}

pub struct MarketDataRouteOutcome {
    pub pane_id: Uuid,
    pub ready_streams: Vec<StreamKind>,
    pub chart_generation: u64,
    pub fetch_specs: Vec<fetcher::FetchSpec>,
    pub cached_dispatches: Vec<CachedMarketDataDispatch>,
    pub reason: &'static str,
}

pub enum CachedMarketDataDispatch {
    Klines {
        key: MarketDataKey,
        range: MarketDataRange,
        timeframe: exchange::Timeframe,
        rows: Vec<Kline>,
    },
    Trades {
        key: MarketDataKey,
        range: MarketDataRange,
        rows: Vec<Trade>,
    },
    OpenInterest {
        key: MarketDataKey,
        range: MarketDataRange,
        timeframe: exchange::Timeframe,
        rows: Vec<OpenInterest>,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct BubbleConsumerConfig {
    pub timeframe_ms: u64,
    pub price_step: PriceStep,
    pub max_candidates_per_candle: usize,
}

impl BubbleConsumerConfig {
    fn from_fetch(fetch: fetcher::FetchRange) -> Option<Self> {
        match fetch {
            fetcher::FetchRange::BubbleSummary {
                timeframe_ms,
                price_step,
                max_candidates_per_candle,
                ..
            } => Some(Self {
                timeframe_ms,
                price_step,
                max_candidates_per_candle,
            }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum MarketDataChartEffect {
    InsertKlinesPartial {
        consumer: PendingMarketDataConsumer,
        stream: Option<StreamKind>,
        timeframe: exchange::Timeframe,
        ticker_info: TickerInfo,
        rows: Vec<Kline>,
    },
    InsertTrades {
        consumer: PendingMarketDataConsumer,
        stream: Option<StreamKind>,
        batch: Vec<Trade>,
        until_time: UnixMs,
    },
    InsertOpenInterestPartial {
        consumer: PendingMarketDataConsumer,
        rows: Vec<OpenInterest>,
    },
    InsertBubbleSummaries {
        consumer: PendingMarketDataConsumer,
        summaries: Vec<data::chart::kline::BubbleVolumeSummary>,
        range: MarketDataRange,
        trades_seen: usize,
        raw_discarded: usize,
        complete: bool,
    },
    CompleteConsumer {
        consumer: PendingMarketDataConsumer,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
    },
    MarkConsumerCompleted {
        consumer: PendingMarketDataConsumer,
    },
    CompleteLegacyTradeFetch {
        pane_id: Uuid,
        req_id: Option<Uuid>,
        fetch: fetcher::FetchRange,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
    },
    MarkPaneReady {
        pane_id: Uuid,
    },
    InsertLegacyTrades {
        pane_id: Uuid,
        req_id: Option<Uuid>,
        batch: Vec<Trade>,
        is_batches_done: bool,
    },
    InsertLegacyBubbleSummaries {
        pane_id: Uuid,
        req_id: Option<Uuid>,
        summaries: Vec<data::chart::kline::BubbleVolumeSummary>,
        range: (UnixMs, UnixMs),
        trades_seen: usize,
        raw_discarded: usize,
    },
    InsertLegacyKlines {
        pane_id: Uuid,
        req_id: Option<Uuid>,
        stream: StreamKind,
        rows: Vec<Kline>,
    },
    InsertLegacyOpenInterest {
        pane_id: Uuid,
        req_id: Option<Uuid>,
        rows: Vec<OpenInterest>,
    },
    ProgressSnapshot,
}

pub struct FetchedMarketDataStore {
    pub key: MarketDataKey,
    pub range: MarketDataRange,
    pub record_count: usize,
}

#[derive(Debug, Default)]
pub struct FetchedDataRuntimeOutcome {
    /// True when the fetched batch belonged to coordinator/runtime-owned market data.
    pub handled: bool,
    /// Chart mutations/completion events that Dashboard should apply.
    pub effects: Vec<MarketDataChartEffect>,
}

pub enum WorkerCompletionKind {
    Completed,
    Empty,
    MissingJob,
}

pub struct WorkerJobCompletion {
    pub job_id: FetchJobId,
    pub job: FetchJob,
    pub consumer_ids: Vec<Uuid>,
    pub records: usize,
}

pub struct WorkerJobFinish {
    pub kind: WorkerCompletionKind,
    pub completion: Option<WorkerJobCompletion>,
}

pub struct StaleWorkerJob {
    pub job_id: FetchJobId,
    pub worker_req: Option<Uuid>,
    pub range: MarketDataRange,
    pub age_ms: u64,
    pub consumer_ids: Vec<Uuid>,
}

pub struct WorkerJobFailure {
    pub job_id: FetchJobId,
    pub range: MarketDataRange,
}

/// Owns all market-data runtime state used by dashboard integration.
pub struct MarketDataRuntime {
    pub coordinator: MarketDataCoordinator,
    pub cache: LocalMarketCache,
    pub live_adapter: LiveDataAdapter,
    last_coverage_save: Instant,
    pending_consumers: Vec<PendingMarketDataConsumer>,
    worker_req_to_job: HashMap<Uuid, FetchJobId>,
    job_to_worker_req: HashMap<FetchJobId, Uuid>,
    job_to_consumers: HashMap<FetchJobId, Vec<Uuid>>,
}

impl MarketDataRuntime {
    pub fn new() -> Self {
        let mut cache = LocalMarketCache::default_cache();
        let mut coordinator = MarketDataCoordinator::new();

        match cache.load_coverage() {
            Ok(coverage) => {
                coordinator.coverage = coverage;
                log::info!(
                    target: "marketdata",
                    "MARKETDATA CoverageLoaded | keys={}",
                    coordinator.coverage.len()
                );
            }
            Err(e) => {
                log::warn!(
                    target: "marketdata",
                    "MARKETDATA CoverageLoadFailed | error={}",
                    e
                );
            }
        }

        Self::from_parts(coordinator, cache)
    }

    pub fn from_config() -> Self {
        let mut cache = LocalMarketCache::default_cache();
        let mut coordinator = MarketDataCoordinator::new();

        match cache.load_coverage() {
            Ok(coverage) => {
                coordinator.coverage = coverage;
                log::info!(
                    target: "marketdata",
                    "MARKETDATA CoverageLoaded | keys={} source=from_config",
                    coordinator.coverage.len()
                );
            }
            Err(e) => {
                log::warn!(
                    target: "marketdata",
                    "MARKETDATA CoverageLoadFailed | error={} source=from_config",
                    e
                );
            }
        }

        Self::from_parts(coordinator, cache)
    }

    pub fn from_parts(coordinator: MarketDataCoordinator, cache: LocalMarketCache) -> Self {
        Self {
            coordinator,
            cache,
            live_adapter: LiveDataAdapter::new(),
            last_coverage_save: Instant::now(),
            pending_consumers: Vec::new(),
            worker_req_to_job: HashMap::new(),
            job_to_worker_req: HashMap::new(),
            job_to_consumers: HashMap::new(),
        }
    }

    pub fn should_save_coverage(&mut self, now: Instant) -> bool {
        if now.duration_since(self.last_coverage_save)
            > std::time::Duration::from_secs(COVERAGE_SAVE_INTERVAL_SECS)
        {
            self.last_coverage_save = now;
            true
        } else {
            false
        }
    }

    pub fn save_coverage(&mut self) {
        if let Err(e) = self.cache.save_coverage(&self.coordinator.coverage) {
            log::warn!(
                target: "marketdata",
                "MARKETDATA CoverageSaveFailed | error={}",
                e
            );
        }
    }

    pub fn progress_snapshot(&self) -> super::progress::MarketDataProgressSnapshot {
        self.coordinator.progress_snapshot()
    }

    pub fn coordinator_job_for_worker(&self, worker_req: Uuid) -> Option<(FetchJobId, FetchJob)> {
        self.worker_req_to_job
            .get(&worker_req)
            .copied()
            .and_then(|job_id| {
                self.coordinator
                    .job(job_id)
                    .cloned()
                    .map(|job| (job_id, job))
            })
    }

    pub fn pending_consumer_by_req(&self, req_id: Uuid) -> Option<PendingMarketDataConsumer> {
        self.pending_consumers
            .iter()
            .find(|consumer| consumer.req_id == req_id)
            .cloned()
    }

    pub fn pending_consumer_count(&self) -> usize {
        self.pending_consumers.len()
    }

    pub fn pending_consumers_empty(&self) -> bool {
        self.pending_consumers.is_empty()
    }

    pub fn worker_mapping_count(&self) -> usize {
        self.worker_req_to_job.len()
    }

    pub fn worker_maps_empty(&self) -> bool {
        self.worker_req_to_job.is_empty()
            && self.job_to_worker_req.is_empty()
            && self.job_to_consumers.is_empty()
    }

    pub fn job_consumer_ids(&self, job_id: FetchJobId) -> Vec<Uuid> {
        self.job_to_consumers
            .get(&job_id)
            .cloned()
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub fn push_pending_consumer_for_test(&mut self, consumer: PendingMarketDataConsumer) {
        self.pending_consumers.push(consumer);
    }

    #[cfg(test)]
    pub fn set_worker_job_mapping_for_test(&mut self, worker_req: Uuid, job_id: FetchJobId) {
        self.worker_req_to_job.insert(worker_req, job_id);
        self.job_to_worker_req.insert(job_id, worker_req);
    }

    #[cfg(test)]
    pub fn set_job_consumers_for_test(&mut self, job_id: FetchJobId, consumers: Vec<Uuid>) {
        self.job_to_consumers.insert(job_id, consumers);
    }

    pub fn matching_pending_consumers(
        &self,
        key: &MarketDataKey,
        range: &MarketDataRange,
    ) -> Vec<PendingMarketDataConsumer> {
        self.pending_consumers
            .iter()
            .filter(|consumer| consumer.key == *key && consumer.range.overlaps(range))
            .cloned()
            .collect()
    }

    pub fn consumers_for_job(&self, job_id: FetchJobId) -> Vec<PendingMarketDataConsumer> {
        self.job_to_consumers
            .get(&job_id)
            .into_iter()
            .flatten()
            .filter_map(|req_id| self.pending_consumer_by_req(*req_id))
            .collect()
    }

    pub fn normalize_fetched_data_for_job(
        &self,
        data: &fetcher::FetchedData,
        coordinator_job: Option<&FetchJob>,
    ) -> fetcher::FetchedData {
        let Some(job) = coordinator_job else {
            return data.clone();
        };
        match data {
            fetcher::FetchedData::Trades {
                batch,
                until_time: _,
                req_id,
            } => {
                let before = batch.len();
                let first_before = batch.first().map(|trade| trade.time);
                let last_before = batch.last().map(|trade| trade.time);
                let filtered = batch
                    .iter()
                    .filter(|trade| job.range.contains_timestamp(trade.time))
                    .copied()
                    .collect::<Vec<_>>();
                if filtered.len() != before {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA TradeOutOfRangeFiltered | worker_req={} requested={} before={} after={} first_before={} last_before={}",
                        req_id.map_or("-".to_string(), fetcher::short_id),
                        job.range.format_display(),
                        before,
                        filtered.len(),
                        fetcher::format_optional_time(first_before),
                        fetcher::format_optional_time(last_before)
                    );
                }
                fetcher::FetchedData::Trades {
                    batch: filtered,
                    until_time: job.range.to,
                    req_id: *req_id,
                }
            }
            fetcher::FetchedData::Klines { data, req_id } => {
                let before = data.len();
                let first_before = data.first().map(|k| k.time);
                let last_before = data.last().map(|k| k.time);
                let filtered = data
                    .iter()
                    .filter(|kline| job.range.contains_timestamp(kline.time))
                    .copied()
                    .collect::<Vec<_>>();
                if filtered.len() != before {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA KlineOutOfRangeFiltered | worker_req={} requested={} before={} after={} first_before={} last_before={}",
                        req_id.map_or("-".to_string(), fetcher::short_id),
                        job.range.format_display(),
                        before,
                        filtered.len(),
                        fetcher::format_optional_time(first_before),
                        fetcher::format_optional_time(last_before)
                    );
                }
                fetcher::FetchedData::Klines {
                    data: filtered,
                    req_id: *req_id,
                }
            }
            _ => data.clone(),
        }
    }

    pub fn store_fetched_market_data(
        &mut self,
        stream_type: StreamKind,
        data: &fetcher::FetchedData,
        coordinator_job: Option<&FetchJob>,
    ) -> Option<FetchedMarketDataStore> {
        let key = coordinator_job
            .map(|job| job.key.clone())
            .or_else(|| key_for_fetched_data(stream_type, data))?;
        match data {
            fetcher::FetchedData::Trades { batch, .. } => {
                let range = coordinator_job
                    .map(|job| job.range)
                    .or_else(|| range_from_trades(batch))?;
                self.coordinator.feed_trades(&key, batch);
                self.cache.insert_trades(&key, batch);
                Some(FetchedMarketDataStore {
                    key,
                    range,
                    record_count: batch.len(),
                })
            }
            fetcher::FetchedData::Klines { data, .. } => {
                if data.is_empty() {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA KlineStoreSkipped | key={} reason=zero_records_after_filter",
                        key.display_key()
                    );
                    return None;
                }
                let range = coordinator_job
                    .map(|job| job.range)
                    .or_else(|| range_from_klines(data))?;
                self.coordinator.feed_klines(&key, data);
                self.cache.insert_klines(&key, data);
                Some(FetchedMarketDataStore {
                    key,
                    range,
                    record_count: data.len(),
                })
            }
            fetcher::FetchedData::OI { data, .. } => {
                if data.is_empty() {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA OIStoreSkipped | key={} reason=zero_records_after_filter",
                        key.display_key()
                    );
                    return None;
                }
                let range = coordinator_job
                    .map(|job| job.range)
                    .or_else(|| range_from_oi(data))?;
                self.coordinator.feed_open_interest(&key, data);
                self.cache.insert_open_interest(&key, data);
                Some(FetchedMarketDataStore {
                    key,
                    range,
                    record_count: data.len(),
                })
            }
            fetcher::FetchedData::BubbleSummary { .. } => None,
        }
    }

    pub fn record_network_delivery(
        &mut self,
        coordinator_job_id: Option<FetchJobId>,
        key: &MarketDataKey,
        range: &MarketDataRange,
        record_count: usize,
    ) {
        let job_ids = coordinator_job_id
            .map(|job_id| vec![job_id])
            .unwrap_or_else(|| {
                self.coordinator
                    .active_jobs()
                    .iter()
                    .filter(|job| job.key == *key && job.range.contains(range))
                    .map(|job| job.id)
                    .collect::<Vec<_>>()
            });

        for job_id in job_ids {
            if let Some(job) = self.coordinator.job_mut(job_id) {
                job.progress.records_fetched =
                    job.progress.records_fetched.saturating_add(record_count);
            }
        }
        if coordinator_job_id.is_some() {
            self.coordinator.record_network_fetched(record_count);
        }
    }

    pub fn mark_consumer_segment_complete(
        &mut self,
        req_id: Uuid,
        segment: MarketDataRange,
    ) -> Option<ConsumerSegmentStatus> {
        let consumer = self
            .pending_consumers
            .iter_mut()
            .find(|consumer| consumer.req_id == req_id)?;
        super::range::add_segment_merged(&mut consumer.completed_segments, segment);
        let raw_missing = compute_missing(consumer.range, &consumer.completed_segments);
        let missing = if matches!(
            consumer.feature,
            ConsumerFeature::TradeHydration | ConsumerFeature::Footprint
        ) {
            super::range::filter_tiny_trade_gaps(raw_missing.clone(), MIN_TRADE_BACKFILL_SEGMENT_MS)
        } else {
            raw_missing.clone()
        };
        let total_logical = consumer.required_segments.len();
        let completed_logical = consumer
            .required_segments
            .iter()
            .filter(|required| compute_missing(**required, &consumer.completed_segments).is_empty())
            .count();
        Some(ConsumerSegmentStatus {
            completed_logical,
            total_logical,
            coverage_complete: missing.is_empty(),
            missing,
        })
    }

    pub fn mark_consumer_segment_delivered(
        &mut self,
        req_id: Uuid,
        segment: MarketDataRange,
    ) -> bool {
        let Some(consumer) = self
            .pending_consumers
            .iter_mut()
            .find(|consumer| consumer.req_id == req_id)
        else {
            return false;
        };
        let missing = super::range::compute_missing(segment, &consumer.delivered_segments);
        if missing.is_empty() {
            log::info!(
                target: "marketdata",
                "MARKETDATA ConsumerSegmentAlreadyDelivered | req={} segment={} action=skip",
                fetcher::short_id(req_id),
                segment.format_display()
            );
            return false;
        }
        super::range::add_segment_merged(&mut consumer.delivered_segments, segment);
        true
    }

    pub fn mark_consumer_completed(&mut self, req_id: Uuid) {
        if let Some(consumer) = self
            .pending_consumers
            .iter_mut()
            .find(|consumer| consumer.req_id == req_id)
        {
            consumer.completed = true;
        }
    }

    pub fn mark_bubble_consumer_partial(&mut self, req_id: Uuid) {
        if let Some(consumer) = self
            .pending_consumers
            .iter_mut()
            .find(|consumer| consumer.req_id == req_id)
        {
            consumer.has_partial_updates = true;
        }
    }

    pub fn mark_bubble_consumer_completed(&mut self, req_id: Uuid) {
        if let Some(consumer) = self
            .pending_consumers
            .iter_mut()
            .find(|consumer| consumer.req_id == req_id)
        {
            if consumer.completed {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA BubbleDuplicateCompleteSuppressed | req={}",
                    fetcher::short_id(req_id)
                );
            } else {
                consumer.completed = true;
            }
        }
    }

    pub fn effective_missing_for_consumer(&self, req_id: Uuid) -> Vec<MarketDataRange> {
        self.pending_consumers
            .iter()
            .find(|consumer| consumer.req_id == req_id)
            .map(|consumer| {
                let raw_missing = compute_missing(consumer.range, &consumer.completed_segments);
                if matches!(
                    consumer.feature,
                    ConsumerFeature::TradeHydration | ConsumerFeature::Footprint
                ) {
                    super::range::filter_tiny_trade_gaps(raw_missing, MIN_TRADE_BACKFILL_SEGMENT_MS)
                } else {
                    raw_missing
                }
            })
            .unwrap_or_default()
    }

    pub fn consumer_remaining_segments(&self, req_id: Uuid) -> Vec<String> {
        self.pending_consumers
            .iter()
            .find(|consumer| consumer.req_id == req_id)
            .map(|consumer| {
                let terminal_segments = consumer
                    .completed_segments
                    .iter()
                    .chain(consumer.failed_segments.iter())
                    .copied()
                    .collect::<Vec<_>>();
                let raw_missing = compute_missing(consumer.range, &terminal_segments);
                let missing = if matches!(
                    consumer.feature,
                    ConsumerFeature::TradeHydration | ConsumerFeature::Footprint
                ) {
                    super::range::filter_tiny_trade_gaps(raw_missing, MIN_TRADE_BACKFILL_SEGMENT_MS)
                } else {
                    raw_missing
                };
                missing.into_iter().map(|r| r.format_display()).collect()
            })
            .unwrap_or_default()
    }

    pub fn consumer_is_fully_satisfied(&self, req_id: Uuid) -> bool {
        self.pending_consumers
            .iter()
            .find(|consumer| consumer.req_id == req_id)
            .is_some_and(|consumer| {
                consumer.required_segments.is_empty()
                    || self.effective_missing_for_consumer(req_id).is_empty()
            })
    }

    pub fn effects_for_fetched_data(
        &mut self,
        stream_type: StreamKind,
        data: &fetcher::FetchedData,
    ) -> FetchedDataRuntimeOutcome {
        let coordinator_job =
            fetched_data_req_id(data).and_then(|req_id| self.coordinator_job_for_worker(req_id));

        // Track raw record count before normalization to distinguish a true
        // empty exchange response from an invalid/out-of-range response.
        let raw_filtered_kind_count = match data {
            fetcher::FetchedData::Klines { data, .. } => data.len(),
            fetcher::FetchedData::OI { data, .. } => data.len(),
            _ => 0,
        };

        let data =
            self.normalize_fetched_data_for_job(data, coordinator_job.as_ref().map(|(_, job)| job));

        let Some(stored) = self.store_fetched_market_data(
            stream_type,
            &data,
            coordinator_job.as_ref().map(|(_, job)| job),
        ) else {
            let mut effects = Vec::new();
            if let Some((job_id, job)) = &coordinator_job
                && matches!(
                    data,
                    fetcher::FetchedData::Klines { .. } | fetcher::FetchedData::OI { .. }
                )
                && let Some(worker_req) = fetched_data_req_id(&data)
            {
                if raw_filtered_kind_count > 0 {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA KlineOutOfRangeFiltered | before={} after=0",
                        raw_filtered_kind_count
                    );
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA KlineInvalidResponse | worker_req={} requested={} reason=all_records_out_of_range",
                        fetcher::short_id(worker_req),
                        job.range.format_display()
                    );
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA JobFailed | job={} reason=invalid_out_of_range_response",
                        super::job::short_id(*job_id)
                    );
                    if self.finish_worker_job_invalid(worker_req) {
                        effects.push(MarketDataChartEffect::ProgressSnapshot);
                    }
                } else {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA JobEmpty | job={} reason=zero_records_after_filter",
                        super::job::short_id(*job_id)
                    );
                    if let Some(done_effects) = self.finish_worker_job_effects(worker_req, None) {
                        effects.extend(done_effects);
                    }
                }
                return FetchedDataRuntimeOutcome {
                    handled: true,
                    effects,
                };
            }
            return FetchedDataRuntimeOutcome {
                handled: false,
                effects,
            };
        };

        let key = stored.key;
        let range = stored.range;
        let record_count = stored.record_count;

        let consumers = if let Some((job_id, _)) = &coordinator_job {
            self.consumers_for_job(*job_id)
        } else {
            self.matching_pending_consumers(&key, &range)
        };

        if consumers.is_empty() {
            let mut effects = Vec::new();
            if coordinator_job.is_some()
                && matches!(
                    data,
                    fetcher::FetchedData::Klines { .. } | fetcher::FetchedData::OI { .. }
                )
                && let Some(worker_req) = fetched_data_req_id(&data)
            {
                if let Some(done_effects) = self.finish_worker_job_effects(worker_req, None) {
                    effects.extend(done_effects);
                }
                return FetchedDataRuntimeOutcome {
                    handled: true,
                    effects,
                };
            }
            return FetchedDataRuntimeOutcome {
                handled: false,
                effects,
            };
        }

        log::info!(
            target: "marketdata",
            "MARKETDATA ConsumerDispatch | key={} range={} consumers={} kind={}",
            key.display_key(),
            range.format_display(),
            consumers.len(),
            key.kind.display_name()
        );

        let mut effects = Vec::new();
        match &data {
            fetcher::FetchedData::Klines { data, .. } => {
                for consumer in consumers {
                    if consumer.feature != ConsumerFeature::ChartKlines {
                        continue;
                    }
                    let stream = consumer.stream.unwrap_or(stream_type);
                    if let StreamKind::Kline {
                        timeframe,
                        ticker_info,
                    } = stream
                    {
                        effects.push(MarketDataChartEffect::InsertKlinesPartial {
                            consumer: consumer.clone(),
                            stream: Some(stream),
                            timeframe,
                            ticker_info,
                            rows: data.clone(),
                        });
                    }
                }
            }
            fetcher::FetchedData::Trades { batch, .. } => {
                for consumer in consumers {
                    effects.extend(self.effects_for_trades_consumer(
                        &consumer,
                        batch.clone(),
                        range,
                    ));
                }
            }
            fetcher::FetchedData::OI { data, .. } => {
                for consumer in consumers {
                    if consumer.feature != ConsumerFeature::OpenInterest {
                        continue;
                    }
                    effects.push(MarketDataChartEffect::InsertOpenInterestPartial {
                        consumer: consumer.clone(),
                        rows: data.clone(),
                    });
                }
            }
            fetcher::FetchedData::BubbleSummary { .. } => {}
        }

        self.record_network_delivery(
            coordinator_job.as_ref().map(|(job_id, _)| *job_id),
            &key,
            &range,
            record_count,
        );

        if coordinator_job.is_some()
            && matches!(
                data,
                fetcher::FetchedData::Klines { .. } | fetcher::FetchedData::OI { .. }
            )
            && let Some(worker_req) = fetched_data_req_id(&data)
            && let Some(done_effects) = self.finish_worker_job_effects(worker_req, None)
        {
            effects.extend(done_effects);
        }

        FetchedDataRuntimeOutcome {
            handled: true,
            effects,
        }
    }

    pub fn legacy_fetched_data_effects(
        &self,
        pane_id: Uuid,
        stream_type: StreamKind,
        data: fetcher::FetchedData,
    ) -> Vec<MarketDataChartEffect> {
        match data {
            fetcher::FetchedData::Trades {
                batch,
                until_time,
                req_id,
            } => {
                let last_trade_time = batch.last().map_or(UnixMs::ZERO, |trade| trade.time);
                let received = batch.len();
                let first_received = batch.first().map(|trade| trade.time);
                let last_received = batch.last().map(|trade| trade.time);

                if last_trade_time < until_time {
                    log::debug!(
                        "DATA Trades Distribute | pane={} req={} stream={} received={} inserted={} dropped_after_until=0 until_time={} first_received={} last_received={}",
                        fetcher::short_id(pane_id),
                        fetcher::format_req_id(req_id),
                        fetcher::format_stream(&stream_type),
                        received,
                        received,
                        fetcher::format_time_short(until_time),
                        fetcher::format_optional_time(first_received),
                        fetcher::format_optional_time(last_received)
                    );
                    vec![MarketDataChartEffect::InsertLegacyTrades {
                        pane_id,
                        req_id,
                        batch,
                        is_batches_done: false,
                    }]
                } else {
                    let filtered_batch = batch
                        .iter()
                        .filter(|trade| trade.time <= until_time)
                        .copied()
                        .collect::<Vec<_>>();
                    let dropped_after_until = received.saturating_sub(filtered_batch.len());

                    log::debug!(
                        "DATA Trades Distribute | pane={} req={} stream={} received={received} inserted={} dropped_after_until={dropped_after_until} until_time={} first_received={} last_received={}",
                        fetcher::short_id(pane_id),
                        fetcher::format_req_id(req_id),
                        fetcher::format_stream(&stream_type),
                        filtered_batch.len(),
                        fetcher::format_time_short(until_time),
                        fetcher::format_optional_time(first_received),
                        fetcher::format_optional_time(last_received)
                    );
                    vec![MarketDataChartEffect::InsertLegacyTrades {
                        pane_id,
                        req_id,
                        batch: filtered_batch,
                        is_batches_done: true,
                    }]
                }
            }
            fetcher::FetchedData::BubbleSummary {
                data,
                range,
                trades_seen,
                raw_discarded,
                req_id,
            } => {
                log::debug!(
                    "BUBBLE Summary Distribute | pane={} req={} stream={} range={} candles={} trades_seen={} raw_discarded={}",
                    fetcher::short_id(pane_id),
                    fetcher::format_req_id(req_id),
                    fetcher::format_stream(&stream_type),
                    fetcher::format_time_range(range.0, range.1),
                    data.len(),
                    trades_seen,
                    raw_discarded
                );
                vec![MarketDataChartEffect::InsertLegacyBubbleSummaries {
                    pane_id,
                    req_id,
                    summaries: data,
                    range,
                    trades_seen,
                    raw_discarded,
                }]
            }
            fetcher::FetchedData::Klines { data, req_id } => {
                log::debug!(
                    "DATA Klines Distribute | pane={} req={} stream={} count={} first={} last={}",
                    fetcher::short_id(pane_id),
                    fetcher::format_req_id(req_id),
                    fetcher::format_stream(&stream_type),
                    data.len(),
                    fetcher::format_optional_time(data.first().map(|kline| kline.time)),
                    fetcher::format_optional_time(data.last().map(|kline| kline.time))
                );
                vec![MarketDataChartEffect::InsertLegacyKlines {
                    pane_id,
                    req_id,
                    stream: stream_type,
                    rows: data,
                }]
            }
            fetcher::FetchedData::OI { data, req_id } => {
                log::debug!(
                    "DATA OI Distribute | pane={} req={} stream={} count={}",
                    fetcher::short_id(pane_id),
                    fetcher::format_req_id(req_id),
                    fetcher::format_stream(&stream_type),
                    data.len()
                );
                vec![MarketDataChartEffect::InsertLegacyOpenInterest {
                    pane_id,
                    req_id,
                    rows: data,
                }]
            }
        }
    }

    pub fn complete_legacy_fetch_effects(
        &mut self,
        key: MarketDataKey,
        range: MarketDataRange,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
        pane_id: Uuid,
    ) -> Vec<MarketDataChartEffect> {
        let consumers = self.complete_legacy_fetch(key.clone(), range, empty_covered_tail, pane_id);
        let mut effects = Vec::new();
        for consumer in consumers {
            effects.extend(self.effects_for_completed_consumer(consumer, empty_covered_tail));
        }
        self.remove_completed_consumers_for(&key, &range);
        effects
    }

    pub fn complete_fetch_effects(
        &mut self,
        pane_id: Uuid,
        req_id: Option<Uuid>,
        fetch: Option<fetcher::FetchRange>,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
        ready_streams: &[StreamKind],
    ) -> Vec<MarketDataChartEffect> {
        if let Some(worker_req) = req_id
            && let Some(effects) = self.finish_worker_job_effects(worker_req, empty_covered_tail)
        {
            return effects;
        }

        let mut effects = Vec::new();
        let mut handled_by_market_consumers = false;

        if let Some(fetch_range) = fetch {
            let pending_context = req_id.and_then(|id| {
                self.pending_consumer_by_req(id)
                    .map(|consumer| (consumer.key, consumer.range))
            });

            let completion_timeframe = pending_context
                .as_ref()
                .and_then(|(key, _)| key.kind.timeframe())
                .or_else(|| {
                    ready_streams.iter().find_map(|stream| match stream {
                        StreamKind::Kline { timeframe, .. } => Some(*timeframe),
                        _ => None,
                    })
                });

            let ticker_hint = ready_streams.iter().find_map(|stream| match stream {
                StreamKind::Kline { ticker_info, .. }
                | StreamKind::Trades { ticker_info }
                | StreamKind::Depth { ticker_info, .. } => Some(*ticker_info),
            });

            let key = pending_context
                .as_ref()
                .map(|(key, _)| key.clone())
                .or_else(|| {
                    bridge::fetch_range_to_key(
                        &fetch_range,
                        ticker_hint.as_ref(),
                        completion_timeframe,
                    )
                });

            if let Some(key) = key
                && let Some(range) = bridge::fetch_range_to_range(&fetch_range)
            {
                let had_matching_consumers =
                    !self.matching_pending_consumers(&key, &range).is_empty();
                let complete_effects =
                    self.complete_legacy_fetch_effects(key, range, empty_covered_tail, pane_id);
                handled_by_market_consumers =
                    had_matching_consumers || !complete_effects.is_empty();
                effects.extend(complete_effects);
            }

            if matches!(fetch_range, fetcher::FetchRange::Trades(_, _))
                && !handled_by_market_consumers
            {
                effects.push(MarketDataChartEffect::CompleteLegacyTradeFetch {
                    pane_id,
                    req_id,
                    fetch: fetch_range,
                    empty_covered_tail,
                });
            }
        }

        effects.push(MarketDataChartEffect::MarkPaneReady { pane_id });
        effects
    }

    pub fn effects_for_cached_dispatch(
        &mut self,
        dispatch: CachedMarketDataDispatch,
    ) -> Vec<MarketDataChartEffect> {
        match dispatch {
            CachedMarketDataDispatch::Klines {
                key,
                range,
                timeframe,
                rows,
            } => self.effects_for_cached_klines(&key, range, timeframe, rows),
            CachedMarketDataDispatch::Trades { key, range, rows } => {
                self.effects_for_cached_trades(&key, range, rows)
            }
            CachedMarketDataDispatch::OpenInterest {
                key,
                range,
                timeframe: _,
                rows,
            } => self.effects_for_cached_open_interest(&key, range, rows),
        }
    }

    fn effects_for_cached_klines(
        &mut self,
        key: &MarketDataKey,
        range: MarketDataRange,
        timeframe: exchange::Timeframe,
        rows: Vec<Kline>,
    ) -> Vec<MarketDataChartEffect> {
        let consumers = self.matching_pending_consumers(key, &range);
        log::info!(
            target: "marketdata",
            "MARKETDATA CacheServe | key={} range={} consumers={}",
            key.display_key(),
            range.format_display(),
            consumers.len()
        );

        let mut effects = Vec::new();
        for consumer in consumers {
            if consumer.feature != ConsumerFeature::ChartKlines {
                continue;
            }
            if !self.mark_consumer_segment_delivered(consumer.req_id, range) {
                continue;
            }
            let stream = consumer.stream;
            if let Some(StreamKind::Kline { ticker_info, .. }) = stream {
                effects.push(MarketDataChartEffect::InsertKlinesPartial {
                    consumer: consumer.clone(),
                    stream,
                    timeframe,
                    ticker_info,
                    rows: rows.clone(),
                });
                effects.extend(self.complete_cached_segment_effects(consumer.req_id, range));
            }
        }
        effects
    }

    fn effects_for_cached_trades(
        &mut self,
        key: &MarketDataKey,
        range: MarketDataRange,
        rows: Vec<Trade>,
    ) -> Vec<MarketDataChartEffect> {
        let consumers = self.matching_pending_consumers(key, &range);
        log::info!(
            target: "marketdata",
            "MARKETDATA CacheServe | key={} range={} consumers={}",
            key.display_key(),
            range.format_display(),
            consumers.len()
        );

        let mut effects = Vec::new();
        for consumer in consumers {
            if !self.mark_consumer_segment_delivered(consumer.req_id, range) {
                continue;
            }
            effects.extend(self.effects_for_trades_consumer(&consumer, rows.clone(), range));
            effects.extend(self.complete_cached_segment_effects(consumer.req_id, range));
        }
        effects
    }

    fn effects_for_cached_open_interest(
        &mut self,
        key: &MarketDataKey,
        range: MarketDataRange,
        rows: Vec<OpenInterest>,
    ) -> Vec<MarketDataChartEffect> {
        let consumers = self.matching_pending_consumers(key, &range);
        log::info!(
            target: "marketdata",
            "MARKETDATA CacheServe | key={} range={} consumers={}",
            key.display_key(),
            range.format_display(),
            consumers.len()
        );

        let mut effects = Vec::new();
        for consumer in consumers {
            if consumer.feature != ConsumerFeature::OpenInterest {
                continue;
            }
            if !self.mark_consumer_segment_delivered(consumer.req_id, range) {
                continue;
            }
            effects.push(MarketDataChartEffect::InsertOpenInterestPartial {
                consumer: consumer.clone(),
                rows: rows.clone(),
            });
            effects.extend(self.complete_cached_segment_effects(consumer.req_id, range));
        }
        effects
    }

    pub fn effects_for_trades_consumer(
        &mut self,
        consumer: &PendingMarketDataConsumer,
        trades: Vec<Trade>,
        range: MarketDataRange,
    ) -> Vec<MarketDataChartEffect> {
        match consumer.feature {
            ConsumerFeature::Footprint | ConsumerFeature::TradeHydration => {
                if consumer.feature == ConsumerFeature::TradeHydration {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA TradeHydrationDispatch | pane={} records={} range={} partial=true",
                        super::job::short_id(consumer.pane_id),
                        trades.len(),
                        range.format_display()
                    );
                }
                vec![MarketDataChartEffect::InsertTrades {
                    consumer: consumer.clone(),
                    stream: consumer.stream,
                    batch: trades,
                    until_time: range.to,
                }]
            }
            ConsumerFeature::VolumeBubbles => {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA ConsumerDispatch | key={} range={} consumer=VolumeBubbles action=derive_bubbles",
                    consumer.key.display_key(),
                    range.format_display()
                );
                let Some(config) = consumer
                    .bubble_config
                    .or_else(|| BubbleConsumerConfig::from_fetch(consumer.fetch))
                else {
                    log::warn!(
                        target: "marketdata",
                        "MARKETDATA BubbleConfigMissing | req={} reason=no_runtime_metadata",
                        fetcher::short_id(consumer.req_id)
                    );
                    return Vec::new();
                };
                log::info!(
                    target: "marketdata",
                    "MARKETDATA BubbleDerivedStart | source=Trades range={}",
                    consumer.range.format_display()
                );
                match self.coordinator.compute_bubble_summaries(
                    &consumer.key,
                    &consumer.range,
                    config.timeframe_ms,
                    config.price_step,
                    config.max_candidates_per_candle,
                ) {
                    Some(summaries) => {
                        let trades_seen = trades.len();
                        let candidates: usize = summaries.iter().map(|s| s.candidates.len()).sum();
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA BubbleReuse | source=coordinatorDerived range={} summaries={} candidates={}",
                            consumer.range.format_display(),
                            summaries.len(),
                            candidates
                        );
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA BubblePartialUpdate | req={} batch={} summaries={}",
                            fetcher::short_id(consumer.req_id),
                            trades_seen,
                            summaries.len()
                        );
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA BubbleChartUpdate | pane={} req={} partial=true summaries={}",
                            super::job::short_id(consumer.pane_id),
                            fetcher::short_id(consumer.req_id),
                            summaries.len()
                        );
                        self.mark_bubble_consumer_partial(consumer.req_id);
                        vec![MarketDataChartEffect::InsertBubbleSummaries {
                            consumer: consumer.clone(),
                            summaries,
                            range: consumer.range,
                            trades_seen,
                            raw_discarded: 0,
                            complete: false,
                        }]
                    }
                    None => {
                        log::warn!(
                            target: "marketdata",
                            "MARKETDATA BubbleFallbackLegacy | reason=no_raw_trades_after_fetch"
                        );
                        Vec::new()
                    }
                }
            }
            _ => Vec::new(),
        }
    }

    pub fn complete_cached_segment_effects(
        &mut self,
        req_id: Uuid,
        segment: MarketDataRange,
    ) -> Vec<MarketDataChartEffect> {
        let Some(status) = self.mark_consumer_segment_complete(req_id, segment) else {
            return Vec::new();
        };
        let Some(consumer) = self.pending_consumer_by_req(req_id) else {
            return Vec::new();
        };

        if status.coverage_complete {
            log::info!(
                target: "marketdata",
                "MARKETDATA ConsumerSegmentComplete | req={} segment={} completed={}/{} source=cache coverage_complete=true missing=",
                fetcher::short_id(req_id),
                segment.format_display(),
                status.completed_logical,
                status.total_logical
            );
        } else {
            log::info!(
                target: "marketdata",
                "MARKETDATA ConsumerSegmentComplete | req={} segment={} completed={}/{} source=cache coverage_complete=false missing={}",
                fetcher::short_id(req_id),
                segment.format_display(),
                status.completed_logical,
                status.total_logical,
                status.missing.iter().map(MarketDataRange::format_display).collect::<Vec<_>>().join(",")
            );
        }

        if self.consumer_is_fully_satisfied(req_id) {
            log::info!(
                target: "marketdata",
                "MARKETDATA ChartReqComplete | chart_req={} feature={}",
                fetcher::short_id(req_id),
                consumer.feature.short_name()
            );
            self.effects_for_completed_consumer(consumer, None)
        } else {
            let remaining = self.consumer_remaining_segments(req_id).join(",");
            log::info!(
                target: "marketdata",
                "MARKETDATA ConsumerWaiting | req={} remaining={}",
                fetcher::short_id(req_id),
                remaining
            );
            Vec::new()
        }
    }

    pub fn effects_for_completed_consumer(
        &mut self,
        consumer: PendingMarketDataConsumer,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
    ) -> Vec<MarketDataChartEffect> {
        match consumer.feature {
            ConsumerFeature::VolumeBubbles => {
                let Some(config) = consumer
                    .bubble_config
                    .or_else(|| BubbleConsumerConfig::from_fetch(consumer.fetch))
                else {
                    log::warn!(
                        target: "marketdata",
                        "MARKETDATA BubbleConfigMissing | req={} reason=no_runtime_metadata",
                        fetcher::short_id(consumer.req_id)
                    );
                    return Vec::new();
                };
                match self.coordinator.compute_bubble_summaries(
                    &consumer.key,
                    &consumer.range,
                    config.timeframe_ms,
                    config.price_step,
                    config.max_candidates_per_candle,
                ) {
                    Some(summaries) => {
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA BubbleFinalUpdate | req={} summaries={} had_partial={}",
                            fetcher::short_id(consumer.req_id),
                            summaries.len(),
                            consumer.has_partial_updates
                        );
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA Derived | kind=VolumeBubbles source=Trades range={} candles={} candidates={}",
                            consumer.range.format_display(),
                            summaries.len(),
                            summaries.iter().map(|summary| summary.candidates.len()).sum::<usize>()
                        );
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA BubbleChartUpdate | pane={} req={} partial=false summaries={}",
                            super::job::short_id(consumer.pane_id),
                            fetcher::short_id(consumer.req_id),
                            summaries.len()
                        );
                        self.mark_bubble_consumer_completed(consumer.req_id);
                        let range = consumer.range;
                        vec![MarketDataChartEffect::InsertBubbleSummaries {
                            consumer,
                            summaries,
                            range,
                            trades_seen: 0,
                            raw_discarded: 0,
                            complete: true,
                        }]
                    }
                    None => {
                        log::warn!(
                            target: "marketdata",
                            "MARKETDATA BubbleFallbackLegacy | reason=no_raw_trades_on_complete"
                        );
                        Vec::new()
                    }
                }
            }
            _ => {
                self.mark_consumer_completed(consumer.req_id);
                vec![MarketDataChartEffect::CompleteConsumer {
                    consumer,
                    empty_covered_tail,
                }]
            }
        }
    }

    pub fn finish_worker_job_effects(
        &mut self,
        worker_req: Uuid,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
    ) -> Option<Vec<MarketDataChartEffect>> {
        let finish = self.finish_worker_job(worker_req, empty_covered_tail)?;
        let Some(completion) = finish.completion else {
            return Some(vec![MarketDataChartEffect::ProgressSnapshot]);
        };

        let mut effects = Vec::new();
        for chart_req in &completion.consumer_ids {
            let Some(status) =
                self.mark_consumer_segment_complete(*chart_req, completion.job.range)
            else {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA ChartReqMissing | chart_req={} feature={} reason=already_removed_or_generation_stale",
                    fetcher::short_id(*chart_req),
                    completion.job.key.kind.display_name()
                );
                continue;
            };

            let Some(consumer) = self.pending_consumer_by_req(*chart_req) else {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA ChartReqMissing | chart_req={} feature={} reason=already_removed_or_generation_stale",
                    fetcher::short_id(*chart_req),
                    completion.job.key.kind.display_name()
                );
                continue;
            };

            match consumer.feature {
                ConsumerFeature::VolumeBubbles => {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA BubbleSegmentComplete | req={} segment={} completed={}/{} missing={}",
                        fetcher::short_id(*chart_req),
                        completion.job.range.format_display(),
                        status.completed_logical,
                        status.total_logical,
                        status.missing.iter().map(MarketDataRange::format_display).collect::<Vec<_>>().join(",")
                    );
                    if self.consumer_is_fully_satisfied(*chart_req) {
                        effects.extend(
                            self.effects_for_completed_consumer(consumer, empty_covered_tail),
                        );
                    } else {
                        let remaining = self.consumer_remaining_segments(*chart_req).join(",");
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA BubbleWaiting | req={} remaining={}",
                            fetcher::short_id(*chart_req),
                            remaining
                        );
                    }
                }
                _ => {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA ConsumerSegmentComplete | req={} segment={} completed={}/{} source=network feature={} coverage_complete={} missing={}",
                        fetcher::short_id(*chart_req),
                        completion.job.range.format_display(),
                        status.completed_logical,
                        status.total_logical,
                        consumer.feature.short_name(),
                        status.coverage_complete,
                        status.missing.iter().map(MarketDataRange::format_display).collect::<Vec<_>>().join(",")
                    );
                    if self.consumer_is_fully_satisfied(*chart_req) {
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA ChartReqComplete | chart_req={} feature={}",
                            fetcher::short_id(*chart_req),
                            consumer.feature.short_name()
                        );
                        effects.extend(
                            self.effects_for_completed_consumer(consumer, empty_covered_tail),
                        );
                    } else {
                        let remaining = self.consumer_remaining_segments(*chart_req).join(",");
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA ConsumerWaiting | req={} remaining={}",
                            fetcher::short_id(*chart_req),
                            remaining
                        );
                    }
                }
            }
        }
        self.retain_effective_pending_consumers();
        effects.push(MarketDataChartEffect::ProgressSnapshot);
        Some(effects)
    }

    pub fn remove_completed_consumers_for(&mut self, key: &MarketDataKey, range: &MarketDataRange) {
        self.pending_consumers.retain(|consumer| {
            !(consumer.key == *key
                && consumer.completed
                && !Self::consumer_has_effective_gaps(consumer)
                && consumer.range.overlaps(range))
        });
    }

    pub fn retain_effective_pending_consumers(&mut self) {
        self.pending_consumers
            .retain(|consumer| !consumer.completed || Self::consumer_has_effective_gaps(consumer));
    }

    pub fn finish_worker_job(
        &mut self,
        worker_req: Uuid,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
    ) -> Option<WorkerJobFinish> {
        let job_id = self.worker_req_to_job.get(&worker_req).copied()?;

        log::info!(
            target: "marketdata",
            "MARKETDATA WorkerDone | worker_req={} job={}",
            fetcher::short_id(worker_req),
            super::job::short_id(job_id)
        );
        log::info!(
            target: "marketdata",
            "MARKETDATA WorkerReqIgnoredByChartHandler | worker_req={} reason=coordinator_owned",
            fetcher::short_id(worker_req)
        );

        let Some(job) = self.coordinator.job(job_id).cloned() else {
            self.cleanup_worker_job(worker_req, job_id);
            return Some(WorkerJobFinish {
                kind: WorkerCompletionKind::MissingJob,
                completion: None,
            });
        };
        let consumer_ids = self
            .job_to_consumers
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let records = job.progress.records_fetched;

        if empty_covered_tail.is_some() || records == 0 {
            self.coordinator.mark_empty_and_remove_job(job_id);
        } else {
            self.coordinator.complete_and_remove_job(job_id, records);
        }
        self.cleanup_worker_job(worker_req, job_id);
        self.retain_effective_pending_consumers();
        self.save_coverage();

        Some(WorkerJobFinish {
            kind: if empty_covered_tail.is_some() || records == 0 {
                WorkerCompletionKind::Empty
            } else {
                WorkerCompletionKind::Completed
            },
            completion: Some(WorkerJobCompletion {
                job_id,
                job,
                consumer_ids,
                records,
            }),
        })
    }

    pub fn finish_worker_job_invalid(&mut self, worker_req: Uuid) -> bool {
        let Some(job_id) = self.worker_req_to_job.get(&worker_req).copied() else {
            return false;
        };
        log::info!(
            target: "marketdata",
            "MARKETDATA WorkerDone | worker_req={} job={}",
            fetcher::short_id(worker_req),
            super::job::short_id(job_id)
        );
        self.coordinator
            .fail_and_remove_job(job_id, "invalid_out_of_range_response".to_string());
        log::info!(
            target: "marketdata",
            "MARKETDATA JobRemoved | job={} reason=invalid_out_of_range_response",
            super::job::short_id(job_id)
        );
        self.cleanup_worker_job(worker_req, job_id);
        true
    }

    pub fn stale_worker_jobs(&self, now_ms: u64, stale_after_ms: u64) -> Vec<StaleWorkerJob> {
        self.coordinator
            .active_jobs()
            .into_iter()
            .filter(|job| {
                now_ms.saturating_sub(job.created_at) >= stale_after_ms
                    && job.progress.records_fetched == 0
            })
            .map(|job| StaleWorkerJob {
                job_id: job.id,
                worker_req: self.job_to_worker_req.get(&job.id).copied(),
                range: job.range,
                age_ms: now_ms.saturating_sub(job.created_at),
                consumer_ids: self
                    .job_to_consumers
                    .get(&job.id)
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect()
    }

    pub fn mark_stale_job_consumers_failed(
        &mut self,
        job_id: FetchJobId,
        range: MarketDataRange,
    ) -> Vec<PendingMarketDataConsumer> {
        let consumer_ids = self
            .job_to_consumers
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let mut satisfied = Vec::new();
        for chart_req in consumer_ids {
            if let Some(consumer) = self
                .pending_consumers
                .iter_mut()
                .find(|consumer| consumer.req_id == chart_req)
                && !consumer.failed_segments.contains(&range)
            {
                consumer.failed_segments.push(range);
            }
            if self.consumer_is_fully_satisfied(chart_req)
                && let Some(consumer) = self.pending_consumer_by_req(chart_req)
            {
                satisfied.push(consumer);
                self.mark_consumer_completed(chart_req);
            }
        }
        satisfied
    }

    pub fn fail_and_remove_worker_job(&mut self, job_id: FetchJobId, reason: String) {
        let worker_req = self.job_to_worker_req.get(&job_id).copied();
        self.coordinator.fail_and_remove_job(job_id, reason);
        if let Some(worker_req) = worker_req {
            self.worker_req_to_job.remove(&worker_req);
        }
        self.job_to_worker_req.remove(&job_id);
        self.job_to_consumers.remove(&job_id);
    }

    pub fn fail_worker_request(
        &mut self,
        worker_req: Uuid,
        error: String,
    ) -> Option<WorkerJobFailure> {
        let job_id = self.worker_req_to_job.get(&worker_req).copied()?;
        let job = self.coordinator.job(job_id).cloned();
        if let Some(job) = &job {
            for chart_req in self
                .job_to_consumers
                .get(&job_id)
                .cloned()
                .unwrap_or_default()
            {
                if let Some(consumer) = self
                    .pending_consumers
                    .iter_mut()
                    .find(|consumer| consumer.req_id == chart_req)
                {
                    consumer.failed_segments.push(job.range);
                }
            }
        }
        self.fail_and_remove_worker_job(job_id, error);
        job.map(|job| WorkerJobFailure {
            job_id,
            range: job.range,
        })
    }

    pub fn complete_legacy_fetch(
        &mut self,
        key: MarketDataKey,
        range: MarketDataRange,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
        pane_id: Uuid,
    ) -> Vec<PendingMarketDataConsumer> {
        if empty_covered_tail.is_some() {
            log::info!(
                target: "marketdata",
                "MARKETDATA LegacyCoverageEmpty | pane={} key={} range={}",
                super::job::short_id(pane_id),
                key.display_key(),
                range.format_display()
            );
            self.coordinator.coverage.mark_empty(key.clone(), range);
        } else {
            log::info!(
                target: "marketdata",
                "MARKETDATA CoverageComplete | pane={} key={} range={}",
                super::job::short_id(pane_id),
                key.display_key(),
                range.format_display()
            );
            self.coordinator
                .coverage
                .mark_complete(key.clone(), range, 0);
        }

        let job_ids = self
            .coordinator
            .active_jobs()
            .iter()
            .filter(|job| job.key == key && job.range.overlaps(&range))
            .map(|job| job.id)
            .collect::<Vec<_>>();
        for job_id in job_ids {
            log::info!(
                target: "marketdata",
                "MARKETDATA BridgeJobComplete | job={}",
                super::job::short_id(job_id)
            );
            self.coordinator.complete_job(job_id, 0);
        }

        let consumers = self.matching_pending_consumers(&key, &range);
        self.save_coverage();
        consumers
    }

    fn cleanup_worker_job(&mut self, worker_req: Uuid, job_id: FetchJobId) {
        self.worker_req_to_job.remove(&worker_req);
        self.job_to_worker_req.remove(&job_id);
        self.job_to_consumers.remove(&job_id);
    }

    pub fn log_progress_snapshot(&self) {
        let progress = self.progress_snapshot();
        log::info!(
            target: "marketdata",
            "MARKETDATA ProgressSnapshot | active={} cached_records={} fetched_records={} jobs=[{}]",
            progress.active_job_count(),
            progress.total_cached_records,
            progress.total_fetched_records,
            progress
                .active_jobs
                .iter()
                .map(|job| format!(
                    "{}:{}",
                    super::job::short_id(job.id),
                    job.range.format_display()
                ))
                .collect::<Vec<_>>()
                .join(",")
        );
    }

    pub fn consumer_has_effective_gaps(consumer: &PendingMarketDataConsumer) -> bool {
        let terminal_segments = consumer
            .completed_segments
            .iter()
            .chain(consumer.failed_segments.iter())
            .copied()
            .collect::<Vec<_>>();
        let raw_missing = compute_missing(consumer.range, &terminal_segments);
        if matches!(
            consumer.feature,
            ConsumerFeature::TradeHydration | ConsumerFeature::Footprint
        ) {
            let filtered =
                super::range::filter_tiny_trade_gaps(raw_missing, MIN_TRADE_BACKFILL_SEGMENT_MS);
            !filtered.is_empty()
        } else {
            !raw_missing.is_empty()
        }
    }

    pub fn insert_live_trades(&mut self, key: &MarketDataKey, buffer: &[Trade]) {
        self.coordinator.feed_trades(key, buffer);
        self.live_adapter.ingest_trades(
            key,
            buffer,
            &mut self.coordinator.store,
            &mut self.coordinator.coverage,
            Some(&mut self.cache),
        );
    }

    pub fn insert_live_klines(&mut self, key: &MarketDataKey, klines: &[Kline]) {
        self.live_adapter.ingest_klines(
            key,
            klines,
            &mut self.coordinator.store,
            &mut self.coordinator.coverage,
            Some(&mut self.cache),
        );
    }

    pub fn route_chart_data_needs(
        &mut self,
        route: DashboardChartNeedRoute,
    ) -> MarketDataRouteOutcome {
        let DashboardChartNeedRoute {
            pane_id,
            ready_streams,
            chart_generation,
            needs,
        } = route;

        let ticker_info = ready_streams.iter().find_map(stream_ticker_info);
        let context_timeframe = ready_streams.iter().find_map(|stream| match stream {
            StreamKind::Kline { timeframe, .. } => Some(*timeframe),
            _ => None,
        });
        let mut registered_any = false;

        for need in &needs {
            let feature = bridge::chart_need_to_feature(need);
            let Some(key) = bridge::chart_need_to_key(need, ticker_info, context_timeframe) else {
                log::warn!(
                    target: "marketdata",
                    "MARKETDATA RequirementSkip | pane={} need={} reason=no_key",
                    super::job::short_id(pane_id),
                    need
                );
                continue;
            };
            let Some(range) = bridge::chart_need_to_range(need) else {
                continue;
            };
            let fetch = bridge::chart_need_to_consumer_fetch(need);
            let stream = ready_streams
                .iter()
                .copied()
                .find(|stream| stream_matches_market_key(stream, &key));

            if self.pending_consumers.iter().any(|consumer| {
                consumer.pane_id == pane_id
                    && consumer.key == key
                    && consumer.range == range
                    && consumer.feature == feature
                    && !consumer.completed
            }) {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA NeedSkipped | pane={} need={} key={} range={} reason=already_pending",
                    super::job::short_id(pane_id),
                    need.label(),
                    key.display_key(),
                    range.format_display()
                );
                continue;
            }

            let req_id = Uuid::new_v4();
            self.pending_consumers.push(PendingMarketDataConsumer {
                pane_id,
                req_id,
                fetch,
                stream,
                key: key.clone(),
                range,
                feature,
                bubble_config: bubble_config_from_chart_need(need),
                chart_generation,
                has_partial_updates: false,
                completed: false,
                required_segments: Vec::new(),
                completed_segments: Vec::new(),
                failed_segments: Vec::new(),
                delivered_segments: Vec::new(),
            });

            if let Some(requirement) =
                bridge::chart_need_to_requirement(need, pane_id, ticker_info, context_timeframe)
            {
                self.coordinator.require(requirement);
                registered_any = true;
            }
        }

        if !registered_any || !self.coordinator.has_pending_requirements() {
            return MarketDataRouteOutcome {
                pane_id,
                ready_streams,
                chart_generation,
                fetch_specs: Vec::new(),
                cached_dispatches: Vec::new(),
                reason: "no_coordinator_requirement",
            };
        }

        let plan = self.coordinator.plan().clone();
        log::info!(
            target: "marketdata",
            "MARKETDATA RuntimePlan | pane={} {} source=chart_needs",
            super::job::short_id(pane_id),
            plan.runtime_summary(self.coordinator.active_job_count())
        );

        self.register_required_segments_from_plan(&plan);
        let (cached_dispatches, mut cache_desync_specs) =
            self.serve_cached_market_segments(&plan, &ready_streams, pane_id);

        let created_jobs = self.coordinator.execute_plan();
        for job_id in &created_jobs {
            self.coordinator.start_job(*job_id);
        }

        let mut network_specs = Vec::new();
        for job_id in &created_jobs {
            let Some(job) = self.coordinator.job(*job_id).cloned() else {
                continue;
            };
            if let Some(spec) = self.fetch_spec_for_market_job(
                *job_id,
                &job.key,
                job.range,
                &ready_streams,
                pane_id,
            ) {
                network_specs.push(spec);
            }
        }
        self.attach_pending_consumers_to_active_jobs("dedup_active_job");
        network_specs.append(&mut cache_desync_specs);

        let reason = if !network_specs.is_empty() {
            "coordinator_new_network_jobs"
        } else if plan.has_cached_data() {
            "coordinator_served_cache"
        } else {
            "coordinator_active_job"
        };

        MarketDataRouteOutcome {
            pane_id,
            ready_streams,
            chart_generation,
            fetch_specs: network_specs,
            cached_dispatches,
            reason,
        }
    }

    pub fn route_fetch_specs(&mut self, route: DashboardFetchRoute) -> MarketDataRouteOutcome {
        let DashboardFetchRoute {
            pane_id,
            ready_streams,
            chart_generation,
            reqs,
        } = route;

        let ticker_info = ready_streams.iter().find_map(stream_ticker_info);
        let mut registered_any = false;

        for spec in &reqs {
            let feature = bridge::fetch_range_to_feature(&spec.fetch);
            let timeframe = resolve_fetch_timeframe(spec, &ready_streams);
            let Some(key) = bridge::fetch_range_to_key(&spec.fetch, ticker_info, timeframe) else {
                log::warn!(
                    target: "marketdata",
                    "MARKETDATA RequirementSkip | pane={} fetch={} reason=no_key",
                    super::job::short_id(pane_id),
                    fetcher::format_fetch_range(&spec.fetch)
                );
                continue;
            };
            let Some(range) = bridge::fetch_range_to_range(&spec.fetch) else {
                continue;
            };

            if matches!(feature, ConsumerFeature::VolumeBubbles) {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA BubbleRequirement | pane={} range={} key={}",
                    super::job::short_id(pane_id),
                    range.format_display(),
                    key.display_key()
                );
            }

            self.pending_consumers.push(PendingMarketDataConsumer {
                pane_id,
                req_id: spec.req_id,
                fetch: spec.fetch,
                stream: spec.stream,
                key: key.clone(),
                range,
                feature,
                bubble_config: BubbleConsumerConfig::from_fetch(spec.fetch),
                chart_generation,
                has_partial_updates: false,
                completed: false,
                required_segments: Vec::new(),
                completed_segments: Vec::new(),
                failed_segments: Vec::new(),
                delivered_segments: Vec::new(),
            });

            if let Some(requirement) = bridge::fetch_range_to_requirement(
                &spec.fetch,
                pane_id,
                feature,
                ticker_info,
                timeframe,
            ) {
                self.coordinator.require(requirement);
                registered_any = true;
            }
        }

        if !registered_any || !self.coordinator.has_pending_requirements() {
            return MarketDataRouteOutcome {
                pane_id,
                ready_streams,
                chart_generation,
                fetch_specs: reqs,
                cached_dispatches: Vec::new(),
                reason: "no_coordinator_requirement",
            };
        }

        let plan = self.coordinator.plan().clone();
        log::info!(
            target: "marketdata",
            "MARKETDATA RuntimePlan | pane={} {}",
            super::job::short_id(pane_id),
            plan.runtime_summary(self.coordinator.active_job_count())
        );

        self.register_required_segments_from_plan(&plan);
        let (cached_dispatches, mut cache_desync_specs) =
            self.serve_cached_market_segments(&plan, &ready_streams, pane_id);

        let created_jobs = self.coordinator.execute_plan();
        for job_id in &created_jobs {
            self.coordinator.start_job(*job_id);
        }

        let mut network_specs = Vec::new();
        for job_id in &created_jobs {
            let Some(job) = self.coordinator.job(*job_id).cloned() else {
                continue;
            };
            if let Some(spec) = self.fetch_spec_for_market_job(
                *job_id,
                &job.key,
                job.range,
                &ready_streams,
                pane_id,
            ) {
                if matches!(job.key.kind, MarketDataKind::Trades)
                    && self.pending_consumers.iter().any(|c| {
                        c.key == job.key
                            && c.range.overlaps(&job.range)
                            && c.feature == ConsumerFeature::VolumeBubbles
                    })
                {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA BubbleRawTradesFetch | range={}",
                        job.range.format_display()
                    );
                }
                network_specs.push(spec);
            }
        }
        self.attach_pending_consumers_to_active_jobs("dedup_active_job");

        network_specs.append(&mut cache_desync_specs);

        let reason = if !network_specs.is_empty() {
            "coordinator_new_network_jobs"
        } else if plan.has_cached_data() {
            "coordinator_served_cache"
        } else {
            "coordinator_active_job"
        };

        MarketDataRouteOutcome {
            pane_id,
            ready_streams,
            chart_generation,
            fetch_specs: network_specs,
            cached_dispatches,
            reason,
        }
    }

    fn fetch_spec_for_market_job(
        &mut self,
        job_id: FetchJobId,
        key: &MarketDataKey,
        range: MarketDataRange,
        ready_streams: &[StreamKind],
        fallback_pane: Uuid,
    ) -> Option<fetcher::FetchSpec> {
        let stream = ready_streams
            .iter()
            .copied()
            .find(|stream| stream_matches_market_key(stream, key));
        let source = self
            .pending_consumers
            .iter()
            .find(|c| c.key == *key && c.range.overlaps(&range));
        let req_id = Uuid::new_v4();
        let fetch = fetch_range_for_key(key, range);
        let stream = stream.or_else(|| source.and_then(|c| c.stream));
        if stream.is_none() {
            log::warn!(
                target: "marketdata",
                "MARKETDATA RuntimeLegacy | pane={} count=0 reason=no_matching_stream key={} range={}",
                super::job::short_id(source.map_or(fallback_pane, |c| c.pane_id)),
                key.display_key(),
                range.format_display()
            );
        }
        let consumers = self
            .pending_consumers
            .iter()
            .filter(|consumer| {
                if consumer.key != *key || !consumer.range.overlaps(&range) {
                    return false;
                }
                if consumer.completed {
                    log::warn!(
                        target: "marketdata",
                        "MARKETDATA ConsumerAttachRejected | chart_req={} job={} reason=consumer_already_completed",
                        fetcher::short_id(consumer.req_id),
                        super::job::short_id(job_id)
                    );
                    return false;
                }
                true
            })
            .map(|consumer| consumer.req_id)
            .collect::<Vec<_>>();
        self.worker_req_to_job.insert(req_id, job_id);
        self.job_to_worker_req.insert(job_id, req_id);
        self.job_to_consumers.insert(job_id, consumers.clone());
        for consumer_id in &consumers {
            self.add_required_segment_to_consumer(*consumer_id, range);
        }
        log::info!(
            target: "marketdata",
            "MARKETDATA WorkerLaunch | job={} worker_req={} key={} range={} consumers={}",
            super::job::short_id(job_id),
            fetcher::short_id(req_id),
            key.display_key(),
            range.format_display(),
            consumers
                .iter()
                .map(|id| fetcher::short_id(*id))
                .collect::<Vec<_>>()
                .join(",")
        );
        Some(fetcher::FetchSpec {
            req_id,
            fetch,
            stream,
        })
    }

    pub fn attach_pending_consumers_to_active_jobs(&mut self, reason: &'static str) -> usize {
        let active_jobs = self
            .coordinator
            .active_jobs()
            .into_iter()
            .map(|job| (job.id, job.key.clone(), job.range))
            .collect::<Vec<_>>();
        let mut attached = 0usize;

        for (job_id, key, job_range) in active_jobs {
            let matching_req_ids = self
                .pending_consumers
                .iter()
                .filter_map(|consumer| {
                    if consumer.key != key || !consumer.range.overlaps(&job_range) {
                        return None;
                    }
                    if consumer.completed {
                        log::warn!(
                            target: "marketdata",
                            "MARKETDATA ConsumerAttachRejected | chart_req={} job={} reason=consumer_already_completed",
                            fetcher::short_id(consumer.req_id),
                            super::job::short_id(job_id)
                        );
                        return None;
                    }
                    Some((consumer.req_id, consumer.feature))
                })
                .collect::<Vec<_>>();

            for (chart_req, feature) in matching_req_ids {
                let consumers = self.job_to_consumers.entry(job_id).or_default();
                if consumers.contains(&chart_req) {
                    continue;
                }

                consumers.push(chart_req);
                self.add_required_segment_to_consumer(chart_req, job_range);
                attached = attached.saturating_add(1);
                log::info!(
                    target: "marketdata",
                    "MARKETDATA ConsumerAttach | job={} chart_req={} feature={} reason={}",
                    super::job::short_id(job_id),
                    fetcher::short_id(chart_req),
                    feature.short_name(),
                    reason
                );
            }
        }

        attached
    }

    fn fetch_spec_for_market_refetch(
        &self,
        key: &MarketDataKey,
        range: MarketDataRange,
        ready_streams: &[StreamKind],
        fallback_pane: Uuid,
    ) -> Option<fetcher::FetchSpec> {
        let stream = ready_streams
            .iter()
            .copied()
            .find(|stream| stream_matches_market_key(stream, key));
        let source = self
            .pending_consumers
            .iter()
            .find(|c| c.key == *key && c.range.overlaps(&range));
        let fetch = fetch_range_for_key(key, range);
        let stream = stream.or_else(|| source.and_then(|c| c.stream));
        if stream.is_none() {
            log::warn!(
                target: "marketdata",
                "MARKETDATA RuntimeLegacy | pane={} count=0 reason=no_matching_stream key={} range={}",
                super::job::short_id(source.map_or(fallback_pane, |c| c.pane_id)),
                key.display_key(),
                range.format_display()
            );
        }
        Some(fetcher::FetchSpec {
            req_id: Uuid::new_v4(),
            fetch,
            stream,
        })
    }

    fn serve_cached_market_segments(
        &mut self,
        plan: &super::planner::DataLoadPlan,
        ready_streams: &[StreamKind],
        fallback_pane: Uuid,
    ) -> (Vec<CachedMarketDataDispatch>, Vec<fetcher::FetchSpec>) {
        let mut dispatches = Vec::new();
        let mut refetch = Vec::new();
        for cached in &plan.cached_segments {
            match &cached.key.kind {
                MarketDataKind::Klines { timeframe } => {
                    let rows = self.cache.query_klines(&cached.key, &cached.range);
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA CacheLoad | key={} range={} kind=Klines records={}",
                        cached.key.display_key(),
                        cached.range.format_display(),
                        rows.len()
                    );
                    if rows.is_empty() {
                        self.mark_cache_desync_and_refetch(
                            &cached.key,
                            cached.range,
                            ready_streams,
                            fallback_pane,
                            &mut refetch,
                        );
                        continue;
                    }

                    let from_ms = cached.range.from.as_u64();
                    let to_ms = cached.range.to.as_u64();
                    let before_count = rows.len();
                    let first_before = rows.first().map(|k| k.time);
                    let last_before = rows.last().map(|k| k.time);
                    let filtered_rows: Vec<_> = rows
                        .into_iter()
                        .filter(|kline| {
                            kline.time.as_u64() >= from_ms && kline.time.as_u64() < to_ms
                        })
                        .collect();
                    if filtered_rows.len() != before_count {
                        log::warn!(
                            target: "marketdata",
                            "MARKETDATA KlineCacheOutOfRangeFiltered | key={} cache_range={} before={} after={} first_before={} last_before={}",
                            cached.key.display_key(),
                            cached.range.format_display(),
                            before_count,
                            filtered_rows.len(),
                            first_before.map_or("-".to_string(), fetcher::format_time_short),
                            last_before.map_or("-".to_string(), fetcher::format_time_short)
                        );
                    }

                    let duration_ms = cached.range.duration_ms();
                    let tf_ms = timeframe.to_milliseconds();
                    let expected_max = (duration_ms / tf_ms) + 2;
                    if filtered_rows.len() as u64 > expected_max {
                        log::warn!(
                            target: "marketdata",
                            "MARKETDATA KlineCacheCorrupt | key={} range={} records={} expected_max={} reason=too_many_rows_for_timeframe",
                            cached.key.display_key(),
                            cached.range.format_display(),
                            filtered_rows.len(),
                            expected_max
                        );
                        self.mark_cache_corrupt_and_refetch(
                            &cached.key,
                            cached.range,
                            "too_many_rows_for_timeframe",
                            ready_streams,
                            fallback_pane,
                            &mut refetch,
                        );
                        continue;
                    }

                    if filtered_rows.is_empty() {
                        self.mark_cache_desync_and_refetch(
                            &cached.key,
                            cached.range,
                            ready_streams,
                            fallback_pane,
                            &mut refetch,
                        );
                        continue;
                    }

                    self.coordinator
                        .feed_klines(&cached.key, filtered_rows.as_slice());
                    self.coordinator.record_cache_served(filtered_rows.len());
                    dispatches.push(CachedMarketDataDispatch::Klines {
                        key: cached.key.clone(),
                        range: cached.range,
                        timeframe: *timeframe,
                        rows: filtered_rows,
                    });
                }
                MarketDataKind::Trades => {
                    let rows = self.cache.query_trades(&cached.key, &cached.range);
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA CacheLoad | key={} range={} kind=Trades records={}",
                        cached.key.display_key(),
                        cached.range.format_display(),
                        rows.len()
                    );
                    if rows.is_empty() {
                        self.mark_cache_desync_and_refetch(
                            &cached.key,
                            cached.range,
                            ready_streams,
                            fallback_pane,
                            &mut refetch,
                        );
                        continue;
                    }
                    self.coordinator.feed_trades(&cached.key, rows.as_slice());
                    self.coordinator.record_cache_served(rows.len());
                    dispatches.push(CachedMarketDataDispatch::Trades {
                        key: cached.key.clone(),
                        range: cached.range,
                        rows,
                    });
                }
                MarketDataKind::OpenInterest { timeframe } => {
                    let rows = self.cache.query_open_interest(&cached.key, &cached.range);
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA CacheLoad | key={} range={} kind=OI records={}",
                        cached.key.display_key(),
                        cached.range.format_display(),
                        rows.len()
                    );
                    if rows.is_empty() {
                        self.mark_cache_desync_and_refetch(
                            &cached.key,
                            cached.range,
                            ready_streams,
                            fallback_pane,
                            &mut refetch,
                        );
                        continue;
                    }
                    let from_ms = cached.range.from.as_u64();
                    let to_ms = cached.range.to.as_u64();
                    let before_count = rows.len();
                    let first_before = rows.first().map(|oi| oi.time);
                    let last_before = rows.last().map(|oi| oi.time);
                    let filtered_rows = rows
                        .into_iter()
                        .filter(|oi| oi.time.as_u64() >= from_ms && oi.time.as_u64() < to_ms)
                        .collect::<Vec<_>>();
                    if filtered_rows.len() != before_count {
                        log::warn!(
                            target: "marketdata",
                            "MARKETDATA OICacheOutOfRangeFiltered | key={} cache_range={} before={} after={} first_before={} last_before={}",
                            cached.key.display_key(),
                            cached.range.format_display(),
                            before_count,
                            filtered_rows.len(),
                            first_before.map_or("-".to_string(), fetcher::format_time_short),
                            last_before.map_or("-".to_string(), fetcher::format_time_short)
                        );
                    }
                    if filtered_rows.is_empty() {
                        self.mark_cache_desync_and_refetch(
                            &cached.key,
                            cached.range,
                            ready_streams,
                            fallback_pane,
                            &mut refetch,
                        );
                        continue;
                    }
                    self.coordinator
                        .feed_open_interest(&cached.key, filtered_rows.as_slice());
                    self.coordinator.record_cache_served(filtered_rows.len());
                    dispatches.push(CachedMarketDataDispatch::OpenInterest {
                        key: cached.key.clone(),
                        range: cached.range,
                        timeframe: *timeframe,
                        rows: filtered_rows,
                    });
                }
            }
        }

        (dispatches, refetch)
    }

    pub fn register_required_segments_from_plan(&mut self, plan: &super::planner::DataLoadPlan) {
        for segment in &plan.cached_segments {
            let key = &segment.key;
            let range = segment.range;
            let source = "cache";
            let records = segment.records;
            if range.duration_ms() < MIN_TRADE_BACKFILL_SEGMENT_MS {
                let should_skip = self.pending_consumers.iter().any(|consumer| {
                    consumer.key == *key
                        && consumer.range.overlaps(&range)
                        && matches!(
                            consumer.feature,
                            ConsumerFeature::TradeHydration | ConsumerFeature::Footprint
                        )
                });
                if should_skip {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA TinyCachedSegmentSkipped | key={} segment={} duration_ms={}",
                        key.display_key(),
                        range.format_display(),
                        range.duration_ms()
                    );
                    continue;
                }
            }
            let consumers = self
                .pending_consumers
                .iter()
                .filter(|consumer| consumer.key == *key && consumer.range.overlaps(&range))
                .map(|consumer| consumer.req_id)
                .collect::<Vec<_>>();
            for req_id in consumers {
                self.add_required_segment_to_consumer(req_id, range);
                log::info!(
                    target: "marketdata",
                    "MARKETDATA SegmentRequired | req={} range={} source={} records={}",
                    fetcher::short_id(req_id),
                    range.format_display(),
                    source,
                    records
                );
            }
        }
    }

    fn mark_cache_corrupt_and_refetch(
        &mut self,
        key: &MarketDataKey,
        range: MarketDataRange,
        reason: &'static str,
        ready_streams: &[StreamKind],
        fallback_pane: Uuid,
        refetch: &mut Vec<fetcher::FetchSpec>,
    ) {
        log::warn!(
            target: "marketdata",
            "MARKETDATA CacheCorrupt | key={} range={} reason={} action=clear_coverage_refetch",
            key.display_key(),
            range.format_display(),
            reason
        );
        self.coordinator
            .coverage
            .mark_stale(key.clone(), range, "corrupt_cache");
        if matches!(key.kind, MarketDataKind::Trades)
            && range.duration_ms() < MIN_TRADE_BACKFILL_SEGMENT_MS
        {
            self.coordinator.coverage.mark_empty(key.clone(), range);
            return;
        }
        if let Some(spec) =
            self.fetch_spec_for_market_refetch(key, range, ready_streams, fallback_pane)
        {
            refetch.push(spec);
        }
    }

    fn mark_cache_desync_and_refetch(
        &mut self,
        key: &MarketDataKey,
        range: MarketDataRange,
        ready_streams: &[StreamKind],
        fallback_pane: Uuid,
        refetch: &mut Vec<fetcher::FetchSpec>,
    ) {
        log::warn!(
            target: "marketdata",
            "MARKETDATA CacheDesync | key={} range={} reason=coverage_without_rows action=clear_coverage_refetch",
            key.display_key(),
            range.format_display()
        );
        self.coordinator
            .coverage
            .mark_stale(key.clone(), range, "cache_desync");
        if matches!(key.kind, MarketDataKind::Trades)
            && range.duration_ms() < MIN_TRADE_BACKFILL_SEGMENT_MS
        {
            log::info!(
                target: "marketdata",
                "MARKETDATA TinyTradeGapSuppressed | key={} range={} reason=below_threshold_cache_desync",
                key.display_key(),
                range.format_display()
            );
            self.coordinator.coverage.mark_empty(key.clone(), range);
            return;
        }
        if let Some(spec) =
            self.fetch_spec_for_market_refetch(key, range, ready_streams, fallback_pane)
        {
            refetch.push(spec);
        }
    }

    pub fn add_required_segment_to_consumer(&mut self, req_id: Uuid, segment: MarketDataRange) {
        if let Some(consumer) = self
            .pending_consumers
            .iter_mut()
            .find(|consumer| consumer.req_id == req_id)
        {
            super::range::add_required_segment_dedup(&mut consumer.required_segments, segment);
        }
    }
}

impl Default for MarketDataRuntime {
    fn default() -> Self {
        Self::new()
    }
}

fn resolve_fetch_timeframe(
    spec: &fetcher::FetchSpec,
    ready_streams: &[StreamKind],
) -> Option<exchange::Timeframe> {
    if let Some(StreamKind::Kline { timeframe, .. }) = spec.stream {
        return Some(timeframe);
    }
    ready_streams.iter().find_map(|stream| match stream {
        StreamKind::Kline { timeframe, .. } => Some(*timeframe),
        _ => None,
    })
}

fn fetch_range_for_key(key: &MarketDataKey, range: MarketDataRange) -> fetcher::FetchRange {
    match key.kind {
        MarketDataKind::Klines { .. } => fetcher::FetchRange::Kline(range.from, range.to),
        MarketDataKind::Trades => fetcher::FetchRange::Trades(range.from, range.to),
        MarketDataKind::OpenInterest { .. } => {
            fetcher::FetchRange::OpenInterest(range.from, range.to)
        }
    }
}

fn stream_ticker_info(stream: &StreamKind) -> Option<&TickerInfo> {
    match stream {
        StreamKind::Kline { ticker_info, .. }
        | StreamKind::Trades { ticker_info }
        | StreamKind::Depth { ticker_info, .. } => Some(ticker_info),
    }
}

fn stream_matches_market_key(stream: &StreamKind, key: &MarketDataKey) -> bool {
    let Some(stream_key) = (match (&key.kind, stream) {
        (MarketDataKind::Trades, StreamKind::Trades { .. }) => bridge::stream_kind_to_key(stream),
        (MarketDataKind::Klines { timeframe }, StreamKind::Kline { timeframe: tf, .. })
            if timeframe == tf =>
        {
            bridge::stream_kind_to_key(stream)
        }
        (
            MarketDataKind::OpenInterest { timeframe },
            StreamKind::Kline {
                timeframe: tf,
                ticker_info,
            },
        ) if timeframe == tf => MarketDataKey::from_ticker_info(
            ticker_info,
            MarketDataKind::OpenInterest {
                timeframe: *timeframe,
            },
        ),
        _ => None,
    }) else {
        return false;
    };
    stream_key == *key
}

fn bubble_config_from_chart_need(need: &ChartDataNeed) -> Option<BubbleConsumerConfig> {
    match need {
        ChartDataNeed::Bubbles {
            timeframe_ms,
            price_step,
            max_candidates_per_candle,
            ..
        } => Some(BubbleConsumerConfig {
            timeframe_ms: *timeframe_ms,
            price_step: *price_step,
            max_candidates_per_candle: *max_candidates_per_candle,
        }),
        _ => None,
    }
}

fn fetched_data_req_id(data: &fetcher::FetchedData) -> Option<Uuid> {
    match data {
        fetcher::FetchedData::Trades { req_id, .. }
        | fetcher::FetchedData::BubbleSummary { req_id, .. }
        | fetcher::FetchedData::Klines { req_id, .. }
        | fetcher::FetchedData::OI { req_id, .. } => *req_id,
    }
}

fn key_for_fetched_data(stream: StreamKind, data: &fetcher::FetchedData) -> Option<MarketDataKey> {
    match data {
        fetcher::FetchedData::Trades { .. } => bridge::stream_kind_to_key(&stream),
        fetcher::FetchedData::Klines { .. } => bridge::stream_kind_to_key(&stream),
        fetcher::FetchedData::OI { .. } => match stream {
            StreamKind::Kline {
                ticker_info,
                timeframe,
            } => MarketDataKey::from_ticker_info(
                &ticker_info,
                MarketDataKind::OpenInterest { timeframe },
            ),
            _ => None,
        },
        fetcher::FetchedData::BubbleSummary { .. } => None,
    }
}

fn range_from_trades(trades: &[Trade]) -> Option<MarketDataRange> {
    let from = trades.iter().map(|trade| trade.time).min()?;
    let to = trades
        .iter()
        .map(|trade| trade.time)
        .max()?
        .saturating_add(1);
    MarketDataRange::new(from, to)
}

fn range_from_klines(klines: &[Kline]) -> Option<MarketDataRange> {
    let from = klines.first()?.time;
    let to = klines.last()?.time.saturating_add(1);
    MarketDataRange::new(from, to)
}

fn range_from_oi(rows: &[OpenInterest]) -> Option<MarketDataRange> {
    let from = rows.first()?.time;
    let to = rows.last()?.time.saturating_add(1);
    MarketDataRange::new(from, to)
}
