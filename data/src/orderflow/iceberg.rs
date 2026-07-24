//! Deterministic L2 replenishment/absorption detector.
//!
//! Scores are explainable heuristics, not calibrated probabilities. All time is supplied by
//! normalized events so the same recording always produces the same output.

use exchange::{
    TickerInfo, UnixMs,
    adapter::Exchange,
    orderflow::{
        AggressorSide, BookContinuity, BookDeltaEvent, BookLevelDelta, NormalizedTradeEvent,
        OrderFlowDataQuality, OrderFlowEvent, PassiveSide,
    },
    unit::{Price, PriceStep, Qty},
};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

const ROLLING_WINDOW_MS: u64 = 60_000;
const MAX_REORDER_EVENTS: usize = 8_192;
const MAX_SAMPLES: usize = 4_096;
const MAX_EPISODES: usize = 128;
const MAX_EPISODE_SAMPLES: usize = 16;
const TRADE_DEDUP_WINDOW: usize = 16_384;
const DEPTH_DEDUP_WINDOW: usize = 4_096;
const WARMUP_TRADES: usize = 12;
const WARMUP_TOUCH_DEPTHS: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct IcebergDetectorConfig {
    pub enabled: bool,
    pub max_distance_from_touch_ticks: u32,
    pub reorder_window_ms: u32,
    pub episode_idle_timeout_ms: u32,
    pub episode_max_duration_ms: u32,
    pub minimum_refill_count: u32,
    pub minimum_executed_to_displayed: f64,
    pub minimum_refill_ratio: f64,
    pub maximum_adverse_ticks: u32,
    pub minimum_score: u8,
    pub show_weak_candidates: bool,
    pub retention_seconds: u32,
    pub recorder_enabled: bool,
}

impl Default for IcebergDetectorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_distance_from_touch_ticks: 2,
            reorder_window_ms: 150,
            episode_idle_timeout_ms: 5_000,
            episode_max_duration_ms: 30_000,
            minimum_refill_count: 3,
            minimum_executed_to_displayed: 2.5,
            minimum_refill_ratio: 0.50,
            maximum_adverse_ticks: 1,
            minimum_score: 70,
            show_weak_candidates: false,
            retention_seconds: 300,
            recorder_enabled: false,
        }
    }
}

impl IcebergDetectorConfig {
    pub fn normalized(mut self) -> Self {
        self.reorder_window_ms = self.reorder_window_ms.clamp(50, 500);
        self.minimum_score = self.minimum_score.min(100);
        self.retention_seconds = self.retention_seconds.max(1);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IcebergEventId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcebergSide {
    PossibleBuy,
    PossibleSell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IcebergConfidence {
    Weak,
    Candidate,
    Strong,
    VeryStrong,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergEpisodeState {
    Tracking,
    Confirmed,
    Completed,
    Invalidated,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergEvidence {
    pub cancellation_ratio: f64,
    pub clip_cv: Option<f64>,
    pub p75_refill_latency_ms: Option<u32>,
    pub persistence_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergEvent {
    pub id: IcebergEventId,
    pub ticker_info: TickerInfo,
    pub side: IcebergSide,
    pub price: Price,
    pub started_at: UnixMs,
    pub confirmed_at: UnixMs,
    pub last_updated_at: UnixMs,
    pub score: u8,
    pub confidence: IcebergConfidence,
    pub aggressive_executed_qty: Qty,
    pub peak_displayed_qty: Qty,
    pub replenished_qty: Qty,
    pub cancelled_qty: Qty,
    pub executed_to_displayed: f64,
    pub refill_ratio: f64,
    pub refill_count: u32,
    pub median_refill_latency_ms: Option<u32>,
    pub maximum_adverse_ticks: u32,
    /// Minimum quantity absorbed beyond the largest displayed clip; never an estimate.
    pub hidden_lower_bound_qty: Qty,
    pub data_quality: OrderFlowDataQuality,
    pub evidence: IcebergEvidence,
}

#[derive(Debug, Clone)]
pub struct IcebergEpisode {
    pub side: PassiveSide,
    pub price: Price,
    pub started_at: UnixMs,
    pub last_event_at: UnixMs,
    pub last_hit_at: Option<UnixMs>,
    pub initial_visible_qty: Qty,
    pub peak_visible_qty: Qty,
    pub minimum_visible_qty: Qty,
    pub aggressive_executed_qty: Qty,
    pub replenished_qty: Qty,
    pub cancelled_qty: Qty,
    pub refill_count: u32,
    pub meaningful_hit_count: u32,
    pub refill_latencies_ms: VecDeque<u32>,
    pub displayed_clips: VecDeque<Qty>,
    pub best_price_before: Option<Price>,
    pub maximum_adverse_ticks: u32,
    pub data_quality: OrderFlowDataQuality,
    pub state: IcebergEpisodeState,
    id: IcebergEventId,
    confirmed_at: Option<UnixMs>,
}

#[derive(Debug, Clone, Copy, Default)]
struct LevelState {
    visible: Qty,
    pending_executed: Qty,
    expected_after: Qty,
    last_trade_time: Option<UnixMs>,
}

#[derive(Debug, Clone)]
struct TimedSample {
    at: UnixMs,
    value: Qty,
}

#[derive(Debug, Default)]
struct RollingBaselines {
    trades: VecDeque<TimedSample>,
    touch_depths: VecDeque<TimedSample>,
}

impl RollingBaselines {
    fn add_trade(&mut self, at: UnixMs, qty: Qty) {
        push_sample(&mut self.trades, TimedSample { at, value: qty });
        self.expire(at);
    }

    fn add_touch(&mut self, at: UnixMs, qty: Qty) {
        push_sample(&mut self.touch_depths, TimedSample { at, value: qty });
        self.expire(at);
    }

    fn expire(&mut self, now: UnixMs) {
        expire_samples(&mut self.trades, now);
        expire_samples(&mut self.touch_depths, now);
    }

    fn warmed_up(&self) -> bool {
        self.trades.len() >= WARMUP_TRADES && self.touch_depths.len() >= WARMUP_TOUCH_DEPTHS
    }

    fn minimum_executed(&self, ticker: TickerInfo) -> Qty {
        let p75_trade = percentile_qty(&self.trades, 0.75).unwrap_or(Qty::ZERO);
        let median_touch = percentile_qty(&self.touch_depths, 0.5).unwrap_or(Qty::ZERO);
        let min_lots = Qty::from_f64(ticker.min_qty.as_f64() * 5.0);
        min_lots
            .max(p75_trade)
            .max(Qty::from_units(median_touch.units / 4))
    }
}

fn push_sample(samples: &mut VecDeque<TimedSample>, sample: TimedSample) {
    if samples.len() == MAX_SAMPLES {
        samples.pop_front();
    }
    samples.push_back(sample);
}

fn expire_samples(samples: &mut VecDeque<TimedSample>, now: UnixMs) {
    while samples
        .front()
        .is_some_and(|sample| now.as_u64().saturating_sub(sample.at.as_u64()) > ROLLING_WINDOW_MS)
    {
        samples.pop_front();
    }
}

fn percentile_qty(samples: &VecDeque<TimedSample>, quantile: f64) -> Option<Qty> {
    let mut values: Vec<i64> = samples.iter().map(|sample| sample.value.units).collect();
    values.sort_unstable();
    let index = ((values.len().saturating_sub(1)) as f64 * quantile).round() as usize;
    values.get(index).copied().map(Qty::from_units)
}

#[derive(Debug, Clone)]
struct BufferedEvent {
    event: OrderFlowEvent,
    exchange_time: UnixMs,
    receive_time: UnixMs,
    type_order: u8,
    sequence: u64,
}

/// Pure, per-(exchange,ticker,market,tick-size) detector.
pub struct BinanceIcebergDetector {
    ticker_info: TickerInfo,
    tick_size: PriceStep,
    config: IcebergDetectorConfig,
    quality: OrderFlowDataQuality,
    buffer: Vec<BufferedEvent>,
    latest_time: UnixMs,
    levels: FxHashMap<(PassiveSide, Price), LevelState>,
    episodes: FxHashMap<(PassiveSide, Price), IcebergEpisode>,
    events: VecDeque<IcebergEvent>,
    baselines: RollingBaselines,
    seen_trades: FxHashSet<u64>,
    trade_order: VecDeque<u64>,
    seen_depth: FxHashSet<u64>,
    depth_order: VecDeque<u64>,
    best_bid: Option<Price>,
    best_ask: Option<Price>,
    next_id: u64,
    warmup_logged: bool,
}

impl BinanceIcebergDetector {
    pub fn new(
        ticker_info: TickerInfo,
        tick_size: PriceStep,
        config: IcebergDetectorConfig,
    ) -> Result<Self, &'static str> {
        if ticker_info.exchange() != Exchange::BinanceLinear {
            return Err("Not supported for this market");
        }
        Ok(Self {
            ticker_info,
            tick_size,
            config: config.normalized(),
            quality: OrderFlowDataQuality::Synchronizing,
            buffer: Vec::new(),
            latest_time: UnixMs::new(0),
            levels: FxHashMap::default(),
            episodes: FxHashMap::default(),
            events: VecDeque::new(),
            baselines: RollingBaselines::default(),
            seen_trades: FxHashSet::default(),
            trade_order: VecDeque::new(),
            seen_depth: FxHashSet::default(),
            depth_order: VecDeque::new(),
            best_bid: None,
            best_ask: None,
            next_id: 1,
            warmup_logged: false,
        })
    }

    pub fn config(&self) -> IcebergDetectorConfig {
        self.config
    }

    pub fn is_warming_up(&self) -> bool {
        !self.baselines.warmed_up()
    }

    pub fn episode_count(&self) -> usize {
        self.episodes.len()
    }

    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }

    pub fn retained_events(&self) -> &VecDeque<IcebergEvent> {
        &self.events
    }

    pub fn ingest(&mut self, event: OrderFlowEvent) -> Vec<IcebergEvent> {
        if event.ticker_info() != self.ticker_info || !self.config.enabled {
            return Vec::new();
        }

        if matches!(event, OrderFlowEvent::Reconnect { .. })
            || matches!(
                event,
                OrderFlowEvent::Quality {
                    quality: OrderFlowDataQuality::Gap,
                    ..
                }
            )
        {
            self.invalidate_all(if matches!(event, OrderFlowEvent::Reconnect { .. }) {
                OrderFlowDataQuality::Degraded
            } else {
                OrderFlowDataQuality::Gap
            });
            return Vec::new();
        }

        let buffered = buffered_event(event);
        self.latest_time = self.latest_time.max(buffered.exchange_time);
        if self.buffer.len() == MAX_REORDER_EVENTS {
            self.buffer.sort_by(buffer_cmp);
            let oldest = self.buffer.remove(0);
            self.process(oldest.event);
            self.quality = OrderFlowDataQuality::Degraded;
        }
        self.buffer.push(buffered);
        self.drain_watermark(false)
    }

    pub fn flush(&mut self) -> Vec<IcebergEvent> {
        self.drain_watermark(true)
    }

    fn drain_watermark(&mut self, all: bool) -> Vec<IcebergEvent> {
        self.buffer.sort_by(buffer_cmp);
        let watermark = self
            .latest_time
            .as_u64()
            .saturating_sub(u64::from(self.config.reorder_window_ms));
        let split = if all {
            self.buffer.len()
        } else {
            self.buffer
                .partition_point(|item| item.exchange_time.as_u64() <= watermark)
        };
        let ready: Vec<_> = self.buffer.drain(..split).collect();
        let mut output = Vec::new();
        for item in ready {
            output.extend(self.process(item.event));
        }
        output
    }

    fn process(&mut self, event: OrderFlowEvent) -> Vec<IcebergEvent> {
        let now = match &event {
            OrderFlowEvent::Trade(e) => e.trade_time,
            OrderFlowEvent::BookDelta(e) => e.transaction_time.unwrap_or(e.exchange_time),
            OrderFlowEvent::Quality { at, .. } | OrderFlowEvent::Reconnect { at, .. } => *at,
        };
        self.expire(now);
        match event {
            OrderFlowEvent::Trade(trade) => self.process_trade(trade),
            OrderFlowEvent::BookDelta(delta) => self.process_depth(delta),
            OrderFlowEvent::Quality { quality, .. } => {
                self.quality = quality;
                if quality != OrderFlowDataQuality::Healthy {
                    self.episodes.clear();
                }
                Vec::new()
            }
            OrderFlowEvent::Reconnect { .. } => Vec::new(),
        }
    }

    fn process_trade(&mut self, trade: NormalizedTradeEvent) -> Vec<IcebergEvent> {
        if !dedup(
            trade.trade_id,
            &mut self.seen_trades,
            &mut self.trade_order,
            TRADE_DEDUP_WINDOW,
        ) {
            return Vec::new();
        }
        self.baselines.add_trade(trade.trade_time, trade.quantity);
        if self.quality != OrderFlowDataQuality::Healthy || !self.baselines.warmed_up() {
            return Vec::new();
        }
        if !self.warmup_logged {
            log::info!(
                "IcebergWarmupCompleted | ticker={}",
                self.ticker_info.ticker
            );
            self.warmup_logged = true;
        }

        let side = match trade.aggressor {
            AggressorSide::Sell => PassiveSide::Bid,
            AggressorSide::Buy => PassiveSide::Ask,
        };
        let key = (side, trade.price);
        let Some(level) = self.levels.get_mut(&key) else {
            return Vec::new();
        };
        if level.visible <= Qty::ZERO
            || !within_touch(
                side,
                trade.price,
                self.best_bid,
                self.best_ask,
                self.tick_size,
                self.config.max_distance_from_touch_ticks,
            )
        {
            return Vec::new();
        }

        let minimum = self.baselines.minimum_executed(self.ticker_info);
        if trade.quantity < minimum && !self.episodes.contains_key(&key) {
            return Vec::new();
        }
        let visible_before = level.expected_after.max(Qty::ZERO);
        level.pending_executed += trade.quantity;
        level.expected_after = (visible_before - trade.quantity).max(Qty::ZERO);
        level.last_trade_time = Some(trade.trade_time);

        if !self.episodes.contains_key(&key) {
            if self.episodes.len() >= MAX_EPISODES {
                return Vec::new();
            }
            let id = IcebergEventId(self.next_id);
            self.next_id = self.next_id.wrapping_add(1);
            self.episodes.insert(
                key,
                IcebergEpisode {
                    side,
                    price: trade.price,
                    started_at: trade.trade_time,
                    last_event_at: trade.trade_time,
                    last_hit_at: Some(trade.trade_time),
                    initial_visible_qty: level.visible,
                    peak_visible_qty: level.visible,
                    minimum_visible_qty: level.visible,
                    aggressive_executed_qty: Qty::ZERO,
                    replenished_qty: Qty::ZERO,
                    cancelled_qty: Qty::ZERO,
                    refill_count: 0,
                    meaningful_hit_count: 0,
                    refill_latencies_ms: VecDeque::new(),
                    displayed_clips: VecDeque::new(),
                    best_price_before: match side {
                        PassiveSide::Bid => self.best_bid,
                        PassiveSide::Ask => self.best_ask,
                    },
                    maximum_adverse_ticks: 0,
                    data_quality: self.quality,
                    state: IcebergEpisodeState::Tracking,
                    id,
                    confirmed_at: None,
                },
            );
            log::debug!(
                "IcebergEpisodeOpened | ticker={} side={side:?} price={:?}",
                self.ticker_info.ticker,
                trade.price
            );
        }
        let episode = self.episodes.get_mut(&key).expect("episode inserted");
        episode.aggressive_executed_qty += trade.quantity;
        episode.meaningful_hit_count += 1;
        episode.last_hit_at = Some(trade.trade_time);
        episode.last_event_at = trade.trade_time;
        Vec::new()
    }

    fn process_depth(&mut self, delta: BookDeltaEvent) -> Vec<IcebergEvent> {
        if !dedup(
            delta.final_update_id,
            &mut self.seen_depth,
            &mut self.depth_order,
            DEPTH_DEDUP_WINDOW,
        ) {
            return Vec::new();
        }
        if delta.continuity == BookContinuity::GapDetected {
            self.invalidate_all(OrderFlowDataQuality::Gap);
            return Vec::new();
        }
        if delta.continuity == BookContinuity::SnapshotBoundary {
            self.levels.clear();
            self.episodes.clear();
            self.quality = OrderFlowDataQuality::Synchronizing;
            log::info!(
                "IcebergBookResync | ticker={} final_update_id={}",
                self.ticker_info.ticker,
                delta.final_update_id
            );
        }

        let at = delta.transaction_time.unwrap_or(delta.exchange_time);
        for level in &delta.bids {
            self.apply_level(PassiveSide::Bid, *level, at);
        }
        for level in &delta.asks {
            self.apply_level(PassiveSide::Ask, *level, at);
        }
        self.recompute_touch();
        if let Some(best) = self
            .best_bid
            .and_then(|price| self.levels.get(&(PassiveSide::Bid, price)))
        {
            self.baselines.add_touch(at, best.visible);
        }
        if let Some(best) = self
            .best_ask
            .and_then(|price| self.levels.get(&(PassiveSide::Ask, price)))
        {
            self.baselines.add_touch(at, best.visible);
        }
        if delta.continuity == BookContinuity::SnapshotBoundary {
            self.quality = OrderFlowDataQuality::Healthy;
        } else if self.quality == OrderFlowDataQuality::Degraded {
            // The depth sequence stayed coherent while the raw-trade socket reconnected.
            // Warm-up was reset, so the missing trade interval cannot immediately signal.
            self.quality = OrderFlowDataQuality::Healthy;
        }
        self.update_adverse_ticks();

        let keys: Vec<_> = self.episodes.keys().copied().collect();
        keys.into_iter()
            .filter_map(|key| self.publish_if_visible(key, at))
            .collect()
    }

    fn apply_level(&mut self, side: PassiveSide, delta: BookLevelDelta, at: UnixMs) {
        let key = (side, delta.price);
        let state = self.levels.entry(key).or_default();
        let previous = state.visible;
        let pending = state.pending_executed;
        let expected_trade_depletion = previous.min(pending);
        let expected_after = (previous - pending).max(Qty::ZERO);
        let replenished = (delta.current_qty - expected_after).max(Qty::ZERO);
        let unexplained = (previous - expected_trade_depletion - delta.current_qty).max(Qty::ZERO);

        if let Some(episode) = self.episodes.get_mut(&key) {
            episode.last_event_at = at;
            episode.minimum_visible_qty = episode.minimum_visible_qty.min(delta.current_qty);
            episode.peak_visible_qty = episode.peak_visible_qty.max(delta.current_qty);
            episode.cancelled_qty += unexplained;
            let refill_threshold =
                Qty::from_units(self.baselines.minimum_executed(self.ticker_info).units / 4);
            if pending > Qty::ZERO
                && replenished
                    >= refill_threshold.max(Qty::from_f64(self.ticker_info.min_qty.as_f64() * 5.0))
            {
                episode.replenished_qty += replenished;
                episode.refill_count += 1;
                push_bounded(&mut episode.displayed_clips, delta.current_qty);
                if let Some(hit_at) = state.last_trade_time {
                    let latency = at.saturating_diff(hit_at).min(u64::from(u32::MAX)) as u32;
                    push_bounded(&mut episode.refill_latencies_ms, latency);
                }
            }
        }
        state.visible = delta.current_qty;
        state.pending_executed = Qty::ZERO;
        state.expected_after = delta.current_qty;
        if delta.current_qty == Qty::ZERO && !self.episodes.contains_key(&key) {
            self.levels.remove(&key);
        }
    }

    fn recompute_touch(&mut self) {
        self.best_bid = self
            .levels
            .iter()
            .filter(|((side, _), state)| *side == PassiveSide::Bid && state.visible > Qty::ZERO)
            .map(|((_, price), _)| *price)
            .max();
        self.best_ask = self
            .levels
            .iter()
            .filter(|((side, _), state)| *side == PassiveSide::Ask && state.visible > Qty::ZERO)
            .map(|((_, price), _)| *price)
            .min();
    }

    fn update_adverse_ticks(&mut self) {
        for episode in self.episodes.values_mut() {
            let adverse = match episode.side {
                PassiveSide::Bid => self
                    .best_bid
                    .filter(|best| *best < episode.price)
                    .map(|best| ticks_between(best, episode.price, self.tick_size)),
                PassiveSide::Ask => self
                    .best_ask
                    .filter(|best| *best > episode.price)
                    .map(|best| ticks_between(episode.price, best, self.tick_size)),
            }
            .unwrap_or(0);
            episode.maximum_adverse_ticks = episode.maximum_adverse_ticks.max(adverse);
            if adverse > self.config.maximum_adverse_ticks {
                episode.state = if episode.confirmed_at.is_some() {
                    IcebergEpisodeState::Completed
                } else {
                    IcebergEpisodeState::Invalidated
                };
                match episode.state {
                    IcebergEpisodeState::Completed => log::info!(
                        "IcebergEpisodeCompleted | ticker={} side={:?} price={:?} adverse_ticks={adverse}",
                        self.ticker_info.ticker,
                        episode.side,
                        episode.price
                    ),
                    IcebergEpisodeState::Invalidated => log::debug!(
                        "IcebergEpisodeInvalidated | ticker={} side={:?} price={:?} reason=price_broken",
                        self.ticker_info.ticker,
                        episode.side,
                        episode.price
                    ),
                    _ => {}
                }
            }
        }
        self.episodes.retain(|_, episode| {
            matches!(
                episode.state,
                IcebergEpisodeState::Tracking | IcebergEpisodeState::Confirmed
            )
        });
    }

    fn publish_if_visible(
        &mut self,
        key: (PassiveSide, Price),
        now: UnixMs,
    ) -> Option<IcebergEvent> {
        let episode = self.episodes.get_mut(&key)?;
        let score = score_episode(episode, now);
        let executed_ratio = ratio(episode.aggressive_executed_qty, episode.peak_visible_qty);
        let refill_ratio = ratio(episode.replenished_qty, episode.aggressive_executed_qty);
        let passes_evidence = episode.refill_count >= self.config.minimum_refill_count
            && executed_ratio >= self.config.minimum_executed_to_displayed
            && refill_ratio >= self.config.minimum_refill_ratio
            && episode.maximum_adverse_ticks <= self.config.maximum_adverse_ticks;
        let visible = passes_evidence && score >= self.config.minimum_score
            || self.config.show_weak_candidates && score >= 50;
        if !visible || self.quality != OrderFlowDataQuality::Healthy {
            return None;
        }
        let confirmed_at = *episode.confirmed_at.get_or_insert(now);
        episode.state = IcebergEpisodeState::Confirmed;
        let event = build_event(self.ticker_info, episode, confirmed_at, now, score);
        if let Some(existing) = self
            .events
            .iter_mut()
            .find(|existing| existing.id == event.id)
        {
            *existing = event.clone();
        } else {
            log::info!(
                "IcebergEpisodeConfirmed | exchange={} ticker={} side={:?} price={:?} score={} refill_count={} executed_displayed={:.2} refill_ratio={:.2} data_quality={:?}",
                self.ticker_info.exchange(),
                self.ticker_info.ticker,
                event.side,
                event.price,
                event.score,
                event.refill_count,
                event.executed_to_displayed,
                event.refill_ratio,
                event.data_quality
            );
            self.events.push_back(event.clone());
        }
        Some(event)
    }

    fn expire(&mut self, now: UnixMs) {
        let idle = u64::from(self.config.episode_idle_timeout_ms);
        let maximum = u64::from(self.config.episode_max_duration_ms);
        self.episodes.retain(|_, episode| {
            now.saturating_diff(episode.last_event_at) <= idle
                && now.saturating_diff(episode.started_at) <= maximum
        });
        let retention = u64::from(self.config.retention_seconds) * 1_000;
        while self
            .events
            .front()
            .is_some_and(|event| now.saturating_diff(event.last_updated_at) > retention)
        {
            self.events.pop_front();
        }
    }

    fn invalidate_all(&mut self, quality: OrderFlowDataQuality) {
        self.buffer.clear();
        self.episodes.clear();
        self.levels.clear();
        self.best_bid = None;
        self.best_ask = None;
        self.quality = quality;
        self.baselines = RollingBaselines::default();
        self.warmup_logged = false;
        if quality == OrderFlowDataQuality::Gap {
            log::warn!(
                "IcebergBookGap | ticker={} quality={quality:?}",
                self.ticker_info.ticker
            );
        } else {
            log::debug!(
                "IcebergEpisodeInvalidated | ticker={} reason=reconnect_or_resync",
                self.ticker_info.ticker
            );
        }
    }
}

fn buffered_event(event: OrderFlowEvent) -> BufferedEvent {
    match &event {
        OrderFlowEvent::Trade(trade) => BufferedEvent {
            exchange_time: trade.trade_time,
            receive_time: trade.receive_time,
            type_order: 0,
            sequence: trade.trade_id,
            event,
        },
        OrderFlowEvent::BookDelta(delta) => BufferedEvent {
            exchange_time: delta.transaction_time.unwrap_or(delta.exchange_time),
            receive_time: delta.receive_time,
            type_order: 1,
            sequence: delta.final_update_id,
            event,
        },
        OrderFlowEvent::Quality { at, .. } => BufferedEvent {
            exchange_time: *at,
            receive_time: *at,
            type_order: 2,
            sequence: 0,
            event,
        },
        OrderFlowEvent::Reconnect { at, .. } => BufferedEvent {
            exchange_time: *at,
            receive_time: *at,
            type_order: 3,
            sequence: 0,
            event,
        },
    }
}

fn buffer_cmp(a: &BufferedEvent, b: &BufferedEvent) -> std::cmp::Ordering {
    (a.exchange_time, a.receive_time, a.type_order, a.sequence).cmp(&(
        b.exchange_time,
        b.receive_time,
        b.type_order,
        b.sequence,
    ))
}

fn dedup(id: u64, seen: &mut FxHashSet<u64>, order: &mut VecDeque<u64>, limit: usize) -> bool {
    if !seen.insert(id) {
        return false;
    }
    order.push_back(id);
    if order.len() > limit
        && let Some(old) = order.pop_front()
    {
        seen.remove(&old);
    }
    true
}

fn within_touch(
    side: PassiveSide,
    price: Price,
    bid: Option<Price>,
    ask: Option<Price>,
    step: PriceStep,
    max_ticks: u32,
) -> bool {
    let touch = match side {
        PassiveSide::Bid => bid,
        PassiveSide::Ask => ask,
    };
    touch.is_some_and(|touch| ticks_between(price.min(touch), price.max(touch), step) <= max_ticks)
}

fn ticks_between(low: Price, high: Price, step: PriceStep) -> u32 {
    if step.units <= 0 {
        return u32::MAX;
    }
    u32::try_from((high.units - low.units).max(0) / step.units).unwrap_or(u32::MAX)
}

fn ratio(numerator: Qty, denominator: Qty) -> f64 {
    numerator.to_f64() / denominator.to_f64().max(f64::EPSILON)
}

fn push_bounded<T>(values: &mut VecDeque<T>, value: T) {
    if values.len() == MAX_EPISODE_SAMPLES {
        values.pop_front();
    }
    values.push_back(value);
}

fn percentile_u32(values: &VecDeque<u32>, quantile: f64) -> Option<u32> {
    let mut values: Vec<_> = values.iter().copied().collect();
    values.sort_unstable();
    let index = ((values.len().saturating_sub(1)) as f64 * quantile).round() as usize;
    values.get(index).copied()
}

fn clip_cv(values: &VecDeque<Qty>) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().map(|q| q.to_f64()).sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|q| (q.to_f64() - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64;
    Some(variance.sqrt() / mean.max(f64::EPSILON))
}

fn score_episode(episode: &IcebergEpisode, now: UnixMs) -> u8 {
    let executed = ratio(episode.aggressive_executed_qty, episode.peak_visible_qty);
    let refill = ratio(episode.replenished_qty, episode.aggressive_executed_qty);
    let latency = percentile_u32(&episode.refill_latencies_ms, 0.5);
    let consistency = clip_cv(&episode.displayed_clips);
    let cancellation = ratio(
        episode.cancelled_qty,
        episode.initial_visible_qty + episode.replenished_qty,
    );
    let mut score = 0.0;
    score += (executed / 4.0).clamp(0.0, 1.0) * 22.0;
    score += refill.clamp(0.0, 1.0) * 18.0;
    score += (episode.refill_count as f64 / 5.0).clamp(0.0, 1.0) * 18.0;
    score += latency.map_or(0.0, |ms| {
        (1.0 - f64::from(ms) / 1_000.0).clamp(0.0, 1.0) * 12.0
    });
    score += consistency.map_or(0.0, |cv| (1.0 - cv).clamp(0.0, 1.0) * 10.0);
    score += (1.0 - episode.maximum_adverse_ticks as f64 / 3.0).clamp(0.0, 1.0) * 15.0;
    score += (now.saturating_diff(episode.started_at) as f64 / 5_000.0).clamp(0.0, 1.0) * 5.0;
    score -= cancellation.clamp(0.0, 1.0) * 20.0;
    score -= match episode.data_quality {
        OrderFlowDataQuality::Healthy => 0.0,
        OrderFlowDataQuality::Degraded => 10.0,
        OrderFlowDataQuality::Synchronizing => 30.0,
        OrderFlowDataQuality::Gap => 30.0,
    };
    score.round().clamp(0.0, 100.0) as u8
}

fn build_event(
    ticker_info: TickerInfo,
    episode: &IcebergEpisode,
    confirmed_at: UnixMs,
    now: UnixMs,
    score: u8,
) -> IcebergEvent {
    let hidden = (episode.aggressive_executed_qty - episode.peak_visible_qty).max(Qty::ZERO);
    IcebergEvent {
        id: episode.id,
        ticker_info,
        side: match episode.side {
            PassiveSide::Bid => IcebergSide::PossibleBuy,
            PassiveSide::Ask => IcebergSide::PossibleSell,
        },
        price: episode.price,
        started_at: episode.started_at,
        confirmed_at,
        last_updated_at: now,
        score,
        confidence: match score {
            0..=59 => IcebergConfidence::Weak,
            60..=69 => IcebergConfidence::Candidate,
            70..=84 => IcebergConfidence::Strong,
            _ => IcebergConfidence::VeryStrong,
        },
        aggressive_executed_qty: episode.aggressive_executed_qty,
        peak_displayed_qty: episode.peak_visible_qty,
        replenished_qty: episode.replenished_qty,
        cancelled_qty: episode.cancelled_qty,
        executed_to_displayed: ratio(episode.aggressive_executed_qty, episode.peak_visible_qty),
        refill_ratio: ratio(episode.replenished_qty, episode.aggressive_executed_qty),
        refill_count: episode.refill_count,
        median_refill_latency_ms: percentile_u32(&episode.refill_latencies_ms, 0.5),
        maximum_adverse_ticks: episode.maximum_adverse_ticks,
        hidden_lower_bound_qty: hidden,
        data_quality: episode.data_quality,
        evidence: IcebergEvidence {
            cancellation_ratio: ratio(
                episode.cancelled_qty,
                episode.initial_visible_qty + episode.replenished_qty,
            ),
            clip_cv: clip_cv(&episode.displayed_clips),
            p75_refill_latency_ms: percentile_u32(&episode.refill_latencies_ms, 0.75),
            persistence_ms: now.saturating_diff(episode.started_at),
        },
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "record", content = "payload", rename_all = "snake_case")]
pub enum OrderFlowRecord {
    SessionMetadata {
        version: u32,
        ticker_info: TickerInfo,
        started_at: UnixMs,
    },
    SnapshotBoundary(BookDeltaEvent),
    DepthDelta(BookDeltaEvent),
    Trade(NormalizedTradeEvent),
    Gap {
        ticker_info: TickerInfo,
        at: UnixMs,
    },
    Reconnect {
        ticker_info: TickerInfo,
        at: UnixMs,
    },
    DetectorOutput(IcebergEvent),
}

impl OrderFlowRecord {
    pub fn from_input(event: &OrderFlowEvent) -> Option<Self> {
        match event {
            OrderFlowEvent::Trade(value) => Some(Self::Trade(*value)),
            OrderFlowEvent::BookDelta(value)
                if value.continuity == BookContinuity::SnapshotBoundary =>
            {
                Some(Self::SnapshotBoundary(value.clone()))
            }
            OrderFlowEvent::BookDelta(value) => Some(Self::DepthDelta(value.clone())),
            OrderFlowEvent::Quality {
                ticker_info,
                at,
                quality: OrderFlowDataQuality::Gap,
            } => Some(Self::Gap {
                ticker_info: *ticker_info,
                at: *at,
            }),
            OrderFlowEvent::Reconnect { ticker_info, at } => Some(Self::Reconnect {
                ticker_info: *ticker_info,
                at: *at,
            }),
            OrderFlowEvent::Quality { .. } => None,
        }
    }

    fn into_input(self) -> Option<OrderFlowEvent> {
        match self {
            Self::SnapshotBoundary(value) | Self::DepthDelta(value) => {
                Some(OrderFlowEvent::BookDelta(value))
            }
            Self::Trade(value) => Some(OrderFlowEvent::Trade(value)),
            Self::Gap { ticker_info, at } => Some(OrderFlowEvent::Quality {
                ticker_info,
                at,
                quality: OrderFlowDataQuality::Gap,
            }),
            Self::Reconnect { ticker_info, at } => {
                Some(OrderFlowEvent::Reconnect { ticker_info, at })
            }
            Self::SessionMetadata { .. } | Self::DetectorOutput(_) => None,
        }
    }
}

pub struct OrderFlowRecorder {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl OrderFlowRecorder {
    pub fn create(
        logs_root: &Path,
        ticker_info: TickerInfo,
        started_at: UnixMs,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(logs_root)?;
        let timestamp = chrono::DateTime::from_timestamp_millis(started_at.as_u64() as i64)
            .unwrap_or_default()
            .format("%Y%m%d_%H%M%S");
        let filename = format!(
            "binance_{}_{}.jsonl",
            ticker_info.ticker.to_string().to_lowercase(),
            timestamp
        );
        let path = logs_root.join(filename);
        let mut recorder = Self {
            writer: BufWriter::new(File::create(&path)?),
            path,
        };
        recorder.write(&OrderFlowRecord::SessionMetadata {
            version: 1,
            ticker_info,
            started_at,
        })?;
        Ok(recorder)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn write(&mut self, record: &OrderFlowRecord) -> std::io::Result<()> {
        serde_json::to_writer(&mut self.writer, record).map_err(std::io::Error::other)?;
        self.writer.write_all(b"\n")
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

pub fn replay_jsonl(
    path: &Path,
    detector: &mut BinanceIcebergDetector,
) -> Result<Vec<IcebergEvent>, Box<dyn std::error::Error>> {
    log::info!("IcebergReplayStarted | path={}", path.display());
    let mut output = Vec::new();
    for line in BufReader::new(File::open(path)?).lines() {
        let record: OrderFlowRecord = serde_json::from_str(&line?)?;
        if let Some(input) = record.into_input() {
            output.extend(detector.ingest(input));
        }
    }
    output.extend(detector.flush());
    log::info!(
        "IcebergReplayCompleted | path={} events={}",
        path.display(),
        output.len()
    );
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange::{Ticker, adapter::Exchange};

    fn ticker() -> TickerInfo {
        TickerInfo::new(
            Ticker::new("BTCUSDT", Exchange::BinanceLinear),
            0.1,
            0.001,
            None,
        )
    }
    fn config() -> IcebergDetectorConfig {
        IcebergDetectorConfig {
            enabled: true,
            reorder_window_ms: 50,
            minimum_score: 0,
            minimum_executed_to_displayed: 0.0,
            minimum_refill_ratio: 0.0,
            ..Default::default()
        }
    }
    fn delta(id: u64, at: u64, bid: f64, ask: f64, continuity: BookContinuity) -> OrderFlowEvent {
        OrderFlowEvent::BookDelta(BookDeltaEvent {
            ticker_info: ticker(),
            exchange_time: at.into(),
            transaction_time: Some(at.into()),
            receive_time: at.into(),
            first_update_id: id,
            final_update_id: id,
            previous_final_update_id: id.checked_sub(1),
            bids: vec![BookLevelDelta {
                price: Price::from_f64(100.0),
                previous_qty: Qty::ZERO,
                current_qty: Qty::from_f64(bid),
            }]
            .into_boxed_slice(),
            asks: vec![BookLevelDelta {
                price: Price::from_f64(100.1),
                previous_qty: Qty::ZERO,
                current_qty: Qty::from_f64(ask),
            }]
            .into_boxed_slice(),
            continuity,
        })
    }
    fn trade(id: u64, at: u64, price: f64, qty: f64, aggressor: AggressorSide) -> OrderFlowEvent {
        OrderFlowEvent::Trade(NormalizedTradeEvent {
            ticker_info: ticker(),
            event_time: at.into(),
            trade_time: at.into(),
            receive_time: at.into(),
            trade_id: id,
            price: Price::from_f64(price),
            quantity: Qty::from_f64(qty),
            aggressor,
        })
    }
    fn warmed() -> BinanceIcebergDetector {
        let mut detector =
            BinanceIcebergDetector::new(ticker(), PriceStep::from(ticker().min_ticksize), config())
                .unwrap();
        detector.ingest(delta(1, 1_000, 1.0, 1.0, BookContinuity::SnapshotBoundary));
        for i in 0..12 {
            detector.ingest(trade(10 + i, 1_010 + i, 99.0, 0.01, AggressorSide::Sell));
            detector.ingest(delta(
                2 + i,
                1_020 + i,
                1.0,
                1.0,
                BookContinuity::Continuous,
            ));
        }
        detector.flush();
        detector
    }

    #[test]
    fn duplicate_trade_is_ignored_and_no_refill_means_no_event() {
        let mut d = warmed();
        d.ingest(trade(100, 2_000, 100.0, 1.0, AggressorSide::Sell));
        d.ingest(trade(100, 2_001, 100.0, 1.0, AggressorSide::Sell));
        assert!(d.flush().is_empty());
        assert_eq!(
            d.episodes
                .get(&(PassiveSide::Bid, Price::from_f64(100.0)))
                .unwrap()
                .meaningful_hit_count,
            1
        );
    }

    #[test]
    fn three_refills_confirm_one_stable_buy_event() {
        let mut d = warmed();
        let mut out = Vec::new();
        for i in 0..3 {
            out.extend(d.ingest(trade(
                100 + i,
                2_000 + i * 100,
                100.0,
                1.0,
                AggressorSide::Sell,
            )));
            out.extend(d.ingest(delta(
                100 + i,
                2_020 + i * 100,
                1.0,
                1.0,
                BookContinuity::Continuous,
            )));
        }
        out.extend(d.flush());
        assert!(!out.is_empty());
        assert!(
            out.iter()
                .all(|event| event.side == IcebergSide::PossibleBuy)
        );
        assert_eq!(
            out.iter()
                .map(|event| event.id)
                .collect::<FxHashSet<_>>()
                .len(),
            1
        );
        assert!(out.last().unwrap().hidden_lower_bound_qty >= Qty::ZERO);
    }

    #[test]
    fn three_refills_confirm_sell_side_and_multiple_hits_are_not_double_counted() {
        let mut d = warmed();
        for i in 0..3 {
            d.ingest(trade(
                200 + i * 2,
                3_000 + i * 100,
                100.1,
                0.5,
                AggressorSide::Buy,
            ));
            d.ingest(trade(
                201 + i * 2,
                3_001 + i * 100,
                100.1,
                0.5,
                AggressorSide::Buy,
            ));
            d.ingest(delta(
                200 + i,
                3_020 + i * 100,
                1.0,
                1.0,
                BookContinuity::Continuous,
            ));
        }
        let out = d.flush();
        let event = out.last().expect("sell event");
        assert_eq!(event.side, IcebergSide::PossibleSell);
        assert_eq!(event.aggressive_executed_qty, Qty::from_f64(3.0));
        assert_eq!(event.replenished_qty, Qty::from_f64(3.0));
        assert!((event.executed_to_displayed - 3.0).abs() < 0.001);
        assert!((event.refill_ratio - 1.0).abs() < 0.001);
    }

    #[test]
    fn unexplained_decrease_is_cancellation_and_lowers_score() {
        let mut d = warmed();
        d.ingest(trade(300, 4_000, 100.0, 0.5, AggressorSide::Sell));
        d.ingest(delta(300, 4_020, 0.1, 1.0, BookContinuity::Continuous));
        d.flush();
        let episode = d
            .episodes
            .get(&(PassiveSide::Bid, Price::from_f64(100.0)))
            .unwrap();
        assert_eq!(episode.cancelled_qty, Qty::from_f64(0.4));
        let mut clean = episode.clone();
        clean.cancelled_qty = Qty::ZERO;
        assert!(
            score_episode(episode, UnixMs::new(4_100)) < score_episode(&clean, UnixMs::new(4_100))
        );
    }

    #[test]
    fn duplicate_depth_update_is_ignored() {
        let mut d = warmed();
        d.ingest(trade(400, 5_000, 100.0, 1.0, AggressorSide::Sell));
        d.ingest(delta(400, 5_020, 1.0, 1.0, BookContinuity::Continuous));
        d.ingest(delta(400, 5_021, 2.0, 1.0, BookContinuity::Continuous));
        d.flush();
        assert_eq!(
            d.episodes
                .get(&(PassiveSide::Bid, Price::from_f64(100.0)))
                .unwrap()
                .refill_count,
            1
        );
    }

    #[test]
    fn no_episode_or_signal_during_warmup() {
        let mut d =
            BinanceIcebergDetector::new(ticker(), PriceStep::from(ticker().min_ticksize), config())
                .unwrap();
        d.ingest(delta(1, 1_000, 1.0, 1.0, BookContinuity::SnapshotBoundary));
        d.ingest(trade(1, 1_010, 100.0, 10.0, AggressorSide::Sell));
        assert!(d.flush().is_empty());
        assert_eq!(d.episode_count(), 0);
        assert!(d.is_warming_up());
    }

    #[test]
    fn adaptive_threshold_uses_trade_and_touch_samples() {
        let mut baseline = RollingBaselines::default();
        for i in 0..20 {
            baseline.add_trade(UnixMs::new(1_000 + i), Qty::from_f64(i as f64 + 1.0));
            baseline.add_touch(UnixMs::new(1_000 + i), Qty::from_f64(100.0));
        }
        assert_eq!(baseline.minimum_executed(ticker()), Qty::from_f64(25.0));
    }

    #[test]
    fn bounded_state_under_thousands_of_events() {
        let start = std::time::Instant::now();
        let mut d = warmed();
        for i in 0..10_000u64 {
            d.ingest(trade(
                10_000 + i,
                10_000 + i,
                99.0,
                0.001,
                AggressorSide::Sell,
            ));
        }
        d.flush();
        assert!(d.buffered_count() <= MAX_REORDER_EVENTS);
        assert!(d.episode_count() <= MAX_EPISODES);
        assert!(d.seen_trades.len() <= TRADE_DEDUP_WINDOW);
        assert!(start.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn jsonl_replay_is_deterministic() {
        let root = std::env::temp_dir().join(format!("flowsurface-iceberg-{}", std::process::id()));
        let mut recorder = OrderFlowRecorder::create(&root, ticker(), UnixMs::new(1_000)).unwrap();
        let mut inputs = vec![delta(1, 1_000, 1.0, 1.0, BookContinuity::SnapshotBoundary)];
        for i in 0..12 {
            inputs.push(trade(10 + i, 1_010 + i, 99.0, 0.01, AggressorSide::Sell));
            inputs.push(delta(
                2 + i,
                1_020 + i,
                1.0,
                1.0,
                BookContinuity::Continuous,
            ));
        }
        for i in 0..3 {
            inputs.push(trade(
                100 + i,
                2_000 + i * 100,
                100.0,
                1.0,
                AggressorSide::Sell,
            ));
            inputs.push(delta(
                100 + i,
                2_020 + i * 100,
                1.0,
                1.0,
                BookContinuity::Continuous,
            ));
        }
        for input in &inputs {
            recorder
                .write(&OrderFlowRecord::from_input(input).unwrap())
                .unwrap();
        }
        recorder.flush().unwrap();
        let path = recorder.path().to_path_buf();
        drop(recorder);
        let mut first =
            BinanceIcebergDetector::new(ticker(), ticker().min_ticksize.into(), config()).unwrap();
        let mut second =
            BinanceIcebergDetector::new(ticker(), ticker().min_ticksize.into(), config()).unwrap();
        assert_eq!(
            replay_jsonl(&path, &mut first).unwrap(),
            replay_jsonl(&path, &mut second).unwrap()
        );
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    #[test]
    fn gap_clears_buffer_and_episodes() {
        let mut d = warmed();
        d.ingest(trade(100, 2_000, 100.0, 1.0, AggressorSide::Sell));
        d.flush();
        d.ingest(OrderFlowEvent::Quality {
            ticker_info: ticker(),
            at: 2_100.into(),
            quality: OrderFlowDataQuality::Gap,
        });
        assert_eq!(d.episode_count(), 0);
        assert_eq!(d.buffered_count(), 0);
    }

    #[test]
    fn reconnect_invalidates_and_restarts_warmup() {
        let mut d = warmed();
        d.ingest(trade(500, 6_000, 100.0, 1.0, AggressorSide::Sell));
        d.flush();
        assert_eq!(d.episode_count(), 1);
        d.ingest(OrderFlowEvent::Reconnect {
            ticker_info: ticker(),
            at: 6_010.into(),
        });
        assert_eq!(d.episode_count(), 0);
        assert!(d.is_warming_up());
        assert!(
            d.ingest(trade(501, 6_020, 100.0, 10.0, AggressorSide::Sell))
                .is_empty()
        );
    }

    #[test]
    fn price_broken_beyond_adverse_limit_invalidates_episode() {
        let mut d = warmed();
        d.ingest(trade(600, 7_000, 100.0, 1.0, AggressorSide::Sell));
        d.flush();
        let broken = OrderFlowEvent::BookDelta(BookDeltaEvent {
            ticker_info: ticker(),
            exchange_time: 7_020.into(),
            transaction_time: Some(7_020.into()),
            receive_time: 7_020.into(),
            first_update_id: 600,
            final_update_id: 600,
            previous_final_update_id: Some(599),
            bids: vec![
                BookLevelDelta {
                    price: Price::from_f64(100.0),
                    previous_qty: Qty::from_f64(1.0),
                    current_qty: Qty::ZERO,
                },
                BookLevelDelta {
                    price: Price::from_f64(99.8),
                    previous_qty: Qty::ZERO,
                    current_qty: Qty::from_f64(1.0),
                },
            ]
            .into_boxed_slice(),
            asks: Box::new([]),
            continuity: BookContinuity::Continuous,
        });
        d.ingest(broken);
        d.flush();
        assert_eq!(d.episode_count(), 0);
    }

    #[test]
    fn receive_order_is_corrected_by_exchange_time() {
        let mut d = warmed();
        for i in 0..3 {
            // The refill reaches the process first, but its exchange time is after the trade.
            d.ingest(delta(
                700 + i,
                8_020 + i * 100,
                1.0,
                1.0,
                BookContinuity::Continuous,
            ));
            d.ingest(trade(
                700 + i,
                8_000 + i * 100,
                100.0,
                1.0,
                AggressorSide::Sell,
            ));
        }
        let out = d.flush();
        assert!(out.iter().any(|event| event.refill_count >= 3));
    }

    #[test]
    fn side_and_price_keys_are_independent() {
        let mut d = warmed();
        d.ingest(trade(800, 9_000, 100.0, 1.0, AggressorSide::Sell));
        d.ingest(trade(801, 9_001, 100.1, 1.0, AggressorSide::Buy));
        d.flush();
        assert!(
            d.episodes
                .contains_key(&(PassiveSide::Bid, Price::from_f64(100.0)))
        );
        assert!(
            d.episodes
                .contains_key(&(PassiveSide::Ask, Price::from_f64(100.1)))
        );
    }

    #[test]
    fn legacy_config_defaults_disabled() {
        let cfg: IcebergDetectorConfig = serde_json::from_str("{}").unwrap();
        assert!(!cfg.enabled);
        assert_eq!(cfg.reorder_window_ms, 150);

        let heatmap: crate::chart::heatmap::Config = serde_json::from_str(
            r#"{"trade_size_filter":0.0,"order_size_filter":0.0,"trade_size_scale":100,"coalescing":{"Average":0.15}}"#,
        )
        .unwrap();
        assert!(!heatmap.iceberg_detector.enabled);
    }

    #[test]
    fn unsupported_market_is_rejected() {
        let spot = TickerInfo::new(
            Ticker::new("BTCUSDT", Exchange::BinanceSpot),
            0.1,
            0.001,
            None,
        );
        assert!(
            BinanceIcebergDetector::new(spot, PriceStep::from(spot.min_ticksize), config())
                .is_err()
        );
    }
}
