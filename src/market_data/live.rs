//! Live WebSocket data routing through the market data layer.
//!
//! This module provides the `LiveDataAdapter` that routes live WebSocket
//! data through the same memory store and cache path as REST historical
//! data. This ensures that live data can be reused after restart and
//! that the coverage ledger stays up to date with live data.

use super::cache::LocalMarketCache;
use super::coverage::CoverageLedger;
use super::key::MarketDataKey;
use super::range::MarketDataRange;
use super::store::MarketDataStore;
use exchange::{Kline, Trade, UnixMs};
use rustc_hash::FxHashMap;

/// Tracks the latest live timestamp per stream for coverage updates.
#[derive(Default)]
struct StreamLiveState {
    last_trade_time: Option<UnixMs>,
    last_kline_time: Option<UnixMs>,
    trade_count: usize,
    kline_count: usize,
}

/// Adapter that routes live WebSocket data through the market data layer.
///
/// This ensures that:
/// 1. Live data is stored in the in-memory MarketDataStore
/// 2. Live data is persisted to LocalMarketCache (if enabled)
/// 3. The CoverageLedger is updated with live data coverage
/// 4. Multiple consumers can reuse live data without re-fetching
pub struct LiveDataAdapter {
    /// Per-stream live state tracking
    stream_state: FxHashMap<MarketDataKey, StreamLiveState>,
    /// Whether to persist live data to cache
    persist_to_cache: bool,
    /// Minimum number of trades before persisting (batch optimization)
    persist_batch_size: usize,
    /// Accumulated trades pending persistence
    pending_trades: FxHashMap<MarketDataKey, Vec<Trade>>,
    /// Accumulated klines pending persistence
    pending_klines: FxHashMap<MarketDataKey, Vec<Kline>>,
}

impl LiveDataAdapter {
    /// Create a new live data adapter.
    pub fn new() -> Self {
        Self {
            stream_state: FxHashMap::default(),
            persist_to_cache: true,
            persist_batch_size: 100,
            pending_trades: FxHashMap::default(),
            pending_klines: FxHashMap::default(),
        }
    }

    /// Create an adapter with custom persistence settings.
    pub fn with_persistence(persist: bool, batch_size: usize) -> Self {
        Self {
            persist_to_cache: persist,
            persist_batch_size: batch_size,
            ..Self::new()
        }
    }

    /// Ingest live trades from WebSocket.
    ///
    /// This method:
    /// 1. Stores trades in the in-memory MarketDataStore
    /// 2. Updates the live state tracking
    /// 3. Accumulates trades for batch persistence
    /// 4. Persists to cache when batch size is reached
    pub fn ingest_trades(
        &mut self,
        key: &MarketDataKey,
        trades: &[Trade],
        store: &mut MarketDataStore,
        _coverage: &mut CoverageLedger,
        cache: Option<&mut LocalMarketCache>,
    ) {
        if trades.is_empty() {
            return;
        }

        // Store in memory
        store.insert_trades(key, trades);

        // Update live state
        let state = self.stream_state.entry(key.clone()).or_default();
        let latest_trade_time = trades.iter().map(|t| t.time).max();

        if let Some(latest) = latest_trade_time
            && state.last_trade_time.is_none_or(|prev| latest > prev)
        {
            state.last_trade_time = Some(latest);
        }
        state.trade_count += trades.len();

        // Log the live data ingestion
        log::info!(
            target: "marketdata",
            "MARKETDATA LiveTrades | key={} count={} latest={} total={}",
            key.display_key(),
            trades.len(),
            latest_trade_time.map_or("-".to_string(), crate::connector::fetcher::format_time_short),
            state.trade_count
        );

        // Accumulate for batch persistence
        if self.persist_to_cache {
            let pending = self.pending_trades.entry(key.clone()).or_default();
            pending.extend_from_slice(trades);

            // Persist when batch is full
            if pending.len() >= self.persist_batch_size
                && let Some(cache) = cache
            {
                let batch: Vec<Trade> = std::mem::take(pending);
                cache.insert_trades(key, &batch);

                // NOTE: Do NOT mark coverage as Complete for live data
                // Live WS data can have gaps; coverage should only be
                // marked Complete after REST historical fetch confirmation.
                // Log the observation instead.
                if let (Some(from), Some(to)) =
                    (batch.first().map(|t| t.time), batch.last().map(|t| t.time))
                {
                    log::trace!(
                        target: "marketdata",
                        "MARKETDATA LivePersisted | key={} range={} count={}",
                        key.display_key(),
                        MarketDataRange::new(from, to.saturating_add(1))
                            .map_or("-".to_string(), |r| r.format_display()),
                        batch.len()
                    );
                }
            }
        }
    }

    /// Ingest live klines from WebSocket.
    ///
    /// This method:
    /// 1. Stores klines in the in-memory MarketDataStore
    /// 2. Updates the live state tracking
    /// 3. Persists to cache immediately (klines are less frequent)
    pub fn ingest_klines(
        &mut self,
        key: &MarketDataKey,
        klines: &[Kline],
        store: &mut MarketDataStore,
        _coverage: &mut CoverageLedger,
        cache: Option<&mut LocalMarketCache>,
    ) {
        if klines.is_empty() {
            return;
        }

        // Store in memory
        store.insert_klines(key, klines);

        // Update live state
        let state = self.stream_state.entry(key.clone()).or_default();
        let latest_kline_time = klines.iter().map(|k| k.time).max();

        if let Some(latest) = latest_kline_time
            && state.last_kline_time.is_none_or(|prev| latest > prev)
        {
            state.last_kline_time = Some(latest);
        }
        state.kline_count += klines.len();

        log::info!(
            target: "marketdata",
            "MARKETDATA LiveKlines | key={} count={} latest={} total={}",
            key.display_key(),
            klines.len(),
            latest_kline_time.map_or("-".to_string(), crate::connector::fetcher::format_time_short),
            state.kline_count
        );

        // Persist klines immediately (they're less frequent than trades)
        if self.persist_to_cache
            && let Some(cache) = cache
        {
            cache.insert_klines(key, klines);

            // NOTE: Do NOT mark coverage as Complete for live klines
            // Live WS data can have gaps; coverage should only be
            // marked Complete after REST historical fetch confirmation.
            if let (Some(from), Some(to)) = (
                klines.first().map(|k| k.time),
                klines.last().map(|k| k.time),
            ) {
                log::trace!(
                    target: "marketdata",
                    "MARKETDATA LiveKlinePersisted | key={} range={} count={}",
                    key.display_key(),
                    MarketDataRange::new(from, to.saturating_add(1))
                        .map_or("-".to_string(), |r| r.format_display()),
                    klines.len()
                );
            }
        }
    }

    /// Flush any pending trades to the cache.
    ///
    /// Call this periodically or when disconnecting to ensure all
    /// accumulated trades are persisted.
    pub fn flush_pending(
        &mut self,
        _coverage: &mut CoverageLedger,
        mut cache: Option<&mut LocalMarketCache>,
    ) {
        if !self.persist_to_cache {
            return;
        }

        for (key, trades) in self.pending_trades.drain() {
            if trades.is_empty() {
                continue;
            }

            if let Some(ref mut cache) = cache {
                cache.insert_trades(&key, &trades);

                // NOTE: Do NOT mark coverage as Complete for live data
                // Log the observation instead
                if let (Some(from), Some(to)) = (
                    trades.first().map(|t| t.time),
                    trades.last().map(|t| t.time),
                ) {
                    log::trace!(
                        target: "marketdata",
                        "MARKETDATA LiveFlush | key={} range={} count={}",
                        key.display_key(),
                        MarketDataRange::new(from, to.saturating_add(1))
                            .map_or("-".to_string(), |r| r.format_display()),
                        trades.len()
                    );
                }
            }
        }

        // Also flush any accumulated klines
        for (key, klines) in self.pending_klines.drain() {
            if klines.is_empty() {
                continue;
            }

            if let Some(ref mut cache) = cache {
                cache.insert_klines(&key, &klines);

                // NOTE: Do NOT mark coverage as Complete for live data
                if let (Some(from), Some(to)) = (
                    klines.first().map(|k| k.time),
                    klines.last().map(|k| k.time),
                ) {
                    log::trace!(
                        target: "marketdata",
                        "MARKETDATA LiveKlineFlush | key={} range={} count={}",
                        key.display_key(),
                        MarketDataRange::new(from, to.saturating_add(1))
                            .map_or("-".to_string(), |r| r.format_display()),
                        klines.len()
                    );
                }
            }
        }
    }

    /// Get the last live trade time for a key.
    pub fn last_trade_time(&self, key: &MarketDataKey) -> Option<UnixMs> {
        self.stream_state.get(key).and_then(|s| s.last_trade_time)
    }

    /// Get the last live kline time for a key.
    pub fn last_kline_time(&self, key: &MarketDataKey) -> Option<UnixMs> {
        self.stream_state.get(key).and_then(|s| s.last_kline_time)
    }

    /// Get the total number of live trades received for a key.
    pub fn trade_count(&self, key: &MarketDataKey) -> usize {
        self.stream_state.get(key).map_or(0, |s| s.trade_count)
    }

    /// Get the total number of live klines received for a key.
    pub fn kline_count(&self, key: &MarketDataKey) -> usize {
        self.stream_state.get(key).map_or(0, |s| s.kline_count)
    }

    /// Check if we have live data for a key.
    pub fn has_live_data(&self, key: &MarketDataKey) -> bool {
        self.stream_state.contains_key(key)
    }

    /// Get all keys with live data.
    pub fn live_keys(&self) -> impl Iterator<Item = &MarketDataKey> {
        self.stream_state.keys()
    }

    /// Clear live state for a key (e.g., on symbol change).
    pub fn clear_key(&mut self, key: &MarketDataKey) {
        self.stream_state.remove(key);
        self.pending_trades.remove(key);
        self.pending_klines.remove(key);
    }

    /// Clear all live state.
    pub fn clear_all(&mut self) {
        self.stream_state.clear();
        self.pending_trades.clear();
        self.pending_klines.clear();
    }
}

impl Default for LiveDataAdapter {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to create a MarketDataKey from a StreamKind for live data routing.
#[allow(dead_code)] // Public API — convenience wrapper over bridge::stream_kind_to_key
pub fn stream_to_live_key(stream: &exchange::adapter::StreamKind) -> Option<MarketDataKey> {
    match stream {
        exchange::adapter::StreamKind::Kline { .. } => super::bridge::stream_kind_to_key(stream),
        exchange::adapter::StreamKind::Trades { .. } => super::bridge::stream_kind_to_key(stream),
        exchange::adapter::StreamKind::Depth { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::key::{MarketKind, Symbol, Venue};
    use exchange::unit::{Price, Qty};

    fn make_trade_key() -> MarketDataKey {
        MarketDataKey::trades(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
        )
    }

    fn make_trade(time_ms: u64, price: f64) -> Trade {
        Trade {
            time: UnixMs::new(time_ms),
            is_sell: false,
            price: Price::from_f64(price),
            qty: Qty::from_f64(1.0),
        }
    }

    #[test]
    fn test_ingest_trades() {
        let mut store = MarketDataStore::new();
        let mut coverage = CoverageLedger::new();
        let mut adapter = LiveDataAdapter::new();

        let key = make_trade_key();
        let trades = vec![make_trade(100, 100.0), make_trade(200, 101.0)];

        adapter.ingest_trades(&key, &trades, &mut store, &mut coverage, None);

        // Check in-memory store
        assert_eq!(store.trade_count(&key), 2);

        // Check live state
        assert!(adapter.has_live_data(&key));
        assert_eq!(adapter.trade_count(&key), 2);
        assert_eq!(adapter.last_trade_time(&key), Some(UnixMs::new(200)));
    }

    #[test]
    fn test_ingest_klines() {
        let mut store = MarketDataStore::new();
        let mut coverage = CoverageLedger::new();
        let mut adapter = LiveDataAdapter::new();

        let key = MarketDataKey::klines(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
            exchange::Timeframe::M5,
        );

        let kline = Kline {
            time: UnixMs::new(100),
            open: Price::from_f64(100.0),
            high: Price::from_f64(110.0),
            low: Price::from_f64(90.0),
            close: Price::from_f64(105.0),
            volume: exchange::Volume::TotalOnly(Qty::from_f64(1000.0)),
        };

        adapter.ingest_klines(&key, &[kline], &mut store, &mut coverage, None);

        assert_eq!(store.kline_count(&key), 1);
        assert!(adapter.has_live_data(&key));
        assert_eq!(adapter.kline_count(&key), 1);
    }

    #[test]
    fn test_clear_key() {
        let mut store = MarketDataStore::new();
        let mut coverage = CoverageLedger::new();
        let mut adapter = LiveDataAdapter::new();

        let key = make_trade_key();
        let trades = vec![make_trade(100, 100.0)];

        adapter.ingest_trades(&key, &trades, &mut store, &mut coverage, None);
        assert!(adapter.has_live_data(&key));

        adapter.clear_key(&key);
        assert!(!adapter.has_live_data(&key));
    }
}
