//! In-memory market data store.
//!
//! `MarketDataStore` provides a BTreeMap-based storage for raw trades, klines,
//! and open interest data, keyed by `MarketDataKey`. Supports range queries,
//! deduplication on insert, and incremental updates.

use std::collections::BTreeMap;

use super::key::MarketDataKey;
use super::range::MarketDataRange;
use exchange::{Kline, OpenInterest, Trade, UnixMs};
use rustc_hash::FxHashMap;

/// Maximum number of raw trades to retain per key.
/// Older trades are pruned FIFO when this cap is exceeded.
/// 50k trades ≈ 1.5-3 MB depending on Trade size.
const MAX_TRADES_PER_KEY: usize = 50_000;

/// Maximum number of klines to retain per key.
/// 10k klines is enough for most timeframes even with deep history.
const MAX_KLINES_PER_KEY: usize = 10_000;

/// Maximum number of open interest entries to retain per key.
const MAX_OI_PER_KEY: usize = 10_000;

/// In-memory store for raw market data.
///
/// Stores trades, klines, and open interest keyed by `MarketDataKey`.
/// Uses BTreeMap for efficient range queries and maintains deduplication.
#[derive(Debug, Default)]
pub struct MarketDataStore {
    /// Raw trades by key, stored by timestamp for range queries.
    trades: FxHashMap<MarketDataKey, BTreeMap<UnixMs, Trade>>,
    /// Klines by key, stored by timestamp.
    klines: FxHashMap<MarketDataKey, BTreeMap<UnixMs, Kline>>,
    /// Open interest by key, stored by timestamp.
    open_interest: FxHashMap<MarketDataKey, BTreeMap<UnixMs, f64>>,
    /// Total record counts for diagnostics.
    trade_counts: FxHashMap<MarketDataKey, usize>,
    kline_counts: FxHashMap<MarketDataKey, usize>,
    oi_counts: FxHashMap<MarketDataKey, usize>,
}

impl MarketDataStore {
    /// Create a new empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a batch of trades for a given key.
    ///
    /// Deduplicates by timestamp (last-write-wins for same timestamp).
    /// Prunes oldest trades if the per-key limit is exceeded.
    pub fn insert_trades(&mut self, key: &MarketDataKey, trades: &[Trade]) {
        if trades.is_empty() {
            return;
        }

        let map = self.trades.entry(key.clone()).or_default();
        let count = self.trade_counts.entry(key.clone()).or_insert(0);

        for trade in trades {
            map.insert(trade.time, *trade);
            *count += 1;
        }

        // Prune if exceeding limit
        while map.len() > MAX_TRADES_PER_KEY {
            if let Some(oldest_key) = map.keys().next().copied() {
                map.remove(&oldest_key);
                *count = count.saturating_sub(1);
            } else {
                break;
            }
        }

        log::trace!(
            target: "marketdata",
            "MARKETDATA StoreInsert | kind=Trades key={} inserted={} total={}",
            key.display_key(),
            trades.len(),
            map.len()
        );
    }

    /// Insert a batch of klines for a given key.
    pub fn insert_klines(&mut self, key: &MarketDataKey, klines: &[Kline]) {
        if klines.is_empty() {
            return;
        }

        let map = self.klines.entry(key.clone()).or_default();
        let count = self.kline_counts.entry(key.clone()).or_insert(0);

        for kline in klines {
            map.insert(kline.time, *kline);
            *count += 1;
        }

        // Prune if exceeding limit
        while map.len() > MAX_KLINES_PER_KEY {
            if let Some(oldest_key) = map.keys().next().copied() {
                map.remove(&oldest_key);
                *count = count.saturating_sub(1);
            } else {
                break;
            }
        }

        log::trace!(
            target: "marketdata",
            "MARKETDATA StoreInsert | kind=Klines key={} inserted={} total={}",
            key.display_key(),
            klines.len(),
            map.len()
        );
    }

    /// Insert a batch of open interest entries for a given key.
    pub fn insert_open_interest(&mut self, key: &MarketDataKey, oi: &[OpenInterest]) {
        if oi.is_empty() {
            return;
        }

        let map = self.open_interest.entry(key.clone()).or_default();
        let count = self.oi_counts.entry(key.clone()).or_insert(0);

        for entry in oi {
            map.insert(entry.time, entry.value);
            *count += 1;
        }

        // Prune if exceeding limit
        while map.len() > MAX_OI_PER_KEY {
            if let Some(oldest_key) = map.keys().next().copied() {
                map.remove(&oldest_key);
                *count = count.saturating_sub(1);
            } else {
                break;
            }
        }

        log::trace!(
            target: "marketdata",
            "MARKETDATA StoreInsert | kind=OI key={} inserted={} total={}",
            key.display_key(),
            oi.len(),
            map.len()
        );
    }

    /// Query trades within a time range.
    ///
    /// Returns trades where `from <= trade.time < to` (exclusive end).
    pub fn query_trades(&self, key: &MarketDataKey, range: &MarketDataRange) -> Vec<&Trade> {
        self.trades
            .get(key)
            .map(|map| {
                map.range(range.from..range.to)
                    .map(|(_, trade)| trade)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Query klines within a time range.
    pub fn query_klines(&self, key: &MarketDataKey, range: &MarketDataRange) -> Vec<&Kline> {
        self.klines
            .get(key)
            .map(|map| {
                map.range(range.from..range.to)
                    .map(|(_, kline)| kline)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Query open interest within a time range.
    pub fn query_open_interest(
        &self,
        key: &MarketDataKey,
        range: &MarketDataRange,
    ) -> Vec<(UnixMs, f64)> {
        self.open_interest
            .get(key)
            .map(|map| {
                map.range(range.from..range.to)
                    .map(|(&time, &value)| (time, value))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get the latest trade time for a key.
    pub fn latest_trade_time(&self, key: &MarketDataKey) -> Option<UnixMs> {
        self.trades
            .get(key)
            .and_then(|map| map.keys().next_back().copied())
    }

    /// Get the latest kline time for a key.
    pub fn latest_kline_time(&self, key: &MarketDataKey) -> Option<UnixMs> {
        self.klines
            .get(key)
            .and_then(|map| map.keys().next_back().copied())
    }

    /// Get the latest open interest time for a key.
    pub fn latest_oi_time(&self, key: &MarketDataKey) -> Option<UnixMs> {
        self.open_interest
            .get(key)
            .and_then(|map| map.keys().next_back().copied())
    }

    /// Get the earliest trade time for a key.
    pub fn earliest_trade_time(&self, key: &MarketDataKey) -> Option<UnixMs> {
        self.trades
            .get(key)
            .and_then(|map| map.keys().next().copied())
    }

    /// Get the earliest kline time for a key.
    pub fn earliest_kline_time(&self, key: &MarketDataKey) -> Option<UnixMs> {
        self.klines
            .get(key)
            .and_then(|map| map.keys().next().copied())
    }

    /// Get the total number of trades stored for a key.
    pub fn trade_count(&self, key: &MarketDataKey) -> usize {
        self.trades.get(key).map_or(0, |map| map.len())
    }

    /// Get the total number of klines stored for a key.
    pub fn kline_count(&self, key: &MarketDataKey) -> usize {
        self.klines.get(key).map_or(0, |map| map.len())
    }

    /// Get the total number of OI entries stored for a key.
    pub fn oi_count(&self, key: &MarketDataKey) -> usize {
        self.open_interest.get(key).map_or(0, |map| map.len())
    }

    /// Check if the store has any data for a given key.
    pub fn has_data(&self, key: &MarketDataKey) -> bool {
        self.trades.contains_key(key)
            || self.klines.contains_key(key)
            || self.open_interest.contains_key(key)
    }

    /// Get all keys that have trades stored.
    pub fn trade_keys(&self) -> impl Iterator<Item = &MarketDataKey> {
        self.trades.keys()
    }

    /// Get all keys that have klines stored.
    pub fn kline_keys(&self) -> impl Iterator<Item = &MarketDataKey> {
        self.klines.keys()
    }

    /// Get all keys that have OI stored.
    pub fn oi_keys(&self) -> impl Iterator<Item = &MarketDataKey> {
        self.open_interest.keys()
    }

    /// Clear all data for a key.
    pub fn clear_key(&mut self, key: &MarketDataKey) {
        self.trades.remove(key);
        self.klines.remove(key);
        self.open_interest.remove(key);
        self.trade_counts.remove(key);
        self.kline_counts.remove(key);
        self.oi_counts.remove(key);
    }

    /// Clear all data.
    pub fn clear_all(&mut self) {
        self.trades.clear();
        self.klines.clear();
        self.open_interest.clear();
        self.trade_counts.clear();
        self.kline_counts.clear();
        self.oi_counts.clear();
    }

    /// Get the time range of stored data for a key's trades.
    pub fn trade_range(&self, key: &MarketDataKey) -> Option<MarketDataRange> {
        let map = self.trades.get(key)?;
        let first = map.keys().next()?;
        let last = map.keys().next_back()?;
        Some(MarketDataRange::new_unchecked(
            *first,
            last.saturating_add(1),
        ))
    }

    /// Get the time range of stored data for a key's klines.
    pub fn kline_range(&self, key: &MarketDataKey) -> Option<MarketDataRange> {
        let map = self.klines.get(key)?;
        let first = map.keys().next()?;
        let last = map.keys().next_back()?;
        Some(MarketDataRange::new_unchecked(
            *first,
            last.saturating_add(1),
        ))
    }

    /// Get a summary of store contents for diagnostics.
    pub fn summary(&self) -> StoreSummary {
        StoreSummary {
            trade_keys: self.trades.len(),
            kline_keys: self.klines.len(),
            oi_keys: self.open_interest.len(),
            total_trades: self.trade_counts.values().sum(),
            total_klines: self.kline_counts.values().sum(),
            total_oi: self.oi_counts.values().sum(),
        }
    }
}

/// Summary of store contents for diagnostics.
#[derive(Debug, Clone)]
pub struct StoreSummary {
    pub trade_keys: usize,
    pub kline_keys: usize,
    pub oi_keys: usize,
    pub total_trades: usize,
    pub total_klines: usize,
    pub total_oi: usize,
}

impl std::fmt::Display for StoreSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "keys(trades={}, klines={}, oi={}) records(trades={}, klines={}, oi={})",
            self.trade_keys,
            self.kline_keys,
            self.oi_keys,
            self.total_trades,
            self.total_klines,
            self.total_oi
        )
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

    fn make_kline_key() -> MarketDataKey {
        MarketDataKey::klines(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
            exchange::Timeframe::M5,
        )
    }

    fn make_trade(time_ms: u64, price: f64, qty: f64) -> Trade {
        Trade {
            time: UnixMs::new(time_ms),
            is_sell: false,
            price: Price::from_f64(price),
            qty: Qty::from_f64(qty),
        }
    }

    fn make_kline(time_ms: u64) -> Kline {
        Kline {
            time: UnixMs::new(time_ms),
            open: Price::from_f64(100.0),
            high: Price::from_f64(110.0),
            low: Price::from_f64(90.0),
            close: Price::from_f64(105.0),
            volume: exchange::Volume::TotalOnly(Qty::from_f64(1000.0)),
        }
    }

    #[test]
    fn test_insert_and_query_trades() {
        let mut store = MarketDataStore::new();
        let key = make_trade_key();

        let trades = vec![
            make_trade(100, 100.0, 1.0),
            make_trade(200, 101.0, 2.0),
            make_trade(300, 102.0, 3.0),
        ];

        store.insert_trades(&key, &trades);

        let range = MarketDataRange::new(UnixMs::new(100), UnixMs::new(300)).unwrap();
        let result = store.query_trades(&key, &range);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].price.to_f64(), 100.0);
        assert_eq!(result[1].price.to_f64(), 101.0);
    }

    #[test]
    fn test_deduplication_trades() {
        let mut store = MarketDataStore::new();
        let key = make_trade_key();

        // Insert trade at time 100
        store.insert_trades(&key, &[make_trade(100, 100.0, 1.0)]);
        // Insert another trade at same time (should overwrite)
        store.insert_trades(&key, &[make_trade(100, 200.0, 2.0)]);

        assert_eq!(store.trade_count(&key), 1);
        let trades = store.query_trades(
            &key,
            &MarketDataRange::new(UnixMs::new(0), UnixMs::new(200)).unwrap(),
        );
        assert_eq!(trades[0].price.to_f64(), 200.0);
    }

    #[test]
    fn test_pruning_trades() {
        let mut store = MarketDataStore::new();
        let key = make_trade_key();

        // Insert more than MAX_TRADES_PER_KEY
        let trades: Vec<Trade> = (0..MAX_TRADES_PER_KEY + 100)
            .map(|i| make_trade(i as u64, 100.0, 1.0))
            .collect();

        store.insert_trades(&key, &trades);

        assert_eq!(store.trade_count(&key), MAX_TRADES_PER_KEY);
    }

    #[test]
    fn test_insert_and_query_klines() {
        let mut store = MarketDataStore::new();
        let key = make_kline_key();

        let klines = vec![make_kline(100), make_kline(200), make_kline(300)];

        store.insert_klines(&key, &klines);

        let range = MarketDataRange::new(UnixMs::new(100), UnixMs::new(300)).unwrap();
        let result = store.query_klines(&key, &range);

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_latest_times() {
        let mut store = MarketDataStore::new();
        let key = make_trade_key();

        store.insert_trades(
            &key,
            &[
                make_trade(100, 100.0, 1.0),
                make_trade(300, 100.0, 1.0),
                make_trade(200, 100.0, 1.0),
            ],
        );

        assert_eq!(store.latest_trade_time(&key), Some(UnixMs::new(300)));
        assert_eq!(store.earliest_trade_time(&key), Some(UnixMs::new(100)));
    }

    #[test]
    fn test_has_data() {
        let mut store = MarketDataStore::new();
        let key = make_trade_key();

        assert!(!store.has_data(&key));

        store.insert_trades(&key, &[make_trade(100, 100.0, 1.0)]);

        assert!(store.has_data(&key));
    }

    #[test]
    fn test_clear_key() {
        let mut store = MarketDataStore::new();
        let key = make_trade_key();

        store.insert_trades(&key, &[make_trade(100, 100.0, 1.0)]);
        assert!(store.has_data(&key));

        store.clear_key(&key);
        assert!(!store.has_data(&key));
    }

    #[test]
    fn test_summary() {
        let mut store = MarketDataStore::new();
        let trade_key = make_trade_key();
        let kline_key = make_kline_key();

        store.insert_trades(
            &trade_key,
            &[make_trade(100, 100.0, 1.0), make_trade(200, 100.0, 1.0)],
        );
        store.insert_klines(&kline_key, &[make_kline(100)]);

        let summary = store.summary();
        assert_eq!(summary.trade_keys, 1);
        assert_eq!(summary.kline_keys, 1);
        assert_eq!(summary.oi_keys, 0);
        assert_eq!(summary.total_trades, 2);
        assert_eq!(summary.total_klines, 1);
    }
}
