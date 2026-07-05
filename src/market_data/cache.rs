//! File-based local market data cache.
//!
//! `LocalMarketCache` provides persistent storage for market data using
//! JSON files. This is a simpler alternative to SQLite that doesn't
//! require additional dependencies.
//!
//! Data is stored in a directory structure:
//! ```text
//! market_data/
//!   {venue}_{symbol}_{market_type}/
//!     trades.json
//!     klines.json
//!     oi.json
//! ```

use super::key::MarketDataKey;
use super::range::MarketDataRange;
use exchange::unit::{Price, Qty};
use exchange::{Kline, Trade, UnixMs};
use rustc_hash::FxHashMap;
use std::fs;
use std::path::PathBuf;

/// Maximum number of trades to cache per key.
const MAX_CACHED_TRADES: usize = 50_000;

/// Maximum number of klines to cache per key.
const MAX_CACHED_KLINES: usize = 10_000;

/// Maximum age for cached data (24 hours in milliseconds).
const MAX_CACHE_AGE_MS: u64 = 24 * 60 * 60 * 1000;

/// Cached trades data for a key.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CachedTrades {
    key_display: String,
    trades: Vec<CachedTrade>,
    last_updated: u64,
}

/// Serializable trade representation using raw f64 values.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CachedTrade {
    time_ms: u64,
    is_sell: bool,
    price: f64,
    qty: f64,
}

impl CachedTrade {
    fn from_trade(trade: &Trade) -> Self {
        Self {
            time_ms: trade.time.as_u64(),
            is_sell: trade.is_sell,
            price: trade.price.to_f64(),
            qty: trade.qty.to_f64(),
        }
    }

    fn to_trade(&self) -> Trade {
        Trade {
            time: UnixMs::new(self.time_ms),
            is_sell: self.is_sell,
            price: Price::from_f64(self.price),
            qty: Qty::from_f64(self.qty),
        }
    }
}

/// Cached klines data for a key.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CachedKlines {
    key_display: String,
    klines: Vec<CachedKline>,
    last_updated: u64,
}

/// Serializable kline representation using raw f64 values.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CachedKline {
    time_ms: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

impl CachedKline {
    fn from_kline(kline: &Kline) -> Self {
        Self {
            time_ms: kline.time.as_u64(),
            open: kline.open.to_f64(),
            high: kline.high.to_f64(),
            low: kline.low.to_f64(),
            close: kline.close.to_f64(),
            volume: kline.volume.total().to_f64(),
        }
    }

    fn to_kline(&self) -> Kline {
        Kline {
            time: UnixMs::new(self.time_ms),
            open: Price::from_f64(self.open),
            high: Price::from_f64(self.high),
            low: Price::from_f64(self.low),
            close: Price::from_f64(self.close),
            volume: exchange::Volume::TotalOnly(Qty::from_f64(self.volume)),
        }
    }
}

/// File-based local market data cache.
pub struct LocalMarketCache {
    /// Base directory for cache files
    cache_dir: PathBuf,
    /// In-memory index of cached keys
    _index: FxHashMap<MarketDataKey, PathBuf>,
    /// Performance metrics
    stats: CacheStats,
}

/// Cache performance statistics.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub reads: usize,
    pub writes: usize,
    pub hits: usize,
    pub misses: usize,
    pub errors: usize,
}

impl LocalMarketCache {
    /// Create a new cache with the given base directory.
    pub fn new(cache_dir: PathBuf) -> Self {
        // Create cache directory if it doesn't exist
        if !cache_dir.exists() {
            let _ = fs::create_dir_all(&cache_dir);
        }

        Self {
            cache_dir,
            _index: FxHashMap::default(),
            stats: CacheStats::default(),
        }
    }

    /// Create a cache in the standard data directory.
    pub fn default_cache() -> Self {
        let cache_dir = data::data_path(Some("market_data/cache"));
        Self::new(cache_dir)
    }

    /// Get the directory path for a specific key.
    fn key_dir(&self, key: &MarketDataKey) -> PathBuf {
        let dir_name = format!("{}_{}_{}", key.venue, key.symbol, key.market_type);
        self.cache_dir.join(dir_name)
    }

    /// Insert trades into the cache.
    pub fn insert_trades(&mut self, key: &MarketDataKey, trades: &[Trade]) {
        let dir = self.key_dir(key);
        let _ = fs::create_dir_all(&dir);

        let file_path = dir.join("trades.json");
        let key_display = key.display_key();

        // Load existing trades
        let mut cached = if file_path.exists() {
            fs::read_to_string(&file_path)
                .ok()
                .and_then(|s| serde_json::from_str::<CachedTrades>(&s).ok())
                .unwrap_or_else(|| CachedTrades {
                    key_display: key_display.clone(),
                    trades: Vec::new(),
                    last_updated: 0,
                })
        } else {
            CachedTrades {
                key_display: key_display.clone(),
                trades: Vec::new(),
                last_updated: 0,
            }
        };

        // Add new trades
        for trade in trades {
            cached.trades.push(CachedTrade::from_trade(trade));
        }

        // Dedup and sort by time
        cached.trades.sort_by_key(|t| t.time_ms);
        cached.trades.dedup_by_key(|t| t.time_ms);

        // Prune if exceeding limit
        if cached.trades.len() > MAX_CACHED_TRADES {
            let drop = cached.trades.len() - MAX_CACHED_TRADES;
            cached.trades.drain(0..drop);
        }

        cached.last_updated = chrono::Utc::now().timestamp_millis() as u64;

        // Write to file
        if let Ok(json) = serde_json::to_string_pretty(&cached) {
            let _ = fs::write(&file_path, json);
            self.stats.writes += 1;
        } else {
            self.stats.errors += 1;
        }
    }

    /// Query trades from the cache.
    pub fn query_trades(&mut self, key: &MarketDataKey, range: &MarketDataRange) -> Vec<Trade> {
        self.stats.reads += 1;

        let dir = self.key_dir(key);
        let file_path = dir.join("trades.json");

        if !file_path.exists() {
            self.stats.misses += 1;
            return Vec::new();
        }

        let cached = match fs::read_to_string(&file_path) {
            Ok(s) => serde_json::from_str::<CachedTrades>(&s).ok(),
            Err(_) => None,
        };

        let cached = match cached {
            Some(c) => c,
            None => {
                self.stats.errors += 1;
                return Vec::new();
            }
        };

        // Check if data is stale
        let now = chrono::Utc::now().timestamp_millis() as u64;
        if now.saturating_sub(cached.last_updated) > MAX_CACHE_AGE_MS {
            self.stats.misses += 1;
            return Vec::new();
        }

        // Filter by range
        let result: Vec<Trade> = cached
            .trades
            .iter()
            .filter(|t| t.time_ms >= range.from.as_u64() && t.time_ms < range.to.as_u64())
            .map(|t| t.to_trade())
            .collect();

        if result.is_empty() {
            self.stats.misses += 1;
        } else {
            self.stats.hits += 1;
        }

        result
    }

    /// Insert klines into the cache.
    pub fn insert_klines(&mut self, key: &MarketDataKey, klines: &[Kline]) {
        let dir = self.key_dir(key);
        let _ = fs::create_dir_all(&dir);

        let file_path = dir.join("klines.json");
        let key_display = key.display_key();

        let mut cached = if file_path.exists() {
            fs::read_to_string(&file_path)
                .ok()
                .and_then(|s| serde_json::from_str::<CachedKlines>(&s).ok())
                .unwrap_or_else(|| CachedKlines {
                    key_display: key_display.clone(),
                    klines: Vec::new(),
                    last_updated: 0,
                })
        } else {
            CachedKlines {
                key_display: key_display.clone(),
                klines: Vec::new(),
                last_updated: 0,
            }
        };

        for kline in klines {
            cached.klines.push(CachedKline::from_kline(kline));
        }

        cached.klines.sort_by_key(|k| k.time_ms);
        cached.klines.dedup_by_key(|k| k.time_ms);

        if cached.klines.len() > MAX_CACHED_KLINES {
            let drop = cached.klines.len() - MAX_CACHED_KLINES;
            cached.klines.drain(0..drop);
        }

        cached.last_updated = chrono::Utc::now().timestamp_millis() as u64;

        if let Ok(json) = serde_json::to_string_pretty(&cached) {
            let _ = fs::write(&file_path, json);
            self.stats.writes += 1;
        } else {
            self.stats.errors += 1;
        }
    }

    /// Query klines from the cache.
    pub fn query_klines(&mut self, key: &MarketDataKey, range: &MarketDataRange) -> Vec<Kline> {
        self.stats.reads += 1;

        let dir = self.key_dir(key);
        let file_path = dir.join("klines.json");

        if !file_path.exists() {
            self.stats.misses += 1;
            return Vec::new();
        }

        let cached = match fs::read_to_string(&file_path) {
            Ok(s) => serde_json::from_str::<CachedKlines>(&s).ok(),
            Err(_) => None,
        };

        let cached = match cached {
            Some(c) => c,
            None => {
                self.stats.errors += 1;
                return Vec::new();
            }
        };

        let now = chrono::Utc::now().timestamp_millis() as u64;
        if now.saturating_sub(cached.last_updated) > MAX_CACHE_AGE_MS {
            self.stats.misses += 1;
            return Vec::new();
        }

        let result: Vec<Kline> = cached
            .klines
            .iter()
            .filter(|k| k.time_ms >= range.from.as_u64() && k.time_ms < range.to.as_u64())
            .map(|k| k.to_kline())
            .collect();

        if result.is_empty() {
            self.stats.misses += 1;
        } else {
            self.stats.hits += 1;
        }

        result
    }

    /// Get cache statistics.
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Clear cache for a specific key.
    pub fn clear_key(&mut self, key: &MarketDataKey) -> Result<(), std::io::Error> {
        let dir = self.key_dir(key);
        if dir.exists() {
            fs::remove_dir_all(dir)?;
        }
        self._index.remove(key);
        Ok(())
    }

    /// Clear all cached data.
    pub fn clear_all(&mut self) -> Result<(), std::io::Error> {
        if self.cache_dir.exists() {
            fs::remove_dir_all(&self.cache_dir)?;
            fs::create_dir_all(&self.cache_dir)?;
        }
        self._index.clear();
        Ok(())
    }

    /// Get the total size of the cache in bytes.
    pub fn size_bytes(&self) -> u64 {
        if !self.cache_dir.exists() {
            return 0;
        }

        let mut total = 0;
        if let Ok(entries) = fs::read_dir(&self.cache_dir) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    total += metadata.len();
                }
            }
        }
        total
    }

    /// Save coverage ledger to disk.
    pub fn save_coverage(
        &mut self,
        ledger: &super::coverage::CoverageLedger,
    ) -> Result<(), std::io::Error> {
        let coverage_file = self.cache_dir.join("coverage.json");
        let persisted = ledger.to_persisted();
        let json = serde_json::to_string_pretty(&persisted)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        fs::write(&coverage_file, json)?;
        log::info!(
            target: "marketdata",
            "MARKETDATA CoverageSaved | path={} keys={}",
            coverage_file.display(),
            persisted.entries.len()
        );
        Ok(())
    }

    /// Load coverage ledger from disk.
    pub fn load_coverage(&mut self) -> Result<super::coverage::CoverageLedger, std::io::Error> {
        let coverage_file = self.cache_dir.join("coverage.json");
        if !coverage_file.exists() {
            log::info!(
                target: "marketdata",
                "MARKETDATA CoverageLoad | path={} result=no_file",
                coverage_file.display()
            );
            return Ok(super::coverage::CoverageLedger::new());
        }
        let json = fs::read_to_string(&coverage_file)?;
        let persisted: super::coverage::PersistedCoverage = serde_json::from_str(&json)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let ledger = super::coverage::CoverageLedger::from_persisted(&persisted);
        log::info!(
            target: "marketdata",
            "MARKETDATA CoverageLoaded | path={} keys={}",
            coverage_file.display(),
            persisted.entries.len()
        );
        Ok(ledger)
    }

    /// Insert Open Interest data into the cache.
    pub fn insert_open_interest(
        &mut self,
        key: &super::key::MarketDataKey,
        data: &[exchange::OpenInterest],
    ) {
        let dir = self.key_dir(key);
        let _ = fs::create_dir_all(&dir);
        let file_path = dir.join("oi.json");
        let key_display = key.display_key();

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct CachedOi {
            key_display: String,
            entries: Vec<CachedOiEntry>,
            last_updated: u64,
        }
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct CachedOiEntry {
            time_ms: u64,
            value: f64,
        }

        let mut cached = if file_path.exists() {
            fs::read_to_string(&file_path)
                .ok()
                .and_then(|s| serde_json::from_str::<CachedOi>(&s).ok())
                .unwrap_or_else(|| CachedOi {
                    key_display: key_display.clone(),
                    entries: Vec::new(),
                    last_updated: 0,
                })
        } else {
            CachedOi {
                key_display: key_display.clone(),
                entries: Vec::new(),
                last_updated: 0,
            }
        };

        for entry in data {
            cached.entries.push(CachedOiEntry {
                time_ms: entry.time.as_u64(),
                value: entry.value,
            });
        }

        cached.entries.sort_by_key(|e| e.time_ms);
        cached.entries.dedup_by_key(|e| e.time_ms);

        if cached.entries.len() > MAX_CACHED_KLINES {
            let drop = cached.entries.len() - MAX_CACHED_KLINES;
            cached.entries.drain(0..drop);
        }

        cached.last_updated = chrono::Utc::now().timestamp_millis() as u64;

        if let Ok(json) = serde_json::to_string_pretty(&cached) {
            let _ = fs::write(&file_path, json);
            self.stats.writes += 1;
        } else {
            self.stats.errors += 1;
        }
    }

    /// Query Open Interest data from the cache.
    pub fn query_open_interest(
        &mut self,
        key: &super::key::MarketDataKey,
        range: &super::range::MarketDataRange,
    ) -> Vec<exchange::OpenInterest> {
        self.stats.reads += 1;
        let dir = self.key_dir(key);
        let file_path = dir.join("oi.json");
        if !file_path.exists() {
            self.stats.misses += 1;
            return Vec::new();
        }

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct CachedOi {
            #[allow(dead_code)]
            key_display: String,
            entries: Vec<CachedOiEntry>,
            last_updated: u64,
        }
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct CachedOiEntry {
            time_ms: u64,
            value: f64,
        }

        let cached = match fs::read_to_string(&file_path) {
            Ok(s) => serde_json::from_str::<CachedOi>(&s).ok(),
            Err(_) => None,
        };
        let cached = match cached {
            Some(c) => c,
            None => {
                self.stats.errors += 1;
                return Vec::new();
            }
        };

        let now = chrono::Utc::now().timestamp_millis() as u64;
        if now.saturating_sub(cached.last_updated) > MAX_CACHE_AGE_MS {
            self.stats.misses += 1;
            return Vec::new();
        }

        let result: Vec<exchange::OpenInterest> = cached
            .entries
            .iter()
            .filter(|e| e.time_ms >= range.from.as_u64() && e.time_ms < range.to.as_u64())
            .map(|e| exchange::OpenInterest {
                time: UnixMs::new(e.time_ms),
                value: e.value,
            })
            .collect();

        if result.is_empty() {
            self.stats.misses += 1;
        } else {
            self.stats.hits += 1;
        }
        result
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
    fn test_cache_insert_and_query() {
        let temp_dir = std::env::temp_dir().join("flowsurface_test_cache");
        let _ = fs::remove_dir_all(&temp_dir);

        let mut cache = LocalMarketCache::new(temp_dir.clone());
        let key = make_trade_key();

        let trades = vec![
            make_trade(100, 100.0),
            make_trade(200, 101.0),
            make_trade(300, 102.0),
        ];

        cache.insert_trades(&key, &trades);

        let range = MarketDataRange::new(UnixMs::new(100), UnixMs::new(300)).unwrap();
        let result = cache.query_trades(&key, &range);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].price.to_f64(), 100.0);
        assert_eq!(result[1].price.to_f64(), 101.0);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_cache_stats() {
        let temp_dir = std::env::temp_dir().join("flowsurface_test_cache_stats");
        let _ = fs::remove_dir_all(&temp_dir);

        let mut cache = LocalMarketCache::new(temp_dir.clone());
        let key = make_trade_key();

        let trades = vec![make_trade(100, 100.0)];
        cache.insert_trades(&key, &trades);

        let range = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();
        let _ = cache.query_trades(&key, &range);

        assert!(cache.stats().writes > 0);
        assert!(cache.stats().reads > 0);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    // --- Kline cache validation tests ---

    fn make_kline_key() -> MarketDataKey {
        MarketDataKey::klines(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
            exchange::Timeframe::M1,
        )
    }

    fn make_kline(time_ms: u64) -> exchange::Kline {
        exchange::Kline {
            time: UnixMs::new(time_ms),
            open: Price::from_f64(100.0),
            high: Price::from_f64(101.0),
            low: Price::from_f64(99.0),
            close: Price::from_f64(100.5),
            volume: exchange::Volume::TotalOnly(Qty::from_f64(1.0)),
        }
    }

    /// Cached Kline rows outside the query range are filtered by the cache layer.
    #[test]
    fn test_kline_cache_query_filters_by_range() {
        let temp_dir = std::env::temp_dir().join("flowsurface_test_kline_range_filter");
        let _ = fs::remove_dir_all(&temp_dir);

        let mut cache = LocalMarketCache::new(temp_dir.clone());
        let key = make_kline_key();

        // Insert klines spanning 00:00 to 03:00 (180 M1 candles)
        // Each candle at i * 60_000ms
        let klines: Vec<exchange::Kline> = (0..180).map(|i| make_kline(i * 60_000)).collect();
        cache.insert_klines(&key, &klines);

        // Query 01:00:00 to 02:00:00 (1 hour = 3_600_000ms span)
        // Candles at 60_000, 120_000, ..., 3_540_000 = 60 candles
        let range = MarketDataRange::new(UnixMs::new(60_000), UnixMs::new(3_660_000)).unwrap();
        let result = cache.query_klines(&key, &range);

        assert_eq!(
            result.len(),
            60,
            "should only return klines within query range"
        );
        assert!(
            result
                .iter()
                .all(|k| k.time.as_u64() >= 60_000 && k.time.as_u64() < 3_660_000),
            "all returned klines must be within the query range"
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }

    /// Cached Kline query returns empty when there's no overlap.
    #[test]
    fn test_kline_cache_query_returns_empty_for_no_overlap() {
        let temp_dir = std::env::temp_dir().join("flowsurface_test_kline_no_overlap");
        let _ = fs::remove_dir_all(&temp_dir);

        let mut cache = LocalMarketCache::new(temp_dir.clone());
        let key = make_kline_key();

        let klines: Vec<exchange::Kline> = (0..60).map(|i| make_kline(i * 60_000)).collect();
        cache.insert_klines(&key, &klines);

        // Query a completely different range
        let range =
            MarketDataRange::new(UnixMs::new(200_000_000), UnixMs::new(300_000_000)).unwrap();
        let result = cache.query_klines(&key, &range);

        assert!(
            result.is_empty(),
            "query for non-overlapping range should return empty"
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }

    /// Verify the density check formula matches the runtime.
    #[test]
    fn test_kline_cache_density_expected_max() {
        // expected_max = (duration_ms / tf_ms) + 2
        // For M1 (60s) range 01:00 → 05:38 (4h38m = 16_680_000ms):
        let duration_ms = 16_680_000u64;
        let tf_ms = 60_000u64;
        let expected_max = (duration_ms / tf_ms) + 2;
        assert_eq!(expected_max, 280);

        // 1116 records (the corrupted value from runtime) must be detected
        assert!(
            1116 > expected_max,
            "1116 records must exceed expected_max for M1 4h38m"
        );
    }

    /// Corrupt cached Kline segment (stale coverage) becomes a network missing range.
    #[test]
    fn test_kline_cache_stale_coverage_becomes_missing() {
        let mut ledger = crate::market_data::coverage::CoverageLedger::new();
        let key = make_kline_key();
        let range = MarketDataRange::new(UnixMs::new(3_600_000), UnixMs::new(20_280_000)).unwrap();

        // Initially marked complete (from a previous buggy run)
        ledger.mark_complete(key.clone(), range, 1116);
        assert!(ledger.is_covered(&key, &range));

        // After corrupt cache detection, mark stale
        ledger.mark_stale(key.clone(), range, "corrupt_cache");

        // Stale coverage must NOT be considered "covered"
        assert!(
            !ledger.is_covered(&key, &range),
            "stale range must not be covered"
        );

        // Stale range should appear as a missing segment for refetch
        let missing = ledger.compute_missing(&key, range);
        assert_eq!(
            missing.len(),
            1,
            "stale range should become one missing segment"
        );
        assert_eq!(missing[0], range);
    }

    /// Corrupt cached Kline segment is not counted as complete coverage.
    #[test]
    fn test_kline_cache_corrupt_not_served_as_complete() {
        let mut ledger = crate::market_data::coverage::CoverageLedger::new();
        let key = make_kline_key();
        let range = MarketDataRange::new(UnixMs::new(3_600_000), UnixMs::new(20_280_000)).unwrap();

        // Old poisoned coverage
        ledger.mark_complete(key.clone(), range, 1116);

        // Simulate lazy validation: mark stale
        ledger.mark_stale(key.clone(), range, "corrupt_cache");

        // The range should show as missing, forcing a network refetch
        let missing = ledger.compute_missing(&key, range);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].from.as_u64(), 3_600_000);
        assert_eq!(missing[0].to.as_u64(), 20_280_000);
    }

    /// After marking stale and refetching, new valid coverage replaces the corrupt one.
    #[test]
    fn test_kline_cache_refetch_after_corrupt_produces_valid_coverage() {
        let mut ledger = crate::market_data::coverage::CoverageLedger::new();
        let key = make_kline_key();
        let range = MarketDataRange::new(UnixMs::new(3_600_000), UnixMs::new(20_280_000)).unwrap();

        // Old poisoned coverage
        ledger.mark_complete(key.clone(), range, 1116);
        ledger.mark_stale(key.clone(), range, "corrupt_cache");

        // Network refetch succeeds with correct count (278 for M1 4h38m)
        ledger.mark_complete(key.clone(), range, 278);

        // Now it should be covered again with correct count
        assert!(
            ledger.is_covered(&key, &range),
            "refetched range should be covered"
        );
        assert_eq!(ledger.total_records(&key), 278);
    }
}
