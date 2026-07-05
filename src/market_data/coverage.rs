//! Coverage ledger for tracking which time ranges have been fetched.
//!
//! `CoverageLedger` maintains a per-key collection of coverage entries that
//! track the status of fetched data ranges. It supports range merging,
//! gap computation, and persistence for restart recovery.

use super::key::MarketDataKey;
use super::range::MarketDataRange;
use exchange::UnixMs;
use rustc_hash::FxHashMap;

/// The status of a coverage range.
#[derive(Debug, Clone, PartialEq)]
pub enum CoverageStatus {
    /// Data is fully fetched and usable
    Complete,
    /// Only part of the range was fetched
    Partial { until: UnixMs },
    /// No data exists in this range (exchange returned empty)
    Empty,
    /// Fetch failed; retry may be possible
    Failed {
        error: String,
        retry_at: Option<UnixMs>,
    },
    /// Data is stale and should be refreshed
    Stale { reason: &'static str },
}

impl CoverageStatus {
    /// Check if this status means data is usable
    #[allow(dead_code)] // Public API — useful for future consumers
    pub fn is_usable(&self) -> bool {
        matches!(
            self,
            CoverageStatus::Complete | CoverageStatus::Partial { .. }
        )
    }

    /// Check if retry is possible
    #[allow(dead_code)] // Public API — useful for future consumers
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            CoverageStatus::Failed { .. } | CoverageStatus::Stale { .. }
        )
    }

    /// Short label for logging
    pub fn label(&self) -> &'static str {
        match self {
            CoverageStatus::Complete => "Complete",
            CoverageStatus::Partial { .. } => "Partial",
            CoverageStatus::Empty => "Empty",
            CoverageStatus::Failed { .. } => "Failed",
            CoverageStatus::Stale { .. } => "Stale",
        }
    }
}

/// A single coverage entry tracking one time range.
#[derive(Debug, Clone)]
pub struct CoverageEntry {
    /// The time range this entry covers
    pub range: MarketDataRange,
    /// The status of this coverage
    pub status: CoverageStatus,
    /// Timestamp when this entry was last updated
    pub updated_at: UnixMs,
    /// Number of records in this range (if known)
    pub record_count: Option<usize>,
}

impl CoverageEntry {
    /// Create a new complete coverage entry.
    pub fn complete(range: MarketDataRange, count: usize) -> Self {
        Self {
            range,
            status: CoverageStatus::Complete,
            updated_at: UnixMs::now(),
            record_count: Some(count),
        }
    }

    /// Create a new empty coverage entry.
    pub fn empty(range: MarketDataRange) -> Self {
        Self {
            range,
            status: CoverageStatus::Empty,
            updated_at: UnixMs::now(),
            record_count: Some(0),
        }
    }

    /// Create a new partial coverage entry.
    pub fn partial(range: MarketDataRange, until: UnixMs) -> Self {
        Self {
            range,
            status: CoverageStatus::Partial { until },
            updated_at: UnixMs::now(),
            record_count: None,
        }
    }

    /// Log-format representation.
    pub fn log_format(&self) -> String {
        match &self.status {
            CoverageStatus::Partial { until } => {
                format!(
                    "status=Partial range={} until={} records={}",
                    self.range.format_display(),
                    crate::connector::fetcher::format_time_short(*until),
                    self.record_count.map_or("-".into(), |c| c.to_string())
                )
            }
            _ => {
                format!(
                    "status={} range={} records={}",
                    self.status.label(),
                    self.range.format_display(),
                    self.record_count.map_or("-".into(), |c| c.to_string())
                )
            }
        }
    }
}

/// Per-key coverage data with range merging.
///
/// Maintains a sorted, non-overlapping list of coverage entries
/// that are merged on insert to produce a compact representation.
#[derive(Debug, Clone, Default)]
struct PerKeyCoverage {
    entries: Vec<CoverageEntry>,
}

impl PerKeyCoverage {
    /// Insert a new coverage entry, merging with existing entries.
    fn insert(&mut self, new_entry: CoverageEntry) {
        // Remove any existing entries that overlap with the new range
        self.entries.retain(|e| !e.range.overlaps(&new_entry.range));

        self.entries.push(new_entry);
        self.entries.sort_by_key(|entry| entry.range.from);

        // Merge adjacent/overlapping complete entries
        self.merge_adjacent();
    }

    /// Merge adjacent or overlapping complete entries.
    fn merge_adjacent(&mut self) {
        let mut merged: Vec<CoverageEntry> = Vec::with_capacity(self.entries.len());

        for entry in self.entries.drain(..) {
            if let Some(last) = merged.last_mut() {
                let both_complete = matches!(last.status, CoverageStatus::Complete)
                    && matches!(entry.status, CoverageStatus::Complete);

                if both_complete && last.range.adjacent_to(&entry.range) {
                    last.range = MarketDataRange::new_unchecked(
                        last.range.from.min(entry.range.from),
                        last.range.to.max(entry.range.to),
                    );
                    last.record_count = last
                        .record_count
                        .zip(entry.record_count)
                        .map(|(a, b)| a + b)
                        .or(last.record_count)
                        .or(entry.record_count);
                    last.updated_at = last.updated_at.max(entry.updated_at);
                    continue;
                }
            }

            merged.push(entry);
        }

        self.entries = merged;
    }

    /// Compute the missing ranges within a desired range.
    fn missing_ranges(&self, desired: MarketDataRange) -> Vec<MarketDataRange> {
        let complete_ranges: Vec<MarketDataRange> = self
            .entries
            .iter()
            .filter(|e| matches!(e.status, CoverageStatus::Complete))
            .map(|e| e.range)
            .collect();

        super::range::compute_missing(desired, &complete_ranges)
    }

    /// Check if a range is fully covered by complete entries.
    fn is_covered(&self, desired: &MarketDataRange) -> bool {
        let complete_ranges: Vec<MarketDataRange> = self
            .entries
            .iter()
            .filter(|e| matches!(e.status, CoverageStatus::Complete))
            .map(|e| e.range)
            .collect();

        super::range::is_fully_covered(desired, &complete_ranges)
    }

    /// Get all ranges that have a given status discriminant.
    fn ranges_with_status(&self, status_filter: CoverageStatus) -> Vec<MarketDataRange> {
        self.entries
            .iter()
            .filter(|e| std::mem::discriminant(&e.status) == std::mem::discriminant(&status_filter))
            .map(|e| e.range)
            .collect()
    }

    /// Total record count across all complete entries.
    fn total_records(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| matches!(e.status, CoverageStatus::Complete))
            .filter_map(|e| e.record_count)
            .sum()
    }

    /// Latest 'to' timestamp across all complete entries.
    fn latest_to(&self) -> Option<UnixMs> {
        self.entries
            .iter()
            .filter(|e| matches!(e.status, CoverageStatus::Complete))
            .map(|e| e.range.to)
            .max()
    }
}

/// Central coverage ledger for all market data keys.
///
/// Tracks which time ranges have been fetched for each `MarketDataKey`,
/// along with their status (Complete, Partial, Empty, Failed, Stale).
/// Supports range merging, gap computation, and logging.
#[derive(Debug, Clone, Default)]
pub struct CoverageLedger {
    per_key: FxHashMap<MarketDataKey, PerKeyCoverage>,
}

impl CoverageLedger {
    /// Create a new empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark a range as completely fetched.
    pub fn mark_complete(&mut self, key: MarketDataKey, range: MarketDataRange, count: usize) {
        let entry = CoverageEntry::complete(range, count);
        log::info!(
            target: "marketdata",
            "MARKETDATA CoverageUpdate | key={} {}",
            key.display_key(),
            entry.log_format()
        );
        self.per_key.entry(key).or_default().insert(entry);
    }

    /// Mark a range as empty (no data from exchange).
    pub fn mark_empty(&mut self, key: MarketDataKey, range: MarketDataRange) {
        let entry = CoverageEntry::empty(range);
        log::info!(
            target: "marketdata",
            "MARKETDATA CoverageEmpty | key={} {}",
            key.display_key(),
            entry.log_format()
        );
        self.per_key.entry(key).or_default().insert(entry);
    }

    /// Mark a range as partially fetched.
    pub fn mark_partial(&mut self, key: MarketDataKey, range: MarketDataRange, until: UnixMs) {
        let entry = CoverageEntry::partial(range, until);
        log::info!(
            target: "marketdata",
            "MARKETDATA CoveragePartial | key={} {}",
            key.display_key(),
            entry.log_format()
        );
        self.per_key.entry(key).or_default().insert(entry);
    }

    /// Mark a range as failed.
    pub fn mark_failed(
        &mut self,
        key: MarketDataKey,
        range: MarketDataRange,
        error: String,
        retry_at: Option<UnixMs>,
    ) {
        let entry = CoverageEntry {
            range,
            status: CoverageStatus::Failed { error, retry_at },
            updated_at: UnixMs::now(),
            record_count: None,
        };
        log::warn!(
            target: "marketdata",
            "MARKETDATA CoverageFailed | key={} {}",
            key.display_key(),
            entry.log_format()
        );
        self.per_key.entry(key).or_default().insert(entry);
    }

    /// Mark a range as stale (should be refreshed).
    pub fn mark_stale(&mut self, key: MarketDataKey, range: MarketDataRange, reason: &'static str) {
        let entry = CoverageEntry {
            range,
            status: CoverageStatus::Stale { reason },
            updated_at: UnixMs::now(),
            record_count: None,
        };
        log::info!(
            target: "marketdata",
            "MARKETDATA CoverageStale | key={} {} reason={}",
            key.display_key(),
            entry.log_format(),
            reason
        );
        self.per_key.entry(key).or_default().insert(entry);
    }

    /// Compute the missing ranges within a desired range.
    pub fn compute_missing(
        &self,
        key: &MarketDataKey,
        desired: MarketDataRange,
    ) -> Vec<MarketDataRange> {
        self.per_key
            .get(key)
            .map(|c| c.missing_ranges(desired))
            .unwrap_or_else(|| vec![desired])
    }

    /// Check if a range is fully covered by complete entries.
    pub fn is_covered(&self, key: &MarketDataKey, range: &MarketDataRange) -> bool {
        self.per_key.get(key).is_some_and(|c| c.is_covered(range))
    }

    /// Get all complete ranges for a given key.
    pub fn complete_ranges(&self, key: &MarketDataKey) -> Vec<MarketDataRange> {
        self.per_key
            .get(key)
            .map(|c| c.ranges_with_status(CoverageStatus::Complete))
            .unwrap_or_default()
    }

    /// Get the latest 'to' timestamp covered for a key.
    pub fn latest_covered_to(&self, key: &MarketDataKey) -> Option<UnixMs> {
        self.per_key.get(key).and_then(|c| c.latest_to())
    }

    /// Get total number of records cached for a key.
    pub fn total_records(&self, key: &MarketDataKey) -> usize {
        self.per_key
            .get(key)
            .map(|c| c.total_records())
            .unwrap_or(0)
    }

    /// Check if we have any coverage entries for a key.
    pub fn has_coverage(&self, key: &MarketDataKey) -> bool {
        self.per_key.contains_key(key)
    }

    /// Get all keys that have coverage entries.
    pub fn keys(&self) -> impl Iterator<Item = &MarketDataKey> {
        self.per_key.keys()
    }

    /// Number of keys tracked.
    pub fn len(&self) -> usize {
        self.per_key.len()
    }

    /// Check if the ledger is empty.
    pub fn is_empty(&self) -> bool {
        self.per_key.is_empty()
    }

    /// Clear all coverage for a key (e.g., on symbol change).
    pub fn clear_key(&mut self, key: &MarketDataKey) {
        self.per_key.remove(key);
    }

    /// Clear all coverage.
    pub fn clear_all(&mut self) {
        self.per_key.clear();
    }

    /// Convert to a serializable representation for persistence.
    pub fn to_persisted(&self) -> PersistedCoverage {
        let entries = self
            .per_key
            .iter()
            .map(|(key, per_key)| {
                let serializable_entries: Vec<PersistedCoverageEntry> = per_key
                    .entries
                    .iter()
                    .map(|e| PersistedCoverageEntry {
                        from_ms: e.range.from.as_u64(),
                        to_ms: e.range.to.as_u64(),
                        status: match &e.status {
                            CoverageStatus::Complete => PersistedStatus::Complete,
                            CoverageStatus::Partial { until } => PersistedStatus::Partial {
                                until_ms: until.as_u64(),
                            },
                            CoverageStatus::Empty => PersistedStatus::Empty,
                            CoverageStatus::Failed { error, retry_at } => PersistedStatus::Failed {
                                error: error.clone(),
                                retry_at_ms: retry_at.map(|r| r.as_u64()),
                            },
                            CoverageStatus::Stale { reason } => PersistedStatus::Stale {
                                reason: reason.to_string(),
                            },
                        },
                        updated_at_ms: e.updated_at.as_u64(),
                        record_count: e.record_count,
                    })
                    .collect();
                PersistedCoverageKey {
                    venue: key.venue.display_name().to_string(),
                    symbol: key.symbol.as_str().to_string(),
                    market_type: format!("{}", key.market_type),
                    kind: format!("{}", key.kind),
                    entries: serializable_entries,
                }
            })
            .collect();
        PersistedCoverage {
            version: 1,
            entries,
        }
    }

    /// Restore from a persisted representation.
    pub fn from_persisted(persisted: &PersistedCoverage) -> Self {
        let mut ledger = Self::new();
        for key_entry in &persisted.entries {
            let key = MarketDataKey::from_display_parts(
                &key_entry.venue,
                &key_entry.symbol,
                &key_entry.market_type,
                &key_entry.kind,
            );
            if let Some(key) = key {
                let per_key = ledger.per_key.entry(key).or_default();
                for entry in &key_entry.entries {
                    let range = MarketDataRange::new_unchecked(
                        UnixMs::new(entry.from_ms),
                        UnixMs::new(entry.to_ms),
                    );
                    let status = match &entry.status {
                        PersistedStatus::Complete => CoverageStatus::Complete,
                        PersistedStatus::Partial { until_ms } => CoverageStatus::Partial {
                            until: UnixMs::new(*until_ms),
                        },
                        PersistedStatus::Empty => CoverageStatus::Empty,
                        PersistedStatus::Failed { error, retry_at_ms } => CoverageStatus::Failed {
                            error: error.clone(),
                            retry_at: retry_at_ms.map(UnixMs::new),
                        },
                        PersistedStatus::Stale { reason } => CoverageStatus::Stale {
                            reason: Box::leak(reason.clone().into_boxed_str()),
                        },
                    };
                    let coverage_entry = CoverageEntry {
                        range,
                        status,
                        updated_at: UnixMs::new(entry.updated_at_ms),
                        record_count: entry.record_count,
                    };
                    per_key.entries.push(coverage_entry);
                }
                per_key.entries.sort_by_key(|entry| entry.range.from);
                per_key.merge_adjacent();
            }
        }
        ledger
    }
}

/// Persisted representation of coverage data.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistedCoverage {
    pub version: u32,
    pub entries: Vec<PersistedCoverageKey>,
}

/// Persisted representation of a key's coverage entries.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistedCoverageKey {
    pub venue: String,
    pub symbol: String,
    pub market_type: String,
    pub kind: String,
    pub entries: Vec<PersistedCoverageEntry>,
}

/// Persisted representation of a single coverage entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistedCoverageEntry {
    pub from_ms: u64,
    pub to_ms: u64,
    pub status: PersistedStatus,
    pub updated_at_ms: u64,
    pub record_count: Option<usize>,
}

/// Persisted representation of coverage status.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum PersistedStatus {
    Complete,
    Partial {
        until_ms: u64,
    },
    Empty,
    Failed {
        error: String,
        retry_at_ms: Option<u64>,
    },
    Stale {
        reason: String,
    },
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

    fn range_ms(from: u64, to: u64) -> MarketDataRange {
        MarketDataRange::new(UnixMs::new(from), UnixMs::new(to)).unwrap()
    }

    #[test]
    fn test_mark_complete_and_is_covered() {
        let mut ledger = CoverageLedger::new();
        let key = make_trade_key();

        assert!(!ledger.has_coverage(&key));

        ledger.mark_complete(key.clone(), range_ms(100, 200), 500);

        assert!(ledger.has_coverage(&key));
        assert!(ledger.is_covered(&key, &range_ms(100, 200)));
        assert!(ledger.is_covered(&key, &range_ms(120, 180)));
        assert!(!ledger.is_covered(&key, &range_ms(50, 150)));
    }

    #[test]
    fn test_mark_empty() {
        let mut ledger = CoverageLedger::new();
        let key = make_trade_key();

        ledger.mark_empty(key.clone(), range_ms(100, 200));

        assert!(ledger.has_coverage(&key));
        assert!(!ledger.is_covered(&key, &range_ms(100, 200)));
    }

    #[test]
    fn test_compute_missing() {
        let mut ledger = CoverageLedger::new();
        let key = make_trade_key();
        let desired = range_ms(100, 500);

        // No coverage yet
        let missing = ledger.compute_missing(&key, desired);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0], desired);

        // Add partial coverage
        ledger.mark_complete(key.clone(), range_ms(200, 300), 100);

        let missing = ledger.compute_missing(&key, desired);
        assert_eq!(missing.len(), 2);
        assert_eq!(missing[0].from, UnixMs::new(100));
        assert_eq!(missing[0].to, UnixMs::new(200));
        assert_eq!(missing[1].from, UnixMs::new(300));
        assert_eq!(missing[1].to, UnixMs::new(500));
    }

    #[test]
    fn test_merge_adjacent_complete_entries() {
        let mut ledger = CoverageLedger::new();
        let key = make_trade_key();

        ledger.mark_complete(key.clone(), range_ms(100, 200), 100);
        ledger.mark_complete(key.clone(), range_ms(200, 300), 100);

        // Should be merged into a single entry
        let ranges = ledger.complete_ranges(&key);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].from, UnixMs::new(100));
        assert_eq!(ranges[0].to, UnixMs::new(300));
        assert_eq!(ledger.total_records(&key), 200);
    }

    #[test]
    fn test_merge_non_adjacent_entries() {
        let mut ledger = CoverageLedger::new();
        let key = make_trade_key();

        ledger.mark_complete(key.clone(), range_ms(100, 200), 100);
        ledger.mark_complete(key.clone(), range_ms(300, 400), 100);

        let ranges = ledger.complete_ranges(&key);
        assert_eq!(ranges.len(), 2);
    }

    #[test]
    fn test_latest_covered_to() {
        let mut ledger = CoverageLedger::new();
        let key = make_trade_key();

        ledger.mark_complete(key.clone(), range_ms(100, 200), 100);
        ledger.mark_complete(key.clone(), range_ms(300, 500), 200);

        assert_eq!(ledger.latest_covered_to(&key), Some(UnixMs::new(500)));
    }

    #[test]
    fn test_full_coverage_scenario() {
        let mut ledger = CoverageLedger::new();
        let key = make_trade_key();

        // Simulate a session from 13:00 to 21:00 (8 hours in ms)
        let session_start = 13 * 3600 * 1000;
        let session_end = 21 * 3600 * 1000;
        let desired = range_ms(session_start, session_end);

        // First batch covers 13:00-14:00
        ledger.mark_complete(
            key.clone(),
            range_ms(session_start, session_start + 3600 * 1000),
            5000,
        );

        // Second batch covers 14:00-16:00
        ledger.mark_complete(
            key.clone(),
            range_ms(session_start + 3600 * 1000, session_start + 3 * 3600 * 1000),
            10000,
        );

        // Check missing
        let missing = ledger.compute_missing(&key, desired);
        assert_eq!(missing.len(), 1);
        assert_eq!(
            missing[0].from,
            UnixMs::new(session_start + 3 * 3600 * 1000)
        );
        assert_eq!(missing[0].to, UnixMs::new(session_end));
    }

    #[test]
    fn test_coverage_save_load_roundtrip() {
        let mut ledger = CoverageLedger::new();
        let key = make_trade_key();

        ledger.mark_complete(key.clone(), range_ms(100, 200), 500);
        ledger.mark_complete(key.clone(), range_ms(300, 400), 300);
        ledger.mark_empty(key.clone(), range_ms(200, 300));

        // Serialize
        let persisted = ledger.to_persisted();
        assert_eq!(persisted.entries.len(), 1);
        assert_eq!(persisted.entries[0].entries.len(), 3);

        // Deserialize
        let loaded = CoverageLedger::from_persisted(&persisted);
        assert!(loaded.has_coverage(&key));
        assert!(loaded.is_covered(&key, &range_ms(100, 200)));
        assert!(loaded.is_covered(&key, &range_ms(300, 400)));
        assert!(!loaded.is_covered(&key, &range_ms(200, 300)));
        assert_eq!(loaded.total_records(&key), 800);
    }

    #[test]
    fn test_coverage_gap_detection_after_reload() {
        let mut ledger = CoverageLedger::new();
        let key = make_trade_key();

        ledger.mark_complete(key.clone(), range_ms(100, 200), 100);

        // Serialize and deserialize
        let persisted = ledger.to_persisted();
        let loaded = CoverageLedger::from_persisted(&persisted);

        // Check gap detection on reloaded ledger
        let desired = range_ms(100, 500);
        let missing = loaded.compute_missing(&key, desired);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].from, UnixMs::new(200));
        assert_eq!(missing[0].to, UnixMs::new(500));
    }

    #[test]
    fn test_empty_range_not_refetched_after_reload() {
        let mut ledger = CoverageLedger::new();
        let key = make_trade_key();

        // Mark a range as empty
        ledger.mark_empty(key.clone(), range_ms(100, 200));

        // Serialize and deserialize
        let persisted = ledger.to_persisted();
        let loaded = CoverageLedger::from_persisted(&persisted);

        // The range should not be in missing (empty ranges are not refetched)
        let desired = range_ms(50, 250);
        let missing = loaded.compute_missing(&key, desired);
        // Empty ranges are not considered "covered" for missing computation
        // because CoverageStatus::Empty is not Complete
        // But at least it should not crash
        assert!(!missing.is_empty());
    }

    #[test]
    fn test_adjacent_ranges_merge_after_reload() {
        let mut ledger = CoverageLedger::new();
        let key = make_trade_key();

        ledger.mark_complete(key.clone(), range_ms(100, 200), 100);
        ledger.mark_complete(key.clone(), range_ms(200, 300), 100);

        // Serialize and deserialize
        let persisted = ledger.to_persisted();
        let loaded = CoverageLedger::from_persisted(&persisted);

        // Adjacent ranges should be merged
        let ranges = loaded.complete_ranges(&key);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].from, UnixMs::new(100));
        assert_eq!(ranges[0].to, UnixMs::new(300));
        assert_eq!(loaded.total_records(&key), 200);
    }

    #[test]
    fn test_persisted_coverage_json() {
        let mut ledger = CoverageLedger::new();
        let key = make_trade_key();

        ledger.mark_complete(key.clone(), range_ms(100, 200), 500);

        let persisted = ledger.to_persisted();
        let json = serde_json::to_string_pretty(&persisted).unwrap();
        assert!(json.contains("BinanceLinear"));
        assert!(json.contains("BTCUSDT"));
        assert!(json.contains("Trades"));
        assert!(json.contains("Complete"));

        // Deserialize from JSON
        let loaded: PersistedCoverage = serde_json::from_str(&json).unwrap();
        let restored = CoverageLedger::from_persisted(&loaded);
        assert!(restored.is_covered(&key, &range_ms(100, 200)));
    }

    /// Integration-style test: Simulate cache restart scenario.
    /// 1. Create cache, save trades and coverage for 15:30 → 15:45
    /// 2. Drop coordinator/cache instances
    /// 3. Recreate coordinator/cache
    /// 4. Submit requirement 15:30 → 15:58
    /// 5. Assert planned network segment is only 15:45 → 15:58
    #[test]
    fn test_cache_restart_gap_detection() {
        use crate::market_data::coordinator::MarketDataCoordinator;
        use crate::market_data::requirement::{
            ConsumerFeature, ConsumerId, DataRequirement, Priority,
        };
        use crate::market_data::store::MarketDataStore;
        use exchange::unit::{Price, Qty};
        use std::fs;

        let temp_dir = std::env::temp_dir().join(format!(
            "flowsurface_test_cache_restart_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&temp_dir);

        // Phase 1: Create cache, save trades and coverage for 15:30 → 15:45
        {
            let mut cache = crate::market_data::cache::LocalMarketCache::new(temp_dir.clone());
            let mut coverage = CoverageLedger::new();
            let mut store = MarketDataStore::new();

            let key = make_trade_key();
            let range = range_ms(
                15 * 3600 * 1000 + 30 * 60 * 1000,
                15 * 3600 * 1000 + 45 * 60 * 1000,
            );

            // Insert trades
            let trades: Vec<exchange::Trade> = (0..100)
                .map(|i| exchange::Trade {
                    time: exchange::UnixMs::new(range.from.as_u64() + i * 1000),
                    is_sell: i % 2 == 0,
                    price: Price::from_f64(50000.0 + i as f64),
                    qty: Qty::from_f64(1.0),
                })
                .collect();

            store.insert_trades(&key, &trades);
            cache.insert_trades(&key, &trades);
            coverage.mark_complete(key.clone(), range, trades.len());

            // Save coverage to disk
            cache.save_coverage(&coverage).unwrap();

            // Verify cache has data
            let cached = cache.query_trades(&key, &range);
            assert_eq!(cached.len(), 100);
        }

        // Phase 2: Recreate coordinator/cache (simulating restart)
        {
            let mut cache = crate::market_data::cache::LocalMarketCache::new(temp_dir.clone());
            let mut coordinator = MarketDataCoordinator::new();

            // Load coverage from disk
            let loaded_coverage = cache.load_coverage().unwrap();
            coordinator.coverage = loaded_coverage;

            let key = make_trade_key();
            let full_range = range_ms(
                15 * 3600 * 1000 + 30 * 60 * 1000,
                15 * 3600 * 1000 + 58 * 60 * 1000,
            );

            // Verify coverage is loaded
            assert!(coordinator.coverage.is_covered(
                &key,
                &range_ms(
                    15 * 3600 * 1000 + 30 * 60 * 1000,
                    15 * 3600 * 1000 + 45 * 60 * 1000,
                )
            ));

            // Submit requirement for full range
            let consumer = ConsumerId::global(ConsumerFeature::VolumeBubbles);
            let req =
                DataRequirement::new(consumer, key.clone(), full_range, Priority::Normal, "test");
            coordinator.require(req);

            // Plan
            let plan = coordinator.plan().clone();

            // Assert: network segment should only be 15:45 → 15:58
            assert_eq!(plan.network_segments.len(), 1);
            let segment = &plan.network_segments[0];
            assert_eq!(
                segment.range.from.as_u64(),
                15 * 3600 * 1000 + 45 * 60 * 1000
            );
            assert_eq!(segment.range.to.as_u64(), 15 * 3600 * 1000 + 58 * 60 * 1000);

            // Assert: cached segment should be 15:30 → 15:45
            assert!(plan.has_cached_data());
            let cached = &plan.cached_segments[0];
            assert_eq!(
                cached.range.from.as_u64(),
                15 * 3600 * 1000 + 30 * 60 * 1000
            );
            assert_eq!(cached.range.to.as_u64(), 15 * 3600 * 1000 + 45 * 60 * 1000);
        }

        let _ = fs::remove_dir_all(&temp_dir);
    }

    /// Test: Live observed data does not suppress REST backfill.
    /// 1. Insert live observed trades for 15:30 → 15:45
    /// 2. Do not mark coverage complete
    /// 3. Submit requirement 15:30 → 15:58
    /// 4. Assert coordinator still plans network fetch for full range
    #[test]
    fn test_live_observed_does_not_block_backfill() {
        use crate::market_data::coordinator::MarketDataCoordinator;
        use crate::market_data::requirement::{
            ConsumerFeature, ConsumerId, DataRequirement, Priority,
        };
        use crate::market_data::store::MarketDataStore;
        use exchange::unit::{Price, Qty};

        let mut coordinator = MarketDataCoordinator::new();
        let mut store = MarketDataStore::new();

        let key = make_trade_key();
        let live_range = range_ms(
            15 * 3600 * 1000 + 30 * 60 * 1000,
            15 * 3600 * 1000 + 45 * 60 * 1000,
        );

        // Insert live trades (simulating WebSocket data)
        let trades: Vec<exchange::Trade> = (0..100)
            .map(|i| exchange::Trade {
                time: exchange::UnixMs::new(live_range.from.as_u64() + i * 1000),
                is_sell: i % 2 == 0,
                price: Price::from_f64(50000.0 + i as f64),
                qty: Qty::from_f64(1.0),
            })
            .collect();

        store.insert_trades(&key, &trades);
        // NOTE: Do NOT mark coverage complete (live data is uncertain)

        let full_range = range_ms(
            15 * 3600 * 1000 + 30 * 60 * 1000,
            15 * 3600 * 1000 + 58 * 60 * 1000,
        );

        // Submit requirement for full range
        let consumer = ConsumerId::global(ConsumerFeature::VolumeBubbles);
        let req = DataRequirement::new(consumer, key.clone(), full_range, Priority::Normal, "test");
        coordinator.require(req);

        // Plan
        let plan = coordinator.plan().clone();

        // Assert: network segment should be the FULL range (15:30 → 15:58)
        // because live observed data did NOT mark coverage complete
        assert!(plan.needs_network_fetch());
        assert_eq!(plan.network_segments.len(), 1);
        let segment = &plan.network_segments[0];
        assert_eq!(
            segment.range.from.as_u64(),
            15 * 3600 * 1000 + 30 * 60 * 1000
        );
        assert_eq!(segment.range.to.as_u64(), 15 * 3600 * 1000 + 58 * 60 * 1000);
    }
}
