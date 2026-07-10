//! Exchange-agnostic planning and lifecycle for historical market data.
//!
//! Charts and indicators describe *what* they need through [`DataRequirement`].
//! They do not decide whether a REST call is necessary. [`MarketDataCoordinator`]
//! owns coverage, in-flight de-duplication and retry cooldowns; exchange adapters
//! remain responsible only for executing the resulting requests.

#![allow(dead_code)]

use exchange::{TickerInfo, UnixMs, adapter::StreamKind};
use rustc_hash::FxHashMap;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeRange {
    pub from: UnixMs,
    pub to: UnixMs,
}

impl TimeRange {
    pub fn new(from: UnixMs, to: UnixMs) -> Option<Self> {
        (from < to).then_some(Self { from, to })
    }

    pub fn contains(self, other: Self) -> bool {
        self.from <= other.from && self.to >= other.to
    }

    fn overlaps_or_touches(self, other: Self) -> bool {
        self.from <= other.to && other.from <= self.to
    }
}

/// Raw datasets that can be shared by any number of consumers.
///
/// VWAP and volume profiles intentionally depend on `Trades`; they must not
/// acquire indicator-specific fetch variants. `Klines` is suitable for a
/// lower fidelity/cheap VWAP mode when a study explicitly requests it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataSet {
    Klines { timeframe_ms: u64 },
    Trades,
    OpenInterest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefreshPolicy {
    Historical,
    LiveTail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataRequirement {
    pub dataset: DataSet,
    pub range: TimeRange,
    pub refresh: RefreshPolicy,
}

impl DataRequirement {
    pub fn trades(range: TimeRange) -> Self {
        Self {
            dataset: DataSet::Trades,
            range,
            refresh: RefreshPolicy::Historical,
        }
    }

    pub fn with_stream(self, stream: StreamKind) -> Option<(RequestKey, Self)> {
        Some((RequestKey::new(stream, self.dataset)?, self))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestKey {
    pub stream: StreamKind,
    pub dataset: DataSet,
}

impl RequestKey {
    pub fn new(stream: StreamKind, dataset: DataSet) -> Option<Self> {
        let compatible = matches!(
            (stream, dataset),
            (StreamKind::Kline { .. }, DataSet::Klines { .. })
                | (StreamKind::Kline { .. }, DataSet::OpenInterest)
                | (StreamKind::Trades { .. }, DataSet::Trades)
        );
        compatible.then_some(Self { stream, dataset })
    }

    pub fn ticker_info(&self) -> TickerInfo {
        self.stream.ticker_info()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverageKind {
    Data,
    /// The venue returned no records. This still covers the interval and
    /// prevents an endless refetch loop at illiquid tails.
    Empty,
}

#[derive(Debug, Default, Clone)]
pub struct CoverageLedger {
    ranges: FxHashMap<RequestKey, Vec<TimeRange>>,
}

impl CoverageLedger {
    pub fn mark(&mut self, key: RequestKey, range: TimeRange, _kind: CoverageKind) {
        let ranges = self.ranges.entry(key).or_default();
        ranges.push(range);
        ranges.sort_by_key(|r| r.from);
        let mut merged: Vec<TimeRange> = Vec::with_capacity(ranges.len());
        for next in ranges.drain(..) {
            if let Some(last) = merged.last_mut()
                && last.overlaps_or_touches(next)
            {
                last.to = last.to.max(next.to);
            } else {
                merged.push(next);
            }
        }
        *ranges = merged;
    }

    pub fn gaps(&self, key: RequestKey, requested: TimeRange) -> Vec<TimeRange> {
        let mut cursor = requested.from;
        let mut gaps = Vec::new();
        if let Some(covered) = self.ranges.get(&key) {
            for range in covered {
                if range.to <= cursor || range.from >= requested.to {
                    continue;
                }
                if range.from > cursor {
                    gaps.push(TimeRange {
                        from: cursor,
                        to: range.from.min(requested.to),
                    });
                }
                cursor = cursor.max(range.to);
                if cursor >= requested.to {
                    break;
                }
            }
        }
        if cursor < requested.to {
            gaps.push(TimeRange {
                from: cursor,
                to: requested.to,
            });
        }
        gaps
    }

    pub fn is_covered(&self, key: RequestKey, range: TimeRange) -> bool {
        self.gaps(key, range).is_empty()
    }

    pub fn clear(&mut self) {
        self.ranges.clear();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannedFetch {
    pub id: Uuid,
    pub key: RequestKey,
    pub range: TimeRange,
    pub generation: u64,
}

#[derive(Debug, Clone)]
struct InFlight {
    fetch: PlannedFetch,
    started_at_ms: u64,
}

#[derive(Debug, Clone)]
struct FailedRange {
    key: RequestKey,
    range: TimeRange,
    retry_at_ms: u64,
    attempts: u32,
}

/// Central, pane-independent admission controller for historical data.
#[derive(Debug, Default)]
pub struct MarketDataCoordinator {
    coverage: CoverageLedger,
    in_flight: FxHashMap<Uuid, InFlight>,
    failed: Vec<FailedRange>,
    generation: u64,
}

impl MarketDataCoordinator {
    pub fn coverage(&self) -> &CoverageLedger {
        &self.coverage
    }

    /// Plans uncovered, non-inflight gaps. Adjacent consumers therefore share
    /// transport work even when their requested windows are not identical.
    pub fn plan(&mut self, key: RequestKey, range: TimeRange, now_ms: u64) -> Vec<PlannedFetch> {
        self.expire(now_ms);
        let mut available = self.coverage.gaps(key, range);
        for active in self.in_flight.values().filter(|f| f.fetch.key == key) {
            available = subtract_many(available, active.fetch.range);
        }
        for failed in self
            .failed
            .iter()
            .filter(|f| f.key == key && now_ms < f.retry_at_ms)
        {
            available = subtract_many(available, failed.range);
        }
        available
            .into_iter()
            .map(|range| {
                let fetch = PlannedFetch {
                    id: Uuid::new_v4(),
                    key,
                    range,
                    generation: self.generation,
                };
                self.in_flight.insert(
                    fetch.id,
                    InFlight {
                        fetch,
                        started_at_ms: now_ms,
                    },
                );
                fetch
            })
            .collect()
    }

    pub fn complete(&mut self, id: Uuid, kind: CoverageKind) -> bool {
        let Some(active) = self.in_flight.remove(&id) else {
            return false;
        };
        if active.fetch.generation != self.generation {
            return false;
        }
        self.coverage
            .mark(active.fetch.key, active.fetch.range, kind);
        self.failed
            .retain(|f| !(f.key == active.fetch.key && f.range == active.fetch.range));
        true
    }

    pub fn fail(&mut self, id: Uuid, now_ms: u64) -> bool {
        let Some(active) = self.in_flight.remove(&id) else {
            return false;
        };
        let previous = self
            .failed
            .iter()
            .find(|f| f.key == active.fetch.key && f.range == active.fetch.range)
            .map_or(0, |f| f.attempts);
        let attempts = previous.saturating_add(1);
        let delay = 2_500u64.saturating_mul(1u64 << attempts.saturating_sub(1).min(5));
        self.failed
            .retain(|f| !(f.key == active.fetch.key && f.range == active.fetch.range));
        self.failed.push(FailedRange {
            key: active.fetch.key,
            range: active.fetch.range,
            retry_at_ms: now_ms.saturating_add(delay),
            attempts,
        });
        true
    }

    pub fn reset(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.in_flight.clear();
        self.failed.clear();
    }

    fn expire(&mut self, now_ms: u64) {
        const TIMEOUT: u64 = 90_000;
        self.in_flight
            .retain(|_, f| now_ms.saturating_sub(f.started_at_ms) < TIMEOUT);
        self.failed.retain(|f| {
            now_ms
                < f.retry_at_ms
                    .saturating_add(Duration::from_secs(300).as_millis() as u64)
        });
    }
}

fn subtract_many(input: Vec<TimeRange>, cover: TimeRange) -> Vec<TimeRange> {
    input
        .into_iter()
        .flat_map(|range| {
            if cover.to <= range.from || cover.from >= range.to {
                return vec![range];
            }
            let mut result = Vec::with_capacity(2);
            if cover.from > range.from {
                result.push(TimeRange {
                    from: range.from,
                    to: cover.from.min(range.to),
                });
            }
            if cover.to < range.to {
                result.push(TimeRange {
                    from: cover.to.max(range.from),
                    to: range.to,
                });
            }
            result
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(from: u64, to: u64) -> TimeRange {
        TimeRange::new(UnixMs::new(from), UnixMs::new(to)).unwrap()
    }

    #[test]
    fn subtracts_overlap_into_two_gaps() {
        assert_eq!(
            subtract_many(vec![r(50, 350)], r(100, 300)),
            vec![r(50, 100), r(300, 350)]
        );
    }

    #[test]
    fn validates_ranges() {
        assert!(TimeRange::new(UnixMs::new(1), UnixMs::new(2)).is_some());
        assert!(TimeRange::new(UnixMs::new(2), UnixMs::new(2)).is_none());
    }
}
