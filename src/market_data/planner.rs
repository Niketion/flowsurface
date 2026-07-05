//! Data load plan computation.
//!
//! `DataLoadPlan` represents the result of analyzing data requirements
//! against current coverage, computing which segments need to be fetched
//! from the network and which are already cached.

use super::coverage::CoverageLedger;
use super::job::FetchJob;
use super::key::MarketDataKey;
use super::range::MarketDataRange;
use super::requirement::{ConsumerId, RequirementGroup};

/// A segment of data that is already cached.
#[derive(Debug, Clone)]
pub struct CachedSegment {
    /// The market data key
    pub key: MarketDataKey,
    /// The time range
    pub range: MarketDataRange,
    /// Number of records in this segment
    pub records: usize,
}

/// A segment of data that needs to be fetched from the network.
#[derive(Debug, Clone)]
pub struct NetworkSegment {
    /// The market data key
    pub key: MarketDataKey,
    /// The time range to fetch
    pub range: MarketDataRange,
    /// Consumers that need this data
    pub consumers: Vec<ConsumerId>,
}

/// A planned derived data computation.
#[derive(Debug, Clone)]
pub struct DerivedPlan {
    /// The source market data key
    pub source_key: MarketDataKey,
    /// The derived data kind
    pub kind: DerivedKind,
    /// Consumers that need this derived data
    pub consumers: Vec<ConsumerId>,
}

/// The kind of derived data to compute.
#[derive(Debug, Clone)]
pub enum DerivedKind {
    /// Volume bubbles from raw trades
    VolumeBubbles {
        timeframe_ms: u64,
        price_step: exchange::unit::PriceStep,
        max_candidates: usize,
    },
    // Future: SessionVolumeProfile, VWAP, CVD, Footprint
}

/// The complete data load plan.
///
/// Computed by analyzing requirements against coverage, this plan
/// specifies which segments need network fetches and which are cached.
#[derive(Debug, Clone)]
pub struct DataLoadPlan {
    /// Segments that are already cached and ready to use
    pub cached_segments: Vec<CachedSegment>,
    /// Segments that need to be fetched from the network
    pub network_segments: Vec<NetworkSegment>,
    /// Derived data computations to perform after raw data is available
    pub derived_plans: Vec<DerivedPlan>,
    /// All consumers involved in this plan
    pub consumers: Vec<ConsumerId>,
    /// Total number of records to fetch from network
    pub total_network_records_estimate: usize,
}

impl DataLoadPlan {
    /// Create an empty plan.
    pub fn empty() -> Self {
        Self {
            cached_segments: Vec::new(),
            network_segments: Vec::new(),
            derived_plans: Vec::new(),
            consumers: Vec::new(),
            total_network_records_estimate: 0,
        }
    }

    /// Check if this plan has any network fetches.
    pub fn needs_network_fetch(&self) -> bool {
        !self.network_segments.is_empty()
    }

    /// Check if this plan has any cached data.
    pub fn has_cached_data(&self) -> bool {
        !self.cached_segments.is_empty()
    }

    /// Check if this plan is empty (no work to do).
    pub fn is_empty(&self) -> bool {
        self.network_segments.is_empty() && self.derived_plans.is_empty()
    }

    /// Number of network fetches needed.
    pub fn fetch_count(&self) -> usize {
        self.network_segments.len()
    }

    /// Log-format representation.
    pub fn log_format(&self) -> String {
        format!(
            "cached={} network={} derived={} consumers={}",
            self.cached_segments.len(),
            self.network_segments.len(),
            self.derived_plans.len(),
            self.consumers.len()
        )
    }

    /// Detailed runtime summary for verification logging.
    pub fn runtime_summary(&self, active_jobs: usize) -> String {
        let cached_ranges: Vec<String> = self
            .cached_segments
            .iter()
            .map(|s| s.range.format_display())
            .collect();
        let network_ranges: Vec<String> = self
            .network_segments
            .iter()
            .map(|s| s.range.format_display())
            .collect();
        let consumer_names: Vec<String> = self
            .consumers
            .iter()
            .map(|c| c.feature.short_name().to_string())
            .collect();
        format!(
            "cached=[{}] network=[{}] active_jobs={} consumers=[{}]",
            cached_ranges.join(", "),
            network_ranges.join(", "),
            active_jobs,
            consumer_names.join(", ")
        )
    }
}

/// Compute a data load plan from requirement groups and current coverage.
///
/// For each requirement group, checks coverage and computes missing ranges
/// that need network fetches.
pub fn compute_plan(groups: Vec<RequirementGroup>, coverage: &CoverageLedger) -> DataLoadPlan {
    let mut plan = DataLoadPlan::empty();
    let mut all_consumers: Vec<ConsumerId> = Vec::new();

    for group in groups {
        // Compute missing ranges for this group's key and merged range
        let missing = coverage.compute_missing(&group.key, group.merged_range);

        // If fully covered, add to cached segments
        if missing.is_empty() {
            plan.cached_segments.push(CachedSegment {
                key: group.key.clone(),
                range: group.merged_range,
                records: coverage.total_records(&group.key),
            });

            log::info!(
                target: "marketdata",
                "MARKETDATA CacheHit | key={} range={} records={}",
                group.key.display_key(),
                group.merged_range.format_display(),
                coverage.total_records(&group.key)
            );
        } else {
            let missing_count = missing.len();

            // Add network segments for missing ranges
            for missing_range in missing {
                plan.network_segments.push(NetworkSegment {
                    key: group.key.clone(),
                    range: missing_range,
                    consumers: group.consumers.clone(),
                });
            }

            // Also add any cached portions
            let complete_ranges = coverage.complete_ranges(&group.key);
            for cached_range in complete_ranges {
                if group.merged_range.overlaps(&cached_range)
                    && let Some(overlap) = group.merged_range.intersection(&cached_range)
                {
                    plan.cached_segments.push(CachedSegment {
                        key: group.key.clone(),
                        range: overlap,
                        records: 0, // Approximate
                    });
                }
            }

            log::info!(
                target: "marketdata",
                "MARKETDATA Plan | key={} cached_range={} network_ranges={} consumers={}",
                group.key.display_key(),
                group.merged_range.format_display(),
                missing_count,
                group.consumer_names()
            );
        }

        // Collect consumers
        for consumer in &group.consumers {
            if !all_consumers.contains(consumer) {
                all_consumers.push(consumer.clone());
            }
        }
    }

    plan.consumers = all_consumers;
    plan.total_network_records_estimate = plan.network_segments.len() * 5000; // rough estimate

    plan
}

/// Create fetch jobs from a data load plan.
///
/// Each network segment becomes a fetch job, with consumers deduplicated.
#[allow(dead_code)] // SVP readiness — may be used when coordinator owns job creation directly
pub fn plan_to_jobs(plan: &DataLoadPlan) -> Vec<FetchJob> {
    plan.network_segments
        .iter()
        .map(|segment| {
            FetchJob::new(
                segment.key.clone(),
                segment.range,
                segment.consumers.clone(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::key::{MarketKind, Symbol, Venue};
    use crate::market_data::requirement::{ConsumerFeature, DataRequirement, Priority};
    use exchange::UnixMs;

    fn make_trade_key() -> MarketDataKey {
        MarketDataKey::trades(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
        )
    }

    #[test]
    fn test_empty_coverage_needs_fetch() {
        let key = make_trade_key();
        let range = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();
        let consumer = ConsumerId::global(ConsumerFeature::VolumeBubbles);

        let req = DataRequirement::new(consumer, key, range, Priority::Normal, "test");
        let groups = super::super::requirement::group_requirements(vec![req]);

        let coverage = CoverageLedger::new();
        let plan = compute_plan(groups, &coverage);

        assert!(plan.needs_network_fetch());
        assert_eq!(plan.network_segments.len(), 1);
        assert!(!plan.has_cached_data());
    }

    #[test]
    fn test_full_coverage_no_fetch() {
        let key = make_trade_key();
        let range = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();
        let consumer = ConsumerId::global(ConsumerFeature::VolumeBubbles);

        let mut coverage = CoverageLedger::new();
        coverage.mark_complete(key.clone(), range, 500);

        let req = DataRequirement::new(consumer, key, range, Priority::Normal, "test");
        let groups = super::super::requirement::group_requirements(vec![req]);

        let plan = compute_plan(groups, &coverage);

        assert!(!plan.needs_network_fetch());
        assert!(plan.has_cached_data());
    }
}
