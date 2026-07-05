//! Data requirement types for the market data layer.
//!
//! Charts, indicators, and derived data engines declare their data needs
//! as `DataRequirement` instances. The `MarketDataCoordinator` collects
//! these requirements, deduplicates them, and plans efficient fetches.

use super::key::MarketDataKey;
use super::range::MarketDataRange;
use uuid::Uuid;

/// Unique identifier for a data consumer (chart, indicator, or derived engine).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConsumerId {
    /// The pane that owns this consumer (if applicable)
    pub pane_id: Option<Uuid>,
    /// The specific feature requesting data
    pub feature: ConsumerFeature,
}

impl ConsumerId {
    /// Create a consumer ID for a pane-scoped feature.
    pub fn pane(pane_id: Uuid, feature: ConsumerFeature) -> Self {
        Self {
            pane_id: Some(pane_id),
            feature,
        }
    }

    /// Create a consumer ID for a global feature (not tied to a specific pane).
    pub fn global(feature: ConsumerFeature) -> Self {
        Self {
            pane_id: None,
            feature,
        }
    }

    /// Display format for logging (e.g., "VolumeBubbles:pane=abcd1234")
    pub fn display_id(&self) -> String {
        match self.pane_id {
            Some(id) => format!(
                "{}:pane={}",
                self.feature,
                crate::connector::fetcher::short_id(id)
            ),
            None => self.feature.to_string(),
        }
    }
}

impl std::fmt::Display for ConsumerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.display_id())
    }
}

/// The feature or purpose for which data is requested.
///
/// Used for grouping requirements, logging, and UI display.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConsumerFeature {
    /// Standard kline chart display
    ChartKlines,
    /// Footprint chart (requires raw trades)
    Footprint,
    /// Volume bubble visualization
    VolumeBubbles,
    /// Trade hydration: insert raw trades into KlineChart for CVD/delta indicators
    TradeHydration,
    /// Session Volume Profile (future)
    SessionVolumeProfile,
    /// VWAP indicator (future)
    #[allow(clippy::upper_case_acronyms)]
    VWAP,
    /// Open Interest indicator
    OpenInterest,
    /// Comparison chart overlay
    ComparisonChart,
    /// Historical backfill (e.g., after WS disconnect)
    Backfill,
}

impl ConsumerFeature {
    /// Display name for UI and logging.
    pub fn display_name(&self) -> &'static str {
        match self {
            ConsumerFeature::ChartKlines => "Chart Klines",
            ConsumerFeature::Footprint => "Footprint",
            ConsumerFeature::VolumeBubbles => "Volume Bubbles",
            ConsumerFeature::TradeHydration => "Trade Hydration",
            ConsumerFeature::SessionVolumeProfile => "Session Volume Profile",
            ConsumerFeature::VWAP => "VWAP",
            ConsumerFeature::OpenInterest => "Open Interest",
            ConsumerFeature::ComparisonChart => "Comparison Chart",
            ConsumerFeature::Backfill => "Backfill",
        }
    }

    /// Short name for compact logging.
    pub fn short_name(&self) -> &'static str {
        match self {
            ConsumerFeature::ChartKlines => "Klines",
            ConsumerFeature::Footprint => "Footprint",
            ConsumerFeature::VolumeBubbles => "Bubbles",
            ConsumerFeature::TradeHydration => "TradeHydration",
            ConsumerFeature::SessionVolumeProfile => "SVP",
            ConsumerFeature::VWAP => "VWAP",
            ConsumerFeature::OpenInterest => "OI",
            ConsumerFeature::ComparisonChart => "Comparison",
            ConsumerFeature::Backfill => "Backfill",
        }
    }

    /// Check if this feature requires raw trades.
    pub fn requires_trades(&self) -> bool {
        matches!(
            self,
            ConsumerFeature::Footprint
                | ConsumerFeature::VolumeBubbles
                | ConsumerFeature::TradeHydration
                | ConsumerFeature::SessionVolumeProfile
                | ConsumerFeature::VWAP
        )
    }

    /// Check if this feature requires klines.
    pub fn requires_klines(&self) -> bool {
        matches!(
            self,
            ConsumerFeature::ChartKlines | ConsumerFeature::ComparisonChart
        )
    }

    /// Check if this feature requires open interest data.
    pub fn requires_open_interest(&self) -> bool {
        matches!(self, ConsumerFeature::OpenInterest)
    }
}

impl std::fmt::Display for ConsumerFeature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.short_name())
    }
}

/// Priority level for data requirements.
///
/// Higher priority requirements are fetched first when resources are limited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Priority {
    /// Background or speculative fetches
    Low,
    /// Normal priority (default)
    #[default]
    Normal,
    /// User-visible data that should be fetched quickly
    High,
    /// Blocking fetch that prevents rendering
    Critical,
}

impl std::fmt::Display for Priority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Priority::Low => write!(f, "Low"),
            Priority::Normal => write!(f, "Normal"),
            Priority::High => write!(f, "High"),
            Priority::Critical => write!(f, "Critical"),
        }
    }
}

/// A data requirement declared by a chart, indicator, or derived engine.
///
/// Requirements are collected by the `MarketDataCoordinator` which deduplicates
/// overlapping requests and plans efficient fetches.
#[derive(Debug, Clone)]
pub struct DataRequirement {
    /// The consumer requesting this data
    pub consumer: ConsumerId,
    /// The market data key identifying the stream
    pub key: MarketDataKey,
    /// The time range needed
    pub range: MarketDataRange,
    /// Priority level
    pub priority: Priority,
    /// Human-readable reason for logging (e.g., "visible range", "session trades")
    pub reason: String,
}

impl DataRequirement {
    /// Create a new data requirement.
    pub fn new(
        consumer: ConsumerId,
        key: MarketDataKey,
        range: MarketDataRange,
        priority: Priority,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            consumer,
            key,
            range,
            priority,
            reason: reason.into(),
        }
    }

    /// Create a high-priority requirement for visible range data.
    pub fn visible_range(consumer: ConsumerId, key: MarketDataKey, range: MarketDataRange) -> Self {
        Self::new(consumer, key, range, Priority::High, "visible range")
    }

    /// Create a normal-priority requirement for session data.
    pub fn session(consumer: ConsumerId, key: MarketDataKey, range: MarketDataRange) -> Self {
        Self::new(consumer, key, range, Priority::Normal, "session trades")
    }

    /// Create a low-priority requirement for backfill data.
    pub fn backfill(consumer: ConsumerId, key: MarketDataKey, range: MarketDataRange) -> Self {
        Self::new(consumer, key, range, Priority::Low, "backfill")
    }

    /// Log format: "MARKETDATA Requirement | consumer=VolumeBubbles key=Trades:BinanceLinear:BTCUSDT range=15:30:00→15:58:00"
    pub fn log_format(&self) -> String {
        format!(
            "consumer={} key={} range={} priority={} reason={}",
            self.consumer.feature,
            self.key,
            self.range.format_display(),
            self.priority,
            self.reason
        )
    }
}

impl std::fmt::Display for DataRequirement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.log_format())
    }
}

/// A group of requirements for the same market data key.
///
/// Used internally by the coordinator to merge overlapping range requests
/// from multiple consumers.
#[derive(Debug)]
pub(crate) struct RequirementGroup {
    /// The market data key
    pub key: MarketDataKey,
    /// All requirements for this key
    pub requirements: Vec<DataRequirement>,
    /// Merged range covering all requirements
    pub merged_range: MarketDataRange,
    /// All consumers in this group
    pub consumers: Vec<ConsumerId>,
    /// Highest priority in the group
    pub max_priority: Priority,
}

impl RequirementGroup {
    /// Create a new group from a single requirement.
    pub fn from_requirement(req: DataRequirement) -> Self {
        let merged_range = req.range;
        let consumers = vec![req.consumer.clone()];
        let max_priority = req.priority;

        Self {
            key: req.key.clone(),
            requirements: vec![req],
            merged_range,
            consumers,
            max_priority,
        }
    }

    /// Create groups from a list of requirements.
    pub fn from_requirements(requirements: Vec<DataRequirement>) -> Vec<Self> {
        let mut groups: rustc_hash::FxHashMap<super::key::MarketDataKey, Self> =
            rustc_hash::FxHashMap::default();

        for req in requirements {
            let key = req.key.clone();
            groups
                .entry(key)
                .and_modify(|group| group.add(req.clone()))
                .or_insert_with(|| Self::from_requirement(req));
        }

        let mut result: Vec<Self> = groups.into_values().collect();
        result.sort_by_key(|group| std::cmp::Reverse(group.max_priority));
        result
    }

    /// Add a requirement to this group, merging the range.
    pub fn add(&mut self, req: DataRequirement) {
        // Merge ranges
        if let Some(merged) = self.merged_range.merge(&req.range) {
            self.merged_range = merged;
        } else {
            // If not adjacent, extend to cover both
            self.merged_range = MarketDataRange::new_unchecked(
                self.merged_range.from.min(req.range.from),
                self.merged_range.to.max(req.range.to),
            );
        }

        // Track consumer if new
        if !self.consumers.contains(&req.consumer) {
            self.consumers.push(req.consumer.clone());
        }

        // Update priority
        self.max_priority = self.max_priority.max(req.priority);

        self.requirements.push(req);
    }

    /// Log format for the group.
    #[allow(dead_code)] // SVP readiness — used for structured logging in requirement groups
    pub fn log_format(&self) -> String {
        format!(
            "key={} range={} consumers={} priority={}",
            self.key,
            self.merged_range.format_display(),
            self.consumer_names(),
            self.max_priority
        )
    }

    /// Get comma-separated consumer names.
    pub fn consumer_names(&self) -> String {
        self.consumers
            .iter()
            .map(|c| c.feature.short_name().to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Group a list of requirements by their market data key.
///
/// Requirements with the same key are merged into a single group with
/// a combined range.
#[allow(dead_code)] // SVP readiness — alternative to RequirementGroup::from_requirements
pub(crate) fn group_requirements(requirements: Vec<DataRequirement>) -> Vec<RequirementGroup> {
    use rustc_hash::FxHashMap;

    let mut groups: FxHashMap<super::key::MarketDataKey, RequirementGroup> = FxHashMap::default();

    for req in requirements {
        let key = req.key.clone();
        groups
            .entry(key)
            .and_modify(|group| group.add(req.clone()))
            .or_insert_with(|| RequirementGroup::from_requirement(req));
    }

    // Sort by priority (highest first)
    let mut result: Vec<RequirementGroup> = groups.into_values().collect();
    result.sort_by_key(|group| std::cmp::Reverse(group.max_priority));

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::key::{MarketKind, Symbol, Venue};
    use exchange::UnixMs;

    fn make_test_key() -> MarketDataKey {
        MarketDataKey::trades(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
        )
    }

    #[test]
    fn test_consumer_id_display() {
        let pane_id = Uuid::parse_str("12345678-1234-5678-1234-567812345678").unwrap();
        let consumer = ConsumerId::pane(pane_id, ConsumerFeature::VolumeBubbles);
        assert!(consumer.display_id().contains("Bubbles"));
        assert!(consumer.display_id().contains("pane="));
    }

    #[test]
    fn test_consumer_feature_properties() {
        assert!(ConsumerFeature::VolumeBubbles.requires_trades());
        assert!(!ConsumerFeature::VolumeBubbles.requires_klines());
        assert!(ConsumerFeature::ChartKlines.requires_klines());
        assert!(!ConsumerFeature::ChartKlines.requires_trades());
        assert!(ConsumerFeature::OpenInterest.requires_open_interest());
        assert!(ConsumerFeature::TradeHydration.requires_trades());
        assert!(!ConsumerFeature::TradeHydration.requires_klines());
    }

    #[test]
    fn test_priority_ordering() {
        assert!(Priority::Critical > Priority::High);
        assert!(Priority::High > Priority::Normal);
        assert!(Priority::Normal > Priority::Low);
    }

    #[test]
    fn test_requirement_group() {
        let key = make_test_key();
        let range1 = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();
        let range2 = MarketDataRange::new(UnixMs::new(150), UnixMs::new(300)).unwrap();

        let req1 = DataRequirement::new(
            ConsumerId::global(ConsumerFeature::VolumeBubbles),
            key.clone(),
            range1,
            Priority::Normal,
            "test1",
        );

        let req2 = DataRequirement::new(
            ConsumerId::global(ConsumerFeature::SessionVolumeProfile),
            key.clone(),
            range2,
            Priority::High,
            "test2",
        );

        let mut group = RequirementGroup::from_requirement(req1);
        group.add(req2);

        assert_eq!(group.consumers.len(), 2);
        assert_eq!(group.max_priority, Priority::High);
        assert_eq!(group.merged_range.from, UnixMs::new(100));
        assert_eq!(group.merged_range.to, UnixMs::new(300));
    }

    #[test]
    fn test_group_requirements() {
        let key1 = make_test_key();
        let key2 = MarketDataKey::klines(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
            exchange::Timeframe::M5,
        );

        let range = MarketDataRange::new(UnixMs::new(100), UnixMs::new(200)).unwrap();

        let requirements = vec![
            DataRequirement::new(
                ConsumerId::global(ConsumerFeature::VolumeBubbles),
                key1.clone(),
                range,
                Priority::Normal,
                "test",
            ),
            DataRequirement::new(
                ConsumerId::global(ConsumerFeature::SessionVolumeProfile),
                key1.clone(),
                range,
                Priority::High,
                "test",
            ),
            DataRequirement::new(
                ConsumerId::global(ConsumerFeature::ChartKlines),
                key2.clone(),
                range,
                Priority::Normal,
                "test",
            ),
        ];

        let groups = group_requirements(requirements);
        assert_eq!(groups.len(), 2);

        // Higher priority group should be first
        assert_eq!(groups[0].max_priority, Priority::High);
    }
}
