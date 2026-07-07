//! Centralized Market Data Layer
//!
//! This module provides the centralized infrastructure for market data planning,
//! caching, deduplication, and derived data computation. It does NOT replace
//! the low-level fetcher (`connector::fetcher`); instead it sits between
//! charts and the fetcher as a planning/caching orchestrator.
//!
//! # Architecture (Bridge Pattern)
//!
//! ```text
//! KlineChart
//!         │
//!         │ emits ChartDataNeed (Phase 2 path)
//!         ↓
//! Dashboard::route_market_data_needs_through_market_data()
//!         │
//!         ├── bridge converts ChartDataNeed → DataRequirement
//!         │
//!         ├── legacy charts/indicators may still emit FetchSpec/FetchRange
//!         │   through route_fetch_specs_through_market_data()
//!         │
//!         ↓
//! MarketDataCoordinator
//!   • plans cached vs network segments (CoverageLedger)
//!   • deduplicates active jobs
//!   • serves cached data (LocalMarketCache / SQLite)
//!   • derives bubble summaries from raw trades
//!   • tracks progress for unified UI
//!         │
//!         │ network jobs delegated via forward_legacy_fetches()
//!         ↓
//! connector::fetcher (REST/network worker)
//!         │
//!         ↓
//! Dashboard dispatches FetchedData → panes
//!
//! Live WebSocket data: coordinator.feed_trades/feed_klines updates store;
//! LiveDataAdapter persists to LocalMarketCache.
//! ```
//!
//! # Key Concepts
//!
//! - [`MarketDataKey`]: Uniquely identifies a market data stream
//! - [`DataRequirement`]: Declares what data a consumer needs
//! - [`MarketDataCoordinator`]: Planning, cache, dedup, progress
//! - [`CoverageLedger`]: Tracks which time ranges have been fetched
//! - [`MarketDataStore`]: In-memory runtime store for raw data
//! - [`LocalMarketCache`]: SQLite persistent cache
//! - [`bridge`]: Converts old FetchRange signals to DataRequirements
//!
//! # Module Ownership
//!
//! | Concern | Owner |
//! |---------|-------|
//! | Data need declaration | `chart/kline.rs` via `ChartDataNeed` |
//! | Requirement registration + planning | `coordinator.rs` |
//! | Cache serve + persistence | `cache.rs` + `live.rs` |
//! | Active job dedup | `coordinator.rs` |
//! | Consumer completion tracking | `market_data::runtime` + dashboard chart effects bridge |
//! | Network dispatch | `connector::fetcher.rs` |
//! | Progress UI | `ui.rs` |

// Core types are defined for the centralized market data layer.
// They will be used as the architecture is implemented in phases 2-9.
// NOTE: removed #![allow(dead_code)] — dead code now flagged by compiler.

// Core types
pub mod key;
pub mod range;
pub mod requirement;
pub mod session;

// Chart-side data needs (Phase 2)
pub mod chart_need;

// Storage and caching (Phase 2 + Phase 5)
pub mod cache;
pub mod coverage;
pub mod store;

// Coordination (Phase 3)
pub mod coordinator;
pub mod job;
pub mod planner;
pub mod progress;
pub mod runtime;

// Compatibility bridge (Phase 4)
pub mod bridge;

// Live data routing (Phase 6)
pub mod live;

// Unified progress UI (Phase 9)
pub mod ui;

// Derived data (Phase 7)
pub mod derived;

// Re-export main types for convenience
// Uncomment when these types are used by other modules:
// pub use key::{MarketDataKey, MarketDataKind, MarketKind, Symbol, Venue};
// pub use range::MarketDataRange;
// pub use requirement::{ConsumerFeature, ConsumerId, DataRequirement, Priority};
// pub use session::{Session, SessionResolver};

#[cfg(test)]
mod integration_tests {
    use super::key::{MarketDataKey, MarketKind, Symbol, Venue};
    use super::range::MarketDataRange;
    use super::requirement::{ConsumerFeature, ConsumerId, DataRequirement, Priority};
    use super::session::{Session, SessionResolver};
    use exchange::UnixMs;

    #[test]
    fn test_full_requirement_flow() {
        // Create a market data key for BTCUSDT trades on Binance Linear
        let key = MarketDataKey::trades(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
        );

        // Define the time range needed (NY session)
        let date = chrono::Utc::now().date_naive();
        let range = SessionResolver::resolve(Session::NewYork, date);

        // Create a consumer
        let consumer = ConsumerId::global(ConsumerFeature::VolumeBubbles);

        // Create the requirement
        let req = DataRequirement::session(consumer, key, range);

        // Verify the requirement
        assert!(req.range.duration_ms() > 0);
        assert_eq!(req.priority, Priority::Normal);
        assert_eq!(req.consumer.feature, ConsumerFeature::VolumeBubbles);
    }

    #[test]
    fn test_multiple_consumers_same_key() {
        let key = MarketDataKey::trades(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
        );

        let range = MarketDataRange::new(UnixMs::new(1000), UnixMs::new(2000)).unwrap();

        // Multiple consumers need the same data
        let requirements = vec![
            DataRequirement::new(
                ConsumerId::global(ConsumerFeature::VolumeBubbles),
                key.clone(),
                range,
                Priority::Normal,
                "volume bubbles",
            ),
            DataRequirement::new(
                ConsumerId::global(ConsumerFeature::SessionVolumeProfile),
                key.clone(),
                range,
                Priority::High,
                "SVP",
            ),
            DataRequirement::new(
                ConsumerId::global(ConsumerFeature::VWAP),
                key.clone(),
                range,
                Priority::Normal,
                "VWAP",
            ),
        ];

        // All should have the same key
        for req in &requirements {
            assert_eq!(req.key, key);
        }
    }
}
