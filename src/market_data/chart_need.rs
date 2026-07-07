//! Chart-level data needs declaration.
//!
//! Charts declare what data they need via [`ChartDataNeed`]. The chart does
//! not create worker-level `FetchRange` / `FetchSpec` values from this type.
//! Conversion into coordinator requirements and compatibility worker metadata
//! is owned by `market_data::bridge` / `market_data::runtime`.
//!
//! # Architecture
//!
//! ```text
//! KlineChart::data_requirements()
//!         │
//!         │ returns Vec<ChartDataNeed>
//!         ↓
//! Action::RequestMarketDataNeeds
//!         ↓
//! Dashboard → MarketDataRuntime
//!         ↓
//! bridge converts ChartDataNeed → DataRequirement
//!         ↓
//! Coordinator plans cache/network and synthesizes worker FetchSpecs
//! ```

use exchange::UnixMs;
use exchange::unit::PriceStep;

/// What a chart needs from the market data layer.
///
/// This is the chart's declarative output: "I need this data".
/// The runtime/coordinator decides how to serve it (cache vs network).
///
/// # Ordering
///
/// Variants are listed in typical priority order. `data_requirements()`
/// returns them sorted by urgency (klines first, then trades, then indicators).
#[derive(Debug, Clone)]
pub enum ChartDataNeed {
    /// Kline data for the given time range.
    /// Priority: high — needed for chart rendering.
    Klines { from: UnixMs, to: UnixMs },

    /// Raw trades for footprint rendering.
    /// Priority: normal — needed for footprint chart mode.
    Trades { from: UnixMs, to: UnixMs },

    /// Raw trades for CVD/delta hydration (inserted into kline chart).
    /// Priority: normal — needed when volume bubbles are enabled on Candles.
    TradeHydration { from: UnixMs, to: UnixMs },

    /// Open interest data.
    /// Priority: normal — needed when OI indicator is enabled.
    OpenInterest { from: UnixMs, to: UnixMs },

    /// Volume bubble summaries (derived from raw trades).
    /// Priority: low — visual enhancement, not blocking.
    Bubbles {
        from: UnixMs,
        to: UnixMs,
        timeframe_ms: u64,
        price_step: PriceStep,
        max_candidates_per_candle: usize,
    },
}

impl ChartDataNeed {
    /// Short label for logging.
    pub fn label(&self) -> &'static str {
        match self {
            ChartDataNeed::Klines { .. } => "Klines",
            ChartDataNeed::Trades { .. } => "Trades",
            ChartDataNeed::TradeHydration { .. } => "TradeHydration",
            ChartDataNeed::OpenInterest { .. } => "OpenInterest",
            ChartDataNeed::Bubbles { .. } => "Bubbles",
        }
    }

    /// The time range of this need.
    pub fn range(&self) -> (UnixMs, UnixMs) {
        match self {
            ChartDataNeed::Klines { from, to }
            | ChartDataNeed::Trades { from, to }
            | ChartDataNeed::TradeHydration { from, to }
            | ChartDataNeed::OpenInterest { from, to }
            | ChartDataNeed::Bubbles { from, to, .. } => (*from, *to),
        }
    }
}

impl std::fmt::Display for ChartDataNeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (from, to) = self.range();
        write!(f, "{}({}-{})", self.label(), from.as_u64(), to.as_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chart_need_label_and_range() {
        let need = ChartDataNeed::Klines {
            from: UnixMs::new(100),
            to: UnixMs::new(200),
        };

        assert_eq!(need.label(), "Klines");
        assert_eq!(need.range(), (UnixMs::new(100), UnixMs::new(200)));
        assert_eq!(need.to_string(), "Klines(100-200)");
    }

    #[test]
    fn test_bubble_need_is_declarative() {
        let need = ChartDataNeed::Bubbles {
            from: UnixMs::new(100),
            to: UnixMs::new(200),
            timeframe_ms: 60_000,
            price_step: PriceStep::default(),
            max_candidates_per_candle: 5,
        };

        assert_eq!(need.label(), "Bubbles");
        assert_eq!(need.range(), (UnixMs::new(100), UnixMs::new(200)));
    }
}
