//! Compatibility bridge between existing RequestHandler and new MarketDataCoordinator.
//!
//! This module provides conversion functions and adapter types that allow
//! the existing code to gradually migrate to the centralized coordinator
//! without breaking existing behavior.

use super::chart_need::ChartDataNeed;
use super::key::{MarketDataKey, MarketDataKind, MarketKind, Symbol, Venue};
use super::range::MarketDataRange;
use super::requirement::{ConsumerFeature, ConsumerId, DataRequirement, Priority};
use exchange::adapter::StreamKind;
use exchange::{TickerInfo, Timeframe};
use uuid::Uuid;

/// Convert an existing `FetchRange` to a `DataRequirement`.
///
/// This allows the existing code to submit requirements through the
/// new coordinator while maintaining backward compatibility.
pub fn fetch_range_to_requirement(
    fetch_range: &crate::connector::fetcher::FetchRange,
    pane_id: Uuid,
    feature: ConsumerFeature,
    ticker_info: Option<&TickerInfo>,
    timeframe: Option<Timeframe>,
) -> Option<DataRequirement> {
    let key = fetch_range_to_key(fetch_range, ticker_info, timeframe)?;
    let range = fetch_range_to_range(fetch_range)?;

    // Note: Kline range canonicalization is done in the coordinator's execute_plan,
    // not here, so that segment tracking uses the original consumer-requested range.

    let consumer = ConsumerId::pane(pane_id, feature);
    let priority = fetch_range_priority(fetch_range);
    let reason = fetch_range_reason(fetch_range);

    Some(DataRequirement::new(consumer, key, range, priority, reason))
}

/// Convert a `FetchRange` to a `MarketDataKey`.
pub fn fetch_range_to_key(
    fetch_range: &crate::connector::fetcher::FetchRange,
    ticker_info: Option<&TickerInfo>,
    context_timeframe: Option<Timeframe>,
) -> Option<MarketDataKey> {
    let ti = ticker_info?;
    let venue = Venue::from_exchange(ti.exchange())?;
    let symbol = Symbol::new(ti.ticker.display_symbol().unwrap_or(&ti.ticker.to_string()));
    let market_type = MarketKind::from_adapter(ti.ticker.market_type());

    let kind = match fetch_range {
        crate::connector::fetcher::FetchRange::Kline(_, _) => {
            let tf = context_timeframe.unwrap_or_else(|| {
                log::warn!(
                    target: "marketdata",
                    "MARKETDATA BridgeTimeframe | default=M5 reason=no_context"
                );
                Timeframe::M5
            });
            MarketDataKind::Klines { timeframe: tf }
        }
        crate::connector::fetcher::FetchRange::OpenInterest(_, _) => {
            let tf = context_timeframe.unwrap_or_else(|| {
                log::warn!(
                    target: "marketdata",
                    "MARKETDATA BridgeTimeframe | default=M5 reason=no_context oi"
                );
                Timeframe::M5
            });
            MarketDataKind::OpenInterest { timeframe: tf }
        }
        crate::connector::fetcher::FetchRange::Trades(_, _) => MarketDataKind::Trades,
        crate::connector::fetcher::FetchRange::TradeHydration(_, _) => MarketDataKind::Trades,
        crate::connector::fetcher::FetchRange::BubbleSummary { .. } => MarketDataKind::Trades,
    };

    Some(MarketDataKey {
        venue,
        symbol,
        market_type,
        kind,
    })
}

/// Convert a `FetchRange` to a `MarketDataRange`.
pub fn fetch_range_to_range(
    fetch_range: &crate::connector::fetcher::FetchRange,
) -> Option<MarketDataRange> {
    let (from, to) = match fetch_range {
        crate::connector::fetcher::FetchRange::Kline(from, to)
        | crate::connector::fetcher::FetchRange::OpenInterest(from, to)
        | crate::connector::fetcher::FetchRange::Trades(from, to)
        | crate::connector::fetcher::FetchRange::TradeHydration(from, to) => (*from, *to),
        crate::connector::fetcher::FetchRange::BubbleSummary { from, to, .. } => (*from, *to),
    };

    MarketDataRange::new(from, to)
}

/// Get the appropriate priority for a `FetchRange`.
fn fetch_range_priority(fetch_range: &crate::connector::fetcher::FetchRange) -> Priority {
    match fetch_range {
        crate::connector::fetcher::FetchRange::Kline(_, _) => Priority::High,
        crate::connector::fetcher::FetchRange::Trades(_, _) => Priority::Normal,
        crate::connector::fetcher::FetchRange::TradeHydration(_, _) => Priority::Normal,
        crate::connector::fetcher::FetchRange::OpenInterest(_, _) => Priority::Normal,
        crate::connector::fetcher::FetchRange::BubbleSummary { .. } => Priority::Low,
    }
}

/// Get a human-readable reason for a `FetchRange`.
fn fetch_range_reason(fetch_range: &crate::connector::fetcher::FetchRange) -> &'static str {
    match fetch_range {
        crate::connector::fetcher::FetchRange::Kline(_, _) => "kline_history",
        crate::connector::fetcher::FetchRange::Trades(_, _) => "trade_history",
        crate::connector::fetcher::FetchRange::TradeHydration(_, _) => "trade_hydration",
        crate::connector::fetcher::FetchRange::OpenInterest(_, _) => "oi_history",
        crate::connector::fetcher::FetchRange::BubbleSummary { .. } => "bubble_summary",
    }
}

/// Get the appropriate `ConsumerFeature` for a `FetchRange`.
pub fn fetch_range_to_feature(
    fetch_range: &crate::connector::fetcher::FetchRange,
) -> ConsumerFeature {
    match fetch_range {
        crate::connector::fetcher::FetchRange::Kline(_, _) => ConsumerFeature::ChartKlines,
        crate::connector::fetcher::FetchRange::Trades(_, _) => ConsumerFeature::Footprint,
        crate::connector::fetcher::FetchRange::TradeHydration(_, _) => {
            ConsumerFeature::TradeHydration
        }
        crate::connector::fetcher::FetchRange::OpenInterest(_, _) => ConsumerFeature::OpenInterest,
        crate::connector::fetcher::FetchRange::BubbleSummary { .. } => {
            ConsumerFeature::VolumeBubbles
        }
    }
}

/// Convert a `StreamKind` to a `MarketDataKey`.
pub fn stream_kind_to_key(stream: &StreamKind) -> Option<MarketDataKey> {
    match stream {
        StreamKind::Kline {
            ticker_info,
            timeframe,
        } => {
            let venue = Venue::from_exchange(ticker_info.exchange())?;
            let symbol = Symbol::new(
                ticker_info
                    .ticker
                    .display_symbol()
                    .unwrap_or(&ticker_info.ticker.to_string()),
            );
            let market_type = MarketKind::from_adapter(ticker_info.ticker.market_type());

            Some(MarketDataKey {
                venue,
                symbol,
                market_type,
                kind: MarketDataKind::Klines {
                    timeframe: *timeframe,
                },
            })
        }
        StreamKind::Trades { ticker_info } => {
            let venue = Venue::from_exchange(ticker_info.exchange())?;
            let symbol = Symbol::new(
                ticker_info
                    .ticker
                    .display_symbol()
                    .unwrap_or(&ticker_info.ticker.to_string()),
            );
            let market_type = MarketKind::from_adapter(ticker_info.ticker.market_type());

            Some(MarketDataKey {
                venue,
                symbol,
                market_type,
                kind: MarketDataKind::Trades,
            })
        }
        StreamKind::Depth { ticker_info: _, .. } => {
            // Depth data is not tracked in the market data layer
            None
        }
    }
}

/// Convert a chart-declared data need into a `DataRequirement` directly.
///
/// This is the Phase 2 path: charts declare needs and the runtime/coordinator
/// decide coverage, cache and worker fetches. It intentionally avoids routing
/// through `FetchRange` except for compatibility metadata stored on pending
/// consumers.
pub fn chart_need_to_requirement(
    need: &ChartDataNeed,
    pane_id: Uuid,
    ticker_info: Option<&TickerInfo>,
    timeframe: Option<Timeframe>,
) -> Option<DataRequirement> {
    let key = chart_need_to_key(need, ticker_info, timeframe)?;
    let range = chart_need_to_range(need)?;
    let feature = chart_need_to_feature(need);
    let consumer = ConsumerId::pane(pane_id, feature);
    let priority = chart_need_priority(need);
    let reason = chart_need_reason(need);

    Some(DataRequirement::new(consumer, key, range, priority, reason))
}

pub fn chart_need_to_feature(need: &ChartDataNeed) -> ConsumerFeature {
    match need {
        ChartDataNeed::Klines { .. } => ConsumerFeature::ChartKlines,
        ChartDataNeed::Trades { .. } => ConsumerFeature::Footprint,
        ChartDataNeed::TradeHydration { .. } => ConsumerFeature::TradeHydration,
        ChartDataNeed::OpenInterest { .. } => ConsumerFeature::OpenInterest,
        ChartDataNeed::Bubbles { .. } => ConsumerFeature::VolumeBubbles,
    }
}

pub fn chart_need_to_key(
    need: &ChartDataNeed,
    ticker_info: Option<&TickerInfo>,
    context_timeframe: Option<Timeframe>,
) -> Option<MarketDataKey> {
    let ti = ticker_info?;
    let venue = Venue::from_exchange(ti.exchange())?;
    let symbol = Symbol::new(ti.ticker.display_symbol().unwrap_or(&ti.ticker.to_string()));
    let market_type = MarketKind::from_adapter(ti.ticker.market_type());

    let kind = match need {
        ChartDataNeed::Klines { .. } => MarketDataKind::Klines {
            timeframe: context_timeframe.unwrap_or(Timeframe::M5),
        },
        ChartDataNeed::OpenInterest { .. } => MarketDataKind::OpenInterest {
            timeframe: context_timeframe.unwrap_or(Timeframe::M5),
        },
        ChartDataNeed::Trades { .. }
        | ChartDataNeed::TradeHydration { .. }
        | ChartDataNeed::Bubbles { .. } => MarketDataKind::Trades,
    };

    Some(MarketDataKey {
        venue,
        symbol,
        market_type,
        kind,
    })
}

pub fn chart_need_to_range(need: &ChartDataNeed) -> Option<MarketDataRange> {
    let (from, to) = need.range();
    MarketDataRange::new(from, to)
}

/// Compatibility metadata for pending consumers.
///
/// `ChartDataNeed::Bubbles` intentionally maps to raw `Trades` here. Bubble
/// derivation metadata (timeframe / price-step / max candidates) is stored in
/// `PendingMarketDataConsumer::bubble_config`, not smuggled through
/// `FetchRange::BubbleSummary`. The legacy BubbleSummary fetch type remains
/// supported only for older direct `FetchSpec` callers.
pub fn chart_need_to_consumer_fetch(need: &ChartDataNeed) -> crate::connector::fetcher::FetchRange {
    match need {
        ChartDataNeed::Klines { from, to } => {
            crate::connector::fetcher::FetchRange::Kline(*from, *to)
        }
        ChartDataNeed::Trades { from, to } => {
            crate::connector::fetcher::FetchRange::Trades(*from, *to)
        }
        ChartDataNeed::TradeHydration { from, to } => {
            crate::connector::fetcher::FetchRange::TradeHydration(*from, *to)
        }
        ChartDataNeed::OpenInterest { from, to } => {
            crate::connector::fetcher::FetchRange::OpenInterest(*from, *to)
        }
        ChartDataNeed::Bubbles { from, to, .. } => {
            crate::connector::fetcher::FetchRange::Trades(*from, *to)
        }
    }
}

fn chart_need_priority(need: &ChartDataNeed) -> Priority {
    match need {
        ChartDataNeed::Klines { .. } => Priority::High,
        ChartDataNeed::Bubbles { .. } => Priority::Low,
        ChartDataNeed::Trades { .. }
        | ChartDataNeed::TradeHydration { .. }
        | ChartDataNeed::OpenInterest { .. } => Priority::Normal,
    }
}

fn chart_need_reason(need: &ChartDataNeed) -> &'static str {
    match need {
        ChartDataNeed::Klines { .. } => "kline_history",
        ChartDataNeed::Trades { .. } => "trade_history",
        ChartDataNeed::TradeHydration { .. } => "trade_hydration",
        ChartDataNeed::OpenInterest { .. } => "oi_history",
        ChartDataNeed::Bubbles { .. } => "volume_bubbles_from_trades",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange::UnixMs;

    #[test]
    fn test_fetch_range_to_range() {
        let from = UnixMs::new(100);
        let to = UnixMs::new(200);

        let fetch_range = crate::connector::fetcher::FetchRange::Kline(from, to);
        let range = fetch_range_to_range(&fetch_range).unwrap();

        assert_eq!(range.from, from);
        assert_eq!(range.to, to);
    }

    #[test]
    fn test_fetch_range_to_feature() {
        let from = UnixMs::new(100);
        let to = UnixMs::new(200);

        let kline = crate::connector::fetcher::FetchRange::Kline(from, to);
        assert_eq!(fetch_range_to_feature(&kline), ConsumerFeature::ChartKlines);

        let trades = crate::connector::fetcher::FetchRange::Trades(from, to);
        assert_eq!(fetch_range_to_feature(&trades), ConsumerFeature::Footprint);

        let oi = crate::connector::fetcher::FetchRange::OpenInterest(from, to);
        assert_eq!(fetch_range_to_feature(&oi), ConsumerFeature::OpenInterest);
    }

    #[test]
    fn test_bubble_summary_routes_to_raw_trades_key() {
        let ticker_info = exchange::TickerInfo::new(
            exchange::Ticker::new("BTCUSDT", exchange::adapter::Exchange::BinanceLinear),
            0.1,
            0.001,
            Some(1.0),
        );
        let fetch = crate::connector::fetcher::FetchRange::BubbleSummary {
            from: UnixMs::new(100),
            to: UnixMs::new(200),
            timeframe_ms: 60_000,
            price_step: exchange::unit::PriceStep::default(),
            max_candidates_per_candle: 5,
        };

        let key = fetch_range_to_key(&fetch, Some(&ticker_info), Some(exchange::Timeframe::M1))
            .expect("bubble summary should route through market data");

        assert!(matches!(key.kind, MarketDataKind::Trades));
        assert_eq!(
            fetch_range_to_feature(&fetch),
            ConsumerFeature::VolumeBubbles
        );
    }

    #[test]
    fn test_kline_key_preserves_context_timeframe_m1() {
        let ticker_info = exchange::TickerInfo::new(
            exchange::Ticker::new("BTCUSDT", exchange::adapter::Exchange::BinanceLinear),
            0.1,
            0.001,
            Some(1.0),
        );
        let fetch =
            crate::connector::fetcher::FetchRange::Kline(UnixMs::new(100), UnixMs::new(200));

        let key = fetch_range_to_key(&fetch, Some(&ticker_info), Some(exchange::Timeframe::M1))
            .expect("kline key should resolve");

        assert!(matches!(
            key.kind,
            MarketDataKind::Klines {
                timeframe: exchange::Timeframe::M1
            }
        ));
    }

    #[test]
    fn test_open_interest_fetch_range_routes_to_oi_key() {
        let ticker_info = exchange::TickerInfo::new(
            exchange::Ticker::new("BTCUSDT", exchange::adapter::Exchange::BinanceLinear),
            0.1,
            0.001,
            Some(1.0),
        );
        let fetch =
            crate::connector::fetcher::FetchRange::OpenInterest(UnixMs::new(100), UnixMs::new(200));

        let key = fetch_range_to_key(&fetch, Some(&ticker_info), Some(exchange::Timeframe::M5))
            .expect("OI fetch should resolve to a market-data key");

        assert_eq!(
            fetch_range_to_feature(&fetch),
            ConsumerFeature::OpenInterest
        );
        assert!(matches!(
            key.kind,
            MarketDataKind::OpenInterest {
                timeframe: exchange::Timeframe::M5
            }
        ));
    }

    #[test]
    fn test_comparison_kline_requirement_can_override_feature() {
        let ticker_info = exchange::TickerInfo::new(
            exchange::Ticker::new("ETHUSDT", exchange::adapter::Exchange::BinanceLinear),
            0.1,
            0.001,
            Some(1.0),
        );
        let pane_id = uuid::Uuid::new_v4();
        let fetch =
            crate::connector::fetcher::FetchRange::Kline(UnixMs::new(100), UnixMs::new(200));

        let requirement = fetch_range_to_requirement(
            &fetch,
            pane_id,
            ConsumerFeature::ComparisonChart,
            Some(&ticker_info),
            Some(exchange::Timeframe::M1),
        )
        .expect("comparison kline fetch should become a requirement");

        assert_eq!(
            requirement.consumer.feature,
            ConsumerFeature::ComparisonChart
        );
        assert_eq!(requirement.consumer.pane_id, Some(pane_id));
        assert!(matches!(
            requirement.key.kind,
            MarketDataKind::Klines {
                timeframe: exchange::Timeframe::M1
            }
        ));
    }

    #[test]
    fn test_fetch_range_priority() {
        let from = UnixMs::new(100);
        let to = UnixMs::new(200);

        let kline = crate::connector::fetcher::FetchRange::Kline(from, to);
        assert_eq!(fetch_range_priority(&kline), Priority::High);

        let trades = crate::connector::fetcher::FetchRange::Trades(from, to);
        assert_eq!(fetch_range_priority(&trades), Priority::Normal);
    }

    #[test]
    fn test_chart_need_bubbles_routes_to_raw_trades_requirement() {
        let ticker_info = exchange::TickerInfo::new(
            exchange::Ticker::new("BTCUSDT", exchange::adapter::Exchange::BinanceLinear),
            0.1,
            0.001,
            Some(1.0),
        );
        let pane_id = uuid::Uuid::new_v4();
        let need = ChartDataNeed::Bubbles {
            from: UnixMs::new(100),
            to: UnixMs::new(200),
            timeframe_ms: 60_000,
            price_step: exchange::unit::PriceStep::default(),
            max_candidates_per_candle: 5,
        };

        let requirement = chart_need_to_requirement(
            &need,
            pane_id,
            Some(&ticker_info),
            Some(exchange::Timeframe::M1),
        )
        .expect("bubble chart need should become a raw-trades requirement");

        assert_eq!(requirement.consumer.feature, ConsumerFeature::VolumeBubbles);
        assert_eq!(requirement.consumer.pane_id, Some(pane_id));
        assert!(matches!(requirement.key.kind, MarketDataKind::Trades));
        assert_eq!(requirement.reason, "volume_bubbles_from_trades");
    }

    #[test]
    fn test_chart_need_bubbles_consumer_fetch_is_raw_trades_metadata_not_bubble_summary() {
        let need = ChartDataNeed::Bubbles {
            from: UnixMs::new(100),
            to: UnixMs::new(200),
            timeframe_ms: 60_000,
            price_step: exchange::unit::PriceStep::default(),
            max_candidates_per_candle: 5,
        };

        assert!(matches!(
            chart_need_to_consumer_fetch(&need),
            crate::connector::fetcher::FetchRange::Trades(from, to)
                if from == UnixMs::new(100) && to == UnixMs::new(200)
        ));
    }
}
