//! Volume Bubbles computation from raw trades.
//!
//! Provides `compute_bubble_summaries` which aggregates trades into
//! per-candle, per-price-level bubble candidates. Used by
//! `MarketDataCoordinator::compute_bubble_summaries` for the
//! coordinator-derived bubble path.

use data::chart::kline::{BubbleCandidate, BubbleVolumeSummary};
use exchange::unit::{Price, PriceStep, Qty};
use exchange::{Trade, UnixMs};
use rustc_hash::FxHashMap;

/// Compute bubble summaries from raw trades.
///
/// This is the core algorithm that aggregates trades into per-candle,
/// per-price-level bubble candidates.
pub fn compute_bubble_summaries(
    trades: &[Trade],
    timeframe_ms: u64,
    price_step: PriceStep,
    max_candidates_per_candle: usize,
) -> Vec<BubbleVolumeSummary> {
    #[derive(Clone, Copy, Default)]
    struct Accum {
        buy_qty: Qty,
        sell_qty: Qty,
        trade_count: usize,
        first_time: Option<UnixMs>,
        last_time: Option<UnixMs>,
    }

    let mut buckets: FxHashMap<(UnixMs, Price), Accum> = FxHashMap::default();

    for trade in trades {
        let candle_time = UnixMs::new(trade.time.as_u64() - (trade.time.as_u64() % timeframe_ms));
        let price = trade.price.round_to_step(price_step);
        let bucket = buckets.entry((candle_time, price)).or_default();

        if trade.is_sell {
            bucket.sell_qty += trade.qty;
        } else {
            bucket.buy_qty += trade.qty;
        }
        bucket.trade_count += 1;
        bucket.first_time = Some(
            bucket
                .first_time
                .map_or(trade.time, |first| first.min(trade.time)),
        );
        bucket.last_time = Some(
            bucket
                .last_time
                .map_or(trade.time, |last| last.max(trade.time)),
        );
    }

    let mut grouped: FxHashMap<UnixMs, Vec<BubbleCandidate>> = FxHashMap::default();
    for ((candle_time, price), bucket) in buckets {
        let total_qty = bucket.buy_qty + bucket.sell_qty;
        let delta_qty = bucket.buy_qty - bucket.sell_qty;
        grouped
            .entry(candle_time)
            .or_default()
            .push(BubbleCandidate {
                candle_time,
                price,
                total_qty,
                buy_qty: bucket.buy_qty,
                sell_qty: bucket.sell_qty,
                delta_qty,
                trade_count: bucket.trade_count,
                score: total_qty.to_f64(),
                first_time: bucket.first_time,
                last_time: bucket.last_time,
            });
    }

    let mut summaries = grouped
        .into_iter()
        .map(|(candle_time, mut candidates)| {
            candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.total_qty));
            candidates.truncate(max_candidates_per_candle);
            BubbleVolumeSummary::new(candle_time, candidates)
        })
        .collect::<Vec<_>>();
    summaries.sort_by_key(|summary| summary.candle_time);
    summaries
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange::unit::{Price, Qty};

    fn make_trade(time_ms: u64, price: f64, qty: f64, is_sell: bool) -> Trade {
        Trade {
            time: UnixMs::new(time_ms),
            is_sell,
            price: Price::from_f64(price),
            qty: Qty::from_f64(qty),
        }
    }

    // Helper: get a PriceStep of 0.01 (two decimal places) for testing
    // PriceStep has a `units` field that represents step size in atomic units
    fn small_price_step() -> PriceStep {
        // Create a PriceStep with units=100 (which represents 0.01 in the exchange's internal representation)
        PriceStep { units: 100 }
    }

    #[test]
    fn test_compute_bubble_summaries() {
        let trades = vec![
            make_trade(100, 100.0, 1.0, false),
            make_trade(110, 100.0, 2.0, true),
            make_trade(120, 101.0, 3.0, false),
            make_trade(130, 100.0, 1.0, false),
        ];

        let summaries = compute_bubble_summaries(&trades, 50, small_price_step(), 10);

        // Should have trades in first candle
        assert!(!summaries.is_empty());

        // First candle should have price levels
        let first_candle = &summaries[0];
        assert!(!first_candle.candidates.is_empty());
    }

    #[test]
    fn test_empty_trades() {
        let summaries = compute_bubble_summaries(&[], 50, small_price_step(), 10);
        assert!(summaries.is_empty());
    }

    #[test]
    fn test_max_candidates() {
        // Create trades at many price levels
        let trades: Vec<Trade> = (0..20)
            .map(|i| make_trade(100, 100.0 + i as f64, 1.0, false))
            .collect();

        let summaries = compute_bubble_summaries(&trades, 50, small_price_step(), 5);

        // Should have at most 5 candidates per candle
        for summary in &summaries {
            assert!(summary.candidates.len() <= 5);
        }
    }
}
