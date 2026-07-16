use crate::chart::{
    Caches, Message, ViewState,
    indicator::{
        indicator_row,
        kline::{
            AvailabilityCause, BasisSeries, BasisSeriesExt, IndicatorAvailability,
            KlineIndicatorImpl,
        },
        plot::{
            PlotTooltip,
            candlestick::{CandlestickPlot, CandlestickValue},
            line::LinePlot,
        },
    },
};

use data::chart::{
    PlotData,
    kline::{CvdConfig, CvdRenderStyle, CvdReset, KlineDataPoint, TradeCoverage},
};
use data::util::format_with_commas;
use exchange::{Kline, Trade, Volume, unit::Qty};

use iced::widget::{center, text};

use std::collections::BTreeMap;
use std::ops::RangeInclusive;

fn cvd_tooltip(point: &CumulativeDeltaPoint, _next: Option<&CumulativeDeltaPoint>) -> PlotTooltip {
    let sign = if point.delta >= Qty::ZERO { "+" } else { "" };
    PlotTooltip::new(format!(
        "CVD O: {}  H: {}\nCVD L: {}  C: {}\nDelta: {sign}{}",
        format_with_commas(point.open.to_f64()),
        format_with_commas(point.high.to_f64()),
        format_with_commas(point.low.to_f64()),
        format_with_commas(point.close.to_f64()),
        format_with_commas(point.delta.to_f64()),
    ))
}

fn directional_volume(dp: &KlineDataPoint, allow_partial: bool) -> DirectionalVolume {
    let footprint_totals = || {
        dp.footprint
            .trades
            .values()
            .fold((Qty::ZERO, Qty::ZERO), |(buy, sell), trades| {
                (buy + trades.buy_qty, sell + trades.sell_qty)
            })
    };
    let complete_path = |buy: Qty, sell: Qty| {
        if dp.trade_coverage != TradeCoverage::Complete || dp.trade_sequence.is_empty() {
            return None;
        }
        let mut trades = dp.trade_sequence.clone();
        trades.sort_by_key(|trade| trade.time);
        let totals = trades
            .iter()
            .fold((Qty::ZERO, Qty::ZERO), |(buy, sell), trade| {
                if trade.is_sell {
                    (buy, sell + trade.qty)
                } else {
                    (buy + trade.qty, sell)
                }
            });
        if totals != (buy, sell) {
            return None;
        }
        let mut running = Qty::ZERO;
        let mut high = Qty::ZERO;
        let mut low = Qty::ZERO;
        for trade in trades {
            running = if trade.is_sell {
                running - trade.qty
            } else {
                running + trade.qty
            };
            high = high.max(running);
            low = low.min(running);
        }
        Some((high, low))
    };

    match dp.kline.volume {
        Volume::BuySell(buy, sell) => DirectionalVolume {
            buy,
            sell,
            path: complete_path(buy, sell),
            reliable: true,
        },
        Volume::TotalOnly(_)
            if !dp.footprint.trades.is_empty()
                && (dp.trade_coverage == TradeCoverage::Complete || allow_partial) =>
        {
            let (buy, sell) = footprint_totals();
            DirectionalVolume {
                buy,
                sell,
                path: complete_path(buy, sell),
                reliable: true,
            }
        }
        Volume::TotalOnly(_) => DirectionalVolume::default(),
    }
}

fn aggregate_directional_volume(volume: Volume) -> DirectionalVolume {
    volume
        .buy_sell()
        .map_or_else(DirectionalVolume::default, |(buy, sell)| {
            DirectionalVolume {
                buy,
                sell,
                path: None,
                reliable: true,
            }
        })
}

#[derive(Debug, Clone, Copy, Default)]
struct DirectionalVolume {
    buy: Qty,
    sell: Qty,
    path: Option<(Qty, Qty)>,
    reliable: bool,
}

impl DirectionalVolume {
    fn delta(self) -> Qty {
        self.buy - self.sell
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct CumulativeDeltaPoint {
    delta: Qty,
    open: Qty,
    high: Qty,
    low: Qty,
    close: Qty,
    reliable: bool,
}

impl CandlestickValue for CumulativeDeltaPoint {
    fn open(&self) -> f32 {
        self.open.to_f64() as f32
    }
    fn high(&self) -> f32 {
        self.high.to_f64() as f32
    }
    fn low(&self) -> f32 {
        self.low.to_f64() as f32
    }
    fn close(&self) -> f32 {
        self.close.to_f64() as f32
    }
}

fn cumulative_point(open: Qty, volume: DirectionalVolume) -> CumulativeDeltaPoint {
    let delta = volume.delta();
    let close = open + delta;
    let (high, low) = volume
        .path
        .map_or((open.max(close), open.min(close)), |(high, low)| {
            (
                (open + high).max(open).max(close),
                (open + low).min(open).min(close),
            )
        });
    CumulativeDeltaPoint {
        delta,
        open,
        high,
        low,
        close,
        reliable: volume.reliable,
    }
}

pub struct CumulativeDeltaIndicator {
    cache: Caches,
    /// Per-bucket delta. Stored separately so inserting/replacing older klines can
    /// rebuild the cumulative line without needing the full chart source.
    delta: BasisSeries<DirectionalVolume>,
    data: BasisSeries<CumulativeDeltaPoint>,
    availability: IndicatorAvailability,
    config: CvdConfig,
}

impl CumulativeDeltaIndicator {
    pub fn new() -> Self {
        Self {
            cache: Caches::default(),
            delta: BasisSeries::default(),
            data: BasisSeries::default(),
            availability: IndicatorAvailability::Unknown,
            config: CvdConfig::default(),
        }
    }

    fn indicator_elem<'a>(
        &'a self,
        main_chart: &'a ViewState,
        data_labels_always_visible: bool,
        visible_range: RangeInclusive<u64>,
    ) -> iced::Element<'a, Message> {
        if let Some(message) = self.unavailable_message(main_chart, "CVD") {
            return center(text(message)).into();
        }

        let invalid_message = "CVD directional trade history is incomplete";

        match self.config.render_style {
            CvdRenderStyle::Line => {
                let plot =
                    LinePlot::new(|point: &CumulativeDeltaPoint| point.close.to_f64() as f32)
                        .stroke_width(self.config.line_width.clamp(0.5, 5.0))
                        .show_points(true)
                        .point_radius_factor(0.2)
                        .padding(0.08)
                        .valid_when(|point: &CumulativeDeltaPoint| point.reliable)
                        .invalid_point_message(invalid_message)
                        .with_tooltip(cvd_tooltip);
                indicator_row(
                    main_chart,
                    &self.cache,
                    data_labels_always_visible,
                    plot,
                    self.data.as_plot_series(),
                    visible_range,
                )
            }
            CvdRenderStyle::Candlesticks => {
                let plot = CandlestickPlot::new()
                    .body_width_factor((self.config.candle_width_percent / 100.0).clamp(0.1, 1.0))
                    .show_wicks(self.config.show_wicks)
                    .padding(0.08)
                    .valid_when(|point: &CumulativeDeltaPoint| point.reliable)
                    .invalid_point_message(invalid_message)
                    .with_tooltip(cvd_tooltip);
                indicator_row(
                    main_chart,
                    &self.cache,
                    data_labels_always_visible,
                    plot,
                    self.data.as_plot_series(),
                    visible_range,
                )
            }
        }
    }

    fn set_availability(&mut self, has_points: bool, has_directional: bool) {
        self.availability = if !has_points {
            IndicatorAvailability::Unknown
        } else if has_directional {
            IndicatorAvailability::Available
        } else {
            IndicatorAvailability::Unavailable(AvailabilityCause::TradeData)
        };
    }

    fn rebuild_cumulative(&mut self) {
        match &self.delta {
            BasisSeries::Time(deltas) => {
                let mut cumulative = Qty::ZERO;
                let mut active_day = None;
                let data: BTreeMap<_, _> = deltas
                    .iter()
                    .map(|(&time, &volume)| {
                        let day = time.as_u64() / 86_400_000;
                        if self.config.reset == CvdReset::DailyUtc && active_day != Some(day) {
                            cumulative = Qty::ZERO;
                            active_day = Some(day);
                        }
                        let open = cumulative;
                        let point = cumulative_point(open, volume);
                        cumulative = point.close;
                        (time, point)
                    })
                    .collect();

                self.data = BasisSeries::Time(data);
            }
            BasisSeries::Tick(deltas) => {
                let mut cumulative = Qty::ZERO;
                let data: BTreeMap<_, _> = deltas
                    .iter()
                    .map(|(&idx, &volume)| {
                        let open = cumulative;
                        let point = cumulative_point(open, volume);
                        cumulative = point.close;
                        (idx, point)
                    })
                    .collect();

                self.data = BasisSeries::Tick(data);
            }
        }

        self.clear_all_caches();
    }

    fn rebuild_from_deltas(&mut self, deltas: BasisSeries<DirectionalVolume>) {
        self.delta = deltas;
        self.rebuild_cumulative();
    }
}

impl KlineIndicatorImpl for CumulativeDeltaIndicator {
    fn clear_all_caches(&mut self) {
        self.cache.clear_all();
    }

    fn clear_crosshair_caches(&mut self) {
        self.cache.clear_crosshair();
    }

    fn element<'a>(
        &'a self,
        chart: &'a ViewState,
        data_labels_always_visible: bool,
        visible_range: RangeInclusive<u64>,
    ) -> iced::Element<'a, Message> {
        self.indicator_elem(chart, data_labels_always_visible, visible_range)
    }

    fn availability(&self, _chart: &ViewState) -> IndicatorAvailability {
        self.availability.clone()
    }

    fn rebuild_from_source(&mut self, source: &PlotData<KlineDataPoint>) {
        let deltas = source.map_basis_series(
            |timeseries| {
                let latest = timeseries
                    .datapoints
                    .last_key_value()
                    .map(|(&time, _)| time);
                timeseries
                    .datapoints
                    .iter()
                    .map(|(&time, dp)| (time, directional_volume(dp, latest == Some(time))))
                    .collect()
            },
            |tickseries| {
                tickseries
                    .datapoints
                    .iter()
                    .enumerate()
                    .map(|(idx, dp)| (idx as u64, aggregate_directional_volume(dp.kline.volume)))
                    .collect()
            },
        );

        let (deltas, has_points, has_directional) = match source {
            PlotData::TimeBased(timeseries) => {
                let has_points = !timeseries.datapoints.is_empty();
                let latest = timeseries
                    .datapoints
                    .last_key_value()
                    .map(|(&time, _)| time);
                let has_directional = timeseries
                    .datapoints
                    .iter()
                    .any(|(&time, dp)| directional_volume(dp, latest == Some(time)).reliable);

                (deltas, has_points, has_directional)
            }
            PlotData::TickBased(tickseries) => {
                let has_points = !tickseries.datapoints.is_empty();
                let has_directional = tickseries
                    .datapoints
                    .iter()
                    .any(|dp| aggregate_directional_volume(dp.kline.volume).reliable);

                (deltas, has_points, has_directional)
            }
        };

        self.set_availability(has_points, has_directional);

        self.rebuild_from_deltas(deltas);
    }

    fn on_insert_klines(&mut self, klines: &[Kline], source: &PlotData<KlineDataPoint>) {
        if !klines.is_empty() {
            self.rebuild_from_source(source);
        }
    }

    fn on_insert_trades(
        &mut self,
        trades: &[Trade],
        old_dp_len: usize,
        source: &PlotData<KlineDataPoint>,
    ) {
        let _ = old_dp_len;
        if !trades.is_empty() {
            self.rebuild_from_source(source);
        }
    }

    fn on_ticksize_change(&mut self, source: &PlotData<KlineDataPoint>) {
        self.rebuild_from_source(source);
    }

    fn on_config_changed(&mut self, config: &data::chart::kline::Config) {
        if self.config != config.cvd {
            let reset_changed = self.config.reset != config.cvd.reset;
            self.config = config.cvd;
            if reset_changed {
                self.rebuild_cumulative();
            } else {
                self.clear_all_caches();
            }
        }
    }

    fn on_basis_change(&mut self, source: &PlotData<KlineDataPoint>) {
        self.rebuild_from_source(source);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::chart::kline::{BubbleVolumeSummary, KlineTrades};
    use exchange::unit::{Price, PriceStep};

    fn aggregate(buy: f64, sell: f64) -> DirectionalVolume {
        DirectionalVolume {
            buy: Qty::from_f64(buy),
            sell: Qty::from_f64(sell),
            path: None,
            reliable: true,
        }
    }

    fn datapoint(volume: Volume) -> KlineDataPoint {
        KlineDataPoint {
            kline: Kline {
                time: exchange::UnixMs::ZERO,
                open: Price::from_f64(1.0),
                high: Price::from_f64(1.0),
                low: Price::from_f64(1.0),
                close: Price::from_f64(1.0),
                volume,
            },
            footprint: KlineTrades::default(),
            bubble_summary: BubbleVolumeSummary::default(),
            trade_coverage: TradeCoverage::Unknown,
            trade_sequence: Vec::new(),
            trade_ids: Default::default(),
        }
    }

    #[test]
    fn cvd_candle_preserves_open_close_and_directional_envelope() {
        let point = cumulative_point(Qty::from_f64(100.0), aggregate(30.0, 10.0));

        assert_eq!(point.open, Qty::from_f64(100.0));
        assert_eq!(point.high, Qty::from_f64(120.0));
        assert_eq!(point.low, Qty::from_f64(100.0));
        assert_eq!(point.close, Qty::from_f64(120.0));
        assert!(point.reliable);
    }

    #[test]
    fn next_cvd_candle_opens_at_previous_close() {
        let first = cumulative_point(Qty::ZERO, aggregate(8.0, 3.0));
        let second = cumulative_point(first.close, aggregate(2.0, 7.0));

        assert_eq!(second.open, first.close);
        assert_eq!(second.close, Qty::ZERO);
    }

    #[test]
    fn zero_delta_is_valid() {
        let point = cumulative_point(Qty::from_f64(4.0), aggregate(5.0, 5.0));
        assert!(point.reliable);
        assert_eq!(point.open, point.close);
    }

    #[test]
    fn buysell_kline_wins_over_partial_footprint() {
        let mut dp = datapoint(Volume::BuySell(Qty::from_f64(10.0), Qty::from_f64(4.0)));
        dp.add_trade(
            &Trade {
                id: Some(1),
                time: exchange::UnixMs::new(1),
                is_sell: true,
                price: Price::from_f64(1.0),
                qty: Qty::from_f64(100.0),
            },
            PriceStep {
                units: Price::from_f64(0.1).units,
            },
        );
        let directional = directional_volume(&dp, true);
        assert_eq!(directional.buy, Qty::from_f64(10.0));
        assert_eq!(directional.sell, Qty::from_f64(4.0));
    }

    #[test]
    fn partial_total_only_bucket_is_only_valid_as_live_provisional() {
        let mut dp = datapoint(Volume::TotalOnly(Qty::from_f64(1.0)));
        dp.add_trade(
            &Trade {
                id: Some(1),
                time: exchange::UnixMs::new(1),
                is_sell: false,
                price: Price::from_f64(1.0),
                qty: Qty::from_f64(1.0),
            },
            PriceStep {
                units: Price::from_f64(0.1).units,
            },
        );
        assert!(!directional_volume(&dp, false).reliable);
        assert!(directional_volume(&dp, true).reliable);
        dp.trade_coverage = TradeCoverage::Complete;
        assert!(directional_volume(&dp, false).reliable);
    }

    #[test]
    fn daily_utc_reset_is_independent_between_days() {
        let mut indicator = CumulativeDeltaIndicator::new();
        indicator.config.reset = CvdReset::DailyUtc;
        indicator.delta = BasisSeries::Time(BTreeMap::from([
            (exchange::UnixMs::new(86_399_000), aggregate(8.0, 3.0)),
            (exchange::UnixMs::new(86_400_000), aggregate(2.0, 1.0)),
        ]));
        indicator.rebuild_cumulative();
        let BasisSeries::Time(data) = &indicator.data else {
            panic!("expected time series");
        };
        assert_eq!(data[&exchange::UnixMs::new(86_400_000)].open, Qty::ZERO);
        assert_eq!(
            data[&exchange::UnixMs::new(86_400_000)].close,
            Qty::from_f64(1.0)
        );
    }

    #[test]
    fn aggregate_totals_do_not_create_synthetic_wicks() {
        let point = cumulative_point(Qty::from_f64(10.0), aggregate(30.0, 10.0));
        assert_eq!(point.high, point.close);
        assert_eq!(point.low, point.open);
    }
}
