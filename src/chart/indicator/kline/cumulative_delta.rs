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
    kline::{CvdConfig, CvdRenderStyle, KlineDataPoint, KlineTrades},
};
use data::util::format_with_commas;
use exchange::{Kline, Trade, Volume, unit::Qty};

use iced::widget::{center, text};

use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;

/// Minimum number of consecutive bars with non-zero delta required
/// before CVD data is considered trustworthy.
const MIN_DIRECTIONAL_RUN: usize = 2;

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

fn directional_volume(volume: Volume, footprint: &KlineTrades) -> DirectionalVolume {
    if !footprint.trades.is_empty() {
        let (buy, sell) = footprint
            .trades
            .values()
            .fold((Qty::ZERO, Qty::ZERO), |(buy, sell), trades| {
                (buy + trades.buy_qty, sell + trades.sell_qty)
            });
        DirectionalVolume { buy, sell }
    } else if let Some((buy, sell)) = volume.buy_sell() {
        DirectionalVolume { buy, sell }
    } else {
        DirectionalVolume::default()
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct DirectionalVolume {
    buy: Qty,
    sell: Qty,
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
    /// Whether this point is considered trustworthy.  A point is
    /// reliable only when it has a directional predecessor within a
    /// run of at least `MIN_DIRECTIONAL_RUN` consecutive bars with
    /// non-zero delta.  The first bar of every qualifying run is
    /// excluded (it lacks a directional predecessor to anchor
    /// against), as are points outside any qualifying run.
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

fn cumulative_point(open: Qty, volume: DirectionalVolume, reliable: bool) -> CumulativeDeltaPoint {
    let delta = volume.delta();
    let close = open + delta;
    CumulativeDeltaPoint {
        delta,
        open,
        high: (open + volume.buy).max(open).max(close),
        low: (open - volume.sell).min(open).min(close),
        close,
        reliable,
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

        let invalid_message = format!(
            "CVD requires {MIN_DIRECTIONAL_RUN}+ consecutive bars\nwith directional volume",
        );

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
                let entries: Vec<_> = deltas.iter().collect();
                let reliable = Self::reliable_indices(&entries, MIN_DIRECTIONAL_RUN);

                let mut cumulative = Qty::ZERO;
                let data: BTreeMap<_, _> = entries
                    .iter()
                    .enumerate()
                    .map(|(i, &(&time, &volume))| {
                        let open = cumulative;
                        let point = cumulative_point(open, volume, reliable[i]);
                        cumulative = point.close;
                        (time, point)
                    })
                    .collect();

                self.data = BasisSeries::Time(data);
            }
            BasisSeries::Tick(deltas) => {
                let entries: Vec<_> = deltas.iter().collect();
                let reliable = Self::reliable_indices(&entries, MIN_DIRECTIONAL_RUN);

                let mut cumulative = Qty::ZERO;
                let data: BTreeMap<_, _> = entries
                    .iter()
                    .enumerate()
                    .map(|(i, &(&idx, &volume))| {
                        let open = cumulative;
                        let point = cumulative_point(open, volume, reliable[i]);
                        cumulative = point.close;
                        (idx, point)
                    })
                    .collect();

                self.data = BasisSeries::Tick(data);
            }
        }

        self.clear_all_caches();
    }

    /// Mark which positions in `entries` belong to a qualifying run
    /// (≥ `min_run` consecutive non-zero deltas, excluding the first
    /// bar of each run).
    fn reliable_indices<K>(entries: &[(&K, &DirectionalVolume)], min_run: usize) -> Vec<bool> {
        let n = entries.len();
        let mut reliable = vec![false; n];
        let mut i = 0;
        while i < n {
            if entries[i].1.delta() != Qty::ZERO {
                let run_start = i;
                while i < n && entries[i].1.delta() != Qty::ZERO {
                    i += 1;
                }
                // Skip the first bar of every qualifying run — it lacks
                // a directional predecessor to anchor its delta against.
                if i - run_start >= min_run {
                    for slot in reliable.iter_mut().take(i).skip(run_start + 1) {
                        *slot = true;
                    }
                }
            } else {
                i += 1;
            }
        }
        reliable
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
                timeseries
                    .datapoints
                    .iter()
                    .map(|(&time, dp)| (time, directional_volume(dp.kline.volume, &dp.footprint)))
                    .collect()
            },
            |tickseries| {
                tickseries
                    .datapoints
                    .iter()
                    .enumerate()
                    .map(|(idx, dp)| {
                        (
                            idx as u64,
                            directional_volume(dp.kline.volume, &dp.footprint),
                        )
                    })
                    .collect()
            },
        );

        let (deltas, has_points, has_directional) = match source {
            PlotData::TimeBased(timeseries) => {
                let has_points = !timeseries.datapoints.is_empty();
                let has_directional = timeseries.datapoints.values().any(|dp| dp.is_directional());

                (deltas, has_points, has_directional)
            }
            PlotData::TickBased(tickseries) => {
                let has_points = !tickseries.datapoints.is_empty();
                let has_directional = tickseries.datapoints.iter().any(|dp| dp.is_directional());

                (deltas, has_points, has_directional)
            }
        };

        self.set_availability(has_points, has_directional);

        self.rebuild_from_deltas(deltas);
    }

    fn on_insert_klines(&mut self, klines: &[Kline], source: &PlotData<KlineDataPoint>) {
        let mut has_directional = false;

        let has_data = {
            let PlotData::TimeBased(timeseries) = source else {
                return;
            };

            let Some(deltas) = self.delta.time_mut() else {
                return;
            };

            for kline in klines {
                let (volume, directional) = if let Some(dp) = timeseries.datapoints.get(&kline.time)
                {
                    (
                        directional_volume(dp.kline.volume, &dp.footprint),
                        dp.is_directional(),
                    )
                } else {
                    (
                        directional_volume(kline.volume, &KlineTrades::default()),
                        kline.volume.is_directional(),
                    )
                };

                deltas.insert(kline.time, volume);
                has_directional |= directional;
            }

            !deltas.is_empty()
        };

        if has_directional {
            self.availability = IndicatorAvailability::Available;
        }

        if self.availability == IndicatorAvailability::Unknown && has_data {
            self.availability = IndicatorAvailability::Unavailable(AvailabilityCause::TradeData);
        }

        self.rebuild_cumulative();
    }

    fn on_insert_trades(
        &mut self,
        trades: &[Trade],
        old_dp_len: usize,
        source: &PlotData<KlineDataPoint>,
    ) {
        let mut touched = false;

        match source {
            PlotData::TimeBased(timeseries) => {
                if trades.is_empty() {
                    return;
                }

                let Some(deltas) = self.delta.time_mut() else {
                    return;
                };

                let mut touched_times = BTreeSet::new();

                for trade in trades {
                    let rounded_time = trade.time.floor_to(timeseries.interval);
                    touched_times.insert(rounded_time);
                }

                for time in touched_times {
                    if let Some(dp) = timeseries.datapoints.get(&time) {
                        deltas.insert(time, directional_volume(dp.kline.volume, &dp.footprint));
                        touched = true;
                    }
                }
            }
            PlotData::TickBased(tickseries) => {
                let Some(deltas) = self.delta.tick_mut() else {
                    return;
                };

                let start_idx = old_dp_len.saturating_sub(1);

                for (idx, dp) in tickseries.datapoints.iter().enumerate().skip(start_idx) {
                    deltas.insert(
                        idx as u64,
                        directional_volume(dp.kline.volume, &dp.footprint),
                    );
                    touched = true;
                }
            }
        }

        if touched {
            self.availability = IndicatorAvailability::Available;
            self.rebuild_cumulative();
        }
    }

    fn on_ticksize_change(&mut self, source: &PlotData<KlineDataPoint>) {
        self.rebuild_from_source(source);
    }

    fn on_config_changed(&mut self, config: &data::chart::kline::Config) {
        if self.config != config.cvd {
            self.config = config.cvd;
            self.clear_all_caches();
        }
    }

    fn on_basis_change(&mut self, source: &PlotData<KlineDataPoint>) {
        self.rebuild_from_source(source);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cvd_candle_preserves_open_close_and_directional_envelope() {
        let point = cumulative_point(
            Qty::from_f64(100.0),
            DirectionalVolume {
                buy: Qty::from_f64(30.0),
                sell: Qty::from_f64(10.0),
            },
            true,
        );

        assert_eq!(point.open, Qty::from_f64(100.0));
        assert_eq!(point.high, Qty::from_f64(130.0));
        assert_eq!(point.low, Qty::from_f64(90.0));
        assert_eq!(point.close, Qty::from_f64(120.0));
        assert!(point.reliable);
    }

    #[test]
    fn next_cvd_candle_opens_at_previous_close() {
        let first = cumulative_point(
            Qty::ZERO,
            DirectionalVolume {
                buy: Qty::from_f64(8.0),
                sell: Qty::from_f64(3.0),
            },
            true,
        );
        let second = cumulative_point(
            first.close,
            DirectionalVolume {
                buy: Qty::from_f64(2.0),
                sell: Qty::from_f64(7.0),
            },
            true,
        );

        assert_eq!(second.open, first.close);
        assert_eq!(second.close, Qty::ZERO);
    }
}
