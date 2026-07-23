use super::{
    Action, Basis, Chart, Interaction, Message, PlotConstants, PlotData, TEXT_SIZE, ViewState,
    indicator, request_fetch, scale::linear::PriceInfoLabel,
};
use crate::chart::indicator::kline::KlineIndicatorImpl;
use crate::connector::fetcher::{
    self, FetchRange, ReqError, RequestHandler, is_trade_fetch_enabled,
};
use crate::{modal::pane::settings::study, style};
use data::aggr::ticks::TickAggr;
use data::aggr::time::TimeSeries;
use data::chart::indicator::{Indicator, KlineIndicator};
use data::chart::kline::{
    BubbleCandidate, BubbleColorMode, BubbleVolumeSummary, ClusterKind, ClusterScaling, Config,
    FootprintStudy, FootprintSummary, KlineDataPoint, KlineTrades, NPoc, PointOfControl,
    SessionProfileMode, SessionProfilePlacement, SessionVolumeProfileConfig, VolumeBubbleConfig,
    VolumeBubbleSession, VwapConfig,
};
use data::chart::{Autoscale, KlineChartKind, ViewConfig};

use data::config::theme::{composite_color, contrast_ratio, mix_color};
use data::util::abbr_large_numbers;
use exchange::unit::{Price, PriceStep, Qty};
use exchange::{Kline, OpenInterest as OIData, TickerInfo, Trade, UnixMs};

use iced::task::Handle;
use iced::theme::palette::Extended;
use iced::widget::canvas::{self, Event, Geometry, Path, Stroke};
use iced::{Alignment, Color, Element, Point, Rectangle, Renderer, Size, Theme, Vector, mouse};

use chrono::{Datelike, TimeZone, Timelike};
use enum_map::EnumMap;
use rustc_hash::{FxHashMap, FxHashSet};
use std::{sync::Arc, time::Instant};

/// Maximum number of raw trades to retain in memory.
/// Older trades are pruned FIFO when this cap is exceeded.
/// 50k trades ≈ 1.5-3 MB depending on Trade size.
const MAX_RAW_TRADES: usize = 50_000;

fn deduplicate_incoming_trades(existing: &[Trade], incoming: &[Trade]) -> Vec<Trade> {
    let mut seen_ids = existing
        .iter()
        .filter_map(|trade| trade.id)
        .collect::<FxHashSet<_>>();
    incoming
        .iter()
        .copied()
        .filter(|trade| trade.id.is_none_or(|id| seen_ids.insert(id)))
        .collect()
}

impl Chart for KlineChart {
    type IndicatorKind = KlineIndicator;

    fn state(&self) -> &ViewState {
        &self.chart
    }

    fn mut_state(&mut self) -> &mut ViewState {
        &mut self.chart
    }

    fn invalidate_crosshair(&mut self) {
        self.chart.cache.clear_crosshair();
        self.indicators
            .values_mut()
            .filter_map(Option::as_mut)
            .for_each(|indi| indi.clear_crosshair_caches());
    }

    fn invalidate_all(&mut self) {
        self.invalidate(None);
    }

    fn view_indicators(&'_ self, enabled: &[Self::IndicatorKind]) -> Vec<Element<'_, Message>> {
        let chart_state = self.state();
        let visible_region = chart_state.visible_region(chart_state.bounds.size());
        let (earliest, latest) = chart_state.interval_range(&visible_region);
        if earliest > latest {
            return vec![];
        }

        let data_labels_always_visible = self.visual_config.data_labels_always_visible;

        let market = chart_state.ticker_info.market_type();
        let mut elements = vec![];

        for selected_indicator in enabled {
            if !self.kind.allows_indicator(*selected_indicator)
                || !KlineIndicator::for_market(market).contains(selected_indicator)
            {
                continue;
            }
            if selected_indicator.is_overlay() {
                continue;
            }
            if let Some(indi) = self.indicators[*selected_indicator].as_ref() {
                elements.push(indi.element(
                    chart_state,
                    data_labels_always_visible,
                    earliest..=latest,
                ));
            }
        }
        elements
    }

    fn visible_timerange(&self) -> Option<(u64, u64)> {
        let chart = self.state();
        let region = chart.visible_region(chart.bounds.size());

        if region.width == 0.0 {
            return None;
        }

        Some(chart.interval_range(&region))
    }

    fn interval_keys(&self) -> Option<Vec<u64>> {
        match &self.data_source {
            PlotData::TimeBased(_) => None,
            PlotData::TickBased(tick_aggr) => Some(
                tick_aggr
                    .datapoints
                    .iter()
                    .map(|dp| dp.kline.time.as_u64())
                    .collect(),
            ),
        }
    }

    fn autoscaled_coords(&self) -> Vector {
        let chart = self.state();
        let x_translation = match &self.kind {
            KlineChartKind::Footprint { .. } => {
                0.5 * (chart.bounds.width / chart.scaling) - (chart.cell_width / chart.scaling)
            }
            KlineChartKind::Candles => {
                0.5 * (chart.bounds.width / chart.scaling)
                    - (8.0 * chart.cell_width / chart.scaling)
            }
        };
        Vector::new(x_translation, chart.translation.y)
    }

    fn supports_fit_autoscaling(&self) -> bool {
        true
    }

    fn is_empty(&self) -> bool {
        match &self.data_source {
            PlotData::TimeBased(timeseries) => timeseries.datapoints.is_empty(),
            PlotData::TickBased(tick_aggr) => tick_aggr.datapoints.is_empty(),
        }
    }
}

impl PlotConstants for KlineChart {
    fn min_scaling(&self) -> f32 {
        self.kind.min_scaling()
    }

    fn max_scaling(&self) -> f32 {
        self.kind.max_scaling()
    }

    fn max_cell_width(&self) -> f32 {
        self.kind.max_cell_width()
    }

    fn min_cell_width(&self) -> f32 {
        self.kind.min_cell_width()
    }

    fn max_cell_height(&self) -> f32 {
        self.kind.max_cell_height()
    }

    fn min_cell_height(&self) -> f32 {
        self.kind.min_cell_height()
    }

    fn default_cell_width(&self) -> f32 {
        self.kind.default_cell_width()
    }
}

pub struct KlineChart {
    chart: ViewState,
    data_source: PlotData<KlineDataPoint>,
    raw_trades: Vec<Trade>,
    covered_trade_ranges: Vec<(UnixMs, UnixMs)>,
    covered_bubble_summary_ranges: Vec<(UnixMs, UnixMs)>,
    indicators: EnumMap<KlineIndicator, Option<Box<dyn KlineIndicatorImpl>>>,
    fetching_trades: (bool, Option<Handle>),
    pub(crate) kind: KlineChartKind,
    request_handler: RequestHandler,
    study_configurator: study::Configurator<FootprintStudy>,
    last_tick: Instant,
    visual_config: Config,
    gex_snapshot: Option<Arc<data::chart::gex::GexSnapshot>>,
}

#[derive(Debug, Clone, Copy)]
pub struct VolumeBubbleQtyScale {
    pub min: f64,
    pub max: f64,
    pub step: f64,
}

impl KlineChart {
    pub fn new(
        layout: ViewConfig,
        basis: Basis,
        step: PriceStep,
        klines_raw: &[Kline],
        raw_trades: Vec<Trade>,
        enabled_indicators: &[KlineIndicator],
        ticker_info: TickerInfo,
        kind: &KlineChartKind,
        visual_config: Option<Config>,
    ) -> Self {
        let mut visual_config = visual_config.unwrap_or_default();
        visual_config.migrate_legacy_indicator_configs();
        let kind = Self::sanitized_kind(kind.clone());
        let raw_trades = deduplicate_incoming_trades(&[], &raw_trades);

        match basis {
            Basis::Time(interval) => {
                let timeseries = TimeSeries::<KlineDataPoint>::new(interval, step, klines_raw)
                    .with_trades(&raw_trades);

                let base_price_y = timeseries.base_price();
                let latest_x = timeseries
                    .latest_timestamp()
                    .map_or(0, |timestamp| timestamp.as_u64());
                let (scale_high, scale_low) = timeseries.price_scale({
                    match &kind {
                        KlineChartKind::Footprint { .. } => 12,
                        KlineChartKind::Candles => 60,
                    }
                });

                let low_rounded = scale_low.round_to_side_step(true, step);
                let high_rounded = scale_high.round_to_side_step(false, step);

                let y_ticks = Price::steps_between_inclusive(low_rounded, high_rounded, step)
                    .map(|n| n.saturating_sub(1))
                    .unwrap_or(1)
                    .max(1) as f32;

                let cell_width = match &kind {
                    KlineChartKind::Footprint { .. } => 80.0,
                    KlineChartKind::Candles => 4.0,
                };
                let cell_height = match &kind {
                    KlineChartKind::Footprint { .. } => 800.0 / y_ticks,
                    KlineChartKind::Candles => 200.0 / y_ticks,
                };

                let mut chart = ViewState::new(
                    basis,
                    step,
                    step.decimal_places(),
                    ticker_info,
                    ViewConfig {
                        splits: layout.splits.clone(),
                        autoscale: Some(Autoscale::FitToVisible),
                    },
                    cell_width,
                    cell_height,
                );
                chart.base_price_y = base_price_y;
                chart.latest_x = latest_x;

                let x_translation = match &kind {
                    KlineChartKind::Footprint { .. } => {
                        0.5 * (chart.bounds.width / chart.scaling)
                            - (chart.cell_width / chart.scaling)
                    }
                    KlineChartKind::Candles => {
                        0.5 * (chart.bounds.width / chart.scaling)
                            - (8.0 * chart.cell_width / chart.scaling)
                    }
                };
                chart.translation.x = x_translation;

                let data_source = PlotData::TimeBased(timeseries);

                let mut indicators = EnumMap::default();
                for &i in enabled_indicators {
                    if !kind.allows_indicator(i) {
                        continue;
                    }
                    let mut indi = indicator::kline::make_empty(i);
                    indi.on_config_changed(&visual_config);
                    indi.rebuild_from_source(&data_source);
                    indicators[i] = Some(indi);
                }

                KlineChart {
                    chart,
                    visual_config,
                    data_source,
                    raw_trades,
                    covered_trade_ranges: Vec::new(),
                    covered_bubble_summary_ranges: Vec::new(),
                    indicators,
                    fetching_trades: (false, None),
                    request_handler: RequestHandler::default(),
                    kind: kind.clone(),
                    study_configurator: study::Configurator::new(),
                    last_tick: Instant::now(),
                    gex_snapshot: None,
                }
            }
            Basis::Tick(interval) => {
                let cell_width = match &kind {
                    KlineChartKind::Footprint { .. } => 80.0,
                    KlineChartKind::Candles => 4.0,
                };
                let cell_height = match &kind {
                    KlineChartKind::Footprint { .. } => 90.0,
                    KlineChartKind::Candles => 8.0,
                };

                let mut chart = ViewState::new(
                    basis,
                    step,
                    step.decimal_places(),
                    ticker_info,
                    ViewConfig {
                        splits: layout.splits.clone(),
                        autoscale: Some(Autoscale::FitToVisible),
                    },
                    cell_width,
                    cell_height,
                );

                let x_translation = match &kind {
                    KlineChartKind::Footprint { .. } => {
                        0.5 * (chart.bounds.width / chart.scaling)
                            - (chart.cell_width / chart.scaling)
                    }
                    KlineChartKind::Candles => {
                        0.5 * (chart.bounds.width / chart.scaling)
                            - (8.0 * chart.cell_width / chart.scaling)
                    }
                };
                chart.translation.x = x_translation;

                let data_source = PlotData::TickBased(TickAggr::new(interval, step, &[]));

                let mut indicators = EnumMap::default();
                for &i in enabled_indicators {
                    if !kind.allows_indicator(i) {
                        continue;
                    }
                    let mut indi = indicator::kline::make_empty(i);
                    indi.on_config_changed(&visual_config);
                    indi.rebuild_from_source(&data_source);
                    indicators[i] = Some(indi);
                }

                KlineChart {
                    chart,
                    visual_config,
                    data_source,
                    raw_trades,
                    covered_trade_ranges: Vec::new(),
                    covered_bubble_summary_ranges: Vec::new(),
                    indicators,
                    fetching_trades: (false, None),
                    request_handler: RequestHandler::default(),
                    kind: kind.clone(),
                    study_configurator: study::Configurator::new(),
                    last_tick: Instant::now(),
                    gex_snapshot: None,
                }
            }
        }
    }

    pub fn update_latest_kline(&mut self, kline: &Kline) {
        match self.data_source {
            PlotData::TimeBased(ref mut timeseries) => {
                let previous_latest_x = self.chart.latest_x;
                let is_new_bucket = !timeseries.datapoints.contains_key(&kline.time);
                timeseries.insert_klines(&[*kline]);
                if is_new_bucket {
                    let bucket_trades = self
                        .raw_trades
                        .iter()
                        .filter(|trade| trade.time.floor_to(timeseries.interval) == kline.time)
                        .copied()
                        .collect::<Vec<_>>();
                    timeseries.insert_trades_existing_buckets(&bucket_trades);
                }

                self.indicators
                    .values_mut()
                    .filter_map(Option::as_mut)
                    .for_each(|indi| indi.on_insert_klines(&[*kline], &self.data_source));

                let chart = self.mut_state();

                let relation = if kline.time.as_u64() > chart.latest_x {
                    chart.latest_x = kline.time.as_u64();
                    "newer"
                } else if kline.time.as_u64() == chart.latest_x {
                    "equal"
                } else {
                    "older"
                };

                chart.last_price = Some(PriceInfoLabel::new(kline.close, kline.open));
                log::trace!(
                    "KLINE UpdateLatest | kline_t={} previous_latest_x={} new_latest_x={} relation={relation}",
                    fetcher::format_time_short(kline.time),
                    previous_latest_x,
                    chart.latest_x
                );
            }
            PlotData::TickBased(_) => {
                log::trace!(
                    "KLINE UpdateLatest | kline_t={} reason=tick_based_ignored",
                    fetcher::format_time_short(kline.time)
                );
            }
        }
    }

    pub fn kind(&self) -> &KlineChartKind {
        &self.kind
    }

    fn fetch_missing_data(&mut self) -> Option<Action> {
        self.request_handler.cleanup_stale();
        if self.fetching_trades.0 && !self.request_handler.has_pending_trade_requests() {
            log::warn!("CHART Footprint | action=clear_fetching reason=no_pending_trade_request");
            self.fetching_trades = (false, None);
        }

        log::debug!(
            "CHART FetchMissingStart | kind={:?} basis={:?} datapoints={} raw_trades={} covered_trade_ranges={} fetching_trades={} bubbles_enabled={} bubbles_session={:?} trade_fetch_enabled={}",
            self.kind,
            self.chart.basis,
            match &self.data_source {
                PlotData::TimeBased(timeseries) => timeseries.datapoints.len(),
                PlotData::TickBased(tick_aggr) => tick_aggr.datapoints.len(),
            },
            self.raw_trades.len(),
            self.covered_trade_ranges.len(),
            self.fetching_trades.0,
            self.indicator_enabled(KlineIndicator::VolumeBubbles),
            self.visual_config.volume_bubbles.session,
            is_trade_fetch_enabled()
        );
        match &self.data_source {
            PlotData::TimeBased(timeseries) => {
                let timeframe_ms = timeseries.interval.to_milliseconds();

                if timeseries.datapoints.is_empty() {
                    let latest = chrono::Utc::now().timestamp_millis() as u64;
                    let earliest = latest.saturating_sub(450 * timeframe_ms);

                    let range = FetchRange::Kline(UnixMs::new(earliest), UnixMs::new(latest));
                    log::info!(
                        "KLINE InitialFetch | reason=empty_data range={}",
                        fetcher::format_time_range(UnixMs::new(earliest), UnixMs::new(latest))
                    );
                    if let Some(action) = request_fetch(
                        &mut self.request_handler,
                        range,
                        Some(&self.chart.ticker_info),
                    ) {
                        log::info!(
                            "KLINE InitialFetchQueued | range={}",
                            fetcher::format_time_range(UnixMs::new(earliest), UnixMs::new(latest))
                        );
                        return Some(action);
                    } else {
                        log::debug!(
                            "KLINE InitialFetchSuppressed | range={} reason=request_handler",
                            fetcher::format_time_range(UnixMs::new(earliest), UnixMs::new(latest))
                        );
                    }
                }

                let Some((visible_earliest, visible_latest)) = self.visible_timerange() else {
                    log::debug!(
                        "CHART FetchMissingSkip | kind={:?} reason=visible_timerange_none bounds={:?}",
                        self.kind,
                        self.chart.bounds
                    );
                    return None;
                };
                let (kline_earliest, kline_latest) = timeseries.timerange();
                let visible_earliest_ms = UnixMs::new(visible_earliest);
                let visible_latest_ms = UnixMs::new(visible_latest);
                let visible_span = visible_latest.saturating_sub(visible_earliest);
                let prefetch_earliest = visible_earliest.saturating_sub(visible_span);
                log::debug!(
                    "CHART FetchMissingRange | visible_range={} kline_range={} visible_span_ms={} prefetch_earliest={}",
                    fetcher::format_time_range(visible_earliest_ms, visible_latest_ms),
                    fetcher::format_time_range(kline_earliest, kline_latest),
                    visible_span,
                    fetcher::format_time_short(UnixMs::new(prefetch_earliest))
                );

                // priority 1, initial klines for visible range
                if visible_earliest_ms < kline_earliest {
                    let range = FetchRange::Kline(UnixMs::new(prefetch_earliest), kline_earliest);
                    log::info!(
                        "KLINE PriorityFetch | reason=visible_before_earliest visible_earliest={} kline_earliest={} fetch={}",
                        fetcher::format_time_short(visible_earliest_ms),
                        fetcher::format_time_short(kline_earliest),
                        fetcher::format_fetch_range(&range)
                    );
                    if let Some(action) = request_fetch(
                        &mut self.request_handler,
                        range,
                        Some(&self.chart.ticker_info),
                    ) {
                        return Some(action);
                    } else {
                        log::debug!(
                            "KLINE PriorityFetchSuppressed | reason=request_handler fetch={}",
                            fetcher::format_fetch_range(&range)
                        );
                    }
                } else {
                    log::trace!(
                        "KLINE PrioritySkip | reason=visible_not_before_earliest visible_earliest={} kline_earliest={}",
                        fetcher::format_time_short(visible_earliest_ms),
                        fetcher::format_time_short(kline_earliest)
                    );
                }

                let now = UnixMs::now();
                let target_to = kline_latest.saturating_add(timeframe_ms).min(now);
                let historical_trade_to =
                    historical_trade_target_to(kline_latest, timeframe_ms, now);
                let vwap_required_from = self.indicator_enabled(KlineIndicator::Vwap).then(|| {
                    let anchor_ms = self.visual_config.vwap.anchor.milliseconds();
                    vwap_required_from(target_to, visible_earliest_ms, anchor_ms)
                });
                let bubble_required_range = self
                    .indicator_enabled(KlineIndicator::VolumeBubbles)
                    .then(|| {
                        volume_bubble_effective_range(
                            kline_latest,
                            timeframe_ms,
                            UnixMs::now(),
                            &self.visual_config.volume_bubbles,
                        )
                    })
                    .flatten();

                // Indicator history must have kline buckets before raw trades
                // or derived summaries can be attached to them.
                let indicator_kline_from = vwap_required_from
                    .into_iter()
                    .chain(bubble_required_range.map(|(from, _)| {
                        UnixMs::new(from.as_u64() - (from.as_u64() % timeframe_ms))
                    }))
                    .min();
                if let Some(required_from) = indicator_kline_from
                    && required_from < kline_earliest
                {
                    let range = FetchRange::Kline(required_from, kline_earliest);
                    if let Some(action) = request_fetch(
                        &mut self.request_handler,
                        range,
                        Some(&self.chart.ticker_info),
                    ) {
                        return Some(action);
                    }
                }

                // priority 2, trades
                if matches!(self.kind, KlineChartKind::Footprint { .. }) {
                    if !self.fetching_trades.0 && is_trade_fetch_enabled() {
                        if let Some((fetch_from, fetch_to)) = timeseries
                            .suggest_trade_fetch_range(visible_earliest_ms, visible_latest_ms)
                        {
                            // The chart intentionally renders whitespace after
                            // the latest candle. It must never turn that visual
                            // future into a historical market-data request.
                            let fetch_to = fetch_to.min(historical_trade_to);
                            log::debug!(
                                "CHART Footprint | action=suggest_missing range={}",
                                fetcher::format_time_range(fetch_from, fetch_to)
                            );
                            if fetch_to <= fetch_from {
                                log::debug!(
                                    "CHART Footprint | action=skip reason=range_after_now range={}",
                                    fetcher::format_time_range(fetch_from, fetch_to)
                                );
                            } else if let Some((fetch_from, fetch_to)) =
                                self.subtract_covered_trade_ranges(fetch_from, fetch_to)
                            {
                                log::info!(
                                    "CHART Footprint | action=fetch_trades reason=missing_range range={}",
                                    fetcher::format_time_range(fetch_from, fetch_to)
                                );
                                let range = FetchRange::Trades(fetch_from, fetch_to);
                                if let Some(action) = request_fetch(
                                    &mut self.request_handler,
                                    range,
                                    Some(&self.chart.ticker_info),
                                ) {
                                    self.fetching_trades = (true, None);
                                    return Some(action);
                                } else {
                                    let reason = self
                                        .request_handler
                                        .last_suppression_reason()
                                        .map_or("throttled", |reason| reason.as_str());
                                    log::info!(
                                        "CHART Footprint | action=suppressed reason={} range={}",
                                        reason,
                                        fetcher::format_fetch_range(&range)
                                    );
                                }
                            } else {
                                log::debug!(
                                    "CHART Footprint | action=skip reason=already_covered range={}",
                                    fetcher::format_time_range(fetch_from, fetch_to)
                                );
                            }
                        } else {
                            log::debug!("CHART Footprint | action=skip reason=no_missing_trades");
                        }
                    } else if !is_trade_fetch_enabled() {
                        log::debug!("CHART Footprint | action=skip reason=trade_fetch_disabled");
                    } else {
                        log::debug!("CHART Footprint | action=skip reason=already_fetching");
                    }
                }

                // Candlestick SVP consumes the same raw trade dataset as
                // footprint and bubbles. Fetch in bounded chronological chunks
                // so daily/weekly profiles never create one unbounded request.
                let svp_enabled = self.indicator_enabled(KlineIndicator::SessionVolumeProfile);
                let vwap_enabled = self.indicator_enabled(KlineIndicator::Vwap);
                if matches!(self.kind, KlineChartKind::Candles)
                    && (svp_enabled || vwap_enabled)
                    && !self.fetching_trades.0
                {
                    let svp_cfg = self.visual_config.session_volume_profile;
                    let vwap_cfg = self.visual_config.vwap;
                    let mut requested_from = visible_earliest_ms;
                    if svp_enabled {
                        requested_from = requested_from.min(UnixMs::new(align_session_start(
                            visible_earliest,
                            svp_cfg.interval.milliseconds(),
                        )));
                    }
                    if vwap_enabled {
                        requested_from =
                            requested_from.min(vwap_required_from.unwrap_or_else(|| {
                                UnixMs::new(align_session_start(
                                    target_to.saturating_sub(1).as_u64(),
                                    vwap_cfg.anchor.milliseconds(),
                                ))
                            }));
                    }
                    let requested_to = if vwap_enabled {
                        historical_trade_to
                    } else {
                        visible_latest_ms.max(kline_latest).min(historical_trade_to)
                    };
                    let requested_from = requested_from.max(kline_earliest);
                    if requested_to > requested_from
                        && let Some((from, to)) =
                            self.latest_uncovered_trade_range(requested_from, requested_to)
                    {
                        // One hour per worker keeps exchange pagination and the
                        // UI responsive. Start from the newest data and move
                        // backwards toward the session boundary.
                        let chunk_from =
                            UnixMs::new(from.as_u64().max(to.as_u64().saturating_sub(60 * 60_000)));
                        let range = FetchRange::Trades(chunk_from, to);
                        log::info!(
                            "OVERLAY Fetch | svp={} vwap={} range={}",
                            svp_enabled,
                            vwap_enabled,
                            fetcher::format_fetch_range(&range)
                        );
                        if let Some(action) = request_fetch(
                            &mut self.request_handler,
                            range,
                            Some(&self.chart.ticker_info),
                        ) {
                            self.fetching_trades = (true, None);
                            return Some(action);
                        }
                    }
                }

                if matches!(self.kind, KlineChartKind::Candles)
                    && self.indicator_enabled(KlineIndicator::VolumeBubbles)
                    && !self.fetching_trades.0
                {
                    const BUBBLE_FETCH_CHUNK_MS: u64 = 15 * 60_000;
                    if let Some((window_from, window_to)) = bubble_required_range
                        && let Some(window_to) = (window_from < historical_trade_to)
                            .then_some(window_to.min(historical_trade_to))
                        && let Some((gap_from, gap_to)) =
                            self.latest_uncovered_bubble_summary_range(window_from, window_to)
                    {
                        let fetch_to = gap_to;
                        let fetch_from = UnixMs::new(
                            gap_from
                                .as_u64()
                                .max(fetch_to.as_u64().saturating_sub(BUBBLE_FETCH_CHUNK_MS)),
                        );
                        let config = self.visual_config.volume_bubbles;
                        let max_candidates = config
                            .max_candidates_per_candle
                            .max(config.max_bubbles_per_bar);

                        if config.use_raw_trades_when_available
                            && self.is_trade_range_covered(fetch_from, fetch_to)
                        {
                            let summaries = self.bubble_summaries_from_raw_trades(
                                fetch_from,
                                fetch_to,
                                timeframe_ms,
                                self.chart.tick_size,
                                max_candidates,
                            );
                            self.insert_bubble_summaries(
                                summaries, fetch_from, fetch_to, 0, 0, None,
                            );
                            return None;
                        }

                        let range = FetchRange::BubbleSummary {
                            from: fetch_from,
                            to: fetch_to,
                            timeframe_ms,
                            price_step: self.chart.tick_size,
                            max_candidates_per_candle: max_candidates,
                        };
                        if let Some(action) = request_fetch(
                            &mut self.request_handler,
                            range,
                            Some(&self.chart.ticker_info),
                        ) {
                            return Some(action);
                        }
                    }
                }

                // priority 3, indicators
                // (e.g. open interest needs external fetch as it's not derived from klines)
                let ctx = indicator::kline::FetchCtx {
                    main_chart: &self.chart,
                    timeframe: timeseries.interval,
                    visible_earliest: visible_earliest_ms,
                    kline_latest,
                    prefetch_earliest: UnixMs::new(prefetch_earliest),
                };
                for (indicator_kind, indi) in self.indicators.iter_mut() {
                    let Some(indi) = indi.as_mut() else {
                        continue;
                    };
                    if let Some(range) = indi.fetch_range(&ctx) {
                        log::debug!(
                            "CHART IndicatorFetch | indicator={:?} range={}",
                            indicator_kind,
                            fetcher::format_fetch_range(&range)
                        );
                        if let Some(action) = request_fetch(
                            &mut self.request_handler,
                            range,
                            Some(&self.chart.ticker_info),
                        ) {
                            log::info!(
                                "CHART IndicatorFetchQueued | indicator={:?} range={}",
                                indicator_kind,
                                fetcher::format_fetch_range(&range)
                            );
                            return Some(action);
                        } else {
                            log::debug!(
                                "CHART IndicatorFetchSuppressed | indicator={:?} range={} reason=request_handler",
                                indicator_kind,
                                fetcher::format_fetch_range(&range)
                            );
                        }
                    }
                }

                // priority 4, missing klines & integrity check
                let check_earliest = UnixMs::new(prefetch_earliest).max(kline_earliest);
                let check_latest = visible_latest_ms.saturating_add(timeframe_ms);
                log::trace!(
                    "KLINE IntegrityCheck | check_earliest={} check_latest={}",
                    fetcher::format_time_short(check_earliest),
                    fetcher::format_time_short(check_latest)
                );

                if let Some(missing_keys) =
                    timeseries.check_kline_integrity(check_earliest, check_latest)
                {
                    let latest = missing_keys
                        .iter()
                        .max()
                        .unwrap_or(&visible_latest_ms)
                        .saturating_add(timeframe_ms);
                    let earliest = missing_keys
                        .iter()
                        .min()
                        .unwrap_or(&visible_earliest_ms)
                        .saturating_sub(timeframe_ms);

                    let range = FetchRange::Kline(earliest, latest);
                    log::warn!(
                        "KLINE IntegrityMissing | missing_count={} min={} max={} fetch={}",
                        missing_keys.len(),
                        missing_keys
                            .iter()
                            .min()
                            .map_or("-".to_string(), |t| fetcher::format_time_short(*t)),
                        missing_keys
                            .iter()
                            .max()
                            .map_or("-".to_string(), |t| fetcher::format_time_short(*t)),
                        fetcher::format_fetch_range(&range)
                    );
                    if let Some(action) = request_fetch(
                        &mut self.request_handler,
                        range,
                        Some(&self.chart.ticker_info),
                    ) {
                        return Some(action);
                    } else {
                        log::debug!(
                            "KLINE IntegrityFetchSuppressed | reason=request_handler fetch={}",
                            fetcher::format_fetch_range(&range)
                        );
                    }
                } else {
                    log::trace!(
                        "KLINE IntegrityPassed | check_range={}",
                        fetcher::format_time_range(check_earliest, check_latest)
                    );
                }
            }
            PlotData::TickBased(_) => {
                // TODO: implement trade fetch
                log::trace!(
                    "CHART TickBased | action=skip reason=trade_fetch_todo kind={:?}",
                    self.kind
                );
            }
        }

        None
    }

    pub fn reset_request_handler(&mut self) {
        let old_generation = self.request_handler.generation_id();
        let superseded_ids = self
            .request_handler
            .supersede_all_pending("settings_changed");

        log::info!(
            "CHART Reset | reason=settings_changed old_generation={} new_generation={} superseded_requests={}",
            old_generation,
            self.request_handler.generation_id(),
            superseded_ids.len()
        );

        // The superseded requests are still in the handler with Superseded status.
        // When their workers complete, they will be detected as stale generation.
        // We keep the handler (don't replace it) so we can track stale results.

        self.fetching_trades = (false, None);
        self.covered_trade_ranges.clear();
        self.covered_bubble_summary_ranges.clear();
    }

    /// Drops all derived historical market data so it is rebuilt from a fresh
    /// persistent-cache/network pass on the next chart tick.
    pub fn invalidate_market_data_cache(&mut self) {
        self.request_handler = RequestHandler::default();
        log::warn!("CHART Reset | reason=cache_invalidated request_history=discarded");
        self.raw_trades.clear();

        match &mut self.data_source {
            PlotData::TimeBased(timeseries) => {
                for data_point in timeseries.datapoints.values_mut() {
                    data_point.clear_trades();
                    data_point.bubble_summary = BubbleVolumeSummary::default();
                    data_point.trade_coverage = data::chart::kline::TradeCoverage::Unknown;
                }
                timeseries.update_poc_status();
            }
            PlotData::TickBased(_) => {}
        }

        for indicator in self.indicators.values_mut().filter_map(Option::as_mut) {
            indicator.rebuild_from_source(&self.data_source);
            indicator.clear_all_caches();
        }
        self.chart.cache.clear_all();
        self.last_tick = Instant::now() - std::time::Duration::from_secs(1);
    }

    /// Check if a fetch result should be applied or discarded as stale.
    pub fn is_fetch_stale(&self, req_id: uuid::Uuid) -> bool {
        self.request_handler.is_stale_generation(req_id)
    }

    /// Get the generation ID of a request for logging.
    pub fn request_generation(&self, req_id: uuid::Uuid) -> Option<u64> {
        self.request_handler.request_generation(req_id)
    }

    /// Get the current generation ID.
    pub fn current_generation(&self) -> u64 {
        self.request_handler.generation_id()
    }

    pub fn register_backfill_request(&mut self, req_id: uuid::Uuid, fetch: FetchRange) -> bool {
        match self
            .request_handler
            .add_request_with_id(req_id, fetch, Some(&self.chart.ticker_info))
        {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(ReqError::Failed(reason)) => {
                log::error!("Failed to request {:?}: {}", fetch, reason);
                false
            }
        }
    }

    pub fn mark_trade_request_completed(&mut self, req_id: uuid::Uuid) {
        self.request_handler.mark_completed(req_id);
    }

    pub fn mark_request_failed(&mut self, req_id: uuid::Uuid, error: String) {
        let failed_trade_request = self.request_handler.is_trade_request(req_id);
        self.request_handler.mark_failed(req_id, error);
        if failed_trade_request {
            self.fetching_trades = (false, None);
        }
    }

    pub fn mark_trade_range_covered(&mut self, from: UnixMs, to: UnixMs) {
        if to <= from {
            log::warn!(
                "DATA Trades CoveredSkip | incoming_range={} reason=invalid_range",
                fetcher::format_time_range(from, to)
            );
            return;
        }

        let before = self.covered_trade_ranges.clone();
        self.covered_trade_ranges.push((from, to));
        self.covered_trade_ranges.sort_by_key(|(from, _)| *from);

        let mut merged: Vec<(UnixMs, UnixMs)> = Vec::new();
        for (range_from, range_to) in self.covered_trade_ranges.drain(..) {
            if let Some((_, last_to)) = merged.last_mut()
                && range_from <= *last_to
            {
                *last_to = (*last_to).max(range_to);
                continue;
            }

            merged.push((range_from, range_to));
        }

        self.covered_trade_ranges = merged;
        log::debug!(
            "DATA Trades Covered | incoming_range={} before={} after={}",
            fetcher::format_time_range(from, to),
            format_trade_ranges(&before),
            format_trade_ranges(&self.covered_trade_ranges)
        );
    }

    pub fn mark_bubble_summary_range_covered(&mut self, from: UnixMs, to: UnixMs) {
        if to <= from {
            log::warn!(
                "BUBBLE Summary Skip | reason=invalid_range range={}",
                fetcher::format_time_range(from, to)
            );
            return;
        }

        let before = self.covered_bubble_summary_ranges.clone();
        self.covered_bubble_summary_ranges.push((from, to));
        self.covered_bubble_summary_ranges
            .sort_by_key(|(from, _)| *from);

        let mut merged: Vec<(UnixMs, UnixMs)> = Vec::new();
        for (range_from, range_to) in self.covered_bubble_summary_ranges.drain(..) {
            if let Some((_, last_to)) = merged.last_mut()
                && range_from <= *last_to
            {
                *last_to = (*last_to).max(range_to);
                continue;
            }

            merged.push((range_from, range_to));
        }

        self.covered_bubble_summary_ranges = merged;
        log::debug!(
            "BUBBLE Summary Covered | incoming_range={} before={} after={}",
            fetcher::format_time_range(from, to),
            format_trade_ranges(&before),
            format_trade_ranges(&self.covered_bubble_summary_ranges)
        );
    }

    pub fn is_trade_range_covered(&self, from: UnixMs, to: UnixMs) -> bool {
        self.covered_trade_ranges
            .iter()
            .any(|(covered_from, covered_to)| from >= *covered_from && to <= *covered_to)
    }

    pub fn subtract_covered_trade_ranges(
        &self,
        from: UnixMs,
        to: UnixMs,
    ) -> Option<(UnixMs, UnixMs)> {
        if to <= from {
            log::debug!(
                "DATA Trades SubtractCovered | input_range={} covered={} returned=- reason=invalid_range",
                fetcher::format_time_range(from, to),
                format_trade_ranges(&self.covered_trade_ranges)
            );
            return None;
        }

        if self.is_trade_range_covered(from, to) {
            log::debug!(
                "DATA Trades SubtractCovered | input_range={} covered={} returned=- reason=fully_covered",
                fetcher::format_time_range(from, to),
                format_trade_ranges(&self.covered_trade_ranges)
            );
            return None;
        }

        let mut cursor = from;
        for (covered_from, covered_to) in &self.covered_trade_ranges {
            if *covered_to <= cursor {
                continue;
            }

            if *covered_from > cursor {
                let result = (cursor, (*covered_from).min(to));
                log::debug!(
                    "DATA Trades SubtractCovered | input_range={} covered={} returned={} reason=gap_before_covered",
                    fetcher::format_time_range(from, to),
                    format_trade_ranges(&self.covered_trade_ranges),
                    fetcher::format_time_range(result.0, result.1)
                );
                return Some(result);
            }

            cursor = cursor.max(*covered_to);
            if cursor >= to {
                log::debug!(
                    "DATA Trades SubtractCovered | input_range={} covered={} returned=- reason=fully_covered_after_merge",
                    fetcher::format_time_range(from, to),
                    format_trade_ranges(&self.covered_trade_ranges)
                );
                return None;
            }
        }

        let result = (cursor, to);
        log::debug!(
            "DATA Trades SubtractCovered | input_range={} covered={} returned={} reason=tail_gap",
            fetcher::format_time_range(from, to),
            format_trade_ranges(&self.covered_trade_ranges),
            fetcher::format_time_range(result.0, result.1)
        );
        Some(result)
    }

    pub fn subtract_covered_bubble_summary_ranges(
        &self,
        from: UnixMs,
        to: UnixMs,
    ) -> Option<(UnixMs, UnixMs)> {
        subtract_covered_ranges(
            &self.covered_bubble_summary_ranges,
            from,
            to,
            "BUBBLE Summary",
        )
    }

    fn latest_uncovered_trade_range(&self, from: UnixMs, to: UnixMs) -> Option<(UnixMs, UnixMs)> {
        select_trade_fetch_gap(&self.covered_trade_ranges, from, to)
    }

    fn latest_uncovered_bubble_summary_range(
        &self,
        from: UnixMs,
        to: UnixMs,
    ) -> Option<(UnixMs, UnixMs)> {
        subtract_covered_ranges_latest(
            &self.covered_bubble_summary_ranges,
            from,
            to,
            "BUBBLE Summary Latest",
        )
    }

    pub fn missing_trade_range(&self, from: UnixMs, to: UnixMs) -> Option<(UnixMs, UnixMs)> {
        self.subtract_covered_trade_ranges(from, to)
    }

    pub fn complete_trade_fetch(
        &mut self,
        req_id: Option<uuid::Uuid>,
        fetch: Option<FetchRange>,
        outcome: fetcher::TradeFetchOutcome,
    ) {
        log::debug!(
            "TRADE CompleteFetch | req={} fetch={} fetching_before={} tail={}",
            fetcher::format_req_id(req_id),
            fetcher::format_fetch_range_compact(fetch),
            self.fetching_trades.0,
            outcome
                .unfilled_tail
                .map(|(f, t)| fetcher::format_time_range(f, t))
                .unwrap_or_else(|| "-".to_string())
        );
        if let Some(id) = req_id {
            self.mark_trade_request_completed(id);
        }

        if let Some(FetchRange::Trades(from, to)) = fetch {
            if let Some((tail_from, tail_to)) = outcome.empty_tail {
                log::info!(
                    "FETCH EmptyCovered | req={} range={}→{} reason=no_progress_near_target",
                    fetcher::format_req_id(req_id),
                    fetcher::format_time_short(tail_from),
                    fetcher::format_time_short(tail_to)
                );
                self.request_handler.mark_empty_trade_range(
                    &self.chart.ticker_info,
                    tail_from,
                    tail_to,
                );
            }
            self.mark_verified_trade_fetch_prefix(from, to, outcome.unfilled_tail);
        }

        self.fetching_trades = (false, None);
        log::debug!(
            "TRADE CompleteFetch | req={} fetching_after=false",
            fetcher::format_req_id(req_id)
        );
    }

    /// Mark a backfill as completed without touching per-pane fetching_trades
    /// state or RequestHandler. Backfill is tracked globally via pending_backfills.
    pub fn complete_backfill(
        &mut self,
        fetch: Option<FetchRange>,
        outcome: fetcher::TradeFetchOutcome,
    ) {
        log::info!(
            "BACKFILL Complete | fetch={} tail={}",
            fetcher::format_fetch_range_compact(fetch),
            outcome
                .unfilled_tail
                .map(|(f, t)| fetcher::format_time_range(f, t))
                .unwrap_or_else(|| "-".to_string())
        );

        if let Some(FetchRange::Trades(from, to)) = fetch {
            if let Some((tail_from, tail_to)) = outcome.empty_tail {
                self.request_handler.mark_empty_trade_range(
                    &self.chart.ticker_info,
                    tail_from,
                    tail_to,
                );
            }
            self.mark_verified_trade_fetch_prefix(from, to, outcome.unfilled_tail);
        }
    }

    /// Marks only the portion a trade worker actually traversed. An empty or
    /// no-progress tail is a retryable gap, not completed order-flow history.
    fn mark_verified_trade_fetch_prefix(
        &mut self,
        from: UnixMs,
        to: UnixMs,
        unfilled_tail: Option<(UnixMs, UnixMs)>,
    ) {
        let verified_to = unfilled_tail
            .map(|(tail_from, _)| tail_from.saturating_sub(1).min(to))
            .unwrap_or(to);

        if verified_to > from {
            self.mark_trade_range_covered(from, verified_to);
            self.mark_trade_buckets_complete(from, verified_to);
        } else {
            log::warn!(
                "DATA Trades CoverageSkipped | requested={} unfilled_tail={} reason=no_verified_prefix",
                fetcher::format_time_range(from, to),
                unfilled_tail
                    .map(|(tail_from, tail_to)| fetcher::format_time_range(tail_from, tail_to))
                    .unwrap_or_else(|| "-".to_string())
            );
        }
    }

    pub fn complete_bubble_summary_fetch(
        &mut self,
        req_id: Option<uuid::Uuid>,
        from: UnixMs,
        to: UnixMs,
    ) {
        log::info!(
            "BUBBLE Summary CompleteFetch | req={} range={}",
            fetcher::format_req_id(req_id),
            fetcher::format_time_range(from, to)
        );
        if let Some(id) = req_id {
            self.mark_trade_request_completed(id);
        }
        self.mark_bubble_summary_range_covered(from, to);
        // A BubbleSummary contains derived price/volume candidates, not the
        // raw executions needed by the footprint. It must not promote raw
        // trade coverage to Complete.
    }

    fn mark_trade_buckets_complete(&mut self, from: UnixMs, to: UnixMs) {
        match &mut self.data_source {
            PlotData::TimeBased(ts) => {
                ts.mark_range_trades_complete(from, to);
            }
            PlotData::TickBased(_) => {}
        }
        if let Some(cvd) = self.indicators[KlineIndicator::CumulativeDelta].as_mut() {
            cvd.rebuild_from_source(&self.data_source);
        }
    }

    /// Mark all fully traversed klines in the visible range as complete.
    /// Called when a trade fetch completes with empty results to prevent
    /// re-requesting the same range.
    pub fn mark_visible_range_trades_complete(&mut self) {
        let (visible_earliest, visible_latest) = match self.visible_timerange() {
            Some(range) => range,
            None => return,
        };
        let earliest_ms = exchange::UnixMs::new(visible_earliest);
        let latest_ms = exchange::UnixMs::new(visible_latest);

        match &mut self.data_source {
            PlotData::TimeBased(ts) => {
                ts.mark_range_trades_complete(earliest_ms, latest_ms);
            }
            PlotData::TickBased(_) => {}
        }
    }

    pub fn raw_trades(&self) -> Vec<Trade> {
        self.raw_trades.clone()
    }

    pub fn set_handle(&mut self, handle: Handle) {
        self.fetching_trades.1 = Some(handle);
    }

    pub fn tick_size(&self) -> PriceStep {
        self.chart.tick_size
    }

    pub fn study_configurator(&self) -> &study::Configurator<FootprintStudy> {
        &self.study_configurator
    }

    pub fn update_study_configurator(&mut self, message: study::Message<FootprintStudy>) {
        let KlineChartKind::Footprint {
            ref mut studies, ..
        } = self.kind
        else {
            return;
        };

        match self.study_configurator.update(message) {
            Some(study::Action::ToggleStudy(study, is_selected)) => {
                if is_selected {
                    let already_exists = studies.iter().any(|s| s.is_same_type(&study));
                    if !already_exists {
                        studies.push(study);
                    }
                } else {
                    studies.retain(|s| !s.is_same_type(&study));
                }
            }
            Some(study::Action::ConfigureStudy(study)) => {
                if let Some(existing_study) = studies.iter_mut().find(|s| s.is_same_type(&study)) {
                    *existing_study = study;
                }
            }
            None => {}
        }

        self.invalidate(None);
    }

    pub fn chart_layout(&self) -> ViewConfig {
        self.chart.layout()
    }

    pub fn visual_config(&self) -> Config {
        self.visual_config
    }

    pub fn indicator_enabled(&self, indicator: KlineIndicator) -> bool {
        self.indicators[indicator].is_some()
    }

    pub fn set_gex_snapshot(&mut self, snapshot: Option<Arc<data::chart::gex::GexSnapshot>>) {
        if self.gex_snapshot.as_ref().map(|value| value.observed_at)
            == snapshot.as_ref().map(|value| value.observed_at)
            && self.gex_snapshot.as_ref().map(|value| value.underlying)
                == snapshot.as_ref().map(|value| value.underlying)
        {
            return;
        }
        self.gex_snapshot = snapshot;
        self.chart.cache.clear_all();
    }

    pub fn volume_bubble_qty_scale(&self) -> VolumeBubbleQtyScale {
        let range = match &self.data_source {
            PlotData::TimeBased(timeseries) => timeseries.latest_timestamp().and_then(|latest| {
                volume_bubble_effective_range(
                    latest,
                    timeseries.interval.to_milliseconds(),
                    UnixMs::now(),
                    &self.visual_config.volume_bubbles,
                )
            }),
            PlotData::TickBased(_) => None,
        };

        volume_bubble_qty_scale(max_bubble_qty_in_range(
            &self.data_source,
            range.map_or(1, |(from, _)| from.as_u64()),
            range.map_or(0, |(_, to)| to.as_u64()),
            self.visual_config
                .volume_bubbles
                .use_raw_trades_when_available,
        ))
    }

    pub fn set_visual_config(&mut self, mut visual_config: Config) {
        visual_config.migrate_legacy_indicator_configs();
        let old_bubbles = self.visual_config.volume_bubbles;
        let new_bubbles = visual_config.volume_bubbles;
        let old_svp = self.visual_config.session_volume_profile;
        let new_svp = visual_config.session_volume_profile;
        let old_vwap = self.visual_config.vwap;
        let new_vwap = visual_config.vwap;

        let should_refetch_volume_bubbles = matches!(self.kind, KlineChartKind::Candles)
            && self.indicator_enabled(KlineIndicator::VolumeBubbles)
            && (old_bubbles.history_window_minutes != new_bubbles.history_window_minutes
                || old_bubbles.session != new_bubbles.session
                || old_bubbles
                    .max_candidates_per_candle
                    .max(old_bubbles.max_bubbles_per_bar)
                    != new_bubbles
                        .max_candidates_per_candle
                        .max(new_bubbles.max_bubbles_per_bar)
                || old_bubbles.use_raw_trades_when_available
                    != new_bubbles.use_raw_trades_when_available);
        let bubble_aggregation_changed = old_bubbles
            .max_candidates_per_candle
            .max(old_bubbles.max_bubbles_per_bar)
            != new_bubbles
                .max_candidates_per_candle
                .max(new_bubbles.max_bubbles_per_bar);
        let should_wake_trade_overlay = matches!(self.kind, KlineChartKind::Candles)
            && ((self.indicator_enabled(KlineIndicator::SessionVolumeProfile)
                && old_svp.interval != new_svp.interval)
                || (self.indicator_enabled(KlineIndicator::Vwap)
                    && old_vwap.anchor != new_vwap.anchor));

        if should_refetch_volume_bubbles {
            log::info!(
                "CHART Settings | bubbles old={:?}→{:?} reason=session_changed",
                old_bubbles.session,
                new_bubbles.session
            );
        }

        self.visual_config = visual_config;
        let config = self.visual_config;
        self.chart.cache.clear_all();
        self.indicators
            .values_mut()
            .filter_map(Option::as_mut)
            .for_each(|indi| {
                indi.on_config_changed(&config);
                indi.clear_all_caches();
            });

        if should_refetch_volume_bubbles {
            if bubble_aggregation_changed {
                self.covered_bubble_summary_ranges.clear();
                if let PlotData::TimeBased(timeseries) = &mut self.data_source {
                    for data_point in timeseries.datapoints.values_mut() {
                        data_point.bubble_summary = BubbleVolumeSummary::default();
                    }
                }
            }
            self.last_tick = Instant::now() - std::time::Duration::from_secs(1);
        } else if should_wake_trade_overlay {
            // Existing raw trades are reusable across session/row settings.
            self.last_tick = Instant::now() - std::time::Duration::from_secs(1);
        }
    }

    pub fn set_cluster_kind(&mut self, new_kind: ClusterKind) {
        if let KlineChartKind::Footprint {
            ref mut clusters,
            ref mut studies,
            ..
        } = self.kind
        {
            *clusters = new_kind;
            studies.retain(|study| new_kind.allows_study(study));
        }

        self.invalidate(None);
    }

    pub fn set_cluster_scaling(&mut self, new_scaling: ClusterScaling) {
        if let KlineChartKind::Footprint {
            ref mut scaling, ..
        } = self.kind
        {
            *scaling = new_scaling;
        }

        self.invalidate(None);
    }

    pub fn basis(&self) -> Basis {
        self.chart.basis
    }

    pub fn change_tick_size(&mut self, new_step: PriceStep) {
        let chart = self.mut_state();

        chart.cell_height *= (new_step.units as f32) / (chart.tick_size.units as f32);
        chart.tick_size = new_step;

        match self.data_source {
            PlotData::TickBased(ref mut tick_aggr) => {
                tick_aggr.change_tick_size(new_step, &self.raw_trades);
            }
            PlotData::TimeBased(ref mut timeseries) => {
                timeseries.change_tick_size(new_step, &self.raw_trades);
            }
        }

        self.indicators
            .values_mut()
            .filter_map(Option::as_mut)
            .for_each(|indi| indi.on_ticksize_change(&self.data_source));

        self.invalidate(None);
    }

    pub fn set_basis(&mut self, new_basis: Basis) -> Option<Action> {
        let previous_basis = self.chart.basis;

        self.chart.last_price = None;
        self.chart.basis = new_basis;

        match new_basis {
            Basis::Time(interval) => {
                if matches!(previous_basis, Basis::Tick(_)) {
                    self.raw_trades.clear();
                };

                let step = self.chart.tick_size;
                let timeseries = TimeSeries::<KlineDataPoint>::new(interval, step, &[]);
                self.data_source = PlotData::TimeBased(timeseries);
            }
            Basis::Tick(tick_count) => {
                let trades = if matches!(previous_basis, Basis::Tick(_)) {
                    &self.raw_trades
                } else {
                    self.raw_trades.clear();
                    &vec![]
                };

                let step = self.chart.tick_size;
                let tick_aggr = TickAggr::new(tick_count, step, trades);
                self.data_source = PlotData::TickBased(tick_aggr);
            }
        }

        self.indicators
            .values_mut()
            .filter_map(Option::as_mut)
            .for_each(|indi| indi.on_basis_change(&self.data_source));

        self.reset_request_handler();
        self.invalidate(Some(Instant::now()))
    }

    pub fn studies(&self) -> Option<Vec<FootprintStudy>> {
        match &self.kind {
            KlineChartKind::Footprint { studies, .. } => Some(studies.clone()),
            _ => None,
        }
    }

    pub fn set_studies(&mut self, new_studies: Vec<FootprintStudy>) {
        if let KlineChartKind::Footprint {
            clusters,
            ref mut studies,
            ..
        } = self.kind
        {
            *studies = new_studies
                .into_iter()
                .filter(|study| clusters.allows_study(study))
                .collect();
        }

        self.invalidate(None);
    }

    pub fn insert_trades(&mut self, buffer: &[Trade]) {
        let buffer = deduplicate_incoming_trades(&self.raw_trades, buffer);
        let raw_before = self.raw_trades.len();
        self.raw_trades.extend_from_slice(&buffer);

        // Prune oldest trades if we exceed the retention cap.
        if self.raw_trades.len() > MAX_RAW_TRADES {
            let excess = self.raw_trades.len() - MAX_RAW_TRADES;
            self.raw_trades.drain(..excess);
            log::debug!(
                "DATA Trades Prune | reason=cap exceeded={} removed={excess} retained={}",
                self.raw_trades.len() + excess,
                self.raw_trades.len()
            );
        }

        let content_type = match self.data_source {
            PlotData::TickBased(_) => "TickBased",
            PlotData::TimeBased(_) => "TimeBased",
        };
        log::trace!(
            "TRADE InsertLive | content_type={content_type} buffer_len={} first_trade_t={} last_trade_t={} raw_before={} raw_after={}",
            buffer.len(),
            fetcher::format_optional_time(buffer.first().map(|trade| trade.time)),
            fetcher::format_optional_time(buffer.last().map(|trade| trade.time)),
            raw_before,
            self.raw_trades.len()
        );

        match self.data_source {
            PlotData::TickBased(ref mut tick_aggr) => {
                let old_dp_len = tick_aggr.datapoints.len();
                tick_aggr.insert_trades(&buffer);

                if let Some(last_dp) = tick_aggr.datapoints.last() {
                    self.chart.last_price =
                        Some(PriceInfoLabel::new(last_dp.kline.close, last_dp.kline.open));
                } else {
                    self.chart.last_price = None;
                }

                self.indicators
                    .values_mut()
                    .filter_map(Option::as_mut)
                    .for_each(|indi| indi.on_insert_trades(&buffer, old_dp_len, &self.data_source));

                self.invalidate(None);
            }
            PlotData::TimeBased(ref mut timeseries) => {
                timeseries.insert_trades_existing_buckets(&buffer);

                self.indicators
                    .values_mut()
                    .filter_map(Option::as_mut)
                    .for_each(|indi| indi.on_insert_trades(&buffer, 0, &self.data_source));

                self.invalidate(None);
            }
        }
    }

    pub fn insert_raw_trades(&mut self, raw_trades: Vec<Trade>, is_batches_done: bool) {
        let received_size = raw_trades.len();
        let raw_trades = deduplicate_incoming_trades(&self.raw_trades, &raw_trades);
        let batch_size = raw_trades.len();
        let duplicate_count = received_size.saturating_sub(batch_size);
        let raw_before = self.raw_trades.len();
        let earliest = raw_trades.first().map(|t| t.time);
        let latest = raw_trades.last().map(|t| t.time);

        log::debug!(
            "DATA Trades | received={received_size} deduplicated={duplicate_count} fetched_batch={batch_size} raw_before={raw_before} raw_after={} first={} last={} is_batches_done={is_batches_done}",
            raw_before + batch_size,
            fetcher::format_optional_time(earliest),
            fetcher::format_optional_time(latest)
        );

        if matches!(&self.data_source, PlotData::TickBased(_)) {
            if is_batches_done {
                self.fetching_trades = (false, None);
            }
            return;
        }

        if let PlotData::TimeBased(ref mut timeseries) = self.data_source {
            timeseries.insert_trades_existing_buckets(&raw_trades);
        }

        self.raw_trades.extend_from_slice(&raw_trades);

        // Prune oldest trades if we exceed the retention cap.
        if self.raw_trades.len() > MAX_RAW_TRADES {
            let excess = self.raw_trades.len() - MAX_RAW_TRADES;
            self.raw_trades.drain(..excess);
            log::debug!(
                "DATA Trades Prune | reason=cap exceeded={} removed={excess} retained={}",
                self.raw_trades.len() + excess,
                self.raw_trades.len()
            );
        }

        self.indicators
            .values_mut()
            .filter_map(Option::as_mut)
            .for_each(|indi| indi.on_insert_trades(&raw_trades, 0, &self.data_source));

        if is_batches_done {
            self.fetching_trades = (false, None);
            log::info!(
                "DATA Trades Done | total_raw={} final_batch={batch_size} is_batches_done={is_batches_done}",
                self.raw_trades.len()
            );
            if batch_size == 0 {
                log::debug!(
                    "DATA Trades Done | final_batch=0 fetching_trades=false reason=terminal_signal_without_new_records"
                );
            }
        }

        self.invalidate(None);
    }

    pub fn insert_bubble_summaries(
        &mut self,
        summaries: Vec<BubbleVolumeSummary>,
        from: UnixMs,
        to: UnixMs,
        trades_seen: usize,
        raw_discarded: usize,
        req_id: Option<uuid::Uuid>,
    ) {
        let candles = summaries.len();
        let candidates = summaries
            .iter()
            .map(|summary| summary.candidates.len())
            .sum::<usize>();

        log::info!(
            "BUBBLE Summary Insert | req={} range={} candles={} candidates={} trades_seen={} raw_discarded={} raw_trades_kept={}",
            fetcher::format_req_id(req_id),
            fetcher::format_time_range(from, to),
            candles,
            candidates,
            trades_seen,
            raw_discarded,
            self.raw_trades.len()
        );

        if let PlotData::TimeBased(ref mut timeseries) = self.data_source {
            timeseries.insert_bubble_summaries(summaries);
        }

        self.complete_bubble_summary_fetch(req_id, from, to);
        self.invalidate(None);
    }

    fn bubble_summaries_from_raw_trades(
        &self,
        from: UnixMs,
        to: UnixMs,
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
        for trade in self
            .raw_trades
            .iter()
            .filter(|trade| trade.time >= from && trade.time <= to)
        {
            let candle_time =
                UnixMs::new(trade.time.as_u64() - (trade.time.as_u64() % timeframe_ms));
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

    pub fn insert_hist_klines(&mut self, req_id: uuid::Uuid, klines_raw: &[Kline]) {
        let count = klines_raw.len();
        let earliest = klines_raw.first().map(|k| k.time);
        let latest = klines_raw.last().map(|k| k.time);

        log::info!(
            "DATA Klines | req={} records={count} first={} last={}",
            fetcher::short_id(req_id),
            fetcher::format_optional_time(earliest),
            fetcher::format_optional_time(latest)
        );

        match self.data_source {
            PlotData::TimeBased(ref mut timeseries) => {
                let new_buckets = klines_raw
                    .iter()
                    .filter(|kline| !timeseries.datapoints.contains_key(&kline.time))
                    .map(|kline| kline.time)
                    .collect::<FxHashSet<_>>();
                timeseries.insert_klines(klines_raw);
                if !new_buckets.is_empty() {
                    let trades_for_new_buckets = self
                        .raw_trades
                        .iter()
                        .filter(|trade| {
                            new_buckets.contains(&trade.time.floor_to(timeseries.interval))
                        })
                        .copied()
                        .collect::<Vec<_>>();
                    timeseries.insert_trades_existing_buckets(&trades_for_new_buckets);
                    log::debug!(
                        "DATA Klines TradeBackfill | new_buckets={} trades={} reason=new_kline_buckets_only",
                        new_buckets.len(),
                        trades_for_new_buckets.len()
                    );
                }

                self.indicators
                    .values_mut()
                    .filter_map(Option::as_mut)
                    .for_each(|indi| indi.on_insert_klines(klines_raw, &self.data_source));

                if klines_raw.is_empty() {
                    log::warn!(
                        "DATA Klines Complete | req={} records=0 transition=failed reason=no_data",
                        fetcher::short_id(req_id)
                    );
                    self.request_handler
                        .mark_failed(req_id, "No data received".to_string());
                } else {
                    log::debug!(
                        "DATA Klines Complete | req={} records={} transition=completed",
                        fetcher::short_id(req_id),
                        klines_raw.len()
                    );
                    self.request_handler.mark_completed(req_id);
                }
                self.invalidate(None);
            }
            PlotData::TickBased(_) => {}
        }
    }

    pub fn insert_open_interest(&mut self, req_id: Option<uuid::Uuid>, oi_data: &[OIData]) {
        if let Some(req_id) = req_id {
            if oi_data.is_empty() {
                self.request_handler
                    .mark_failed(req_id, "No data received".to_string());
            } else {
                self.request_handler.mark_completed(req_id);
            }
        }

        if let Some(indi) = self.indicators[KlineIndicator::OpenInterest].as_mut() {
            indi.on_open_interest(oi_data);
        }
    }

    fn calc_qty_scales(
        &self,
        earliest: u64,
        latest: u64,
        highest: Price,
        lowest: Price,
        step: PriceStep,
        cluster_kind: ClusterKind,
    ) -> f64 {
        let rounded_highest = highest.round_to_side_step(false, step).add_steps(1, step);
        let rounded_lowest = lowest.round_to_side_step(true, step).add_steps(-1, step);

        match &self.data_source {
            PlotData::TimeBased(timeseries) => timeseries
                .max_qty_ts_range(
                    cluster_kind,
                    UnixMs::new(earliest),
                    UnixMs::new(latest),
                    rounded_highest,
                    rounded_lowest,
                )
                .to_f64(),
            PlotData::TickBased(tick_aggr) => {
                let earliest = earliest as usize;
                let latest = latest as usize;

                tick_aggr
                    .max_qty_idx_range(
                        cluster_kind,
                        earliest,
                        latest,
                        rounded_highest,
                        rounded_lowest,
                    )
                    .to_f64()
            }
        }
    }

    pub fn last_update(&self) -> Instant {
        self.last_tick
    }

    pub fn invalidate(&mut self, now: Option<Instant>) -> Option<Action> {
        let chart = &mut self.chart;

        if let Some(autoscale) = chart.layout.autoscale {
            match autoscale {
                super::Autoscale::CenterLatest => {
                    let x_translation = match &self.kind {
                        KlineChartKind::Footprint { .. } => {
                            0.5 * (chart.bounds.width / chart.scaling)
                                - (chart.cell_width / chart.scaling)
                        }
                        KlineChartKind::Candles => {
                            0.5 * (chart.bounds.width / chart.scaling)
                                - (8.0 * chart.cell_width / chart.scaling)
                        }
                    };
                    chart.translation.x = x_translation;

                    let calculate_target_y = |kline: exchange::Kline| -> f32 {
                        let y_low = chart.price_to_y(kline.low);
                        let y_high = chart.price_to_y(kline.high);
                        let y_close = chart.price_to_y(kline.close);

                        let mut target_y_translation = -(y_low + y_high) / 2.0;

                        if chart.bounds.height > f32::EPSILON && chart.scaling > f32::EPSILON {
                            let visible_half_height = (chart.bounds.height / chart.scaling) / 2.0;

                            let view_center_y_centered = -target_y_translation;

                            let visible_y_top = view_center_y_centered - visible_half_height;
                            let visible_y_bottom = view_center_y_centered + visible_half_height;

                            let padding = chart.cell_height;

                            if y_close < visible_y_top {
                                target_y_translation = -(y_close - padding + visible_half_height);
                            } else if y_close > visible_y_bottom {
                                target_y_translation = -(y_close + padding - visible_half_height);
                            }
                        }
                        target_y_translation
                    };

                    chart.translation.y = self.data_source.latest_y_midpoint(calculate_target_y);
                }
                super::Autoscale::FitToVisible => {
                    let visible_region = chart.visible_region(chart.bounds.size());
                    let (start_interval, end_interval) = chart.interval_range(&visible_region);

                    if let Some((lowest, highest)) = self
                        .data_source
                        .visible_price_range(start_interval, end_interval)
                    {
                        let chart_height = chart.bounds.height;
                        let tick_size = chart.tick_size.to_f32_lossy();

                        if chart_height > f32::EPSILON && tick_size > 0.0 {
                            let (fit_lowest, fit_highest) =
                                if let KlineChartKind::Footprint { .. } = self.kind {
                                    if let Some((footprint_low, footprint_high)) = self
                                        .data_source
                                        .visible_footprint_price_range(start_interval, end_interval)
                                    {
                                        let half_tick = tick_size * 0.5;
                                        (
                                            footprint_low.to_f32_lossy() - half_tick,
                                            footprint_high.to_f32_lossy() + half_tick,
                                        )
                                    } else {
                                        (lowest, highest)
                                    }
                                } else {
                                    (lowest, highest)
                                };

                            let visible_span = (fit_highest - fit_lowest).max(tick_size);
                            let base_padding = visible_span * 0.05; // 5% padding on top and bottom

                            let mut top_padding = base_padding;
                            let mut bottom_padding = base_padding;

                            if let KlineChartKind::Footprint { clusters, .. } = self.kind {
                                let provisional_span = visible_span + top_padding + bottom_padding;
                                if provisional_span > 0.0 {
                                    let provisional_cell_height =
                                        (chart_height * tick_size) / provisional_span;

                                    let outer_padding = price_padding_from_pixels(
                                        provisional_cell_height,
                                        tick_size,
                                    );

                                    top_padding += outer_padding;
                                    bottom_padding += outer_padding;

                                    if self.visual_config.show_footprint_summary {
                                        bottom_padding =
                                            bottom_padding.max(footprint_summary_padding(
                                                provisional_cell_height,
                                                chart.scaling,
                                                chart.cell_width,
                                                tick_size,
                                                clusters,
                                            ));
                                    }
                                }
                            }

                            let padded_span = visible_span + top_padding + bottom_padding;
                            if padded_span > 0.0 {
                                chart.cell_height = (chart_height * tick_size) / padded_span;
                                chart.base_price_y = Price::from_f32(fit_highest + top_padding);
                                chart.translation.y = -chart_height / 2.0;
                            }
                        }
                    }
                }
            }
        }

        chart.cache.clear_all();
        for indi in self.indicators.values_mut().filter_map(Option::as_mut) {
            indi.clear_all_caches();
        }

        if let Some(t) = now {
            self.last_tick = t;
            self.fetch_missing_data()
        } else {
            None
        }
    }

    pub fn toggle_indicator(&mut self, indicator: KlineIndicator) {
        if !self.kind.allows_indicator(indicator) {
            return;
        }

        let prev_indi_count = KlineIndicator::for_market(self.chart.ticker_info.market_type())
            .iter()
            .filter(|indicator| !indicator.is_overlay() && self.indicators[**indicator].is_some())
            .count();

        if self.indicators[indicator].is_some() {
            self.indicators[indicator] = None;
        } else {
            let mut box_indi = indicator::kline::make_empty(indicator);
            box_indi.on_config_changed(&self.visual_config);
            box_indi.rebuild_from_source(&self.data_source);
            self.indicators[indicator] = Some(box_indi);
        }

        if let Some(main_split) = self.chart.layout.splits.first() {
            let current_indi_count =
                KlineIndicator::for_market(self.chart.ticker_info.market_type())
                    .iter()
                    .filter(|indicator| {
                        !indicator.is_overlay() && self.indicators[**indicator].is_some()
                    })
                    .count();
            self.chart.layout.splits = data::util::calc_panel_splits(
                *main_split,
                current_indi_count,
                Some(prev_indi_count),
            );
        }
        self.invalidate(None);
        self.last_tick = Instant::now() - std::time::Duration::from_secs(1);
    }
}

fn format_trade_ranges(ranges: &[(UnixMs, UnixMs)]) -> String {
    if ranges.is_empty() {
        return "-".to_string();
    }

    ranges
        .iter()
        .map(|(from, to)| fetcher::format_time_range(*from, *to))
        .collect::<Vec<_>>()
        .join(",")
}

fn subtract_covered_ranges(
    covered_ranges: &[(UnixMs, UnixMs)],
    from: UnixMs,
    to: UnixMs,
    log_prefix: &str,
) -> Option<(UnixMs, UnixMs)> {
    if to <= from {
        log::debug!(
            "{log_prefix} SubtractCovered | input_range={} covered={} returned=- reason=invalid_range",
            fetcher::format_time_range(from, to),
            format_trade_ranges(covered_ranges)
        );
        return None;
    }

    if covered_ranges
        .iter()
        .any(|(covered_from, covered_to)| from >= *covered_from && to <= *covered_to)
    {
        log::debug!(
            "{log_prefix} SubtractCovered | input_range={} covered={} returned=- reason=fully_covered",
            fetcher::format_time_range(from, to),
            format_trade_ranges(covered_ranges)
        );
        return None;
    }

    let mut cursor = from;
    for (covered_from, covered_to) in covered_ranges {
        if *covered_to <= cursor {
            continue;
        }

        if *covered_from > cursor {
            let result = (cursor, (*covered_from).min(to));
            log::debug!(
                "{log_prefix} SubtractCovered | input_range={} covered={} returned={} reason=gap_before_covered",
                fetcher::format_time_range(from, to),
                format_trade_ranges(covered_ranges),
                fetcher::format_time_range(result.0, result.1)
            );
            return Some(result);
        }

        cursor = cursor.max(*covered_to);
        if cursor >= to {
            log::debug!(
                "{log_prefix} SubtractCovered | input_range={} covered={} returned=- reason=fully_covered_after_merge",
                fetcher::format_time_range(from, to),
                format_trade_ranges(covered_ranges)
            );
            return None;
        }
    }

    let result = (cursor, to);
    log::debug!(
        "{log_prefix} SubtractCovered | input_range={} covered={} returned={} reason=tail_gap",
        fetcher::format_time_range(from, to),
        format_trade_ranges(covered_ranges),
        fetcher::format_time_range(result.0, result.1)
    );
    Some(result)
}

/// Returns the newest uncovered sub-range inside `[from, to)`. Covered ranges
/// are expected to be sorted and merged, as maintained by `KlineChart`.
fn subtract_covered_ranges_latest(
    covered_ranges: &[(UnixMs, UnixMs)],
    from: UnixMs,
    to: UnixMs,
    log_prefix: &str,
) -> Option<(UnixMs, UnixMs)> {
    if to <= from {
        log::debug!(
            "{log_prefix} SubtractCovered | input_range={} covered={} returned=- reason=invalid_range",
            fetcher::format_time_range(from, to),
            format_trade_ranges(covered_ranges)
        );
        return None;
    }

    let mut cursor = to;
    for (covered_from, covered_to) in covered_ranges.iter().rev() {
        if *covered_from >= cursor {
            continue;
        }

        if *covered_to < cursor {
            let result = ((*covered_to).max(from), cursor);
            if result.0 < result.1 {
                log::debug!(
                    "{log_prefix} SubtractCovered | input_range={} covered={} returned={} reason=latest_gap",
                    fetcher::format_time_range(from, to),
                    format_trade_ranges(covered_ranges),
                    fetcher::format_time_range(result.0, result.1)
                );
                return Some(result);
            }
        }

        cursor = cursor.min(*covered_from);
        if cursor <= from {
            log::debug!(
                "{log_prefix} SubtractCovered | input_range={} covered={} returned=- reason=fully_covered",
                fetcher::format_time_range(from, to),
                format_trade_ranges(covered_ranges)
            );
            return None;
        }
    }

    let result = (from, cursor);
    (result.0 < result.1).then_some(result)
}

/// Keep the moving live edge from starving a long historical backfill. A
/// recent tail of at most one minute can wait for the live stream while the
/// next worker advances the older gap. Once that tail grows, it is refreshed
/// before historical traversal resumes.
fn select_trade_fetch_gap(
    covered_ranges: &[(UnixMs, UnixMs)],
    from: UnixMs,
    to: UnixMs,
) -> Option<(UnixMs, UnixMs)> {
    const LIVE_TAIL_DEFER_MS: u64 = 60_000;

    let latest = subtract_covered_ranges_latest(covered_ranges, from, to, "DATA Trades Latest")?;
    let latest_is_short_live_tail =
        latest.1 == to && latest.1.saturating_diff(latest.0) <= LIVE_TAIL_DEFER_MS;

    if latest_is_short_live_tail
        && let Some(oldest) =
            subtract_covered_ranges(covered_ranges, from, to, "DATA Trades Historical")
        && oldest != latest
    {
        return Some(oldest);
    }

    Some(latest)
}

impl KlineChart {
    fn sanitized_kind(mut kind: KlineChartKind) -> KlineChartKind {
        if let KlineChartKind::Footprint {
            clusters, studies, ..
        } = &mut kind
        {
            studies.retain(|study| clusters.allows_study(study));
        }

        kind
    }
}

impl canvas::Program<Message> for KlineChart {
    type State = Interaction;

    fn update(
        &self,
        interaction: &mut Interaction,
        event: &Event,
        bounds: Rectangle,
        cursor: mouse::Cursor,
    ) -> Option<canvas::Action<Message>> {
        super::canvas_interaction(self, interaction, event, bounds, cursor)
    }

    fn draw(
        &self,
        interaction: &Interaction,
        renderer: &Renderer,
        theme: &Theme,
        bounds: Rectangle,
        cursor: mouse::Cursor,
    ) -> Vec<Geometry> {
        let chart = self.state();

        if chart.bounds.width == 0.0 {
            return vec![];
        }

        let bounds_size = bounds.size();
        let palette = theme.extended_palette();

        let klines = chart.cache.main.draw(renderer, bounds_size, |frame| {
            let center = Vector::new(bounds.width / 2.0, bounds.height / 2.0);

            frame.translate(center);
            frame.scale(chart.scaling);
            frame.translate(chart.translation);

            let region = chart.visible_region(frame.size());
            let (earliest, latest) = chart.interval_range(&region);

            let price_to_y = |price| chart.price_to_y(price);
            let interval_to_x = |interval| chart.interval_to_x(interval);

            match &self.kind {
                KlineChartKind::Footprint {
                    clusters,
                    scaling,
                    studies,
                } => {
                    let (highest, lowest) = chart.price_range(&region);

                    let max_cluster_qty = self.calc_qty_scales(
                        earliest,
                        latest,
                        highest,
                        lowest,
                        chart.tick_size,
                        *clusters,
                    );

                    let cell_height_unscaled = chart.cell_height * chart.scaling;
                    let cell_width_unscaled = chart.cell_width * chart.scaling;

                    let text_size =
                        footprint_cluster_text_size(cell_height_unscaled, cell_width_unscaled);

                    let candle_width = 0.1 * chart.cell_width;
                    let content_spacing = ContentGaps::from_view(candle_width, chart.scaling);

                    let imbalance = studies.iter().find_map(|study| {
                        if let FootprintStudy::Imbalance {
                            threshold,
                            color_scale,
                            ignore_zeros,
                        } = study
                        {
                            Some((*threshold, *color_scale, *ignore_zeros))
                        } else {
                            None
                        }
                    });

                    let show_text = should_show_text(
                        cell_height_unscaled,
                        cell_width_unscaled,
                        footprint_cluster_min_width(*clusters),
                    );
                    let cell_layout = FootprintCellLayout {
                        cell_w: chart.cell_width,
                        cell_h: chart.cell_height,
                        candle_w: candle_width,
                        pal: palette,
                        cluster: *clusters,
                        gaps: content_spacing,
                    };

                    if *clusters != ClusterKind::Table {
                        draw_all_npocs(
                            &self.data_source,
                            frame,
                            price_to_y,
                            interval_to_x,
                            &cell_layout,
                            studies,
                            earliest,
                            latest,
                            imbalance.is_some(),
                        );
                    }

                    render_data_source(
                        &self.data_source,
                        frame,
                        earliest,
                        latest,
                        interval_to_x,
                        |frame, x_position, kline, trades, _summary| {
                            let cluster_scaling = effective_cluster_qty(
                                *scaling,
                                max_cluster_qty,
                                trades,
                                cell_layout.cluster,
                            );

                            draw_clusters(
                                frame,
                                price_to_y,
                                x_position,
                                &cell_layout,
                                chart.scaling,
                                cluster_scaling,
                                text_size,
                                self.tick_size(),
                                show_text,
                                self.visual_config.show_footprint_summary,
                                imbalance,
                                kline,
                                trades,
                            );
                        },
                    );
                }
                KlineChartKind::Candles => {
                    let candle_width = chart.cell_width * 0.8;
                    let svp = self.visual_config.session_volume_profile;
                    if self.indicator_enabled(KlineIndicator::SessionVolumeProfile) {
                        draw_session_volume_profiles(
                            &self.data_source,
                            frame,
                            earliest,
                            latest,
                            interval_to_x,
                            price_to_y,
                            chart.cell_height,
                            chart.tick_size,
                            &svp,
                            palette,
                        );
                    }
                    if self.indicator_enabled(KlineIndicator::Vwap) {
                        draw_vwap_overlay(
                            &self.data_source,
                            frame,
                            earliest,
                            latest,
                            interval_to_x,
                            price_to_y,
                            &self.visual_config.vwap,
                            palette,
                        );
                    }
                    if self.indicator_enabled(KlineIndicator::GexLevels)
                        && let Some(snapshot) = &self.gex_snapshot
                    {
                        draw_gex_levels(
                            frame,
                            price_to_y,
                            snapshot,
                            &self.visual_config.gex_levels(),
                            chart.tick_size,
                            self.data_source
                                .latest_y_midpoint(|kline| kline.close.to_f32_lossy())
                                as f64,
                            region,
                            chart.scaling,
                            palette,
                        );
                    }
                    let volume_bubbles = self.visual_config.volume_bubbles;
                    let bubbles_enabled = self.indicator_enabled(KlineIndicator::VolumeBubbles);
                    let volume_bubble_range = bubbles_enabled
                        .then(|| match &self.data_source {
                            PlotData::TimeBased(timeseries) => {
                                timeseries.latest_timestamp().and_then(|latest| {
                                    volume_bubble_effective_range(
                                        latest,
                                        timeseries.interval.to_milliseconds(),
                                        UnixMs::now(),
                                        &volume_bubbles,
                                    )
                                })
                            }
                            PlotData::TickBased(_) => None,
                        })
                        .flatten();
                    let visible_max_bubble_qty = if bubbles_enabled {
                        visible_max_bubble_qty(
                            &self.data_source,
                            volume_bubble_range
                                .map_or(earliest, |(from, _)| earliest.max(from.as_u64())),
                            volume_bubble_range.map_or(latest, |(_, to)| latest.min(to.as_u64())),
                            volume_bubbles.min_qty,
                            volume_bubbles.use_raw_trades_when_available,
                        )
                    } else {
                        0.0
                    };

                    render_data_source(
                        &self.data_source,
                        frame,
                        earliest,
                        latest,
                        interval_to_x,
                        |frame, x_position, kline, trades, summary| {
                            draw_candle_dp(
                                frame,
                                price_to_y,
                                candle_width,
                                palette,
                                x_position,
                                kline,
                            );

                            if bubbles_enabled
                                && volume_bubble_range.is_some_and(|(from, to)| {
                                    kline.time >= from && kline.time <= to
                                })
                            {
                                draw_volume_bubbles(
                                    frame,
                                    price_to_y,
                                    x_position,
                                    trades,
                                    summary,
                                    &volume_bubbles,
                                    visible_max_bubble_qty,
                                    chart.scaling,
                                    palette,
                                );
                            }
                        },
                    );
                }
            }

            chart.draw_last_price_line(frame, palette, region);
        });

        let crosshair = chart.cache.crosshair.draw(renderer, bounds_size, |frame| {
            let visible_region = chart.visible_region(bounds_size);
            let visible_range = chart.interval_range(&visible_region);

            if let Some(cursor_position) = cursor.position_in(bounds) {
                let (_, rounded_aggregation) =
                    chart.draw_crosshair(frame, theme, bounds_size, cursor_position, interaction);

                draw_crosshair_tooltip(
                    &self.data_source,
                    &chart.ticker_info,
                    frame,
                    palette,
                    chart.basis,
                    Some(rounded_aggregation),
                    visible_range,
                );
            } else if self.visual_config.data_labels_always_visible {
                draw_crosshair_tooltip(
                    &self.data_source,
                    &chart.ticker_info,
                    frame,
                    palette,
                    chart.basis,
                    None,
                    visible_range,
                );
            }
        });

        vec![klines, crosshair]
    }

    fn mouse_interaction(
        &self,
        interaction: &Interaction,
        bounds: Rectangle,
        cursor: mouse::Cursor,
    ) -> mouse::Interaction {
        match interaction {
            Interaction::Panning { .. } => mouse::Interaction::Grabbing,
            Interaction::Zoomin { .. } => mouse::Interaction::ZoomIn,
            Interaction::None | Interaction::Ruler { .. } => {
                if cursor.is_over(bounds) {
                    mouse::Interaction::Crosshair
                } else {
                    mouse::Interaction::default()
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct VwapPoint {
    time: u64,
    value: Price,
    upper: Price,
    lower: Price,
}

fn build_vwap_sessions(
    data_source: &PlotData<KlineDataPoint>,
    earliest: u64,
    latest: u64,
    config: &VwapConfig,
) -> Vec<Vec<VwapPoint>> {
    let PlotData::TimeBased(timeseries) = data_source else {
        return Vec::new();
    };
    let anchor_ms = config.anchor.milliseconds();
    let from = align_session_start(earliest, anchor_ms);
    let mut sessions = Vec::<Vec<VwapPoint>>::new();
    let mut active_session = None;
    let mut sum_volume = 0.0;
    let mut sum_price_volume = 0.0;
    let mut sum_price_squared_volume = 0.0;

    for (_, dp) in timeseries
        .datapoints
        .range(UnixMs::new(from)..=UnixMs::new(latest))
    {
        let session = align_session_start(dp.kline.time.as_u64(), anchor_ms);
        if active_session != Some(session) {
            active_session = Some(session);
            sum_volume = 0.0;
            sum_price_volume = 0.0;
            sum_price_squared_volume = 0.0;
            sessions.push(Vec::new());
        }
        for (price, trades) in &dp.footprint.trades {
            let volume = trades.total_qty().to_f64();
            let price = price.to_f64();
            sum_volume += volume;
            sum_price_volume += price * volume;
            sum_price_squared_volume += price * price * volume;
        }
        if sum_volume <= 0.0 {
            continue;
        }
        let vwap = sum_price_volume / sum_volume;
        let variance = (sum_price_squared_volume / sum_volume - vwap * vwap).max(0.0);
        let band = variance.sqrt() * f64::from(config.band_multiplier.max(0.0));
        if let Some(points) = sessions.last_mut() {
            points.push(VwapPoint {
                time: dp.kline.time.as_u64(),
                value: Price::from_f64(vwap),
                upper: Price::from_f64(vwap + band),
                lower: Price::from_f64(vwap - band),
            });
        }
    }
    sessions.retain(|points| !points.is_empty());
    sessions
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GexOverlayKind {
    GammaFlip,
    CallWall,
    PutWall,
    GammaCluster,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GexOverlayPrimitive {
    Line,
    Band,
}

fn gex_overlay_primitive(kind: GexOverlayKind) -> GexOverlayPrimitive {
    match kind {
        GexOverlayKind::GammaCluster => GexOverlayPrimitive::Band,
        GexOverlayKind::GammaFlip | GexOverlayKind::CallWall | GexOverlayKind::PutWall => {
            GexOverlayPrimitive::Line
        }
    }
}

fn draw_gex_levels(
    frame: &mut canvas::Frame,
    price_to_y: impl Fn(Price) -> f32,
    snapshot: &data::chart::gex::GexSnapshot,
    config: &data::chart::gex::GexLevelsConfig,
    tick_size: PriceStep,
    latest_chart_price: f64,
    visible_region: Rectangle,
    chart_scaling: f32,
    palette: &Extended,
) {
    use data::chart::gex::{GexBasisMode, GexLevelColor};

    struct Level {
        kind: GexOverlayKind,
        raw: f64,
        half_band: f64,
        color: Color,
    }

    let resolve_color = |role| match role {
        GexLevelColor::Primary => palette.primary.strong.color,
        GexLevelColor::Success => palette.success.strong.color,
        GexLevelColor::Danger => palette.danger.strong.color,
        GexLevelColor::Warning => palette.warning.strong.color,
        GexLevelColor::Secondary => palette.secondary.strong.color,
    };

    let basis = if config.basis_mode == GexBasisMode::ShiftToChartPrice
        && latest_chart_price.is_finite()
        && latest_chart_price > 0.0
    {
        latest_chart_price - snapshot.source_spot
    } else {
        0.0
    };
    let Some(horizontal) = gex_overlay_horizontal_bounds(visible_region, chart_scaling) else {
        return;
    };
    let mut levels = Vec::new();
    if config.show_gamma_flip
        && let Some(value) = snapshot.gamma_flip
    {
        levels.push(Level {
            kind: GexOverlayKind::GammaFlip,
            raw: value,
            half_band: 0.0,
            color: resolve_color(config.gamma_flip_color),
        });
    }
    if config.show_call_wall
        && let Some(value) = snapshot.call_wall
    {
        levels.push(Level {
            kind: GexOverlayKind::CallWall,
            raw: value,
            half_band: 0.0,
            color: resolve_color(config.call_wall_color),
        });
    }
    if config.show_put_wall
        && let Some(value) = snapshot.put_wall
    {
        levels.push(Level {
            kind: GexOverlayKind::PutWall,
            raw: value,
            half_band: 0.0,
            color: resolve_color(config.put_wall_color),
        });
    }
    if config.show_top_clusters {
        let mut clusters = snapshot.strikes.iter().enumerate().collect::<Vec<_>>();
        clusters.sort_by(|a, b| b.1.absolute_gamma_1pct.total_cmp(&a.1.absolute_gamma_1pct));
        for (index, strike) in clusters.into_iter().take(config.max_clusters.min(10)) {
            let previous_gap = index
                .checked_sub(1)
                .map(|previous| strike.strike - snapshot.strikes[previous].strike);
            let next_gap = snapshot
                .strikes
                .get(index + 1)
                .map(|next| next.strike - strike.strike);
            let adjacent_gap = previous_gap
                .into_iter()
                .chain(next_gap)
                .filter(|gap| gap.is_finite() && *gap > 0.0)
                .fold(f64::INFINITY, f64::min);
            let minimum = tick_size.to_f64_lossy().abs().max(f64::EPSILON);
            let half_band = if adjacent_gap.is_finite() {
                adjacent_gap * f64::from(config.cluster_band_width.clamp(0.1, 1.5))
            } else {
                minimum
            }
            .max(minimum)
            .min(if adjacent_gap.is_finite() {
                adjacent_gap * 0.25
            } else {
                minimum * 4.0
            });
            levels.push(Level {
                kind: GexOverlayKind::GammaCluster,
                raw: strike.strike,
                half_band,
                color: resolve_color(config.cluster_color),
            });
        }
    }

    levels.sort_by_key(|level| match level.kind {
        GexOverlayKind::GammaCluster => 0,
        GexOverlayKind::CallWall | GexOverlayKind::PutWall => 1,
        GexOverlayKind::GammaFlip => 2,
    });
    let mut cluster_bands = Vec::new();
    let mut line_levels = Vec::new();
    for level in levels {
        let displayed = level.raw + basis;
        if !displayed.is_finite() || displayed <= 0.0 {
            continue;
        }
        let y = price_to_y(Price::from_f64(displayed));
        if !y.is_finite() {
            continue;
        }
        if gex_overlay_primitive(level.kind) == GexOverlayPrimitive::Band {
            let upper_y = price_to_y(Price::from_f64(displayed + level.half_band));
            let lower_y = price_to_y(Price::from_f64(
                (displayed - level.half_band).max(f64::EPSILON),
            ));
            if !upper_y.is_finite() || !lower_y.is_finite() {
                continue;
            }
            let cap = gex_screen_width_to_world(18.0, chart_scaling);
            let minimum = gex_screen_width_to_world(1.0, chart_scaling);
            let height = (upper_y - lower_y).abs().clamp(minimum, cap);
            let top = y - height * 0.5;
            let bottom = y + height * 0.5;
            let region_bottom = visible_region.y + visible_region.height;
            if bottom < visible_region.y || top > region_bottom {
                continue;
            }
            cluster_bands.push((
                top.max(visible_region.y),
                bottom.min(region_bottom),
                level.color,
                gex_level_label(level.kind, displayed),
            ));
        } else {
            if !gex_level_is_visible(y, visible_region) {
                continue;
            }
            let width_px = if level.kind == GexOverlayKind::GammaFlip {
                config.gamma_flip_width.clamp(1.0, 4.0)
            } else {
                config.line_width.clamp(0.5, 3.0)
            };
            line_levels.push((
                y,
                level.color,
                width_px,
                gex_level_label(level.kind, displayed),
            ));
        }
    }
    let mut labels = Vec::new();
    for (top, bottom, color, label) in cluster_bands {
        frame.fill(
            &Path::rectangle(
                Point::new(horizontal.start_x, top),
                Size::new(horizontal.end_x - horizontal.start_x, bottom - top),
            ),
            color.scale_alpha(config.band_opacity.clamp(0.02, 0.4)),
        );
        labels.push(((top + bottom) * 0.5, color, label));
    }
    for (y, color, width_px, label) in line_levels {
        frame.stroke(
            &Path::line(
                Point::new(horizontal.start_x, y),
                Point::new(horizontal.end_x, y),
            ),
            Stroke::default()
                .with_color(color.scale_alpha(config.line_opacity.clamp(0.1, 1.0)))
                .with_width(gex_screen_width_to_world(width_px, chart_scaling)),
        );
        labels.push((y, color, label));
    }
    for (y, color, label) in labels {
        draw_gex_level_label(
            frame,
            &label,
            Point::new(
                horizontal.start_x + gex_screen_width_to_world(6.0, chart_scaling),
                y,
            ),
            color,
            chart_scaling,
            palette,
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct GexOverlayHorizontalBounds {
    start_x: f32,
    end_x: f32,
}

fn gex_level_is_visible(y: f32, region: Rectangle) -> bool {
    y.is_finite() && y >= region.y && y <= region.y + region.height
}

fn gex_screen_width_to_world(px: f32, scaling: f32) -> f32 {
    if scaling.is_finite() && scaling > 0.0 {
        px / scaling
    } else {
        0.0
    }
}

fn gex_overlay_horizontal_bounds(
    region: Rectangle,
    scaling: f32,
) -> Option<GexOverlayHorizontalBounds> {
    if !scaling.is_finite() || scaling <= 0.0 || region.width <= 0.0 {
        return None;
    }
    let padding = gex_screen_width_to_world(10.0, scaling);
    let end_x = region.x + region.width - padding;
    (end_x > region.x + padding).then_some(GexOverlayHorizontalBounds {
        start_x: region.x + padding,
        end_x,
    })
}

fn gex_level_label(kind: GexOverlayKind, price: f64) -> String {
    let prefix = match kind {
        GexOverlayKind::GammaFlip => "GF",
        GexOverlayKind::CallWall => "CW",
        GexOverlayKind::PutWall => "PW",
        GexOverlayKind::GammaCluster => "GC",
    };
    format!("{prefix} {price:.2}")
}

fn draw_gex_level_label(
    frame: &mut canvas::Frame,
    label: &str,
    position: Point,
    color: Color,
    scaling: f32,
    palette: &Extended,
) {
    let width = gex_screen_width_to_world(10.0 + label.chars().count() as f32 * 6.2, scaling);
    let height = gex_screen_width_to_world(16.0, scaling);
    frame.fill(
        &Path::rectangle(
            Point::new(position.x, position.y - height * 0.5),
            Size::new(width, height),
        ),
        palette.background.base.color.scale_alpha(0.86),
    );
    draw_cluster_text(
        frame,
        label,
        Point::new(
            position.x + gex_screen_width_to_world(5.0, scaling),
            position.y,
        ),
        gex_screen_width_to_world(10.0, scaling),
        color,
        Alignment::Start,
        Alignment::Center,
    );
}

#[cfg(test)]
mod gex_overlay_tests {
    use super::*;
    use data::chart::gex::{GexLevelColor, GexLevelsConfig};

    #[test]
    fn overlay_contains_only_three_line_kinds_and_cluster_bands() {
        assert_eq!(
            gex_overlay_primitive(GexOverlayKind::GammaFlip),
            GexOverlayPrimitive::Line
        );
        assert_eq!(
            gex_overlay_primitive(GexOverlayKind::CallWall),
            GexOverlayPrimitive::Line
        );
        assert_eq!(
            gex_overlay_primitive(GexOverlayKind::PutWall),
            GexOverlayPrimitive::Line
        );
        assert_eq!(
            gex_overlay_primitive(GexOverlayKind::GammaCluster),
            GexOverlayPrimitive::Band
        );
    }

    #[test]
    fn semantic_default_colors_are_distinct() {
        let config = GexLevelsConfig::default();
        assert_eq!(config.call_wall_color, GexLevelColor::Success);
        assert_eq!(config.put_wall_color, GexLevelColor::Danger);
        assert_eq!(config.gamma_flip_color, GexLevelColor::Warning);
        assert_eq!(config.cluster_color, GexLevelColor::Primary);
    }

    #[test]
    fn visibility_uses_world_space_visible_region() {
        let region = Rectangle::new(Point::new(400.0, 800.0), Size::new(300.0, 200.0));
        assert!(gex_level_is_visible(900.0, region));
        assert!(!gex_level_is_visible(399.0, region));
        assert!(!gex_level_is_visible(1_001.0, region));
    }

    #[test]
    fn horizontal_bounds_cover_the_visible_chart_across_pan_and_zoom() {
        let base = Rectangle::new(Point::new(100.0, 50.0), Size::new(1_000.0, 500.0));
        let panned = Rectangle::new(Point::new(600.0, 50.0), Size::new(1_000.0, 500.0));
        let first = gex_overlay_horizontal_bounds(base, 1.0).expect("bounds");
        let second = gex_overlay_horizontal_bounds(panned, 1.0).expect("bounds");
        assert_eq!(second.start_x - first.start_x, 500.0);
        assert_eq!(second.end_x - first.end_x, 500.0);
        assert!((first.end_x - first.start_x) > base.width * 0.95);

        let zoomed = gex_overlay_horizontal_bounds(base, 2.0).expect("zoom bounds");
        assert_eq!((zoomed.start_x - base.x) * 2.0, 10.0);
        assert_eq!((base.x + base.width - zoomed.end_x) * 2.0, 10.0);
    }

    #[test]
    fn screen_widths_remain_constant_after_scaling() {
        assert_eq!(gex_screen_width_to_world(2.0, 1.0), 2.0);
        assert_eq!(gex_screen_width_to_world(2.0, 2.0) * 2.0, 2.0);
        assert_eq!(gex_screen_width_to_world(18.0, 3.0) * 3.0, 18.0);
    }

    #[test]
    fn labels_are_compact_and_keep_the_exact_level_value() {
        assert_eq!(
            gex_level_label(GexOverlayKind::GammaFlip, 63_259.0),
            "GF 63259.00"
        );
        assert_eq!(
            gex_level_label(GexOverlayKind::CallWall, 66_000.0),
            "CW 66000.00"
        );
        assert_eq!(
            gex_level_label(GexOverlayKind::PutWall, 62_500.0),
            "PW 62500.00"
        );
        assert_eq!(
            gex_level_label(GexOverlayKind::GammaCluster, 64_000.0),
            "GC 64000.00"
        );
    }
}

fn draw_vwap_overlay(
    data_source: &PlotData<KlineDataPoint>,
    frame: &mut canvas::Frame,
    earliest: u64,
    latest: u64,
    interval_to_x: impl Fn(u64) -> f32,
    price_to_y: impl Fn(Price) -> f32,
    config: &VwapConfig,
    palette: &Extended,
) {
    let sessions = build_vwap_sessions(data_source, earliest, latest, config);
    let vwap_color = palette.warning.strong.color.scale_alpha(0.96);
    let band_color = palette.secondary.strong.color.scale_alpha(0.62);
    for points in sessions {
        let draw_series = |frame: &mut canvas::Frame,
                           select: fn(&VwapPoint) -> Price,
                           color: Color,
                           width: f32| {
            let mut builder = canvas::path::Builder::new();
            if let Some(first) = points.first() {
                builder.move_to(Point::new(
                    interval_to_x(first.time),
                    price_to_y(select(first)),
                ));
                for point in points.iter().skip(1) {
                    builder.line_to(Point::new(
                        interval_to_x(point.time),
                        price_to_y(select(point)),
                    ));
                }
                frame.stroke(
                    &builder.build(),
                    Stroke::default().with_color(color).with_width(width),
                );
            }
        };
        draw_series(
            frame,
            |point| point.value,
            vwap_color,
            config.line_width.clamp(0.5, 5.0),
        );
        if config.show_bands {
            draw_series(frame, |point| point.upper, band_color, 0.8);
            draw_series(frame, |point| point.lower, band_color, 0.8);
        }
        if config.show_labels
            && let Some(last) = points.last()
        {
            let x = interval_to_x(last.time) + 2.0;
            draw_cluster_text(
                frame,
                "VWAP",
                Point::new(x, price_to_y(last.value)),
                7.0,
                vwap_color,
                Alignment::Start,
                Alignment::Center,
            );
            if config.show_bands {
                draw_cluster_text(
                    frame,
                    "+σ",
                    Point::new(x, price_to_y(last.upper)),
                    7.0,
                    band_color,
                    Alignment::Start,
                    Alignment::Center,
                );
                draw_cluster_text(
                    frame,
                    "-σ",
                    Point::new(x, price_to_y(last.lower)),
                    7.0,
                    band_color,
                    Alignment::Start,
                    Alignment::Center,
                );
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ProfileBin {
    buy: f64,
    sell: f64,
}

impl ProfileBin {
    fn volume(self) -> f64 {
        self.buy + self.sell
    }
    fn delta(self) -> f64 {
        self.buy - self.sell
    }
}

#[derive(Debug)]
struct SessionProfile {
    start: u64,
    end: u64,
    rows: Vec<(Price, ProfileBin)>,
    poc: Price,
    vah: Price,
    val: Price,
    vwap: Price,
    high: Price,
    low: Price,
}

fn align_session_start(timestamp: u64, interval_ms: u64) -> u64 {
    if interval_ms == 7 * 24 * 60 * 60_000 {
        // Unix epoch was a Thursday; shift by three days so weekly profiles
        // open on Monday 00:00 UTC.
        const MONDAY_SHIFT: u64 = 3 * 24 * 60 * 60_000;
        return (timestamp.saturating_add(MONDAY_SHIFT) / interval_ms * interval_ms)
            .saturating_sub(MONDAY_SHIFT);
    }
    timestamp / interval_ms * interval_ms
}

fn vwap_required_from(target_to: UnixMs, visible_earliest: UnixMs, anchor_ms: u64) -> UnixMs {
    let last_real_ms = target_to.saturating_sub(1).as_u64();
    let current_session = align_session_start(last_real_ms, anchor_ms);
    let visible_session =
        align_session_start(visible_earliest.as_u64().min(last_real_ms), anchor_ms);
    UnixMs::new(current_session.min(visible_session))
}

/// REST backfills stop at the last fully closed candle. Trades for the open
/// candle arrive through the live stream; chasing `now()` here would otherwise
/// create one historical request per chart tick and starve older gaps.
fn historical_trade_target_to(kline_latest: UnixMs, timeframe_ms: u64, now: UnixMs) -> UnixMs {
    let latest_candle_end = kline_latest.saturating_add(timeframe_ms);
    if now < latest_candle_end {
        kline_latest
    } else {
        latest_candle_end
    }
}

fn build_session_profiles(
    data_source: &PlotData<KlineDataPoint>,
    earliest: u64,
    latest: u64,
    tick_size: PriceStep,
    config: &SessionVolumeProfileConfig,
) -> Vec<SessionProfile> {
    let PlotData::TimeBased(timeseries) = data_source else {
        return Vec::new();
    };
    let session_ms = config.interval.milliseconds();
    let row_units = tick_size
        .units
        .saturating_mul(i64::from(config.row_size_ticks.max(1)))
        .max(1);
    let from = align_session_start(earliest, session_ms);
    let mut grouped: FxHashMap<u64, (FxHashMap<i64, ProfileBin>, Price, Price)> =
        FxHashMap::default();

    for (_, dp) in timeseries
        .datapoints
        .range(UnixMs::new(from)..=UnixMs::new(latest))
    {
        if dp.footprint.trades.is_empty() {
            continue;
        }
        let session_start = align_session_start(dp.kline.time.as_u64(), session_ms);
        let entry = grouped
            .entry(session_start)
            .or_insert_with(|| (FxHashMap::default(), dp.kline.high, dp.kline.low));
        entry.1 = entry.1.max(dp.kline.high);
        entry.2 = entry.2.min(dp.kline.low);
        for (price, trades) in &dp.footprint.trades {
            let bin_units = price.units.div_euclid(row_units).saturating_mul(row_units);
            let bin = entry.0.entry(bin_units).or_default();
            bin.buy += trades.buy_qty.to_f64();
            bin.sell += trades.sell_qty.to_f64();
        }
    }

    let mut result = grouped
        .into_iter()
        .filter_map(|(start, (bins, high, low))| {
            let mut rows: Vec<_> = bins
                .into_iter()
                .map(|(units, bin)| (Price::from_units(units.saturating_add(row_units / 2)), bin))
                .filter(|(_, bin)| bin.volume() > 0.0)
                .collect();
            rows.sort_by_key(|(price, _)| *price);
            if rows.is_empty() {
                return None;
            }

            let poc_index = rows
                .iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.1.volume().total_cmp(&b.1.volume()))
                .map(|(index, _)| index)?;
            let total: f64 = rows.iter().map(|(_, bin)| bin.volume()).sum();
            let target = total * (f64::from(config.value_area_percent.clamp(1.0, 100.0)) / 100.0);
            let mut included = rows[poc_index].1.volume();
            let mut low_index = poc_index;
            let mut high_index = poc_index;
            while included < target && (low_index > 0 || high_index + 1 < rows.len()) {
                let below = if low_index > 0 {
                    rows[low_index - 1].1.volume()
                } else {
                    -1.0
                };
                let above = if high_index + 1 < rows.len() {
                    rows[high_index + 1].1.volume()
                } else {
                    -1.0
                };
                if above >= below {
                    high_index += 1;
                    included += rows[high_index].1.volume();
                } else {
                    low_index -= 1;
                    included += rows[low_index].1.volume();
                }
            }
            let weighted: f64 = rows
                .iter()
                .map(|(price, bin)| price.to_f64() * bin.volume())
                .sum();
            Some(SessionProfile {
                start,
                end: start.saturating_add(session_ms),
                poc: rows[poc_index].0,
                vah: rows[high_index].0,
                val: rows[low_index].0,
                vwap: Price::from_f64(weighted / total.max(f64::EPSILON)),
                high,
                low,
                rows,
            })
        })
        .collect::<Vec<_>>();
    result.sort_by_key(|profile| profile.start);
    result
}

#[allow(clippy::too_many_arguments)]
fn draw_session_volume_profiles(
    data_source: &PlotData<KlineDataPoint>,
    frame: &mut canvas::Frame,
    earliest: u64,
    latest: u64,
    interval_to_x: impl Fn(u64) -> f32,
    price_to_y: impl Fn(Price) -> f32,
    cell_height: f32,
    tick_size: PriceStep,
    config: &SessionVolumeProfileConfig,
    palette: &Extended,
) {
    let profiles = build_session_profiles(data_source, earliest, latest, tick_size, config);
    let row_height = cell_height * f32::from(config.row_size_ticks.max(1)) * 0.86;
    for profile in profiles {
        let session_left = interval_to_x(profile.start);
        let session_right = interval_to_x(profile.end);
        let full_width = (session_right - session_left).abs();
        let max_width = full_width * (config.width_percent.clamp(1.0, 100.0) / 100.0);
        let max_value = profile
            .rows
            .iter()
            .map(|(_, bin)| match config.mode {
                SessionProfileMode::Volume => bin.volume(),
                SessionProfileMode::Delta => bin.delta().abs(),
            })
            .fold(0.0f64, f64::max);
        if max_value <= 0.0 {
            continue;
        }

        for (price, bin) in &profile.rows {
            let value = match config.mode {
                SessionProfileMode::Volume => bin.volume(),
                SessionProfileMode::Delta => bin.delta().abs(),
            };
            let width = max_width * (value / max_value) as f32;
            let x = match config.placement {
                SessionProfilePlacement::Left => session_left,
                SessionProfilePlacement::Right => session_right - width,
            };
            let in_value_area = *price >= profile.val && *price <= profile.vah;
            let base = match config.mode {
                SessionProfileMode::Volume => palette.primary.strong.color,
                SessionProfileMode::Delta if bin.delta() >= 0.0 => palette.success.strong.color,
                SessionProfileMode::Delta => palette.danger.strong.color,
            };
            frame.fill_rectangle(
                Point::new(x, price_to_y(*price) - row_height / 2.0),
                Size::new(width.max(0.1), row_height.max(0.1)),
                base.scale_alpha(if in_value_area { 0.38 } else { 0.18 }),
            );
        }

        let draw_level = |frame: &mut canvas::Frame, price: Price, color: Color, width: f32| {
            frame.stroke(
                &Path::line(
                    Point::new(session_left, price_to_y(price)),
                    Point::new(session_right, price_to_y(price)),
                ),
                Stroke::default().with_color(color).with_width(width),
            );
        };
        let draw_label = |frame: &mut canvas::Frame, text: &str, price: Price, color: Color| {
            let (x, alignment) = match config.placement {
                SessionProfilePlacement::Left => (session_left + 2.0, Alignment::Start),
                SessionProfilePlacement::Right => (session_right - 2.0, Alignment::End),
            };
            draw_cluster_text(
                frame,
                text,
                Point::new(x, price_to_y(price) - 1.0),
                7.0,
                color,
                alignment,
                Alignment::End,
            );
        };
        if config.show_poc {
            let color = palette.warning.strong.color.scale_alpha(0.95);
            draw_level(frame, profile.poc, color, 1.6);
            draw_label(frame, "POC", profile.poc, color);
        }
        if config.show_value_area {
            let color = palette.primary.strong.color.scale_alpha(0.82);
            draw_level(frame, profile.vah, color, 1.0);
            draw_level(frame, profile.val, color, 1.0);
            draw_label(frame, "VAH", profile.vah, color);
            draw_label(frame, "VAL", profile.val, color);
        }
        if config.show_vwap {
            let color = palette.success.base.color.scale_alpha(0.85);
            draw_level(frame, profile.vwap, color, 1.0);
            draw_label(frame, "VWAP", profile.vwap, color);
        }
        if config.show_session_high_low {
            let color = palette.background.strong.text.scale_alpha(0.42);
            draw_level(frame, profile.high, color, 0.7);
            draw_level(frame, profile.low, color, 0.7);
        }
    }
}

fn draw_footprint_kline(
    frame: &mut canvas::Frame,
    price_to_y: impl Fn(Price) -> f32,
    x_position: f32,
    candle_width: f32,
    kline: &Kline,
    palette: &Extended,
) {
    let y_open = price_to_y(kline.open);
    let y_high = price_to_y(kline.high);
    let y_low = price_to_y(kline.low);
    let y_close = price_to_y(kline.close);

    let body_color = if kline.close >= kline.open {
        palette.success.weak.color
    } else {
        palette.danger.weak.color
    };
    frame.fill_rectangle(
        Point::new(x_position - (candle_width / 8.0), y_open.min(y_close)),
        Size::new(candle_width / 4.0, (y_open - y_close).abs()),
        body_color,
    );

    let wick_color = if kline.close >= kline.open {
        palette.success.weak.color
    } else {
        palette.danger.weak.color
    };
    let marker_line = Stroke::with_color(
        Stroke {
            width: 1.0,
            ..Default::default()
        },
        wick_color.scale_alpha(0.6),
    );
    frame.stroke(
        &Path::line(
            Point::new(x_position, y_high),
            Point::new(x_position, y_low),
        ),
        marker_line,
    );
}

fn draw_candle_dp(
    frame: &mut canvas::Frame,
    price_to_y: impl Fn(Price) -> f32,
    candle_width: f32,
    palette: &Extended,
    x_position: f32,
    kline: &Kline,
) {
    let y_open = price_to_y(kline.open);
    let y_high = price_to_y(kline.high);
    let y_low = price_to_y(kline.low);
    let y_close = price_to_y(kline.close);

    let body_color = if kline.close >= kline.open {
        palette.success.base.color
    } else {
        palette.danger.base.color
    };
    frame.fill_rectangle(
        Point::new(x_position - (candle_width / 2.0), y_open.min(y_close)),
        Size::new(candle_width, (y_open - y_close).abs()),
        body_color,
    );

    let wick_color = if kline.close >= kline.open {
        palette.success.base.color
    } else {
        palette.danger.base.color
    };
    frame.fill_rectangle(
        Point::new(x_position - (candle_width / 8.0), y_high),
        Size::new(candle_width / 4.0, (y_high - y_low).abs()),
        wick_color,
    );
}

#[derive(Clone, Copy)]
struct VolumeBubblePoint {
    price: Price,
    total_qty: f64,
    buy_qty: f64,
    sell_qty: f64,
}

fn draw_volume_bubbles(
    frame: &mut canvas::Frame,
    price_to_y: impl Fn(Price) -> f32,
    x_position: f32,
    trades: &KlineTrades,
    summary: &BubbleVolumeSummary,
    config: &VolumeBubbleConfig,
    visible_max_qty: f64,
    scaling: f32,
    palette: &Extended,
) {
    if visible_max_qty <= 0.0 || config.max_bubbles_per_bar == 0 || scaling <= f32::EPSILON {
        return;
    }

    for bubble in collect_volume_bubble_points(trades, summary, config) {
        let radius_px = volume_bubble_radius(bubble.total_qty, visible_max_qty, config);
        let radius = radius_px / scaling;
        let center = Point::new(x_position, price_to_y(bubble.price));
        let color = volume_bubble_color(bubble, config.color_mode, palette);

        let circle = Path::circle(center, radius);
        frame.stroke(
            &circle,
            Stroke::default()
                .with_color(color.scale_alpha(0.86))
                .with_width((1.6 / scaling).max(0.75 / scaling)),
        );

        if config.show_labels && radius_px >= 9.0 {
            draw_cluster_text(
                frame,
                &abbr_large_numbers(bubble.total_qty),
                center,
                (radius_px * 0.72).clamp(7.0, 10.0) / scaling,
                palette.background.base.text,
                Alignment::Center,
                Alignment::Center,
            );
        }
    }
}

fn collect_volume_bubble_points(
    trades: &KlineTrades,
    summary: &BubbleVolumeSummary,
    config: &VolumeBubbleConfig,
) -> Vec<VolumeBubblePoint> {
    // When raw trades exist they are the same source used by the footprint.
    // Prefer them over a cached/derived summary so a bubble can never reflect a
    // stale or previously over-merged quantity for that candle.
    let prefer_raw = config.use_raw_trades_when_available && !trades.trades.is_empty();
    let mut points: Vec<_> = if !prefer_raw && !summary.is_empty() {
        summary
            .candidates
            .iter()
            .filter_map(|candidate| bubble_point_from_candidate(candidate, config))
            .collect()
    } else {
        trades
            .trades
            .iter()
            .filter_map(|(price, group)| {
                let buy_qty = group.buy_qty.to_f64();
                let sell_qty = group.sell_qty.to_f64();
                let total_qty = buy_qty + sell_qty;

                (total_qty > 0.0 && total_qty >= config.min_qty).then_some(VolumeBubblePoint {
                    price: *price,
                    total_qty,
                    buy_qty,
                    sell_qty,
                })
            })
            .collect()
    };

    points.sort_by(|a, b| b.total_qty.total_cmp(&a.total_qty));
    points.truncate(config.max_bubbles_per_bar);
    points
}

fn bubble_point_from_candidate(
    candidate: &BubbleCandidate,
    config: &VolumeBubbleConfig,
) -> Option<VolumeBubblePoint> {
    let total_qty = candidate.total_qty.to_f64();
    (total_qty > 0.0 && total_qty >= config.min_qty).then_some(VolumeBubblePoint {
        price: candidate.price,
        total_qty,
        buy_qty: candidate.buy_qty.to_f64(),
        sell_qty: candidate.sell_qty.to_f64(),
    })
}

fn volume_bubble_radius(qty: f64, visible_max_qty: f64, config: &VolumeBubbleConfig) -> f32 {
    let min_radius = config.min_radius_px.min(config.max_radius_px);
    let max_radius = config.min_radius_px.max(config.max_radius_px);
    let intensity = if visible_max_qty > 0.0 {
        (qty / visible_max_qty).clamp(0.0, 1.0) as f32
    } else {
        0.0
    };

    min_radius + intensity * (max_radius - min_radius)
}

fn volume_bubble_color(
    bubble: VolumeBubblePoint,
    color_mode: BubbleColorMode,
    palette: &Extended,
) -> Color {
    let total_qty = bubble.total_qty.max(f64::EPSILON);
    let delta = bubble.buy_qty - bubble.sell_qty;
    let dominance = (delta.abs() / total_qty).clamp(0.0, 1.0) as f32;

    if dominance < 0.10 {
        return palette.background.strong.text;
    }

    match color_mode {
        BubbleColorMode::Delta => {
            let base = if delta > 0.0 {
                palette.success.strong.color
            } else {
                palette.danger.strong.color
            };
            mix_color(
                base,
                palette.background.base.color,
                0.55 + (dominance * 0.35),
            )
        }
        BubbleColorMode::DominantSide => {
            if bubble.buy_qty >= bubble.sell_qty {
                palette.success.strong.color
            } else {
                palette.danger.strong.color
            }
        }
    }
}

fn current_volume_bubble_session_start_ms(
    now: chrono::DateTime<chrono::Utc>,
    session: VolumeBubbleSession,
) -> UnixMs {
    let rome_now = now.with_timezone(&chrono_tz::Europe::Rome);
    let active_session = match session {
        VolumeBubbleSession::Auto => {
            let minutes_since_midnight = rome_now.hour() * 60 + rome_now.minute();
            match minutes_since_midnight {
                0..540 => VolumeBubbleSession::Asian,
                540..930 => VolumeBubbleSession::London,
                _ => VolumeBubbleSession::NewYork,
            }
        }
        selected => selected,
    };

    let (hour, minute) = match active_session {
        VolumeBubbleSession::Auto => unreachable!("auto session is resolved above"),
        VolumeBubbleSession::Asian => (0, 0),
        VolumeBubbleSession::London => (9, 0),
        VolumeBubbleSession::NewYork => (15, 30),
    };

    let session_start = chrono_tz::Europe::Rome
        .with_ymd_and_hms(
            rome_now.year(),
            rome_now.month(),
            rome_now.day(),
            hour,
            minute,
            0,
        )
        .earliest()
        .unwrap_or(rome_now)
        .with_timezone(&chrono::Utc);

    UnixMs::new(session_start.timestamp_millis().max(0) as u64)
}

fn volume_bubble_effective_range(
    kline_latest: UnixMs,
    timeframe_ms: u64,
    now: UnixMs,
    config: &VolumeBubbleConfig,
) -> Option<(UnixMs, UnixMs)> {
    let window_to = kline_latest.saturating_add(timeframe_ms).min(now);
    let window_from =
        window_to.saturating_sub(config.history_window_minutes.max(1).saturating_mul(60_000));
    let at_window_end =
        chrono::DateTime::from_timestamp_millis(window_to.saturating_sub(1).as_u64() as i64)?;
    let session_start = current_volume_bubble_session_start_ms(at_window_end, config.session);
    let effective_from = window_from.max(session_start);
    (effective_from < window_to).then_some((effective_from, window_to))
}

fn visible_max_bubble_qty(
    data_source: &PlotData<KlineDataPoint>,
    earliest: u64,
    latest: u64,
    min_qty: f64,
    prefer_raw_trades: bool,
) -> f64 {
    max_bubble_qty_in_range(data_source, earliest, latest, prefer_raw_trades)
        .filter(|qty| *qty >= min_qty)
        .unwrap_or_default()
}

fn max_bubble_qty_in_range(
    data_source: &PlotData<KlineDataPoint>,
    earliest: u64,
    latest: u64,
    prefer_raw_trades: bool,
) -> Option<f64> {
    if latest < earliest {
        return None;
    }

    let max_from_sources = |trades: &KlineTrades, summary: &BubbleVolumeSummary| {
        if !prefer_raw_trades && !summary.is_empty() {
            return summary
                .candidates
                .iter()
                .map(|candidate| candidate.total_qty.to_f64())
                .filter(|qty| *qty > 0.0)
                .max_by(f64::total_cmp);
        }

        let raw_max = trades
            .trades
            .values()
            .map(|group| group.total_qty().to_f64())
            .filter(|qty| *qty > 0.0)
            .max_by(f64::total_cmp);
        if raw_max.is_some() || summary.is_empty() {
            raw_max
        } else {
            summary
                .candidates
                .iter()
                .map(|candidate| candidate.total_qty.to_f64())
                .filter(|qty| *qty > 0.0)
                .max_by(f64::total_cmp)
        }
    };

    match data_source {
        PlotData::TickBased(tick_aggr) => {
            let earliest = earliest as usize;
            let latest = latest as usize;

            tick_aggr
                .datapoints
                .iter()
                .enumerate()
                .filter(|(index, _)| *index >= earliest && *index <= latest)
                .filter_map(|(_, dp)| {
                    max_from_sources(&dp.footprint, &BubbleVolumeSummary::default())
                })
                .max_by(f64::total_cmp)
        }
        PlotData::TimeBased(timeseries) => timeseries
            .datapoints
            .range(UnixMs::new(earliest)..=UnixMs::new(latest))
            .filter_map(|(_, dp)| max_from_sources(&dp.footprint, &dp.bubble_summary))
            .max_by(f64::total_cmp),
    }
}

fn volume_bubble_qty_scale(max_qty: Option<f64>) -> VolumeBubbleQtyScale {
    let max = max_qty
        .filter(|value| value.is_finite() && *value > 0.0)
        .map(|value| nice_ceiling(value * 1.1))
        .unwrap_or(100.0)
        .max(1.0);
    let step = nice_step(max / 100.0);

    VolumeBubbleQtyScale {
        min: 0.0,
        max,
        step,
    }
}

fn nice_ceiling(value: f64) -> f64 {
    if !value.is_finite() || value <= 0.0 {
        return 1.0;
    }

    let magnitude = 10.0f64.powf(value.log10().floor());
    let normalized = value / magnitude;
    let nice = if normalized <= 1.0 {
        1.0
    } else if normalized <= 2.0 {
        2.0
    } else if normalized <= 2.5 {
        2.5
    } else if normalized <= 5.0 {
        5.0
    } else {
        10.0
    };

    nice * magnitude
}

fn nice_step(value: f64) -> f64 {
    if !value.is_finite() || value <= 0.0 {
        return 1.0;
    }

    let magnitude = 10.0f64.powf(value.log10().floor());
    let normalized = value / magnitude;
    let nice = if normalized <= 1.0 {
        1.0
    } else if normalized <= 2.0 {
        2.0
    } else if normalized <= 5.0 {
        5.0
    } else {
        10.0
    };

    nice * magnitude
}

fn render_data_source<F>(
    data_source: &PlotData<KlineDataPoint>,
    frame: &mut canvas::Frame,
    earliest: u64,
    latest: u64,
    interval_to_x: impl Fn(u64) -> f32,
    draw_fn: F,
) where
    F: Fn(&mut canvas::Frame, f32, &Kline, &KlineTrades, &BubbleVolumeSummary),
{
    match data_source {
        PlotData::TickBased(tick_aggr) => {
            let earliest = earliest as usize;
            let latest = latest as usize;

            tick_aggr
                .datapoints
                .iter()
                .rev()
                .enumerate()
                .filter(|(index, _)| *index <= latest && *index >= earliest)
                .for_each(|(index, tick_aggr)| {
                    let x_position = interval_to_x(index as u64);

                    draw_fn(
                        frame,
                        x_position,
                        &tick_aggr.kline,
                        &tick_aggr.footprint,
                        &BubbleVolumeSummary::default(),
                    );
                });
        }
        PlotData::TimeBased(timeseries) => {
            if latest < earliest {
                return;
            }

            timeseries
                .datapoints
                .range(UnixMs::new(earliest)..=UnixMs::new(latest))
                .for_each(|(timestamp, dp)| {
                    let x_position = interval_to_x(timestamp.as_u64());

                    draw_fn(
                        frame,
                        x_position,
                        &dp.kline,
                        &dp.footprint,
                        &dp.bubble_summary,
                    );
                });
        }
    }
}

fn draw_all_npocs(
    data_source: &PlotData<KlineDataPoint>,
    frame: &mut canvas::Frame,
    price_to_y: impl Fn(Price) -> f32,
    interval_to_x: impl Fn(u64) -> f32,
    layout: &FootprintCellLayout<'_>,
    studies: &[FootprintStudy],
    visible_earliest: u64,
    visible_latest: u64,
    imb_study_on: bool,
) {
    let Some(lookback) = studies.iter().find_map(|study| {
        if let FootprintStudy::NPoC { lookback } = study {
            Some(*lookback)
        } else {
            None
        }
    }) else {
        return;
    };

    let (filled_color, naked_color) = (
        layout.pal.background.strong.color,
        if layout.pal.is_dark {
            layout.pal.warning.weak.color.scale_alpha(0.5)
        } else {
            layout.pal.warning.strong.color
        },
    );

    let line_height = layout.cell_h.min(1.0);

    let bar_width_factor: f32 = 0.9;
    let inset = (layout.cell_w * (1.0 - bar_width_factor)) / 2.0;

    let candle_lane_factor: f32 = match layout.cluster {
        ClusterKind::VolumeProfile | ClusterKind::DeltaProfile => 0.25,
        ClusterKind::BidAsk | ClusterKind::Table => 1.0,
    };

    let start_x_for = |cell_center_x: f32| -> f32 {
        match layout.cluster {
            ClusterKind::BidAsk | ClusterKind::Table => {
                cell_center_x + (layout.candle_w / 2.0) + layout.gaps.candle_to_cluster
            }
            ClusterKind::VolumeProfile | ClusterKind::DeltaProfile => {
                let content_left = (cell_center_x - (layout.cell_w / 2.0)) + inset;
                let candle_lane_left = content_left
                    + if imb_study_on {
                        layout.candle_w + layout.gaps.marker_to_candle
                    } else {
                        0.0
                    };
                candle_lane_left
                    + layout.candle_w * candle_lane_factor
                    + layout.gaps.candle_to_cluster
            }
        }
    };

    let wick_x_for = |cell_center_x: f32| -> f32 {
        match layout.cluster {
            ClusterKind::BidAsk | ClusterKind::Table => cell_center_x,
            ClusterKind::VolumeProfile | ClusterKind::DeltaProfile => {
                let content_left = (cell_center_x - (layout.cell_w / 2.0)) + inset;
                let candle_lane_left = content_left
                    + if imb_study_on {
                        layout.candle_w + layout.gaps.marker_to_candle
                    } else {
                        0.0
                    };
                candle_lane_left + (layout.candle_w * candle_lane_factor) / 2.0
                    - (layout.gaps.candle_to_cluster * 0.5)
            }
        }
    };

    let end_x_for = |cell_center_x: f32| -> f32 {
        match layout.cluster {
            ClusterKind::BidAsk | ClusterKind::Table => {
                cell_center_x - (layout.candle_w / 2.0) - layout.gaps.candle_to_cluster
            }
            ClusterKind::VolumeProfile | ClusterKind::DeltaProfile => wick_x_for(cell_center_x),
        }
    };

    let rightmost_cell_center_x = {
        let earliest_x = interval_to_x(visible_earliest);
        let latest_x = interval_to_x(visible_latest);
        if earliest_x > latest_x {
            earliest_x
        } else {
            latest_x
        }
    };

    let mut draw_the_line = |interval: u64, poc: &PointOfControl| {
        let start_x = start_x_for(interval_to_x(interval));

        let (line_width, color) = match poc.status {
            NPoc::Naked => {
                let end_x = end_x_for(rightmost_cell_center_x);
                let line_width = end_x - start_x;
                if line_width.abs() <= layout.cell_w {
                    return;
                }
                (line_width, naked_color)
            }
            NPoc::Filled { at } => {
                let end_x = end_x_for(interval_to_x(at));
                let line_width = end_x - start_x;
                if line_width.abs() <= layout.cell_w {
                    return;
                }
                (line_width, filled_color)
            }
            _ => return,
        };

        frame.fill_rectangle(
            Point::new(start_x, price_to_y(poc.price) - line_height / 2.0),
            Size::new(line_width, line_height),
            color,
        );
    };

    match data_source {
        PlotData::TickBased(tick_aggr) => {
            tick_aggr
                .datapoints
                .iter()
                .rev()
                .enumerate()
                .take(lookback)
                .filter_map(|(index, dp)| dp.footprint.poc.as_ref().map(|poc| (index as u64, poc)))
                .for_each(|(interval, poc)| draw_the_line(interval, poc));
        }
        PlotData::TimeBased(timeseries) => {
            timeseries
                .datapoints
                .iter()
                .rev()
                .take(lookback)
                .filter_map(|(timestamp, dp)| {
                    dp.footprint
                        .poc
                        .as_ref()
                        .map(|poc| (timestamp.as_u64(), poc))
                })
                .for_each(|(interval, poc)| draw_the_line(interval, poc));
        }
    }
}

fn effective_cluster_qty(
    scaling: ClusterScaling,
    visible_max: f64,
    footprint: &KlineTrades,
    cluster_kind: ClusterKind,
) -> f64 {
    let individual_max = match cluster_kind {
        ClusterKind::BidAsk | ClusterKind::Table => footprint
            .trades
            .values()
            .map(|group| group.buy_qty.max(group.sell_qty))
            .max()
            .unwrap_or_default(),
        ClusterKind::DeltaProfile => footprint
            .trades
            .values()
            .map(|group| group.buy_qty.abs_diff(group.sell_qty))
            .max()
            .unwrap_or_default(),
        ClusterKind::VolumeProfile => footprint
            .trades
            .values()
            .map(|group| group.buy_qty + group.sell_qty)
            .max()
            .unwrap_or_default(),
    };

    match scaling {
        ClusterScaling::VisibleRange => Qty::scale_or_one(visible_max),
        ClusterScaling::Datapoint => individual_max.to_scale_or_one(),
        ClusterScaling::Hybrid { weight } => {
            let w = weight.clamp(0.0, 1.0) as f64;
            Qty::scale_or_one(visible_max * w + individual_max.to_f64() * (1.0 - w))
        }
    }
}

fn draw_clusters(
    frame: &mut canvas::Frame,
    price_to_y: impl Fn(Price) -> f32,
    x_position: f32,
    layout: &FootprintCellLayout<'_>,
    scaling: f32,
    max_cluster_qty: f64,
    text_size: f32,
    step: PriceStep,
    show_text: bool,
    show_summary: bool,
    imbalance: Option<(usize, Option<usize>, bool)>,
    kline: &Kline,
    footprint: &KlineTrades,
) {
    let text_color = layout.pal.background.weakest.text;

    let bar_width_factor: f32 = 0.9;
    let inset = (layout.cell_w * (1.0 - bar_width_factor)) / 2.0;

    let cell_left = x_position - (layout.cell_w / 2.0);
    let content_left = cell_left + inset;
    let content_right = x_position + (layout.cell_w / 2.0) - inset;

    match layout.cluster {
        ClusterKind::VolumeProfile | ClusterKind::DeltaProfile => {
            let area = ProfileArea::new(
                content_left,
                content_right,
                layout.candle_w,
                layout.gaps,
                imbalance.is_some(),
            );
            let bar_alpha = if show_text { 0.25 } else { 1.0 };

            for (price, group) in &footprint.trades {
                let buy_qty = group.buy_qty.to_f64();
                let sell_qty = group.sell_qty.to_f64();
                let y = price_to_y(*price);

                match layout.cluster {
                    ClusterKind::VolumeProfile => {
                        super::draw_volume_bar(
                            frame,
                            area.bars_left,
                            y,
                            buy_qty,
                            sell_qty,
                            max_cluster_qty,
                            area.bars_width,
                            layout.cell_h,
                            layout.pal.success.base.color,
                            layout.pal.danger.base.color,
                            bar_alpha,
                            true,
                        );

                        if show_text {
                            draw_cluster_text(
                                frame,
                                &abbr_large_numbers(f64::from(group.total_qty())),
                                Point::new(area.bars_left, y),
                                text_size,
                                text_color,
                                Alignment::Start,
                                Alignment::Center,
                            );
                        }
                    }
                    ClusterKind::DeltaProfile => {
                        let delta = group.delta_qty().to_f64();
                        if show_text {
                            draw_cluster_text(
                                frame,
                                &abbr_large_numbers(delta),
                                Point::new(area.bars_left, y),
                                text_size,
                                text_color,
                                Alignment::Start,
                                Alignment::Center,
                            );
                        }

                        let bar_width = (delta.abs() / max_cluster_qty) as f32 * area.bars_width;
                        if bar_width > 0.0 {
                            let color = if delta >= 0.0 {
                                layout.pal.success.base.color.scale_alpha(bar_alpha)
                            } else {
                                layout.pal.danger.base.color.scale_alpha(bar_alpha)
                            };
                            frame.fill_rectangle(
                                Point::new(area.bars_left, y - (layout.cell_h / 2.0)),
                                Size::new(bar_width, layout.cell_h),
                                color,
                            );
                        }
                    }
                    _ => {}
                }

                if let Some((threshold, color_scale, ignore_zeros)) = imbalance {
                    let higher_price = price.add_steps(1, step);

                    let rect_w = ((area.imb_marker_width - 1.0) / 2.0).max(1.0);
                    let buyside_x = area.imb_marker_left + area.imb_marker_width - rect_w;
                    let sellside_x =
                        area.imb_marker_left + area.imb_marker_width - (2.0 * rect_w) - 1.0;

                    draw_imbalance_markers(
                        frame,
                        &price_to_y,
                        footprint,
                        *price,
                        sell_qty,
                        higher_price,
                        threshold,
                        color_scale,
                        ignore_zeros,
                        layout.cell_h,
                        layout.pal,
                        buyside_x,
                        sellside_x,
                        rect_w,
                    );
                }
            }

            draw_footprint_kline(
                frame,
                &price_to_y,
                area.candle_center_x,
                layout.candle_w,
                kline,
                layout.pal,
            );
        }
        ClusterKind::Table => {
            let area = TableArea::new(
                frame,
                &price_to_y,
                content_left,
                content_right,
                layout.candle_w,
                kline,
                layout.pal,
                layout.gaps,
            );
            let table_width = area.width();
            let half_width = table_width / 2.0;
            let cell_border = 1.0;
            let grid_color = layout.pal.background.weakest.text.scale_alpha(0.32);
            for (price, group) in &footprint.trades {
                let buy_qty = group.buy_qty.to_f64();
                let sell_qty = group.sell_qty.to_f64();
                let y = price_to_y(*price);
                let row_top = y - (layout.cell_h / 2.0);

                frame.fill_rectangle(
                    Point::new(area.table_left, row_top),
                    Size::new(half_width, layout.cell_h),
                    volume_cell_background(
                        layout.pal,
                        ImbalanceSide::Sell,
                        sell_qty,
                        max_cluster_qty,
                    ),
                );
                frame.fill_rectangle(
                    Point::new(area.table_left + half_width, row_top),
                    Size::new(half_width, layout.cell_h),
                    volume_cell_background(
                        layout.pal,
                        ImbalanceSide::Buy,
                        buy_qty,
                        max_cluster_qty,
                    ),
                );
                let sell_text_color = volume_cell_text_color(
                    layout.pal,
                    ImbalanceSide::Sell,
                    sell_qty,
                    max_cluster_qty,
                    text_color,
                );
                let buy_text_color = volume_cell_text_color(
                    layout.pal,
                    ImbalanceSide::Buy,
                    buy_qty,
                    max_cluster_qty,
                    text_color,
                );

                if let Some((threshold, color_scale, ignore_zeros)) = imbalance {
                    if let Some(alpha) = sell_imbalance_alpha(
                        footprint,
                        *price,
                        sell_qty,
                        step,
                        threshold,
                        color_scale,
                        ignore_zeros,
                    ) {
                        draw_table_imbalance_marker(
                            frame,
                            layout.pal,
                            ImbalanceSide::Sell,
                            alpha,
                            sell_qty,
                            max_cluster_qty,
                            Rectangle::new(
                                Point::new(area.table_left, row_top),
                                Size::new(half_width, layout.cell_h),
                            ),
                        );
                    }

                    if let Some(alpha) = buy_imbalance_alpha(
                        footprint,
                        *price,
                        buy_qty,
                        step,
                        threshold,
                        color_scale,
                        ignore_zeros,
                    ) {
                        draw_table_imbalance_marker(
                            frame,
                            layout.pal,
                            ImbalanceSide::Buy,
                            alpha,
                            buy_qty,
                            max_cluster_qty,
                            Rectangle::new(
                                Point::new(area.table_left + half_width, row_top),
                                Size::new(half_width, layout.cell_h),
                            ),
                        );
                    }
                }

                frame.fill_rectangle(
                    Point::new(area.table_left, row_top),
                    Size::new(table_width, cell_border),
                    grid_color,
                );
                frame.fill_rectangle(
                    Point::new(area.table_left, row_top + layout.cell_h - cell_border),
                    Size::new(table_width, cell_border),
                    grid_color,
                );
                frame.fill_rectangle(
                    Point::new(area.table_left, row_top),
                    Size::new(cell_border, layout.cell_h),
                    grid_color,
                );
                frame.fill_rectangle(
                    Point::new(area.table_left + half_width, row_top),
                    Size::new(cell_border, layout.cell_h),
                    grid_color,
                );
                frame.fill_rectangle(
                    Point::new(area.table_right - cell_border, row_top),
                    Size::new(cell_border, layout.cell_h),
                    grid_color,
                );

                if show_text {
                    draw_cluster_text(
                        frame,
                        &abbr_large_numbers(sell_qty),
                        Point::new(area.table_left + half_width - 3.0, y),
                        text_size,
                        sell_text_color,
                        Alignment::End,
                        Alignment::Center,
                    );
                    draw_cluster_text(
                        frame,
                        &abbr_large_numbers(buy_qty),
                        Point::new(area.table_left + half_width + 3.0, y),
                        text_size,
                        buy_text_color,
                        Alignment::Start,
                        Alignment::Center,
                    );
                }
            }
        }
        ClusterKind::BidAsk => {
            let area = BidAskArea::new(
                x_position,
                content_left,
                content_right,
                layout.candle_w,
                layout.gaps,
            );

            let bar_alpha = if show_text { 0.25 } else { 1.0 };

            let imb_marker_reserve = if imbalance.is_some() {
                ((area.imb_marker_width - 1.0) / 2.0).max(1.0)
            } else {
                0.0
            };

            let right_max_x =
                area.bid_area_right - imb_marker_reserve - (2.0 * layout.gaps.marker_to_bars);
            let right_area_width = (right_max_x - area.bid_area_left).max(0.0);

            let left_min_x =
                area.ask_area_left + imb_marker_reserve + (2.0 * layout.gaps.marker_to_bars);
            let left_area_width = (area.ask_area_right - left_min_x).max(0.0);

            for (price, group) in &footprint.trades {
                let buy_qty = group.buy_qty.to_f64();
                let sell_qty = group.sell_qty.to_f64();
                let y = price_to_y(*price);

                if buy_qty > 0.0 && right_area_width > 0.0 {
                    if show_text {
                        draw_cluster_text(
                            frame,
                            &abbr_large_numbers(buy_qty),
                            Point::new(area.bid_area_left, y),
                            text_size,
                            text_color,
                            Alignment::Start,
                            Alignment::Center,
                        );
                    }

                    let bar_width = (buy_qty / max_cluster_qty) as f32 * right_area_width;
                    if bar_width > 0.0 {
                        frame.fill_rectangle(
                            Point::new(area.bid_area_left, y - (layout.cell_h / 2.0)),
                            Size::new(bar_width, layout.cell_h),
                            layout.pal.success.base.color.scale_alpha(bar_alpha),
                        );
                    }
                }
                if sell_qty > 0.0 && left_area_width > 0.0 {
                    if show_text {
                        draw_cluster_text(
                            frame,
                            &abbr_large_numbers(sell_qty),
                            Point::new(area.ask_area_right, y),
                            text_size,
                            text_color,
                            Alignment::End,
                            Alignment::Center,
                        );
                    }

                    let bar_width = (sell_qty / max_cluster_qty) as f32 * left_area_width;
                    if bar_width > 0.0 {
                        frame.fill_rectangle(
                            Point::new(area.ask_area_right, y - (layout.cell_h / 2.0)),
                            Size::new(-bar_width, layout.cell_h),
                            layout.pal.danger.base.color.scale_alpha(bar_alpha),
                        );
                    }
                }

                if let Some((threshold, color_scale, ignore_zeros)) = imbalance
                    && area.imb_marker_width > 0.0
                {
                    let higher_price = price.add_steps(1, step);

                    let rect_width = ((area.imb_marker_width - 1.0) / 2.0).max(1.0);

                    let buyside_x = area.bid_area_right - rect_width - layout.gaps.marker_to_bars;
                    let sellside_x = area.ask_area_left + layout.gaps.marker_to_bars;

                    draw_imbalance_markers(
                        frame,
                        &price_to_y,
                        footprint,
                        *price,
                        sell_qty,
                        higher_price,
                        threshold,
                        color_scale,
                        ignore_zeros,
                        layout.cell_h,
                        layout.pal,
                        buyside_x,
                        sellside_x,
                        rect_width,
                    );
                }
            }

            draw_footprint_kline(
                frame,
                &price_to_y,
                area.candle_center_x,
                layout.candle_w,
                kline,
                layout.pal,
            );
        }
    }

    if show_text && show_summary {
        let Some(summary) = FootprintSummary::from_trades(footprint) else {
            return;
        };

        let text_size = style::text_size::TINY;
        let lowest_trade_price = footprint.trades.keys().min();

        let line_spacing = (text_size * 1.2) / scaling;
        let line_height = text_size / scaling;
        let summary_padding = line_height + line_spacing + line_height;

        let summary_y = match lowest_trade_price {
            Some(p) => price_to_y(*p) + layout.cell_h / 2.0 + summary_padding,
            None => price_to_y(kline.low) + layout.cell_h / 2.0 + summary_padding,
        };

        draw_cluster_text(
            frame,
            &format!("V: {}", abbr_large_numbers(summary.total.to_f64())),
            Point::new(x_position, summary_y),
            text_size,
            layout.pal.background.weakest.text,
            Alignment::Center,
            Alignment::Start,
        );

        let delta_color = if summary.delta >= Qty::ZERO {
            layout.pal.success.base.color
        } else {
            layout.pal.danger.base.color
        };

        draw_cluster_text(
            frame,
            &format!("Δ: {}", abbr_large_numbers(summary.delta.to_f64())),
            Point::new(x_position, summary_y + line_spacing),
            text_size,
            delta_color,
            Alignment::Center,
            Alignment::Start,
        );
    }
}

fn draw_imbalance_markers(
    frame: &mut canvas::Frame,
    price_to_y: &impl Fn(Price) -> f32,
    footprint: &KlineTrades,
    price: Price,
    sell_qty: f64,
    higher_price: Price,
    threshold: usize,
    color_scale: Option<usize>,
    ignore_zeros: bool,
    cell_height: f32,
    palette: &Extended,
    buyside_x: f32,
    sellside_x: f32,
    rect_width: f32,
) {
    if ignore_zeros && sell_qty <= 0.0 {
        return;
    }

    if let Some(group) = footprint.trades.get(&higher_price) {
        let diagonal_buy_qty = group.buy_qty.to_f64();

        if ignore_zeros && diagonal_buy_qty <= 0.0 {
            return;
        }

        let rect_height = cell_height / 2.0;

        let alpha_from_ratio = |ratio: f64| -> f32 {
            if let Some(scale) = color_scale {
                let divisor = (scale as f64 / 10.0) - 1.0;
                (0.2 + 0.8 * ((ratio - 1.0) / divisor).min(1.0)).min(1.0) as f32
            } else {
                1.0
            }
        };

        if diagonal_buy_qty >= sell_qty {
            let required_qty = sell_qty * (100 + threshold) as f64 / 100.0;
            if diagonal_buy_qty > required_qty {
                let ratio = diagonal_buy_qty / required_qty;
                let alpha = alpha_from_ratio(ratio);

                let y = price_to_y(higher_price);
                frame.fill_rectangle(
                    Point::new(buyside_x, y - (rect_height / 2.0)),
                    Size::new(rect_width, rect_height),
                    imbalance_background(palette, ImbalanceSide::Buy, alpha),
                );
            }
        } else {
            let required_qty = diagonal_buy_qty * (100 + threshold) as f64 / 100.0;
            if sell_qty > required_qty {
                let ratio = sell_qty / required_qty;
                let alpha = alpha_from_ratio(ratio);

                let y = price_to_y(price);
                frame.fill_rectangle(
                    Point::new(sellside_x, y - (rect_height / 2.0)),
                    Size::new(rect_width, rect_height),
                    imbalance_background(palette, ImbalanceSide::Sell, alpha),
                );
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ImbalanceSide {
    Buy,
    Sell,
}

fn volume_cell_background(
    palette: &Extended,
    side: ImbalanceSide,
    qty: f64,
    max_qty: f64,
) -> Color {
    const MIN_ALPHA: f32 = 0.04;

    let intensity = if max_qty > 0.0 {
        (qty / max_qty).clamp(0.0, 1.0) as f32
    } else {
        0.0
    };
    let alpha = MIN_ALPHA + intensity * (1.0 - MIN_ALPHA);

    match side {
        ImbalanceSide::Buy => palette.success.base.color.scale_alpha(alpha),
        ImbalanceSide::Sell => palette.danger.base.color.scale_alpha(alpha),
    }
}

fn volume_cell_text_color(
    palette: &Extended,
    side: ImbalanceSide,
    qty: f64,
    max_qty: f64,
    default_color: Color,
) -> Color {
    let cell_color = volume_cell_background(palette, side, qty, max_qty);
    let cell_background = composite_color(cell_color, palette.background.base.color);
    let inverted_color = palette.background.base.color;

    if contrast_ratio(cell_background, inverted_color)
        > contrast_ratio(cell_background, default_color)
    {
        inverted_color
    } else {
        default_color
    }
}

fn draw_table_imbalance_marker(
    frame: &mut canvas::Frame,
    palette: &Extended,
    side: ImbalanceSide,
    alpha: f32,
    qty: f64,
    max_qty: f64,
    cell: Rectangle,
) {
    let marker_width = (cell.width * 0.24).clamp(5.0, 11.0).min(cell.width * 0.42);
    let marker_height = (cell.height * 0.72)
        .clamp(6.0, 13.0)
        .min(cell.height.max(0.0));
    if marker_width <= 0.0 || marker_height <= 0.0 {
        return;
    }

    let inset = (cell.width * 0.04).clamp(1.0, 3.0);
    let center_y = cell.y + (cell.height / 2.0);
    let top = center_y - (marker_height / 2.0);
    let bottom = center_y + (marker_height / 2.0);
    let volume_intensity = if max_qty > 0.0 {
        (qty / max_qty).clamp(0.0, 1.0) as f32
    } else {
        0.0
    };
    let imbalance_strength = alpha.clamp(0.0, 1.0);
    let marker_alpha = 0.58 + (volume_intensity * 0.34) + (imbalance_strength * 0.08);
    let marker_alpha = marker_alpha.clamp(0.58, 1.0);
    let color = match side {
        ImbalanceSide::Buy => palette.success.strong.color.scale_alpha(marker_alpha),
        ImbalanceSide::Sell => palette.danger.strong.color.scale_alpha(marker_alpha),
    };

    let mut builder = canvas::path::Builder::new();
    match side {
        ImbalanceSide::Buy => {
            let right = cell.x + cell.width - inset;
            let left = right - marker_width;
            builder.move_to(Point::new(right, top));
            builder.line_to(Point::new(left, center_y));
            builder.line_to(Point::new(right, bottom));
        }
        ImbalanceSide::Sell => {
            let left = cell.x + inset;
            let right = left + marker_width;
            builder.move_to(Point::new(left, top));
            builder.line_to(Point::new(right, center_y));
            builder.line_to(Point::new(left, bottom));
        }
    }
    builder.close();

    frame.fill(&builder.build(), color);
}

fn imbalance_background(palette: &Extended, side: ImbalanceSide, alpha: f32) -> Color {
    let accent = match side {
        ImbalanceSide::Buy => palette.success.strong.color,
        ImbalanceSide::Sell => palette.danger.strong.color,
    };
    let alpha = alpha.clamp(0.0, 1.0);

    if palette.is_dark {
        let tint = 0.28 + (alpha * 0.32);
        mix_color(accent, palette.background.strongest.color, tint)
    } else {
        let tint = 0.18 + (alpha * 0.24);
        mix_color(accent, palette.background.weak.color, tint)
    }
}

fn buy_imbalance_alpha(
    footprint: &KlineTrades,
    price: Price,
    buy_qty: f64,
    step: PriceStep,
    threshold: usize,
    color_scale: Option<usize>,
    ignore_zeros: bool,
) -> Option<f32> {
    let lower_price = price.add_steps(-1, step);
    let diagonal_sell_qty = footprint
        .trades
        .get(&lower_price)
        .map(|group| group.sell_qty.to_f64())
        .unwrap_or_default();

    if ignore_zeros && (buy_qty <= 0.0 || diagonal_sell_qty <= 0.0) {
        return None;
    }

    imbalance_alpha(buy_qty, diagonal_sell_qty, threshold, color_scale)
}

fn sell_imbalance_alpha(
    footprint: &KlineTrades,
    price: Price,
    sell_qty: f64,
    step: PriceStep,
    threshold: usize,
    color_scale: Option<usize>,
    ignore_zeros: bool,
) -> Option<f32> {
    let higher_price = price.add_steps(1, step);
    let diagonal_buy_qty = footprint
        .trades
        .get(&higher_price)
        .map(|group| group.buy_qty.to_f64())
        .unwrap_or_default();

    if ignore_zeros && (sell_qty <= 0.0 || diagonal_buy_qty <= 0.0) {
        return None;
    }

    imbalance_alpha(sell_qty, diagonal_buy_qty, threshold, color_scale)
}

fn imbalance_alpha(
    dominant_qty: f64,
    opposite_qty: f64,
    threshold: usize,
    color_scale: Option<usize>,
) -> Option<f32> {
    let required_qty = opposite_qty * (100 + threshold) as f64 / 100.0;

    if required_qty <= 0.0 {
        return (dominant_qty > 0.0).then_some(1.0);
    }

    if dominant_qty <= required_qty {
        return None;
    }

    let ratio = dominant_qty / required_qty;
    Some(if let Some(scale) = color_scale {
        let divisor = (scale as f64 / 10.0) - 1.0;
        (0.2 + 0.8 * ((ratio - 1.0) / divisor).min(1.0)).min(1.0) as f32
    } else {
        1.0
    })
}

impl ContentGaps {
    fn from_view(candle_width: f32, scaling: f32) -> Self {
        let px = |p: f32| p / scaling;
        let base = (candle_width * 0.2).max(px(2.0));
        Self {
            marker_to_candle: base,
            candle_to_cluster: base,
            marker_to_bars: px(2.0),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ContentGaps {
    /// Space between imb. markers candle body
    marker_to_candle: f32,
    /// Space between candle body and clusters
    candle_to_cluster: f32,
    /// Inner space reserved between imb. markers and clusters (used for BidAsk)
    marker_to_bars: f32,
}

/// Layout and style parameters shared across footprint cell draw functions.
struct FootprintCellLayout<'a> {
    cell_w: f32,
    cell_h: f32,
    candle_w: f32,
    pal: &'a Extended,
    cluster: ClusterKind,
    gaps: ContentGaps,
}

fn draw_cluster_text(
    frame: &mut canvas::Frame,
    text: &str,
    position: Point,
    text_size: f32,
    color: iced::Color,
    align_x: Alignment,
    align_y: Alignment,
) {
    frame.fill_text(canvas::Text {
        content: text.to_string(),
        position,
        size: iced::Pixels(text_size),
        color,
        align_x: align_x.into(),
        align_y: align_y.into(),
        font: style::AZERET_MONO,
        ..canvas::Text::default()
    });
}

fn draw_crosshair_tooltip(
    data: &PlotData<KlineDataPoint>,
    ticker_info: &TickerInfo,
    frame: &mut canvas::Frame,
    palette: &Extended,
    basis: Basis,
    at_interval: Option<u64>,
    visible_range: (u64, u64),
) {
    let (visible_earliest, visible_latest) = visible_range;

    let kline_opt = match (data, at_interval) {
        (PlotData::TimeBased(timeseries), Some(at_interval)) => {
            let in_visible = at_interval >= visible_earliest && at_interval <= visible_latest;

            timeseries
                .datapoints
                .get(&UnixMs::new(at_interval))
                .map(|dp| &dp.kline)
                .or_else(|| {
                    if in_visible {
                        let search_end = at_interval.min(visible_latest);
                        timeseries
                            .datapoints
                            .range(UnixMs::new(visible_earliest)..=UnixMs::new(search_end))
                            .next_back()
                            .map(|(_, dp)| &dp.kline)
                    } else {
                        None
                    }
                })
                .or_else(|| {
                    let right_of_latest = match basis {
                        Basis::Time(_) => at_interval > visible_latest,
                        Basis::Tick(_) => at_interval < visible_earliest,
                    };

                    if right_of_latest {
                        timeseries
                            .datapoints
                            .range(UnixMs::new(visible_earliest)..=UnixMs::new(visible_latest))
                            .next_back()
                            .map(|(_, dp)| &dp.kline)
                    } else {
                        None
                    }
                })
                .or_else(|| {
                    let (last_time, dp) = timeseries.datapoints.last_key_value()?;
                    (at_interval > last_time.as_u64()).then_some(&dp.kline)
                })
        }
        (PlotData::TickBased(tick_aggr), Some(at_interval)) => {
            let kline_at = |interval: u64| {
                let index = (interval / u64::from(tick_aggr.interval.0)) as usize;
                (index < tick_aggr.datapoints.len())
                    .then(|| &tick_aggr.datapoints[tick_aggr.datapoints.len() - 1 - index].kline)
            };

            let in_visible = at_interval >= visible_earliest && at_interval <= visible_latest;

            kline_at(at_interval).or_else(|| {
                let right_of_latest = match basis {
                    Basis::Time(_) => at_interval > visible_latest,
                    Basis::Tick(_) => at_interval < visible_earliest,
                };

                if in_visible || right_of_latest {
                    kline_at(visible_earliest)
                } else {
                    None
                }
            })
        }
        (PlotData::TimeBased(timeseries), None) => timeseries
            .datapoints
            .last_key_value()
            .map(|(_, dp)| &dp.kline),
        (PlotData::TickBased(tick_aggr), None) => tick_aggr.datapoints.last().map(|dp| &dp.kline),
    };

    if let Some(kline) = kline_opt {
        let change_pct = ((kline.close - kline.open) / kline.open * 100.0) as f32;
        let change_color = if change_pct >= 0.0 {
            palette.success.base.color
        } else {
            palette.danger.base.color
        };

        let base_color = palette.background.base.text;
        let precision = ticker_info.min_ticksize;

        let segments = [
            ("O", base_color, false),
            (&kline.open.to_string(precision), change_color, true),
            ("H", base_color, false),
            (&kline.high.to_string(precision), change_color, true),
            ("L", base_color, false),
            (&kline.low.to_string(precision), change_color, true),
            ("C", base_color, false),
            (&kline.close.to_string(precision), change_color, true),
            (&format!("{change_pct:+.2}%"), change_color, true),
        ];

        let total_width: f32 = segments
            .iter()
            .map(|(s, _, _)| s.len() as f32 * (TEXT_SIZE * 0.8))
            .sum();

        let position = Point::new(8.0, 8.0);

        let tooltip_rect = Rectangle {
            x: position.x,
            y: position.y,
            width: total_width,
            height: 16.0,
        };

        frame.fill_rectangle(
            tooltip_rect.position(),
            tooltip_rect.size(),
            palette.background.weakest.color.scale_alpha(0.9),
        );

        let mut x = position.x;
        for (text, seg_color, is_value) in segments {
            frame.fill_text(canvas::Text {
                content: text.to_string(),
                position: Point::new(x, position.y),
                size: iced::Pixels(crate::style::text_size::BODY),
                color: seg_color,
                font: style::AZERET_MONO,
                ..canvas::Text::default()
            });
            x += text.len() as f32 * 8.0;
            x += if is_value { 6.0 } else { 2.0 };
        }
    }
}

struct ProfileArea {
    imb_marker_left: f32,
    imb_marker_width: f32,
    bars_left: f32,
    bars_width: f32,
    candle_center_x: f32,
}

impl ProfileArea {
    fn new(
        content_left: f32,
        content_right: f32,
        candle_width: f32,
        gaps: ContentGaps,
        has_imbalance: bool,
    ) -> Self {
        let candle_lane_left = if has_imbalance {
            content_left + candle_width + gaps.marker_to_candle
        } else {
            content_left
        };
        let candle_lane_width = candle_width * 0.25;

        let bars_left = candle_lane_left + candle_lane_width + gaps.candle_to_cluster;
        let bars_width = (content_right - bars_left).max(0.0);

        let candle_center_x = candle_lane_left + (candle_lane_width / 2.0);

        Self {
            imb_marker_left: content_left,
            imb_marker_width: if has_imbalance { candle_width } else { 0.0 },
            bars_left,
            bars_width,
            candle_center_x,
        }
    }
}

struct BidAskArea {
    bid_area_left: f32,
    bid_area_right: f32,
    ask_area_left: f32,
    ask_area_right: f32,
    candle_center_x: f32,
    imb_marker_width: f32,
}

impl BidAskArea {
    fn new(
        x_position: f32,
        content_left: f32,
        content_right: f32,
        candle_width: f32,
        spacing: ContentGaps,
    ) -> Self {
        let candle_body_width = candle_width * 0.25;

        let candle_left = x_position - (candle_body_width / 2.0);
        let candle_right = x_position + (candle_body_width / 2.0);

        let ask_area_right = candle_left - spacing.candle_to_cluster;
        let bid_area_left = candle_right + spacing.candle_to_cluster;

        Self {
            bid_area_left,
            bid_area_right: content_right,
            ask_area_left: content_left,
            ask_area_right,
            candle_center_x: x_position,
            imb_marker_width: candle_width,
        }
    }
}

struct TableArea {
    table_left: f32,
    table_right: f32,
}

impl TableArea {
    fn new(
        frame: &mut canvas::Frame,
        price_to_y: &impl Fn(Price) -> f32,
        content_left: f32,
        content_right: f32,
        candle_width: f32,
        kline: &Kline,
        palette: &Extended,
        spacing: ContentGaps,
    ) -> Self {
        let candle_center_x = content_left + candle_width / 2.0;
        draw_footprint_kline(
            frame,
            price_to_y,
            candle_center_x,
            candle_width,
            kline,
            palette,
        );

        Self {
            table_left: (content_left + candle_width + spacing.candle_to_cluster)
                .min(content_right),
            table_right: content_right,
        }
    }

    fn width(&self) -> f32 {
        (self.table_right - self.table_left).max(0.0)
    }
}

#[inline]
fn footprint_cluster_min_width(cluster_kind: ClusterKind) -> f32 {
    match cluster_kind {
        ClusterKind::VolumeProfile | ClusterKind::DeltaProfile => 80.0,
        ClusterKind::BidAsk => 120.0,
        ClusterKind::Table => 100.0,
    }
}

#[inline]
fn footprint_cluster_text_size(cell_height_unscaled: f32, cell_width_unscaled: f32) -> f32 {
    let text_size_from_height = cell_height_unscaled.round().min(16.0) - 3.0;
    let text_size_from_width = (cell_width_unscaled * 0.1).round().min(16.0) - 3.0;

    text_size_from_height.min(text_size_from_width)
}

#[inline]
fn price_padding_from_pixels(cell_height: f32, tick_size: f32) -> f32 {
    const OUTER_BOUND_PADDING_PX: f32 = 4.0;

    if cell_height <= f32::EPSILON {
        return 0.0;
    }

    (OUTER_BOUND_PADDING_PX / cell_height) * tick_size
}

fn footprint_summary_padding(
    cell_height: f32,
    scaling: f32,
    cell_width: f32,
    tick_size: f32,
    cluster_kind: ClusterKind,
) -> f32 {
    if cell_height <= f32::EPSILON {
        return 0.0;
    }

    let cell_height_unscaled = cell_height * scaling;
    let cell_width_unscaled = cell_width * scaling;

    if !should_show_text(
        cell_height_unscaled,
        cell_width_unscaled,
        footprint_cluster_min_width(cluster_kind),
    ) {
        return 0.0;
    }

    let text_size = style::text_size::TINY;

    let lowest_cell_bottom = cell_height / 2.0;

    let line_spacing = (text_size * 1.2) / scaling;
    let line_height = text_size / scaling;

    let summary_padding = line_height + line_spacing + line_height;
    let summary_y_start = lowest_cell_bottom + summary_padding;

    let second_line_y_start = summary_y_start + line_spacing;
    let summary_y_end = second_line_y_start + line_height;

    let extra_bottom_padding = line_height;
    let summary_y_end_with_padding = summary_y_end + extra_bottom_padding;
    let summary_ticks = summary_y_end_with_padding / cell_height;

    summary_ticks * tick_size
}

#[inline]
fn should_show_text(cell_height_unscaled: f32, cell_width_unscaled: f32, min_w: f32) -> bool {
    cell_height_unscaled > 8.0 && cell_width_unscaled > min_w
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_trade(id: u64, time: u64, qty: f64) -> Trade {
        Trade {
            id: Some(id),
            time: UnixMs::new(time),
            is_sell: false,
            price: Price::from_f64(100.0),
            qty: Qty::from_f64(qty),
        }
    }

    #[test]
    fn retained_trade_ids_prevent_bubble_reaggregation() {
        let trade = test_trade(42, 61_000, 10.0);
        assert!(deduplicate_incoming_trades(&[trade], &[trade]).is_empty());
    }

    #[test]
    fn evicted_trade_ids_must_still_prevent_bubble_reaggregation() {
        let previously_aggregated = test_trade(42, 61_000, 10.0);
        let retained_newer_trade = test_trade(99, 120_000, 1.0);
        let price_step = PriceStep {
            units: Price::from_f64(0.1).units,
        };
        let mut candle = KlineDataPoint {
            kline: Kline {
                time: UnixMs::new(60_000),
                open: previously_aggregated.price,
                high: previously_aggregated.price,
                low: previously_aggregated.price,
                close: previously_aggregated.price,
                volume: exchange::Volume::empty_buy_sell(),
            },
            footprint: KlineTrades::new(),
            bubble_summary: BubbleVolumeSummary::default(),
            trade_coverage: data::chart::kline::TradeCoverage::Unknown,
            trade_sequence: Vec::new(),
            trade_ids: Default::default(),
        };
        candle.add_trade(&previously_aggregated, price_step);

        // The candle bucket still contains trade 42, but raw_trades no longer
        // does after retention pruning. Re-fetching an overlapping range must
        // not let trade 42 be aggregated into the bucket a second time.
        let redelivered =
            deduplicate_incoming_trades(&[retained_newer_trade], &[previously_aggregated]);
        for trade in redelivered {
            candle.add_trade(&trade, price_step);
        }

        let total_qty = candle
            .footprint
            .trades
            .values()
            .map(|group| group.total_qty())
            .fold(Qty::ZERO, |total, qty| total + qty);
        assert_eq!(total_qty.to_f64(), 10.0);
    }

    #[test]
    fn bubble_rendering_uses_raw_or_summary_without_summing_both() {
        let price = Price::from_f64(100.0);
        let mut trades = KlineTrades::new();
        trades.trades.insert(
            price,
            data::chart::kline::GroupedTrades {
                buy_qty: Qty::from_f64(10.0),
                sell_qty: Qty::ZERO,
                first_time: UnixMs::new(61_000),
                last_time: UnixMs::new(61_000),
                buy_count: 1,
                sell_count: 0,
            },
        );
        let summary = BubbleVolumeSummary::new(
            UnixMs::new(60_000),
            vec![BubbleCandidate {
                candle_time: UnixMs::new(60_000),
                price,
                total_qty: Qty::from_f64(50.0),
                buy_qty: Qty::from_f64(50.0),
                sell_qty: Qty::ZERO,
                delta_qty: Qty::from_f64(50.0),
                trade_count: 5,
                score: 50.0,
                first_time: Some(UnixMs::new(61_000)),
                last_time: Some(UnixMs::new(62_000)),
            }],
        );

        let raw_points = collect_volume_bubble_points(
            &trades,
            &summary,
            &VolumeBubbleConfig {
                use_raw_trades_when_available: true,
                ..VolumeBubbleConfig::default()
            },
        );
        assert_eq!(raw_points.len(), 1);
        assert_eq!(raw_points[0].total_qty, 10.0);

        let summary_points = collect_volume_bubble_points(
            &trades,
            &summary,
            &VolumeBubbleConfig {
                use_raw_trades_when_available: false,
                ..VolumeBubbleConfig::default()
            },
        );
        assert_eq!(summary_points.len(), 1);
        assert_eq!(summary_points[0].total_qty, 50.0);
    }

    #[test]
    fn latest_uncovered_range_starts_from_the_newest_gap() {
        let covered = [
            (UnixMs::new(120), UnixMs::new(140)),
            (UnixMs::new(160), UnixMs::new(180)),
        ];

        assert_eq!(
            subtract_covered_ranges_latest(&covered, UnixMs::new(100), UnixMs::new(200), "TEST",),
            Some((UnixMs::new(180), UnixMs::new(200)))
        );
    }

    #[test]
    fn latest_uncovered_range_moves_back_after_the_tail_is_covered() {
        let covered = [
            (UnixMs::new(120), UnixMs::new(140)),
            (UnixMs::new(160), UnixMs::new(200)),
        ];

        assert_eq!(
            subtract_covered_ranges_latest(&covered, UnixMs::new(100), UnixMs::new(200), "TEST",),
            Some((UnixMs::new(140), UnixMs::new(160)))
        );
    }

    #[test]
    fn latest_uncovered_range_returns_none_when_fully_covered() {
        assert_eq!(
            subtract_covered_ranges_latest(
                &[(UnixMs::new(90), UnixMs::new(210))],
                UnixMs::new(100),
                UnixMs::new(200),
                "TEST",
            ),
            None
        );
    }

    #[test]
    fn vwap_daily_always_starts_at_current_session_open() {
        let day = 86_400_000;
        let target_to = UnixMs::new(2 * day + 12 * 60 * 60_000);
        assert_eq!(
            vwap_required_from(target_to, UnixMs::new(2 * day + 10 * 60 * 60_000), day),
            UnixMs::new(2 * day)
        );
    }

    #[test]
    fn vwap_covers_visible_previous_session_from_its_open() {
        let day = 86_400_000;
        assert_eq!(
            vwap_required_from(
                UnixMs::new(2 * day + 12 * 60 * 60_000),
                UnixMs::new(day + 18 * 60 * 60_000),
                day,
            ),
            UnixMs::new(day)
        );
    }

    #[test]
    fn bubble_window_uses_latest_candle_end_and_fixed_history() {
        let now = chrono::Utc
            .with_ymd_and_hms(2026, 1, 15, 12, 0, 0)
            .single()
            .unwrap();
        let now = UnixMs::new(now.timestamp_millis() as u64);
        let config = VolumeBubbleConfig::default();
        let range = volume_bubble_effective_range(now.saturating_sub(30_000), 60_000, now, &config)
            .unwrap();
        assert_eq!(range.1, now);
        assert_eq!(range.0, now.saturating_sub(30 * 60_000));
    }

    #[test]
    fn short_live_trade_tail_does_not_starve_historical_gap() {
        let covered = [(UnixMs::new(100), UnixMs::new(900))];
        assert_eq!(
            select_trade_fetch_gap(&covered, UnixMs::new(0), UnixMs::new(1_000)),
            Some((UnixMs::new(0), UnixMs::new(100)))
        );
    }

    #[test]
    fn grown_live_trade_tail_is_refreshed_before_history() {
        let covered = [(UnixMs::new(100), UnixMs::new(61_000))];
        assert_eq!(
            select_trade_fetch_gap(&covered, UnixMs::new(0), UnixMs::new(122_000)),
            Some((UnixMs::new(61_000), UnixMs::new(122_000)))
        );
    }

    #[test]
    fn historical_trade_fetch_does_not_chase_open_candle() {
        let candle_open = UnixMs::new(600_000);
        assert_eq!(
            historical_trade_target_to(candle_open, 60_000, UnixMs::new(630_000)),
            candle_open
        );
        assert_eq!(
            historical_trade_target_to(candle_open, 60_000, UnixMs::new(660_000)),
            UnixMs::new(660_000)
        );
    }
}
