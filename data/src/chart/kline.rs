use crate::{aggr::time::DataPoint, chart::indicator::KlineIndicator};
use exchange::{
    Kline, Trade, UnixMs,
    unit::price::{Price, PriceStep},
    unit::qty::Qty,
};

use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct KlineDataPoint {
    pub kline: Kline,
    pub footprint: KlineTrades,
    pub bubble_summary: BubbleVolumeSummary,
    /// Completeness of the raw directional executions for this bucket.
    pub trade_coverage: TradeCoverage,
    /// Raw executions retained in arrival order so indicators can reconstruct
    /// a real intrabar path when the bucket has complete historical coverage.
    pub trade_sequence: Vec<Trade>,
    /// Stable exchange IDs already aggregated into this candle. Unlike the
    /// chart-level raw buffer, this lives as long as the candle bucket.
    pub trade_ids: FxHashSet<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TradeCoverage {
    #[default]
    Unknown,
    Partial,
    Complete,
}

impl KlineDataPoint {
    pub fn max_cluster_qty(&self, cluster_kind: ClusterKind, highest: Price, lowest: Price) -> Qty {
        self.footprint
            .max_cluster_qty(cluster_kind, highest, lowest)
    }

    pub fn add_trade(&mut self, trade: &Trade, step: PriceStep) {
        if let Some(id) = trade.id
            && !self.trade_ids.insert(id)
        {
            return;
        }
        self.footprint.add_trade_to_nearest_bin(trade, step);
        self.trade_sequence.push(*trade);
        if self.trade_coverage == TradeCoverage::Unknown {
            self.trade_coverage = TradeCoverage::Partial;
        }
    }

    pub fn poc_price(&self) -> Option<Price> {
        self.footprint.poc_price()
    }

    pub fn set_poc_status(&mut self, status: NPoc) {
        self.footprint.set_poc_status(status);
    }

    pub fn clear_trades(&mut self) {
        self.footprint.clear();
        self.trade_sequence.clear();
        self.trade_ids.clear();
    }

    pub fn set_bubble_summary(&mut self, summary: BubbleVolumeSummary) {
        self.bubble_summary = summary;
    }

    pub fn calculate_poc(&mut self) {
        self.footprint.calculate_poc();
    }

    pub fn last_trade_time(&self) -> Option<UnixMs> {
        self.footprint.last_trade_t()
    }

    pub fn first_trade_time(&self) -> Option<UnixMs> {
        self.footprint.first_trade_t()
    }

    pub fn volume_delta(&self) -> Qty {
        if self.kline.volume.is_directional() {
            self.kline.volume.delta()
        } else if !self.footprint.trades.is_empty() {
            self.footprint
                .trades
                .values()
                .fold(Qty::ZERO, |acc, group| acc + group.delta_qty())
        } else {
            Qty::ZERO
        }
    }

    /// Whether this datapoint has directional (buy vs sell) data.
    pub fn is_directional(&self) -> bool {
        !self.footprint.trades.is_empty() || self.kline.volume.is_directional()
    }
}

impl DataPoint for KlineDataPoint {
    fn add_trade(&mut self, trade: &Trade, step: PriceStep) {
        self.add_trade(trade, step);
    }

    fn clear_trades(&mut self) {
        self.clear_trades();
    }

    fn last_trade_time(&self) -> Option<UnixMs> {
        self.last_trade_time()
    }

    fn first_trade_time(&self) -> Option<UnixMs> {
        self.first_trade_time()
    }

    fn last_price(&self) -> Price {
        self.kline.close
    }

    fn kline(&self) -> Option<&Kline> {
        Some(&self.kline)
    }

    fn value_high(&self) -> Price {
        self.kline.high
    }

    fn value_low(&self) -> Price {
        self.kline.low
    }
}

#[derive(Debug, Clone, Default)]
pub struct GroupedTrades {
    pub buy_qty: Qty,
    pub sell_qty: Qty,
    pub first_time: UnixMs,
    pub last_time: UnixMs,
    pub buy_count: usize,
    pub sell_count: usize,
}

impl GroupedTrades {
    fn new(trade: &Trade) -> Self {
        Self {
            buy_qty: if trade.is_sell {
                Qty::default()
            } else {
                trade.qty
            },
            sell_qty: if trade.is_sell {
                trade.qty
            } else {
                Qty::default()
            },
            first_time: trade.time,
            last_time: trade.time,
            buy_count: if trade.is_sell { 0 } else { 1 },
            sell_count: if trade.is_sell { 1 } else { 0 },
        }
    }

    fn add_trade(&mut self, trade: &Trade) {
        if trade.is_sell {
            self.sell_qty += trade.qty;
            self.sell_count += 1;
        } else {
            self.buy_qty += trade.qty;
            self.buy_count += 1;
        }
        self.last_time = trade.time;
    }

    pub fn total_qty(&self) -> Qty {
        self.buy_qty + self.sell_qty
    }

    pub fn delta_qty(&self) -> Qty {
        self.buy_qty - self.sell_qty
    }

    pub fn max_cluster_qty(&self, cluster_kind: ClusterKind) -> Qty {
        match cluster_kind {
            ClusterKind::BidAsk | ClusterKind::Table => self.buy_qty.max(self.sell_qty),
            ClusterKind::DeltaProfile => self.buy_qty.abs_diff(self.sell_qty),
            ClusterKind::VolumeProfile => self.total_qty(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum BubbleHistoricalMode {
    #[default]
    SummaryOnly,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct BubbleCandidate {
    pub candle_time: UnixMs,
    pub price: Price,
    pub total_qty: Qty,
    pub buy_qty: Qty,
    pub sell_qty: Qty,
    pub delta_qty: Qty,
    pub trade_count: usize,
    pub score: f64,
    pub first_time: Option<UnixMs>,
    pub last_time: Option<UnixMs>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct BubbleVolumeSummary {
    pub candle_time: UnixMs,
    pub candidates: Vec<BubbleCandidate>,
}

impl BubbleVolumeSummary {
    pub fn new(candle_time: UnixMs, candidates: Vec<BubbleCandidate>) -> Self {
        Self {
            candle_time,
            candidates,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }
}

#[derive(Debug, Clone, Default)]
pub struct KlineTrades {
    pub trades: FxHashMap<Price, GroupedTrades>,
    pub poc: Option<PointOfControl>,
}

impl KlineTrades {
    pub fn new() -> Self {
        Self {
            trades: FxHashMap::default(),
            poc: None,
        }
    }

    pub fn first_trade_t(&self) -> Option<UnixMs> {
        self.trades.values().map(|group| group.first_time).min()
    }

    pub fn last_trade_t(&self) -> Option<UnixMs> {
        self.trades.values().map(|group| group.last_time).max()
    }

    /// Add trade to the bin at the step multiple computed with side-based rounding.
    /// Intended for order-book ladder/quotes; Floor for sells, ceil for buys.
    /// Introduces side bias at bin edges and should not be used for OHLC/footprint aggregation
    pub fn add_trade_to_side_bin(&mut self, trade: &Trade, step: PriceStep) {
        let price = trade.price.round_to_side_step(trade.is_sell, step);

        self.trades
            .entry(price)
            .and_modify(|group| group.add_trade(trade))
            .or_insert_with(|| GroupedTrades::new(trade));
    }

    /// Add trade to the bin at the nearest step multiple (side-agnostic).
    /// Ties (exactly half a step) round up to the higher multiple.
    /// Intended for footprint/OHLC trade aggregation
    pub fn add_trade_to_nearest_bin(&mut self, trade: &Trade, step: PriceStep) {
        let price = trade.price.round_to_step(step);

        self.trades
            .entry(price)
            .and_modify(|group| group.add_trade(trade))
            .or_insert_with(|| GroupedTrades::new(trade));
    }

    pub fn max_qty_by<F>(&self, highest: Price, lowest: Price, f: F) -> Qty
    where
        F: Fn(&GroupedTrades) -> Qty,
    {
        let mut max_qty = Qty::default();
        for (price, group) in &self.trades {
            if *price >= lowest && *price <= highest {
                max_qty = max_qty.max(f(group));
            }
        }
        max_qty
    }

    pub fn max_cluster_qty(&self, cluster_kind: ClusterKind, highest: Price, lowest: Price) -> Qty {
        self.max_qty_by(highest, lowest, |group| group.max_cluster_qty(cluster_kind))
    }

    pub fn calculate_poc(&mut self) {
        if self.trades.is_empty() {
            return;
        }

        let mut max_volume = Qty::ZERO;
        let mut poc_price = Price::from_f32(0.0);

        for (price, group) in &self.trades {
            let total_volume = group.total_qty();
            if total_volume > max_volume {
                max_volume = total_volume;
                poc_price = *price;
            }
        }

        self.poc = Some(PointOfControl {
            price: poc_price,
            volume: max_volume,
            status: NPoc::default(),
        });
    }

    pub fn set_poc_status(&mut self, status: NPoc) {
        if let Some(poc) = &mut self.poc {
            poc.status = status;
        }
    }

    pub fn poc_price(&self) -> Option<Price> {
        self.poc.map(|poc| poc.price)
    }

    pub fn clear(&mut self) {
        self.trades.clear();
        self.poc = None;
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FootprintSummary {
    pub buy: Qty,
    pub sell: Qty,
    pub total: Qty,
    pub delta: Qty,
    pub delta_pct: f64,
}

impl FootprintSummary {
    pub fn new(buy: Qty, sell: Qty) -> Self {
        let total = buy + sell;
        let delta = buy - sell;
        let total_f = total.to_f64();
        let delta_pct = if total_f > 0.0 {
            (delta.to_f64() / total_f) * 100.0
        } else {
            0.0
        };

        Self {
            buy,
            sell,
            total,
            delta,
            delta_pct,
        }
    }

    pub fn from_trades(footprint: &KlineTrades) -> Option<Self> {
        if footprint.trades.is_empty() {
            return None;
        }

        let (buy, sell) = footprint
            .trades
            .values()
            .fold((Qty::ZERO, Qty::ZERO), |(buy, sell), group| {
                (buy + group.buy_qty, sell + group.sell_qty)
            });

        Some(Self::new(buy, sell))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum KlineChartKind {
    #[default]
    Candles,
    Footprint {
        clusters: ClusterKind,
        #[serde(default)]
        scaling: ClusterScaling,
        studies: Vec<FootprintStudy>,
    },
}

impl KlineChartKind {
    pub fn allows_indicator(&self, indicator: KlineIndicator) -> bool {
        match self {
            KlineChartKind::Candles => !matches!(indicator, KlineIndicator::BarAnalysis),
            KlineChartKind::Footprint { .. } => !indicator.is_overlay(),
        }
    }

    pub fn min_scaling(&self) -> f32 {
        match self {
            KlineChartKind::Footprint { .. } => 0.4,
            KlineChartKind::Candles => 0.6,
        }
    }

    pub fn max_scaling(&self) -> f32 {
        match self {
            KlineChartKind::Footprint { .. } => 1.2,
            KlineChartKind::Candles => 2.5,
        }
    }

    pub fn max_cell_width(&self) -> f32 {
        match self {
            KlineChartKind::Footprint { .. } => 360.0,
            KlineChartKind::Candles => 16.0,
        }
    }

    pub fn min_cell_width(&self) -> f32 {
        match self {
            KlineChartKind::Footprint { .. } => 80.0,
            KlineChartKind::Candles => 1.0,
        }
    }

    pub fn max_cell_height(&self) -> f32 {
        match self {
            KlineChartKind::Footprint { .. } => 90.0,
            KlineChartKind::Candles => 8.0,
        }
    }

    pub fn min_cell_height(&self) -> f32 {
        match self {
            KlineChartKind::Footprint { .. } => 1.0,
            KlineChartKind::Candles => 0.001,
        }
    }

    pub fn default_cell_width(&self) -> f32 {
        match self {
            KlineChartKind::Footprint { .. } => 80.0,
            KlineChartKind::Candles => 4.0,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum ClusterKind {
    #[default]
    BidAsk,
    VolumeProfile,
    DeltaProfile,
    Table,
}

impl ClusterKind {
    pub const ALL: [ClusterKind; 4] = [
        ClusterKind::BidAsk,
        ClusterKind::VolumeProfile,
        ClusterKind::DeltaProfile,
        ClusterKind::Table,
    ];

    pub fn allows_study(&self, study: &FootprintStudy) -> bool {
        !matches!(
            (self, study),
            (ClusterKind::Table, FootprintStudy::NPoC { .. })
        )
    }
}

impl std::fmt::Display for ClusterKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterKind::BidAsk => write!(f, "Bid/Ask"),
            ClusterKind::VolumeProfile => write!(f, "Volume Profile"),
            ClusterKind::DeltaProfile => write!(f, "Delta Profile"),
            ClusterKind::Table => write!(f, "Table"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    // Whether to show last value labels on top right/left when not hovering
    // e.g. OHLC/bar change values for the main chart, or last value of an indicator series
    pub data_labels_always_visible: bool,
    // Whether to show the footprint per-bar summary below each candle.
    pub show_footprint_summary: bool,
    // Whether to show a small candle next to footprint table clusters.
    pub show_footprint_table_candle: bool,
    // Optional main-chart order-flow bubbles for regular candlesticks.
    pub volume_bubbles: VolumeBubbleConfig,
    /// Session volume profile overlay for regular candlesticks.
    pub session_volume_profile: SessionVolumeProfileConfig,
    /// Settings owned by the VWAP overlay indicator.
    pub vwap: VwapConfig,
    /// Settings owned by the CVD panel indicator.
    pub cvd: CvdConfig,
    /// Visual settings owned by configurable indicators.
    pub indicator_configs: IndicatorConfigs,
    /// Compatibility input for states saved before `indicator_configs`.
    #[serde(default, rename = "gex_levels", skip_serializing)]
    pub legacy_gex_levels: Option<crate::chart::gex::GexLevelsConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_labels_always_visible: false,
            show_footprint_summary: true,
            show_footprint_table_candle: true,
            volume_bubbles: VolumeBubbleConfig::default(),
            session_volume_profile: SessionVolumeProfileConfig::default(),
            vwap: VwapConfig::default(),
            cvd: CvdConfig::default(),
            indicator_configs: IndicatorConfigs::default(),
            legacy_gex_levels: None,
        }
    }
}

impl Config {
    pub fn gex_levels(&self) -> crate::chart::gex::GexLevelsConfig {
        self.legacy_gex_levels
            .unwrap_or(self.indicator_configs.gex_levels)
    }

    pub fn with_gex_levels(mut self, config: crate::chart::gex::GexLevelsConfig) -> Self {
        self.indicator_configs.gex_levels = config;
        self.legacy_gex_levels = None;
        self
    }

    pub fn migrate_legacy_indicator_configs(&mut self) {
        if let Some(legacy) = self.legacy_gex_levels.take() {
            self.indicator_configs.gex_levels = legacy;
        }
        self.indicator_configs.gex_levels.migrate_legacy_defaults();
    }
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct IndicatorConfigs {
    pub gex_levels: crate::chart::gex::GexLevelsConfig,
}

#[derive(Debug, Copy, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct CvdConfig {
    pub render_style: CvdRenderStyle,
    pub candle_width_percent: f32,
    pub show_wicks: bool,
    pub line_width: f32,
    pub reset: CvdReset,
}

impl Default for CvdConfig {
    fn default() -> Self {
        Self {
            render_style: CvdRenderStyle::Candlesticks,
            candle_width_percent: 70.0,
            show_wicks: false,
            line_width: 1.0,
            reset: CvdReset::DailyUtc,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum CvdReset {
    #[default]
    DailyUtc,
    Continuous,
}

impl CvdReset {
    pub const ALL: [Self; 2] = [Self::DailyUtc, Self::Continuous];
}

impl std::fmt::Display for CvdReset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::DailyUtc => "Daily UTC",
            Self::Continuous => "Continuous",
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum CvdRenderStyle {
    Line,
    #[default]
    Candlesticks,
}

impl CvdRenderStyle {
    pub const ALL: [Self; 2] = [Self::Candlesticks, Self::Line];
}

impl std::fmt::Display for CvdRenderStyle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Line => "Line",
            Self::Candlesticks => "Candlesticks",
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct VwapConfig {
    pub anchor: SessionProfileInterval,
    pub line_width: f32,
    pub show_bands: bool,
    pub band_multiplier: f32,
    pub show_labels: bool,
}

impl Default for VwapConfig {
    fn default() -> Self {
        Self {
            anchor: SessionProfileInterval::Daily,
            line_width: 1.6,
            show_bands: true,
            band_multiplier: 1.0,
            show_labels: true,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct SessionVolumeProfileConfig {
    pub enabled: bool,
    pub interval: SessionProfileInterval,
    pub placement: SessionProfilePlacement,
    pub mode: SessionProfileMode,
    /// Percentage of session volume enclosed by VAH/VAL.
    pub value_area_percent: f32,
    /// Maximum profile width as percentage of the session width.
    pub width_percent: f32,
    /// Number of chart ticks aggregated into one profile row.
    pub row_size_ticks: u16,
    pub show_poc: bool,
    pub show_value_area: bool,
    pub show_vwap: bool,
    pub show_session_high_low: bool,
}

impl Default for SessionVolumeProfileConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval: SessionProfileInterval::Hourly,
            placement: SessionProfilePlacement::Left,
            mode: SessionProfileMode::Volume,
            value_area_percent: 70.0,
            width_percent: 35.0,
            row_size_ticks: 1,
            show_poc: true,
            show_value_area: true,
            show_vwap: true,
            show_session_high_low: true,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum SessionProfileInterval {
    Minutes30,
    #[default]
    Hourly,
    Hours4,
    Daily,
    Weekly,
}

impl SessionProfileInterval {
    pub const ALL: [Self; 5] = [
        Self::Minutes30,
        Self::Hourly,
        Self::Hours4,
        Self::Daily,
        Self::Weekly,
    ];

    pub fn milliseconds(self) -> u64 {
        match self {
            Self::Minutes30 => 30 * 60_000,
            Self::Hourly => 60 * 60_000,
            Self::Hours4 => 4 * 60 * 60_000,
            Self::Daily => 24 * 60 * 60_000,
            Self::Weekly => 7 * 24 * 60 * 60_000,
        }
    }
}

impl std::fmt::Display for SessionProfileInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Minutes30 => "30 minutes",
            Self::Hourly => "Hourly",
            Self::Hours4 => "4 hours",
            Self::Daily => "Daily",
            Self::Weekly => "Weekly",
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum SessionProfilePlacement {
    #[default]
    Left,
    Right,
}

impl SessionProfilePlacement {
    pub const ALL: [Self; 2] = [Self::Left, Self::Right];
}

impl std::fmt::Display for SessionProfilePlacement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Left => "Left / session open",
            Self::Right => "Right / session close",
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum SessionProfileMode {
    #[default]
    Volume,
    Delta,
}

impl SessionProfileMode {
    pub const ALL: [Self; 2] = [Self::Volume, Self::Delta];
}

impl std::fmt::Display for SessionProfileMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Volume => "Volume",
            Self::Delta => "Delta",
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct VolumeBubbleConfig {
    pub enabled: bool,
    pub min_qty: f64,
    pub max_bubbles_per_bar: usize,
    pub historical_mode: BubbleHistoricalMode,
    pub max_candidates_per_candle: usize,
    pub history_window_minutes: u64,
    pub use_raw_trades_when_available: bool,
    pub min_radius_px: f32,
    pub max_radius_px: f32,
    pub show_labels: bool,
    pub color_mode: BubbleColorMode,
    pub session: VolumeBubbleSession,
}

impl Default for VolumeBubbleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_qty: 0.0,
            max_bubbles_per_bar: 3,
            historical_mode: BubbleHistoricalMode::SummaryOnly,
            max_candidates_per_candle: 3,
            history_window_minutes: 30,
            use_raw_trades_when_available: true,
            min_radius_px: 3.0,
            max_radius_px: 14.0,
            show_labels: false,
            color_mode: BubbleColorMode::Delta,
            session: VolumeBubbleSession::Auto,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum BubbleColorMode {
    #[default]
    Delta,
    DominantSide,
}

impl BubbleColorMode {
    pub const ALL: [BubbleColorMode; 2] = [BubbleColorMode::Delta, BubbleColorMode::DominantSide];
}

impl std::fmt::Display for BubbleColorMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BubbleColorMode::Delta => write!(f, "Delta"),
            BubbleColorMode::DominantSide => write!(f, "Dominant side"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum VolumeBubbleSession {
    #[default]
    Auto,
    Asian,
    London,
    NewYork,
}

impl VolumeBubbleSession {
    pub const ALL: [VolumeBubbleSession; 4] = [
        VolumeBubbleSession::Auto,
        VolumeBubbleSession::Asian,
        VolumeBubbleSession::London,
        VolumeBubbleSession::NewYork,
    ];
}

impl std::fmt::Display for VolumeBubbleSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VolumeBubbleSession::Auto => write!(f, "Auto session"),
            VolumeBubbleSession::Asian => write!(f, "Asian"),
            VolumeBubbleSession::London => write!(f, "London"),
            VolumeBubbleSession::NewYork => write!(f, "New York"),
        }
    }
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
pub enum ClusterScaling {
    #[default]
    /// Scale based on the maximum quantity in the visible range.
    VisibleRange,
    /// Blend global VisibleRange and per-cluster Individual using a weight in [0.0, 1.0].
    /// weight = fraction of global contribution (1.0 == all-global, 0.0 == all-individual).
    Hybrid { weight: f32 },
    /// Scale based only on the maximum quantity inside the datapoint (per-candle).
    Datapoint,
}

impl ClusterScaling {
    pub const ALL: [ClusterScaling; 3] = [
        ClusterScaling::VisibleRange,
        ClusterScaling::Hybrid { weight: 0.2 },
        ClusterScaling::Datapoint,
    ];
}

impl std::fmt::Display for ClusterScaling {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterScaling::VisibleRange => write!(f, "Visible Range"),
            ClusterScaling::Hybrid { weight } => write!(f, "Hybrid (weight: {:.2})", weight),
            ClusterScaling::Datapoint => write!(f, "Per-candle"),
        }
    }
}

impl std::cmp::Eq for ClusterScaling {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum FootprintStudy {
    NPoC {
        lookback: usize,
    },
    Imbalance {
        threshold: usize,
        color_scale: Option<usize>,
        ignore_zeros: bool,
    },
}

impl FootprintStudy {
    pub fn is_same_type(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (FootprintStudy::NPoC { .. }, FootprintStudy::NPoC { .. })
                | (
                    FootprintStudy::Imbalance { .. },
                    FootprintStudy::Imbalance { .. }
                )
        )
    }
}

impl FootprintStudy {
    pub const ALL: [FootprintStudy; 2] = [
        FootprintStudy::NPoC { lookback: 80 },
        FootprintStudy::Imbalance {
            threshold: 200,
            color_scale: Some(400),
            ignore_zeros: true,
        },
    ];
}

impl std::fmt::Display for FootprintStudy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FootprintStudy::NPoC { .. } => write!(f, "Naked Point of Control"),
            FootprintStudy::Imbalance { .. } => write!(f, "Imbalance"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PointOfControl {
    pub price: Price,
    pub volume: Qty,
    pub status: NPoc,
}

impl Default for PointOfControl {
    fn default() -> Self {
        Self {
            price: Price::from_f32(0.0),
            volume: Qty::ZERO,
            status: NPoc::default(),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum NPoc {
    #[default]
    None,
    Naked,
    Filled {
        at: u64,
    },
}

impl NPoc {
    pub fn filled(&mut self, at: u64) {
        *self = NPoc::Filled { at };
    }

    pub fn unfilled(&mut self) {
        *self = NPoc::Naked;
    }
}
