use exchange::{
    UnixMs,
    options::{
        OptionRight, OptionsProvider, OptionsUnderlying, RawOptionChainSnapshot,
        RawOptionContractSnapshot,
    },
};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, f64::consts::PI, sync::Arc};

const MILLIS_PER_DAY: u64 = 86_400_000;
const MILLIS_PER_YEAR: f64 = 365.25 * MILLIS_PER_DAY as f64;
const MAX_VOLATILITY: f64 = 10.0;
const MIN_DENOMINATOR: f64 = 1.0e-12;
const DEFAULT_FLIP_RANGE_PERCENT: f64 = 30.0;
const FLIP_SCAN_STEPS: usize = 240;
const FLIP_BISECTION_STEPS: usize = 60;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum GexSignModel {
    AbsoluteGamma,
    #[default]
    CallPutOiProxy,
}

impl std::fmt::Display for GexSignModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AbsoluteGamma => f.write_str("Absolute Gamma"),
            Self::CallPutOiProxy => f.write_str("GEX OI Proxy"),
        }
    }
}

impl GexSignModel {
    pub const ALL: [Self; 2] = [Self::CallPutOiProxy, Self::AbsoluteGamma];
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum GexExpiryFilter {
    NextExpiry,
    OneDay,
    #[default]
    SevenDays,
    ThirtyDays,
    All,
}

impl GexExpiryFilter {
    pub const ALL: [Self; 5] = [
        Self::NextExpiry,
        Self::OneDay,
        Self::SevenDays,
        Self::ThirtyDays,
        Self::All,
    ];
}

impl std::fmt::Display for GexExpiryFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NextExpiry => f.write_str("Next expiry"),
            Self::OneDay => f.write_str("Next 1 day"),
            Self::SevenDays => f.write_str("Next 7 days"),
            Self::ThirtyDays => f.write_str("Next 30 days"),
            Self::All => f.write_str("All expiries"),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum GexBasisMode {
    #[default]
    RawStrike,
    ShiftToChartPrice,
}

impl GexBasisMode {
    pub const ALL: [Self; 2] = [Self::RawStrike, Self::ShiftToChartPrice];
}

impl std::fmt::Display for GexBasisMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RawStrike => f.write_str("Raw strike"),
            Self::ShiftToChartPrice => f.write_str("Shift to chart price"),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum GexLevelColor {
    Primary,
    Success,
    Danger,
    #[default]
    Warning,
    Secondary,
}

impl GexLevelColor {
    pub const ALL: [Self; 5] = [
        Self::Primary,
        Self::Success,
        Self::Danger,
        Self::Warning,
        Self::Secondary,
    ];
}

impl std::fmt::Display for GexLevelColor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Primary => "Primary",
            Self::Success => "Success",
            Self::Danger => "Danger",
            Self::Warning => "Warning",
            Self::Secondary => "Secondary",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct GexLevelsConfig {
    pub enabled_model: GexSignModel,
    pub expiry_filter: GexExpiryFilter,
    pub show_gamma_flip: bool,
    pub show_call_wall: bool,
    pub show_put_wall: bool,
    pub show_top_clusters: bool,
    pub max_clusters: usize,
    pub clusters_as_bands: bool,
    /// Half-width of a cluster band as a fraction of the adjacent strike gap.
    pub cluster_band_width: f32,
    pub show_value: bool,
    pub show_distance_percent: bool,
    pub basis_mode: GexBasisMode,
    pub line_width: f32,
    pub gamma_flip_width: f32,
    pub line_opacity: f32,
    pub band_opacity: f32,
    pub horizontal_span_percent: f32,
    pub gamma_flip_color: GexLevelColor,
    pub call_wall_color: GexLevelColor,
    pub put_wall_color: GexLevelColor,
    pub cluster_color: GexLevelColor,
    #[serde(default)]
    pub cluster_color_customized: bool,
}

impl Default for GexLevelsConfig {
    fn default() -> Self {
        Self {
            enabled_model: GexSignModel::CallPutOiProxy,
            expiry_filter: GexExpiryFilter::SevenDays,
            show_gamma_flip: true,
            show_call_wall: true,
            show_put_wall: true,
            show_top_clusters: true,
            max_clusters: 3,
            clusters_as_bands: true,
            cluster_band_width: 0.5,
            show_value: true,
            show_distance_percent: true,
            basis_mode: GexBasisMode::RawStrike,
            line_width: 1.0,
            gamma_flip_width: 1.8,
            line_opacity: 0.78,
            band_opacity: 0.12,
            horizontal_span_percent: 35.0,
            gamma_flip_color: GexLevelColor::Warning,
            call_wall_color: GexLevelColor::Success,
            put_wall_color: GexLevelColor::Danger,
            cluster_color: GexLevelColor::Primary,
            cluster_color_customized: false,
        }
    }
}

impl GexLevelsConfig {
    pub fn migrate_legacy_defaults(&mut self) {
        if !self.cluster_color_customized && self.cluster_color == GexLevelColor::Secondary {
            self.cluster_color = GexLevelColor::Primary;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub sign_model: GexSignModel,
    pub expiry_filter: GexExpiryFilter,
    pub min_open_interest: f64,
    pub min_absolute_gex: f64,
    pub max_visible_strikes: usize,
    pub price_range_percent: f64,
    pub show_call_gex: bool,
    pub show_put_gex: bool,
    pub show_net_gex: bool,
    pub show_absolute_gamma: bool,
    pub show_current_price: bool,
    pub show_call_wall: bool,
    pub show_put_wall: bool,
    pub show_gamma_flip: bool,
    pub show_summary: bool,
    pub show_header_net_gex: bool,
    pub show_header_absolute_gex: bool,
    pub show_header_gamma_flip: bool,
    pub show_header_call_wall: bool,
    pub show_header_put_wall: bool,
    pub show_header_expiry: bool,
    pub show_header_freshness: bool,
    pub show_header_snapshot: bool,
    pub show_header_model: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sign_model: GexSignModel::CallPutOiProxy,
            expiry_filter: GexExpiryFilter::SevenDays,
            min_open_interest: 0.0,
            min_absolute_gex: 0.0,
            max_visible_strikes: 40,
            price_range_percent: 15.0,
            show_call_gex: true,
            show_put_gex: true,
            show_net_gex: true,
            show_absolute_gamma: false,
            show_current_price: true,
            show_call_wall: true,
            show_put_wall: true,
            show_gamma_flip: true,
            show_summary: true,
            show_header_net_gex: true,
            show_header_absolute_gex: false,
            show_header_gamma_flip: true,
            show_header_call_wall: false,
            show_header_put_wall: false,
            show_header_expiry: true,
            show_header_freshness: true,
            show_header_snapshot: false,
            show_header_model: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct GexStrike {
    pub strike: f64,
    pub call_gex_1pct: f64,
    pub put_gex_1pct: f64,
    pub net_gex_1pct: f64,
    pub absolute_gamma_1pct: f64,
    pub call_open_interest: f64,
    pub put_open_interest: f64,
    pub expiration_count: usize,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct GexSnapshot {
    pub provider: OptionsProvider,
    pub underlying: OptionsUnderlying,
    pub model: GexSignModel,
    pub source_spot: f64,
    pub observed_at: UnixMs,
    pub calculated_at: UnixMs,
    pub net_gex_1pct: Option<f64>,
    pub absolute_gex_1pct: f64,
    pub call_wall: Option<f64>,
    pub put_wall: Option<f64>,
    pub gamma_flip: Option<f64>,
    pub strikes: Arc<[GexStrike]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GexFreshness {
    Loading,
    Fresh,
    Stale,
    Expired,
    Error,
}

#[derive(Default)]
struct StrikeAccumulator {
    strike: f64,
    call_gex: f64,
    put_gex_abs: f64,
    absolute: f64,
    call_oi: f64,
    put_oi: f64,
    expirations: FxHashSet<UnixMs>,
}

pub fn normal_pdf(value: f64) -> Option<f64> {
    value
        .is_finite()
        .then(|| (-0.5 * value * value).exp() / (2.0 * PI).sqrt())
        .filter(|result| result.is_finite())
}

pub fn black_scholes_gamma(
    spot: f64,
    strike: f64,
    years_to_expiry: f64,
    interest_rate: f64,
    volatility: f64,
) -> Option<f64> {
    if ![spot, strike, years_to_expiry, interest_rate, volatility]
        .iter()
        .all(|value| value.is_finite())
        || spot <= 0.0
        || strike <= 0.0
        || years_to_expiry <= 0.0
        || volatility <= 0.0
        || volatility > MAX_VOLATILITY
    {
        return None;
    }
    let sqrt_time = years_to_expiry.sqrt();
    let denominator = volatility * sqrt_time;
    if denominator <= MIN_DENOMINATOR {
        return None;
    }
    let d1 = ((spot / strike).ln()
        + (interest_rate + 0.5 * volatility * volatility) * years_to_expiry)
        / denominator;
    let gamma_denominator = spot * denominator;
    if gamma_denominator <= MIN_DENOMINATOR {
        return None;
    }
    let gamma = normal_pdf(d1)? / gamma_denominator;
    (gamma.is_finite() && gamma >= 0.0).then_some(gamma)
}

pub fn years_to_expiry(expiration: UnixMs, now: UnixMs) -> Option<f64> {
    expiration
        .as_u64()
        .checked_sub(now.as_u64())
        .map(|millis| millis as f64 / MILLIS_PER_YEAR)
        .filter(|years| years.is_finite() && *years > 0.0)
}

pub fn iv_percent_to_decimal(iv_percent: f64) -> Option<f64> {
    let volatility = iv_percent / 100.0;
    (iv_percent.is_finite() && volatility > 0.0 && volatility <= MAX_VOLATILITY)
        .then_some(volatility)
}

pub fn calculate_gex(chain: &RawOptionChainSnapshot, config: &Config) -> GexSnapshot {
    calculate_gex_at(chain, config, UnixMs::now())
}

pub fn calculate_gex_at(
    chain: &RawOptionChainSnapshot,
    config: &Config,
    calculated_at: UnixMs,
) -> GexSnapshot {
    let selected = select_contracts(chain, config.expiry_filter, calculated_at);
    let mut by_strike: FxHashMap<u64, StrikeAccumulator> = FxHashMap::default();

    for contract in selected.iter().copied() {
        let oi = contract.market.open_interest_underlying;
        if !oi.is_finite() || oi < config.min_open_interest || oi < 0.0 {
            continue;
        }
        let Some(gex) = contract_gex(contract, chain.source_spot, calculated_at) else {
            continue;
        };
        if gex < config.min_absolute_gex {
            continue;
        }
        let entry = by_strike
            .entry(contract.instrument.strike.to_bits())
            .or_insert_with(|| StrikeAccumulator {
                strike: contract.instrument.strike,
                ..StrikeAccumulator::default()
            });
        entry.absolute += gex;
        entry
            .expirations
            .insert(contract.instrument.expiration_timestamp);
        match contract.instrument.right {
            OptionRight::Call => {
                entry.call_gex += gex;
                entry.call_oi += oi;
            }
            OptionRight::Put => {
                entry.put_gex_abs += gex;
                entry.put_oi += oi;
            }
        }
    }

    let mut strikes = by_strike
        .into_values()
        .map(|entry| {
            let net = entry.call_gex - entry.put_gex_abs;
            GexStrike {
                strike: entry.strike,
                call_gex_1pct: entry.call_gex,
                put_gex_1pct: -entry.put_gex_abs,
                net_gex_1pct: net,
                absolute_gamma_1pct: entry.absolute,
                call_open_interest: entry.call_oi,
                put_open_interest: entry.put_oi,
                expiration_count: entry.expirations.len(),
            }
        })
        .collect::<Vec<_>>();
    strikes.sort_by(|a, b| a.strike.partial_cmp(&b.strike).unwrap_or(Ordering::Equal));

    let absolute_gex_1pct = strikes
        .iter()
        .map(|strike| strike.absolute_gamma_1pct)
        .sum();
    let proxy_net = strikes.iter().map(|strike| strike.net_gex_1pct).sum();
    let call_wall = strikes
        .iter()
        .max_by(|a, b| {
            a.call_gex_1pct
                .partial_cmp(&b.call_gex_1pct)
                .unwrap_or(Ordering::Equal)
        })
        .filter(|strike| strike.call_gex_1pct > 0.0)
        .map(|strike| strike.strike);
    let put_wall = strikes
        .iter()
        .max_by(|a, b| {
            a.put_gex_1pct
                .abs()
                .partial_cmp(&b.put_gex_1pct.abs())
                .unwrap_or(Ordering::Equal)
        })
        .filter(|strike| strike.put_gex_1pct < 0.0)
        .map(|strike| strike.strike);
    let gamma_flip = (config.sign_model == GexSignModel::CallPutOiProxy)
        .then(|| {
            find_gamma_flip(
                &selected,
                chain.source_spot,
                calculated_at,
                DEFAULT_FLIP_RANGE_PERCENT,
            )
        })
        .flatten();

    GexSnapshot {
        provider: chain.provider,
        underlying: chain.underlying,
        model: config.sign_model,
        source_spot: chain.source_spot,
        observed_at: chain.observed_at,
        calculated_at,
        net_gex_1pct: (config.sign_model == GexSignModel::CallPutOiProxy).then_some(proxy_net),
        absolute_gex_1pct,
        call_wall,
        put_wall,
        gamma_flip,
        strikes: strikes.into(),
    }
}

fn select_contracts(
    chain: &RawOptionChainSnapshot,
    filter: GexExpiryFilter,
    now: UnixMs,
) -> Vec<&RawOptionContractSnapshot> {
    let next_expiry = chain
        .contracts
        .iter()
        .filter(|contract| contract.instrument.expiration_timestamp > now)
        .map(|contract| contract.instrument.expiration_timestamp)
        .min();
    let max_expiry = match filter {
        GexExpiryFilter::OneDay => Some(now.saturating_add(MILLIS_PER_DAY)),
        GexExpiryFilter::SevenDays => Some(now.saturating_add(7 * MILLIS_PER_DAY)),
        GexExpiryFilter::ThirtyDays => Some(now.saturating_add(30 * MILLIS_PER_DAY)),
        GexExpiryFilter::NextExpiry | GexExpiryFilter::All => None,
    };
    chain
        .contracts
        .iter()
        .filter(|contract| {
            let expiration = contract.instrument.expiration_timestamp;
            if expiration <= now {
                return false;
            }
            match filter {
                GexExpiryFilter::NextExpiry => Some(expiration) == next_expiry,
                GexExpiryFilter::All => true,
                _ => max_expiry.is_some_and(|limit| expiration <= limit),
            }
        })
        .collect()
}

fn contract_gex(
    contract: &RawOptionContractSnapshot,
    spot: f64,
    calculated_at: UnixMs,
) -> Option<f64> {
    let years = years_to_expiry(contract.instrument.expiration_timestamp, calculated_at)?;
    let volatility = iv_percent_to_decimal(contract.market.mark_iv_percent)?;
    let gamma = black_scholes_gamma(
        spot,
        contract.instrument.strike,
        years,
        contract.market.interest_rate,
        volatility,
    )?;
    // Deribit option book summaries express open_interest in the underlying
    // currency already. Multiplying by contract_size again would double-scale
    // BTC/ETH exposure and is intentionally avoided.
    let gex = gamma * contract.market.open_interest_underlying * spot * spot * 0.01;
    (gex.is_finite() && gex >= 0.0).then_some(gex)
}

fn proxy_total_at_price(
    contracts: &[&RawOptionContractSnapshot],
    price: f64,
    now: UnixMs,
) -> Option<f64> {
    let mut total = 0.0;
    let mut valid = 0usize;
    for contract in contracts {
        let Some(gex) = contract_gex(contract, price, now) else {
            continue;
        };
        total += match contract.instrument.right {
            OptionRight::Call => gex,
            OptionRight::Put => -gex,
        };
        valid += 1;
    }
    (valid > 0 && total.is_finite()).then_some(total)
}

fn find_gamma_flip(
    contracts: &[&RawOptionContractSnapshot],
    spot: f64,
    now: UnixMs,
    range_percent: f64,
) -> Option<f64> {
    if contracts.is_empty() || !spot.is_finite() || spot <= 0.0 {
        return None;
    }
    let fraction = (range_percent.max(DEFAULT_FLIP_RANGE_PERCENT) / 100.0).min(0.95);
    let low = spot * (1.0 - fraction);
    let high = spot * (1.0 + fraction);
    let step = (high - low) / FLIP_SCAN_STEPS as f64;
    let mut crossings = Vec::new();
    let mut left_price = low;
    let mut left_value = proxy_total_at_price(contracts, left_price, now)?;

    for index in 1..=FLIP_SCAN_STEPS {
        let right_price = low + step * index as f64;
        let Some(right_value) = proxy_total_at_price(contracts, right_price, now) else {
            continue;
        };
        if left_value == 0.0 {
            crossings.push(left_price);
        } else if left_value.signum() != right_value.signum() {
            let mut a = left_price;
            let mut b = right_price;
            let mut fa = left_value;
            for _ in 0..FLIP_BISECTION_STEPS {
                let midpoint = (a + b) * 0.5;
                let Some(fm) = proxy_total_at_price(contracts, midpoint, now) else {
                    break;
                };
                if fm.abs() <= 1.0e-9 {
                    a = midpoint;
                    b = midpoint;
                    break;
                }
                if fa.signum() == fm.signum() {
                    a = midpoint;
                    fa = fm;
                } else {
                    b = midpoint;
                }
            }
            crossings.push((a + b) * 0.5);
        }
        left_price = right_price;
        left_value = right_value;
    }
    crossings.into_iter().min_by(|a, b| {
        (a - spot)
            .abs()
            .partial_cmp(&(b - spot).abs())
            .unwrap_or(Ordering::Equal)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange::options::{OptionInstrument, OptionMarketPoint};

    const NOW: UnixMs = UnixMs::new(1_700_000_000_000);

    fn contract(
        strike: f64,
        right: OptionRight,
        days: u64,
        oi: f64,
        contract_size: f64,
    ) -> RawOptionContractSnapshot {
        let name = format!("{strike:?}-{right:?}-{days}");
        RawOptionContractSnapshot {
            instrument: OptionInstrument {
                instrument_name: name.clone(),
                underlying: OptionsUnderlying::Btc,
                expiration_timestamp: NOW.saturating_add(days * MILLIS_PER_DAY),
                strike,
                right,
                contract_size,
            },
            market: OptionMarketPoint {
                instrument_name: name,
                open_interest_underlying: oi,
                mark_iv_percent: 50.0,
                underlying_price: 100.0,
                interest_rate: 0.01,
                observed_at: NOW,
            },
        }
    }

    fn chain(contracts: Vec<RawOptionContractSnapshot>) -> RawOptionChainSnapshot {
        RawOptionChainSnapshot {
            provider: OptionsProvider::Deribit,
            underlying: OptionsUnderlying::Btc,
            source_spot: 100.0,
            contracts: contracts.into(),
            observed_at: NOW,
        }
    }

    #[test]
    fn normal_distribution_and_known_gamma() {
        assert!((normal_pdf(0.0).expect("pdf") - 0.398_942_280_4).abs() < 1.0e-10);
        let gamma = black_scholes_gamma(100.0, 100.0, 1.0, 0.05, 0.2).expect("gamma");
        assert!((gamma - 0.018_762).abs() < 1.0e-6);
        assert!(black_scholes_gamma(0.0, 100.0, 1.0, 0.0, 0.2).is_none());
        assert!(black_scholes_gamma(100.0, 100.0, 1.0, 0.0, f64::NAN).is_none());
        assert_eq!(iv_percent_to_decimal(55.0), Some(0.55));
    }

    #[test]
    fn expiry_filters_use_real_timestamps() {
        let source = chain(vec![
            contract(90.0, OptionRight::Put, 1, 1.0, 1.0),
            contract(100.0, OptionRight::Call, 7, 1.0, 1.0),
            contract(110.0, OptionRight::Call, 30, 1.0, 1.0),
        ]);
        assert_eq!(
            select_contracts(&source, GexExpiryFilter::NextExpiry, NOW).len(),
            1
        );
        assert_eq!(
            select_contracts(&source, GexExpiryFilter::OneDay, NOW).len(),
            1
        );
        assert_eq!(
            select_contracts(&source, GexExpiryFilter::SevenDays, NOW).len(),
            2
        );
        assert_eq!(
            select_contracts(&source, GexExpiryFilter::ThirtyDays, NOW).len(),
            3
        );
    }

    #[test]
    fn aggregates_proxy_absolute_walls_and_thresholds() {
        let source = chain(vec![
            contract(90.0, OptionRight::Put, 7, 20.0, 1.0),
            contract(100.0, OptionRight::Call, 7, 30.0, 1.0),
            contract(110.0, OptionRight::Call, 7, 5.0, 1.0),
        ]);
        let snapshot = calculate_gex_at(&source, &Config::default(), NOW);
        assert_eq!(snapshot.strikes.len(), 3);
        assert_eq!(snapshot.call_wall, Some(100.0));
        assert_eq!(snapshot.put_wall, Some(90.0));
        assert!(snapshot.net_gex_1pct.is_some());
        assert!(snapshot.absolute_gex_1pct > 0.0);

        let absolute = calculate_gex_at(
            &source,
            &Config {
                sign_model: GexSignModel::AbsoluteGamma,
                ..Config::default()
            },
            NOW,
        );
        assert!(absolute.net_gex_1pct.is_none());
        assert!(absolute.gamma_flip.is_none());

        let filtered = calculate_gex_at(
            &source,
            &Config {
                min_open_interest: 10.0,
                ..Config::default()
            },
            NOW,
        );
        assert_eq!(filtered.strikes.len(), 2);
        let none = calculate_gex_at(
            &source,
            &Config {
                min_absolute_gex: f64::MAX,
                ..Config::default()
            },
            NOW,
        );
        assert!(none.strikes.is_empty());
    }

    #[test]
    fn deribit_oi_is_not_multiplied_by_contract_size() {
        let one = chain(vec![contract(100.0, OptionRight::Call, 7, 10.0, 1.0)]);
        let ten = chain(vec![contract(100.0, OptionRight::Call, 7, 10.0, 10.0)]);
        let a = calculate_gex_at(&one, &Config::default(), NOW);
        let b = calculate_gex_at(&ten, &Config::default(), NOW);
        assert_eq!(a.absolute_gex_1pct, b.absolute_gex_1pct);
    }

    #[test]
    fn expired_and_non_finite_contracts_are_excluded() {
        let mut expired = contract(100.0, OptionRight::Call, 1, 1.0, 1.0);
        expired.instrument.expiration_timestamp = NOW;
        let mut invalid = contract(110.0, OptionRight::Call, 1, 1.0, 1.0);
        invalid.market.mark_iv_percent = f64::INFINITY;
        let snapshot = calculate_gex_at(&chain(vec![expired, invalid]), &Config::default(), NOW);
        assert!(snapshot.strikes.is_empty());
    }

    #[test]
    fn gamma_flip_is_scanned_and_bisected() {
        let source = chain(vec![
            contract(80.0, OptionRight::Call, 7, 50.0, 1.0),
            contract(120.0, OptionRight::Put, 7, 50.0, 1.0),
        ]);
        let snapshot = calculate_gex_at(&source, &Config::default(), NOW);
        assert!(snapshot.gamma_flip.is_some());

        let no_crossing = chain(vec![contract(100.0, OptionRight::Call, 7, 50.0, 1.0)]);
        assert!(
            calculate_gex_at(&no_crossing, &Config::default(), NOW)
                .gamma_flip
                .is_none()
        );
    }

    #[test]
    fn multiple_gamma_flips_choose_crossing_nearest_spot() {
        let source = chain(vec![
            contract(75.0, OptionRight::Call, 7, 30.0, 1.0),
            contract(90.0, OptionRight::Put, 7, 30.0, 1.0),
            contract(110.0, OptionRight::Call, 7, 30.0, 1.0),
            contract(125.0, OptionRight::Put, 7, 30.0, 1.0),
        ]);
        let selected = select_contracts(&source, GexExpiryFilter::SevenDays, NOW);
        let mut crossings = Vec::new();
        let mut previous_price = 70.0;
        let mut previous = proxy_total_at_price(&selected, previous_price, NOW).expect("proxy");
        for price in 71..=130 {
            let price = f64::from(price);
            let value = proxy_total_at_price(&selected, price, NOW).expect("proxy");
            if previous.signum() != value.signum() {
                crossings.push((previous_price, price));
            }
            previous_price = price;
            previous = value;
        }
        assert!(crossings.len() >= 2);
        let flip = find_gamma_flip(&selected, source.source_spot, NOW, 30.0).expect("flip");
        let nearest = crossings
            .iter()
            .map(|(a, b)| (a + b) * 0.5)
            .min_by(|a, b| {
                (a - source.source_spot)
                    .abs()
                    .total_cmp(&(b - source.source_spot).abs())
            })
            .expect("crossing");
        assert!((flip - nearest).abs() <= 1.0);
    }

    #[test]
    fn incomplete_config_uses_defaults_and_unknown_fields_are_ignored() {
        let cfg: Config = serde_json::from_str(r#"{"price_range_percent":20,"future_field":true}"#)
            .expect("backwards compatible");
        assert_eq!(cfg.price_range_percent, 20.0);
        assert_eq!(cfg.max_visible_strikes, 40);
        assert!(cfg.show_header_net_gex);
        assert!(cfg.show_header_gamma_flip);
        assert!(cfg.show_header_expiry);
        assert!(cfg.show_header_freshness);
        assert!(cfg.show_header_model);
        assert!(!cfg.show_header_absolute_gex);
        assert!(!cfg.show_header_call_wall);
        assert!(!cfg.show_header_put_wall);
        assert!(!cfg.show_header_snapshot);
    }

    #[test]
    fn legacy_levels_config_loads_and_migrates_old_cluster_default() {
        let mut legacy: GexLevelsConfig = serde_json::from_str(
            r#"{
                "clusters_as_bands": false,
                "show_value": false,
                "show_distance_percent": false,
                "cluster_color": "Secondary"
            }"#,
        )
        .expect("legacy levels config");
        legacy.migrate_legacy_defaults();
        assert_eq!(legacy.cluster_color, GexLevelColor::Primary);
        assert_eq!(legacy.horizontal_span_percent, 35.0);

        let mut customized = GexLevelsConfig {
            cluster_color: GexLevelColor::Secondary,
            cluster_color_customized: true,
            ..GexLevelsConfig::default()
        };
        customized.migrate_legacy_defaults();
        assert_eq!(customized.cluster_color, GexLevelColor::Secondary);
    }
}
