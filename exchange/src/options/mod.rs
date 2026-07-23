//! Public options analytics providers.
//!
//! Options providers are deliberately separate from chartable exchanges: they
//! do not participate in normal trade, depth, or kline streaming.

pub mod deribit;

use crate::{Ticker, UnixMs};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum OptionsProvider {
    Deribit,
}

impl std::fmt::Display for OptionsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Deribit => f.write_str("Deribit"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum OptionsUnderlying {
    Btc,
    Eth,
}

impl OptionsUnderlying {
    pub const ALL: [Self; 2] = [Self::Btc, Self::Eth];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Btc => "BTC",
            Self::Eth => "ETH",
        }
    }
}

impl std::fmt::Display for OptionsUnderlying {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum OptionRight {
    Call,
    Put,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct OptionInstrument {
    pub instrument_name: String,
    pub underlying: OptionsUnderlying,
    pub expiration_timestamp: UnixMs,
    pub strike: f64,
    pub right: OptionRight,
    pub contract_size: f64,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct OptionMarketPoint {
    pub instrument_name: String,
    pub open_interest_underlying: f64,
    pub mark_iv_percent: f64,
    pub underlying_price: f64,
    pub interest_rate: f64,
    pub observed_at: UnixMs,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct RawOptionContractSnapshot {
    pub instrument: OptionInstrument,
    pub market: OptionMarketPoint,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct RawOptionChainSnapshot {
    pub provider: OptionsProvider,
    pub underlying: OptionsUnderlying,
    pub source_spot: f64,
    pub contracts: Arc<[RawOptionContractSnapshot]>,
    pub observed_at: UnixMs,
}

/// Resolve a normal FlowSurface market ticker to a supported options
/// underlying. Only explicit base/quote combinations are accepted.
pub fn resolve_options_underlying(ticker: Ticker) -> Option<OptionsUnderlying> {
    let (symbol, _) = ticker.display_symbol_and_type();
    resolve_symbol(&symbol)
}

fn resolve_symbol(symbol: &str) -> Option<OptionsUnderlying> {
    let normalized = symbol.to_ascii_uppercase();
    const BTC: &[&str] = &["BTCUSD", "BTCUSDT", "BTCUSDC"];
    const ETH: &[&str] = &["ETHUSD", "ETHUSDT", "ETHUSDC"];

    let without_known_suffix = normalized
        .strip_suffix("-PERP")
        .or_else(|| normalized.strip_suffix("_PERP"))
        .or_else(|| normalized.strip_suffix("PERP"))
        .unwrap_or(&normalized);

    if BTC.contains(&without_known_suffix) {
        Some(OptionsUnderlying::Btc)
    } else if ETH.contains(&without_known_suffix) {
        Some(OptionsUnderlying::Eth)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::Exchange;

    fn ticker(symbol: &str, exchange: Exchange) -> Ticker {
        Ticker::new(symbol, exchange)
    }

    #[test]
    fn strict_underlying_resolver() {
        assert_eq!(
            resolve_options_underlying(ticker("BTCUSDT", Exchange::BinanceLinear)),
            Some(OptionsUnderlying::Btc)
        );
        assert_eq!(
            resolve_options_underlying(ticker("BTCUSD", Exchange::BinanceInverse)),
            Some(OptionsUnderlying::Btc)
        );
        assert_eq!(
            resolve_options_underlying(ticker("ETHUSDT", Exchange::BybitLinear)),
            Some(OptionsUnderlying::Eth)
        );
        assert_eq!(
            resolve_options_underlying(ticker("ETHUSDC", Exchange::HyperliquidSpot)),
            Some(OptionsUnderlying::Eth)
        );
        assert_eq!(
            resolve_options_underlying(ticker("SOLUSDT", Exchange::BinanceLinear)),
            None
        );
        assert_eq!(
            resolve_options_underlying(ticker("WBTCUSDT", Exchange::BinanceSpot)),
            None
        );
        assert_eq!(
            resolve_options_underlying(ticker("1000BTCUSDT", Exchange::BinanceLinear)),
            None
        );
        assert_eq!(
            resolve_options_underlying(ticker("BTC2LUSDT", Exchange::BinanceSpot)),
            None
        );
    }
}
