//! Core types for identifying market data streams.
//!
//! `MarketDataKey` uniquely identifies a market data stream by venue, symbol,
//! market type, and data kind. This is the primary key for coverage tracking,
//! caching, and deduplication.

use exchange::Timeframe;

/// Unique identifier for a market data stream.
///
/// Used as the primary key for coverage tracking, caching, and deduplication
/// across all consumers (charts, indicators, derived data engines).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketDataKey {
    /// Exchange venue (e.g., BinanceLinear, BinanceSpot)
    pub venue: Venue,
    /// Trading symbol (e.g., BTCUSDT)
    pub symbol: Symbol,
    /// Market type (Spot, LinearPerps, InversePerps)
    pub market_type: MarketKind,
    /// The kind of data (Trades, Klines, OpenInterest)
    pub kind: MarketDataKind,
}

/// Exchange venue identifier.
///
/// Wraps the exchange adapter's `Exchange` enum but provides a stable
/// identifier for caching and persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Venue {
    BinanceSpot,
    BinanceLinear,
    BinanceInverse,
    BybitLinear,
    BybitInverse,
    HyperliquidLinear,
    MexcSpot,
    MexcLinear,
    MexcInverse,
    OkexSpot,
    OkexLinear,
}

impl Venue {
    /// Create from exchange adapter's Exchange type.
    pub fn from_exchange(exchange: exchange::adapter::Exchange) -> Option<Self> {
        use exchange::adapter::Exchange;
        match exchange {
            Exchange::BinanceSpot => Some(Venue::BinanceSpot),
            Exchange::BinanceLinear => Some(Venue::BinanceLinear),
            Exchange::BinanceInverse => Some(Venue::BinanceInverse),
            Exchange::BybitLinear => Some(Venue::BybitLinear),
            Exchange::BybitInverse => Some(Venue::BybitInverse),
            Exchange::HyperliquidLinear => Some(Venue::HyperliquidLinear),
            Exchange::MexcSpot => Some(Venue::MexcSpot),
            Exchange::MexcLinear => Some(Venue::MexcLinear),
            Exchange::MexcInverse => Some(Venue::MexcInverse),
            Exchange::OkexSpot => Some(Venue::OkexSpot),
            Exchange::OkexLinear => Some(Venue::OkexLinear),
            _ => None,
        }
    }

    /// Convert back to exchange adapter's Exchange type.
    pub fn to_exchange(self) -> exchange::adapter::Exchange {
        use exchange::adapter::Exchange;
        match self {
            Venue::BinanceSpot => Exchange::BinanceSpot,
            Venue::BinanceLinear => Exchange::BinanceLinear,
            Venue::BinanceInverse => Exchange::BinanceInverse,
            Venue::BybitLinear => Exchange::BybitLinear,
            Venue::BybitInverse => Exchange::BybitInverse,
            Venue::HyperliquidLinear => Exchange::HyperliquidLinear,
            Venue::MexcSpot => Exchange::MexcSpot,
            Venue::MexcLinear => Exchange::MexcLinear,
            Venue::MexcInverse => Exchange::MexcInverse,
            Venue::OkexSpot => Exchange::OkexSpot,
            Venue::OkexLinear => Exchange::OkexLinear,
        }
    }

    /// Display name for logging (e.g., "BinanceLinear")
    pub fn display_name(&self) -> &'static str {
        match self {
            Venue::BinanceSpot => "BinanceSpot",
            Venue::BinanceLinear => "BinanceLinear",
            Venue::BinanceInverse => "BinanceInverse",
            Venue::BybitLinear => "BybitLinear",
            Venue::BybitInverse => "BybitInverse",
            Venue::HyperliquidLinear => "HyperliquidLinear",
            Venue::MexcSpot => "MexcSpot",
            Venue::MexcLinear => "MexcLinear",
            Venue::MexcInverse => "MexcInverse",
            Venue::OkexSpot => "OkexSpot",
            Venue::OkexLinear => "OkexLinear",
        }
    }
}

impl std::fmt::Display for Venue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.display_name())
    }
}

/// Trading symbol identifier.
///
/// Wraps the raw symbol string (e.g., "BTCUSDT") for type safety.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Symbol(pub String);

impl Symbol {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for Symbol {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for Symbol {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Market type (Spot, Linear perpetuals, Inverse perpetuals).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketKind {
    Spot,
    LinearPerps,
    InversePerps,
}

impl MarketKind {
    pub fn from_adapter(kind: exchange::adapter::MarketKind) -> Self {
        match kind {
            exchange::adapter::MarketKind::Spot => MarketKind::Spot,
            exchange::adapter::MarketKind::LinearPerps => MarketKind::LinearPerps,
            exchange::adapter::MarketKind::InversePerps => MarketKind::InversePerps,
        }
    }

    pub fn to_adapter(self) -> exchange::adapter::MarketKind {
        match self {
            MarketKind::Spot => exchange::adapter::MarketKind::Spot,
            MarketKind::LinearPerps => exchange::adapter::MarketKind::LinearPerps,
            MarketKind::InversePerps => exchange::adapter::MarketKind::InversePerps,
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            MarketKind::Spot => "Spot",
            MarketKind::LinearPerps => "Linear",
            MarketKind::InversePerps => "Inverse",
        }
    }
}

impl std::fmt::Display for MarketKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.display_name())
    }
}

/// The kind of market data.
///
/// Identifies what type of data a stream provides: raw trades, klines (OHLCV),
/// or open interest.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MarketDataKind {
    /// Raw trade stream
    Trades,
    /// Kline (OHLCV candlestick) stream
    Klines { timeframe: Timeframe },
    /// Open interest stream
    OpenInterest { timeframe: Timeframe },
}

impl MarketDataKind {
    /// Display name for logging (e.g., "Trades", "Klines:M5")
    pub fn display_name(&self) -> String {
        match self {
            MarketDataKind::Trades => "Trades".to_string(),
            MarketDataKind::Klines { timeframe } => format!("Klines:{timeframe}"),
            MarketDataKind::OpenInterest { timeframe } => format!("OI:{timeframe}"),
        }
    }

    /// Check if this is a trade stream
    pub fn is_trades(&self) -> bool {
        matches!(self, MarketDataKind::Trades)
    }

    /// Check if this is a kline stream
    pub fn is_klines(&self) -> bool {
        matches!(self, MarketDataKind::Klines { .. })
    }

    /// Check if this is an open interest stream
    pub fn is_open_interest(&self) -> bool {
        matches!(self, MarketDataKind::OpenInterest { .. })
    }

    /// Get the timeframe if this is a kline or OI stream
    pub fn timeframe(&self) -> Option<Timeframe> {
        match self {
            MarketDataKind::Klines { timeframe } | MarketDataKind::OpenInterest { timeframe } => {
                Some(*timeframe)
            }
            MarketDataKind::Trades => None,
        }
    }

    /// Get the timeframe in milliseconds for Kline/OI kinds.
    pub fn timeframe_ms(&self) -> Option<u64> {
        match self {
            MarketDataKind::Klines { timeframe } | MarketDataKind::OpenInterest { timeframe } => {
                Some(timeframe.to_milliseconds())
            }
            MarketDataKind::Trades => None,
        }
    }
}

impl std::fmt::Display for MarketDataKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.display_name())
    }
}

impl MarketDataKey {
    /// Create a new key for trade data.
    pub fn trades(venue: Venue, symbol: Symbol, market_type: MarketKind) -> Self {
        Self {
            venue,
            symbol,
            market_type,
            kind: MarketDataKind::Trades,
        }
    }

    /// Create a new key for kline data.
    pub fn klines(
        venue: Venue,
        symbol: Symbol,
        market_type: MarketKind,
        timeframe: Timeframe,
    ) -> Self {
        Self {
            venue,
            symbol,
            market_type,
            kind: MarketDataKind::Klines { timeframe },
        }
    }

    /// Create a new key for open interest data.
    pub fn open_interest(
        venue: Venue,
        symbol: Symbol,
        market_type: MarketKind,
        timeframe: Timeframe,
    ) -> Self {
        Self {
            venue,
            symbol,
            market_type,
            kind: MarketDataKind::OpenInterest { timeframe },
        }
    }

    /// Create from a TickerInfo and MarketDataKind.
    pub fn from_ticker_info(
        ticker_info: &exchange::TickerInfo,
        kind: MarketDataKind,
    ) -> Option<Self> {
        let venue = Venue::from_exchange(ticker_info.exchange())?;
        let symbol = Symbol::new(
            ticker_info
                .ticker
                .display_symbol()
                .unwrap_or(&ticker_info.ticker.to_string()),
        );
        let market_type = MarketKind::from_adapter(ticker_info.ticker.market_type());

        Some(Self {
            venue,
            symbol,
            market_type,
            kind,
        })
    }

    /// Reconstruct a key from display string parts (for deserialization).
    pub fn from_display_parts(
        venue: &str,
        symbol: &str,
        market_type: &str,
        kind: &str,
    ) -> Option<Self> {
        let venue = match venue {
            "BinanceSpot" => Venue::BinanceSpot,
            "BinanceLinear" => Venue::BinanceLinear,
            "BinanceInverse" => Venue::BinanceInverse,
            "BybitLinear" => Venue::BybitLinear,
            "BybitInverse" => Venue::BybitInverse,
            "HyperliquidLinear" => Venue::HyperliquidLinear,
            "MexcSpot" => Venue::MexcSpot,
            "MexcLinear" => Venue::MexcLinear,
            "MexcInverse" => Venue::MexcInverse,
            "OkexSpot" => Venue::OkexSpot,
            "OkexLinear" => Venue::OkexLinear,
            _ => return None,
        };

        let market_type = match market_type {
            "Spot" => MarketKind::Spot,
            "Linear" => MarketKind::LinearPerps,
            "Inverse" => MarketKind::InversePerps,
            _ => return None,
        };

        let kind = if kind == "Trades" {
            MarketDataKind::Trades
        } else if let Some(tf_str) = kind.strip_prefix("Klines:") {
            let timeframe = parse_timeframe(tf_str)?;
            MarketDataKind::Klines { timeframe }
        } else if let Some(tf_str) = kind.strip_prefix("OI:") {
            let timeframe = parse_timeframe(tf_str)?;
            MarketDataKind::OpenInterest { timeframe }
        } else {
            return None;
        };

        Some(Self {
            venue,
            symbol: Symbol::new(symbol),
            market_type,
            kind,
        })
    }

    /// Display format for logging: "Trades:BinanceLinear:BTCUSDT"
    pub fn display_key(&self) -> String {
        format!("{}:{}:{}", self.kind, self.venue, self.symbol)
    }
}

impl std::fmt::Display for MarketDataKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.kind, self.venue, self.symbol)
    }
}

/// Parse a timeframe string (e.g., "5m", "1h") to `Timeframe`.
fn parse_timeframe(s: &str) -> Option<Timeframe> {
    match s {
        "100ms" => Some(Timeframe::MS100),
        "200ms" => Some(Timeframe::MS200),
        "300ms" => Some(Timeframe::MS300),
        "500ms" => Some(Timeframe::MS500),
        "1s" => Some(Timeframe::MS1000),
        "1m" => Some(Timeframe::M1),
        "3m" => Some(Timeframe::M3),
        "5m" => Some(Timeframe::M5),
        "15m" => Some(Timeframe::M15),
        "30m" => Some(Timeframe::M30),
        "1h" => Some(Timeframe::H1),
        "2h" => Some(Timeframe::H2),
        "4h" => Some(Timeframe::H4),
        "12h" => Some(Timeframe::H12),
        "1d" => Some(Timeframe::D1),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_market_data_key_display() {
        let key = MarketDataKey::trades(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
        );
        assert_eq!(key.to_string(), "Trades:BinanceLinear:BTCUSDT");

        let key = MarketDataKey::klines(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
            Timeframe::M5,
        );
        assert_eq!(key.to_string(), "Klines:5m:BinanceLinear:BTCUSDT");
    }

    #[test]
    fn test_market_data_kind_timeframe() {
        let trades = MarketDataKind::Trades;
        assert!(trades.timeframe().is_none());
        assert!(trades.is_trades());
        assert!(!trades.is_klines());

        let klines = MarketDataKind::Klines {
            timeframe: Timeframe::M15,
        };
        assert_eq!(klines.timeframe(), Some(Timeframe::M15));
        assert!(!klines.is_trades());
        assert!(klines.is_klines());
    }

    #[test]
    fn test_venue_from_exchange() {
        use exchange::adapter::Exchange;
        assert_eq!(
            Venue::from_exchange(Exchange::BinanceLinear),
            Some(Venue::BinanceLinear)
        );
    }
}
