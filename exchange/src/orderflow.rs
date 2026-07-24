//! Venue-normalized, UI-independent order-flow events.

use crate::{Price, TickerInfo, UnixMs, unit::Qty};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggressorSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PassiveSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BookContinuity {
    Continuous,
    SnapshotBoundary,
    GapDetected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderFlowDataQuality {
    Synchronizing,
    Healthy,
    Degraded,
    Gap,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BookLevelDelta {
    pub price: Price,
    pub previous_qty: Qty,
    pub current_qty: Qty,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BookDeltaEvent {
    pub ticker_info: TickerInfo,
    pub exchange_time: UnixMs,
    pub transaction_time: Option<UnixMs>,
    pub receive_time: UnixMs,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub previous_final_update_id: Option<u64>,
    pub bids: Box<[BookLevelDelta]>,
    pub asks: Box<[BookLevelDelta]>,
    pub continuity: BookContinuity,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct NormalizedTradeEvent {
    pub ticker_info: TickerInfo,
    pub event_time: UnixMs,
    pub trade_time: UnixMs,
    pub receive_time: UnixMs,
    pub trade_id: u64,
    pub price: Price,
    pub quantity: Qty,
    pub aggressor: AggressorSide,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderFlowEvent {
    Trade(NormalizedTradeEvent),
    BookDelta(BookDeltaEvent),
    Quality {
        ticker_info: TickerInfo,
        at: UnixMs,
        quality: OrderFlowDataQuality,
    },
    Reconnect {
        ticker_info: TickerInfo,
        at: UnixMs,
    },
}

impl OrderFlowEvent {
    pub fn ticker_info(&self) -> TickerInfo {
        match self {
            Self::Trade(event) => event.ticker_info,
            Self::BookDelta(event) => event.ticker_info,
            Self::Quality { ticker_info, .. } | Self::Reconnect { ticker_info, .. } => *ticker_info,
        }
    }
}
