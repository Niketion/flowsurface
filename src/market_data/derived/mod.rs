//! Derived data computation from raw market data.
//!
//! Contains standalone computation functions (not trait-based engines)
//! that derive higher-level data from raw trades.
//!
//! Currently provides:
//! - [`bubbles::compute_bubble_summaries`]: Volume bubble aggregation from raw trades

pub mod bubbles;
