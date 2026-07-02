//! Market service abstraction for decoupling market data from UI rendering.
//!
//! The market data pipeline must continue even if the UI/event loop/redraw
//! is delayed. This module provides traits and structures to support that.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Shared state between market data backend and UI frontend.
///
/// The backend writes to this store; the UI reads from it.
/// This allows the backend to continue updating even when the UI
/// is lagging or not rendering.
#[derive(Debug)]
pub struct MarketStore {
    /// Last time a WS event was received from the exchange.
    last_ws_event_at: AtomicU64,
    /// Number of market events queued but not yet consumed by UI.
    queued_events: AtomicU64,
    /// Whether the UI is currently focused/visible.
    ui_focused: AtomicBool,
    /// Whether market streams are currently connected.
    streams_connected: AtomicBool,
}

impl MarketStore {
    pub fn new() -> Self {
        Self {
            last_ws_event_at: AtomicU64::new(0),
            queued_events: AtomicU64::new(0),
            ui_focused: AtomicBool::new(true),
            streams_connected: AtomicBool::new(false),
        }
    }

    /// Record that a WS event was received.
    pub fn record_ws_event(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_ws_event_at.store(now, Ordering::Relaxed);
    }

    /// Get the timestamp of the last WS event.
    pub fn last_ws_event_at(&self) -> u64 {
        self.last_ws_event_at.load(Ordering::Relaxed)
    }

    /// Increment the queued event count.
    pub fn enqueue_event(&self) {
        self.queued_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Reset the queued event count (called when UI consumes events).
    pub fn drain_events(&self) -> u64 {
        self.queued_events.swap(0, Ordering::Relaxed)
    }

    /// Get the current queued event count.
    pub fn queued_events(&self) -> u64 {
        self.queued_events.load(Ordering::Relaxed)
    }

    /// Set whether the UI is focused.
    pub fn set_ui_focused(&self, focused: bool) {
        self.ui_focused.store(focused, Ordering::Relaxed);
    }

    /// Check if the UI is focused.
    pub fn is_ui_focused(&self) -> bool {
        self.ui_focused.load(Ordering::Relaxed)
    }

    /// Set whether streams are connected.
    pub fn set_streams_connected(&self, connected: bool) {
        self.streams_connected.store(connected, Ordering::Relaxed);
    }

    /// Check if streams are connected.
    pub fn is_streams_connected(&self) -> bool {
        self.streams_connected.load(Ordering::Relaxed)
    }
}

impl Default for MarketStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Diagnostics for the market service.
pub struct MarketDiagnostics {
    store: Arc<MarketStore>,
    last_diagnostic_log: Instant,
    diagnostic_interval: std::time::Duration,
}

impl MarketDiagnostics {
    pub fn new(store: Arc<MarketStore>) -> Self {
        Self {
            store,
            last_diagnostic_log: Instant::now(),
            diagnostic_interval: std::time::Duration::from_secs(30),
        }
    }

    /// Log diagnostics if enough time has elapsed.
    /// Returns true if diagnostics were logged.
    pub fn maybe_log(&mut self) -> bool {
        if self.last_diagnostic_log.elapsed() < self.diagnostic_interval {
            return false;
        }

        let last_ws = self.store.last_ws_event_at();
        let queued = self.store.queued_events();
        let focused = self.store.is_ui_focused();
        let connected = self.store.is_streams_connected();

        log::info!(
            "MARKET BackendAlive | last_ws_event={} queued_events={queued} ui_focused={focused} streams_connected={connected}",
            if last_ws > 0 {
                crate::connector::fetcher::format_time_short(exchange::UnixMs::new(last_ws))
            } else {
                "-".to_string()
            }
        );

        self.last_diagnostic_log = Instant::now();
        true
    }

    /// Log UI lag diagnostics when the UI consumes events after a delay.
    pub fn log_ui_lag(&self, store_latest: u64, ui_latest: u64) {
        if ui_latest < store_latest {
            log::debug!(
                "MARKET UiLag | queued_events={} store_latest={store_latest} ui_latest={ui_latest}",
                self.store.queued_events()
            );
        }
    }
}
