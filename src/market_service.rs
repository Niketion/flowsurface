//! Market service abstraction for decoupling market data from UI rendering.
//!
//! The market data pipeline must continue even if the UI/event loop/redraw
//! is delayed. This module provides traits and structures to support that.

use exchange::adapter::StreamKind;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Avoids flashing the blocking overlay for very short network hiccups while
/// still making a real disconnection visible quickly.
const OFFLINE_GRACE: Duration = Duration::from_millis(1_500);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectivityPhase {
    Connecting,
    Online,
    Offline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectivityTransition {
    None,
    WentOffline,
    Restored,
}

/// Aggregates all independently managed WebSocket subscriptions into one
/// application-level connection state.
///
/// FlowSurface commonly has multiple connections (depth, trades and klines,
/// potentially across several venues). A single `Connected` event must not
/// hide the offline overlay while the other required streams are still down.
#[derive(Debug)]
pub struct MarketConnectivity {
    expected: HashSet<StreamKind>,
    connected: HashSet<StreamKind>,
    phase: ConnectivityPhase,
    incomplete_since: Option<Instant>,
    last_reason: Option<String>,
    had_online_disconnect: bool,
}

impl MarketConnectivity {
    pub fn new() -> Self {
        Self {
            expected: HashSet::new(),
            connected: HashSet::new(),
            phase: ConnectivityPhase::Connecting,
            incomplete_since: None,
            last_reason: None,
            had_online_disconnect: false,
        }
    }

    /// Keeps the tracker aligned with the streams required by the active
    /// dashboard. Removed streams cannot keep the whole application offline.
    pub fn sync_expected(
        &mut self,
        streams: &[StreamKind],
        now: Instant,
    ) -> ConnectivityTransition {
        self.expected = streams.iter().copied().collect();
        self.connected
            .retain(|stream| self.expected.contains(stream));
        self.evaluate(now)
    }

    pub fn record_connected(
        &mut self,
        streams: &[StreamKind],
        now: Instant,
    ) -> ConnectivityTransition {
        // Events can arrive before the next dashboard/tick synchronization.
        // Preserve those streams as expected so startup state remains useful.
        self.expected.extend(streams.iter().copied());
        self.connected.extend(streams.iter().copied());
        self.evaluate(now)
    }

    pub fn record_disconnected(
        &mut self,
        streams: &[StreamKind],
        reason: String,
        now: Instant,
    ) -> ConnectivityTransition {
        if self.phase == ConnectivityPhase::Online {
            self.had_online_disconnect = true;
        }
        self.expected.extend(streams.iter().copied());
        for stream in streams {
            self.connected.remove(stream);
        }
        self.last_reason = Some(reason);
        self.evaluate(now)
    }

    /// Advances the grace timer even when no new WS event is received.
    pub fn tick(&mut self, now: Instant) -> ConnectivityTransition {
        self.evaluate(now)
    }

    pub fn is_online(&self) -> bool {
        self.phase == ConnectivityPhase::Online
    }

    pub fn overlay_visible(&self) -> bool {
        self.phase == ConnectivityPhase::Offline
    }

    pub fn connected_count(&self) -> usize {
        self.connected
            .iter()
            .filter(|stream| self.expected.contains(stream))
            .count()
    }

    pub fn expected_count(&self) -> usize {
        self.expected.len()
    }

    pub fn last_reason(&self) -> Option<&str> {
        self.last_reason.as_deref()
    }

    fn evaluate(&mut self, now: Instant) -> ConnectivityTransition {
        let complete = self.expected.is_empty() || self.expected.is_subset(&self.connected);

        if complete {
            self.incomplete_since = None;
            let restored = self.phase == ConnectivityPhase::Offline || self.had_online_disconnect;
            self.phase = ConnectivityPhase::Online;
            if restored {
                self.had_online_disconnect = false;
                self.last_reason = None;
                ConnectivityTransition::Restored
            } else {
                ConnectivityTransition::None
            }
        } else {
            let since = *self.incomplete_since.get_or_insert(now);
            let grace_elapsed = now
                .checked_duration_since(since)
                .is_some_and(|elapsed| elapsed >= OFFLINE_GRACE);

            if self.phase != ConnectivityPhase::Offline && grace_elapsed {
                self.phase = ConnectivityPhase::Offline;
                ConnectivityTransition::WentOffline
            } else {
                if self.phase != ConnectivityPhase::Offline {
                    self.phase = ConnectivityPhase::Connecting;
                }
                ConnectivityTransition::None
            }
        }
    }
}

impl Default for MarketConnectivity {
    fn default() -> Self {
        Self::new()
    }
}

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
