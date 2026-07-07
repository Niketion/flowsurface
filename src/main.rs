#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod audio;
mod chart;
mod connector;
mod layout;
mod logger;
mod market_data;
mod market_service;
mod modal;
mod notify;
mod power_guard;
mod render_scheduler;
mod screen;
mod style;
mod version;
mod widget;
mod window;
mod windowing;

use data::config::theme::default_theme;
use data::{layout::WindowSpec, sidebar};
use layout::{LayoutId, configuration};
use modal::{
    LayoutManager, ThemeEditor,
    audio::AudioStream,
    network_manager::{self, NetworkManager},
};
use modal::{dashboard_modal, main_dialog_modal};
use notify::Notifications;
use screen::dashboard::{self, Dashboard};
use widget::{
    confirm_dialog_container,
    toast::{self, Toast},
    tooltip,
};

use iced::{
    Alignment, Element, Length, Subscription, Task, keyboard, padding,
    widget::{
        button, column, container, pane_grid, pick_list, row, rule, scrollable, text, text_input,
        tooltip::Position as TooltipPosition,
    },
};
use std::{borrow::Cow, collections::HashMap, path::PathBuf, sync::Arc, time::Duration, vec};
use windowing::WindowingMode;

/// Set to `true` to emit window focus/unfocus and tick diagnostic logs.
/// These are useful for debugging multi-window issues but noisy in normal use.
const DEBUG_WINDOW_DIAGNOSTICS: bool = false;

const DEBUG_TERMINAL_VSCROLL_ID: &str = "debug-terminal-vscroll";
const DEBUG_TERMINAL_HSCROLL_ID: &str = "debug-terminal-hscroll";

fn main() {
    logger::install_panic_hook();

    if let Err(err) = logger::setup(cfg!(debug_assertions)) {
        logger::report_stderr(&format!("Failed to initialize logger: {err}"));
    }

    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    log::info!(
        "BUILD Info | git_sha={} branch={} profile={}",
        version::BUILD_GIT_SHA.unwrap_or("unknown"),
        version::BUILD_GIT_BRANCH.unwrap_or("unknown"),
        profile
    );

    std::thread::spawn(data::cleanup_old_market_data);

    let daemon = iced::daemon(Flowsurface::new, Flowsurface::update, Flowsurface::view)
        .settings(iced::Settings {
            antialiasing: true,
            fonts: vec![
                Cow::Borrowed(style::AZERET_MONO_BYTES),
                Cow::Borrowed(style::ICONS_BYTES),
            ],
            default_text_size: style::text_size::BODY.into(),
            ..Default::default()
        })
        .title(Flowsurface::title)
        .theme(Flowsurface::theme)
        .scale_factor(Flowsurface::scale_factor)
        .subscription(Flowsurface::subscription);

    if let Err(err) = daemon.run() {
        let message = format!("Runtime error: {err}");
        log::error!("{message}");
        logger::report_stderr(&message);
    }
}

struct Flowsurface {
    main_window: window::Window,
    sidebar: dashboard::Sidebar,
    handles: exchange::adapter::AdapterHandles,
    layout_manager: LayoutManager,
    theme_editor: ThemeEditor,
    network: NetworkManager,
    audio_stream: AudioStream,
    confirm_dialog: Option<screen::ConfirmDialog<Message>>,
    startup_warning: Option<StartupWarning>,
    save_state_enabled: bool,
    volume_size_unit: exchange::SizeUnit,
    ui_scale_factor: data::ScaleFactor,
    timezone: data::UserTimezone,
    theme: data::Theme,
    notifications: Notifications,
    windowing_mode: WindowingMode,
    market_store: Arc<market_service::MarketStore>,
    market_diagnostics: market_service::MarketDiagnostics,
    dirty_flag: render_scheduler::DirtyFlag,
    debug_terminal_enabled: bool,
    debug_terminal_window: Option<window::Id>,
    debug_terminal_embedded: bool,
    debug_terminal_logs: Vec<String>,
    debug_terminal_level_filter: DebugLevelFilter,
    debug_terminal_category_filter: DebugLogCategory,
    debug_terminal_search: String,
    debug_terminal_auto_scroll: bool,
    debug_terminal_app_only: bool,
    debug_terminal_compact_mode: bool,
}

#[derive(Debug, Clone)]
enum StartupWarning {
    SavedStateCorrupt {
        error: String,
        original_path: PathBuf,
        backup_path: Option<PathBuf>,
    },
    SavedStateRecovered {
        warnings: Vec<String>,
        backup_path: Option<PathBuf>,
    },
    SavedStateMigrated {
        from_version: u32,
        to_version: u32,
        backup_path: Option<PathBuf>,
    },
}

#[derive(Debug, Clone)]
enum Message {
    Sidebar(dashboard::sidebar::Message),
    MarketWsEvent(exchange::Event),
    Dashboard {
        /// If `None`, the active layout is used for the event.
        layout_id: Option<uuid::Uuid>,
        event: dashboard::Message,
    },
    Tick(std::time::Instant),
    WindowEvent(window::Event),
    ExitRequested(HashMap<window::Id, WindowSpec>),
    RestartRequested(Option<HashMap<window::Id, WindowSpec>>),
    SaveStateRequested(HashMap<window::Id, WindowSpec>),
    GoBack,
    DataFolderRequested,
    OpenUrlRequested(Cow<'static, str>),
    ThemeSelected(iced_core::Theme),
    ScaleFactorChanged(data::ScaleFactor),
    SetTimezone(data::UserTimezone),
    ToggleTradeFetch(bool),
    ToggleDebugTerminal(bool),
    DebugTerminalOpened(window::Id),
    DebugTerminalRefresh,
    DebugTerminalClear,
    DebugTerminalCopyAll,
    DebugTerminalCopyVisible,
    DebugTerminalSearchChanged(String),
    DebugTerminalToggleLevel(DebugLogLevel, bool),
    DebugTerminalToggleAutoScroll(bool),
    DebugTerminalCategoryFilterChanged(DebugLogCategory),
    DebugTerminalToggleAppOnly(bool),
    DebugTerminalToggleCompactMode(bool),
    ApplyVolumeSizeUnit(exchange::SizeUnit),
    RemoveNotification(usize),
    StartupContinueWithDefault,
    StartupExitWithoutOverwrite,
    StartupWarningNoop,
    ToggleDialogModal(Option<screen::ConfirmDialog<Message>>),
    ThemeEditor(modal::theme_editor::Message),
    NetworkManager(modal::network_manager::Message),
    Layouts(modal::layout_manager::Message),
    AudioStream(modal::audio::Message),
}

/// Multi-level filter for the Debug Terminal.
/// Each level can be independently enabled/disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DebugLevelFilter {
    error: bool,
    warn: bool,
    info: bool,
    debug: bool,
    trace: bool,
}

impl DebugLevelFilter {
    /// Default levels: ERROR, WARN, INFO enabled; DEBUG, TRACE disabled.
    const DEFAULT: Self = Self {
        error: true,
        warn: true,
        info: true,
        debug: false,
        trace: false,
    };

    fn matches(self, line: &str) -> bool {
        let level = debug_line_level(line);
        match level {
            Some(DebugLogLevel::Error) => self.error,
            Some(DebugLogLevel::Warn) => self.warn,
            Some(DebugLogLevel::Info) => self.info,
            Some(DebugLogLevel::Debug) => self.debug,
            Some(DebugLogLevel::Trace) => self.trace,
            // Unknown-level logs show when INFO is enabled (simpler than an extra toggle).
            None => self.info,
        }
    }

    fn toggle(&mut self, level: DebugLogLevel, enabled: bool) {
        match level {
            DebugLogLevel::Error => self.error = enabled,
            DebugLogLevel::Warn => self.warn = enabled,
            DebugLogLevel::Info => self.info = enabled,
            DebugLogLevel::Debug => self.debug = enabled,
            DebugLogLevel::Trace => self.trace = enabled,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DebugLogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

fn debug_line_level(line: &str) -> Option<DebugLogLevel> {
    let level_start = line.find("] [")? + 3;
    let level_end = line[level_start..].find(']')? + level_start;

    match line[level_start..level_end].trim() {
        "ERROR" | "FATAL" => Some(DebugLogLevel::Error),
        "WARN" => Some(DebugLogLevel::Warn),
        "INFO" => Some(DebugLogLevel::Info),
        "DEBUG" => Some(DebugLogLevel::Debug),
        "TRACE" => Some(DebugLogLevel::Trace),
        _ => None,
    }
}

fn debug_log_text_style(
    level: Option<DebugLogLevel>,
) -> impl Fn(&iced::Theme) -> iced::widget::text::Style {
    move |theme| {
        let palette = theme.extended_palette();
        let color = match level {
            Some(DebugLogLevel::Error) => Some(palette.danger.base.color),
            Some(DebugLogLevel::Warn) => Some(palette.primary.strong.color),
            Some(DebugLogLevel::Info) => None,
            Some(DebugLogLevel::Debug) => Some(palette.secondary.strong.color),
            Some(DebugLogLevel::Trace) => Some(palette.background.strongest.color),
            None => None,
        };

        iced::widget::text::Style { color }
    }
}

impl std::fmt::Display for DebugLogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Error => write!(f, "Error"),
            Self::Warn => write!(f, "Warn"),
            Self::Info => write!(f, "Info"),
            Self::Debug => write!(f, "Debug"),
            Self::Trace => write!(f, "Trace"),
        }
    }
}

// ── Debug log entry parsing ─────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DebugLogCategory {
    All,
    Fetch,
    Cache,
    Ws,
    Stream,
    Backfill,
    Chart,
    Bubbles,
    Footprint,
    Kline,
    Oi,
    Data,
    Ui,
    App,
    ThirdParty,
}

impl DebugLogCategory {
    const ALL: [Self; 15] = [
        Self::All,
        Self::Fetch,
        Self::Cache,
        Self::Ws,
        Self::Stream,
        Self::Backfill,
        Self::Chart,
        Self::Bubbles,
        Self::Footprint,
        Self::Kline,
        Self::Oi,
        Self::Data,
        Self::Ui,
        Self::App,
        Self::ThirdParty,
    ];
}

impl std::fmt::Display for DebugLogCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::All => write!(f, "All"),
            Self::Fetch => write!(f, "Fetch"),
            Self::Cache => write!(f, "Cache"),
            Self::Ws => write!(f, "WS"),
            Self::Stream => write!(f, "Stream"),
            Self::Backfill => write!(f, "Backfill"),
            Self::Chart => write!(f, "Chart"),
            Self::Bubbles => write!(f, "Bubbles"),
            Self::Footprint => write!(f, "Footprint"),
            Self::Kline => write!(f, "Kline"),
            Self::Oi => write!(f, "OI"),
            Self::Data => write!(f, "Data"),
            Self::Ui => write!(f, "UI"),
            Self::App => write!(f, "App"),
            Self::ThirdParty => write!(f, "Third-party"),
        }
    }
}

#[derive(Debug, Clone)]
struct DebugLogEntry {
    raw: String,
    timestamp: Option<String>,
    level: Option<DebugLogLevel>,
    target: Option<String>,
    category: DebugLogCategory,
    event: String,
    summary: String,
}

fn parse_debug_log_entry(line: &str) -> DebugLogEntry {
    let raw = line.to_string();
    let mut timestamp = None;
    let mut level = None;
    let mut target = None;

    // Parse format: [timestamp] [LEVEL] [target] message
    let mut remaining = line;

    // Extract timestamp
    if let Some(start) = remaining.find('[')
        && let Some(end) = remaining[start + 1..].find(']')
    {
        timestamp = Some(remaining[start + 1..start + 1 + end].to_string());
        remaining = &remaining[start + 1 + end + 1..];
    }

    // Extract level
    if let Some(start) = remaining.find('[')
        && let Some(end) = remaining[start + 1..].find(']')
    {
        let level_str = remaining[start + 1..start + 1 + end].trim();
        level = match level_str {
            "ERROR" | "FATAL" => Some(DebugLogLevel::Error),
            "WARN" => Some(DebugLogLevel::Warn),
            "INFO" => Some(DebugLogLevel::Info),
            "DEBUG" => Some(DebugLogLevel::Debug),
            "TRACE" => Some(DebugLogLevel::Trace),
            _ => None,
        };
        remaining = &remaining[start + 1 + end + 1..];
    }

    // Extract target
    if let Some(start) = remaining.find('[')
        && let Some(end) = remaining[start + 1..].find(']')
    {
        target = Some(remaining[start + 1..start + 1 + end].to_string());
        remaining = &remaining[start + 1 + end + 1..];
    }

    let message = remaining.trim();
    let (category, event, summary) = classify_log_message(message, target.as_deref());

    DebugLogEntry {
        raw,
        timestamp,
        level,
        target,
        category,
        event,
        summary,
    }
}

fn classify_log_message(message: &str, target: Option<&str>) -> (DebugLogCategory, String, String) {
    // Check for our structured log format: CATEGORY Event | key=value ...
    if let Some(pipe_pos) = message.find('|') {
        let prefix = message[..pipe_pos].trim();
        let details = message[pipe_pos + 1..].trim();

        let parts: Vec<&str> = prefix.split_whitespace().collect();
        if parts.len() >= 2 {
            let cat_str = parts[0];
            let event = parts[1..].join(" ");

            let category = match cat_str {
                "FETCH" | "TRADE" => DebugLogCategory::Fetch,
                "KLINE" => DebugLogCategory::Kline,
                "OI" => DebugLogCategory::Oi,
                "CACHE" => DebugLogCategory::Cache,
                "WS" if event.contains("Backfill") => DebugLogCategory::Backfill,
                "WS" => DebugLogCategory::Ws,
                "STREAM" => DebugLogCategory::Stream,
                "BACKFILL" => DebugLogCategory::Backfill,
                "CHART" if event.contains("Bubbles") => DebugLogCategory::Bubbles,
                "CHART" if event.contains("Footprint") => DebugLogCategory::Footprint,
                "CHART" => DebugLogCategory::Chart,
                "DATA" => DebugLogCategory::Data,
                _ => DebugLogCategory::App,
            };

            // Extract key info for summary
            let summary = extract_summary(details, cat_str);
            return (category, event, summary);
        }
    }

    // Fallback: classify by target
    let category = match target {
        Some(t) if t.starts_with("flowsurface") || t.starts_with("flowsurface_") => {
            if t.contains("exchange") {
                DebugLogCategory::Fetch
            } else {
                DebugLogCategory::App
            }
        }
        Some(t) if t == "iced_wgpu" || t.contains("wgpu") || t.contains("winit") => {
            DebugLogCategory::ThirdParty
        }
        Some("panic") => DebugLogCategory::App,
        Some(_) => DebugLogCategory::ThirdParty,
        None => DebugLogCategory::App,
    };

    (category, String::new(), message.to_string())
}

fn extract_summary(details: &str, cat_str: &str) -> String {
    let mut summary_parts = Vec::new();

    for part in details.split_whitespace() {
        if let Some((key, value)) = part.split_once('=') {
            match key {
                "symbol" | "venue" | "stream" | "range" | "records" | "raw_records"
                | "retained_records" | "trades" | "duration" | "requests" | "session"
                | "reason" | "error" | "req" | "pane" | "panes" | "gap_ms" => {
                    summary_parts.push(format!("{key}={value}"));
                }
                _ => {}
            }
        }
    }

    if summary_parts.is_empty() {
        // For TRADE/KLINE/OI, try to extract symbol and venue from details
        if matches!(cat_str, "TRADE" | "KLINE" | "OI") {
            for part in details.split_whitespace() {
                if let Some(("venue" | "symbol" | "records" | "duration", value)) =
                    part.split_once('=')
                {
                    summary_parts.push(value.to_string());
                }
            }
        }

        if summary_parts.is_empty() {
            return details.to_string();
        }
    }

    summary_parts.join(" ")
}

fn is_app_target(target: Option<&str>) -> bool {
    match target {
        Some(t) => t.starts_with("flowsurface") || t.starts_with("flowsurface_") || t == "panic",
        None => true,
    }
}

impl Flowsurface {
    fn new() -> (Self, Task<Message>) {
        let load_outcome = layout::load_saved_state();
        let (saved_state, startup_warning, save_state_enabled) = match load_outcome {
            layout::SavedStateLoadOutcome::Loaded(state)
            | layout::SavedStateLoadOutcome::MissingDefault(state) => (state, None, true),
            layout::SavedStateLoadOutcome::Migrated {
                state,
                from_version,
                to_version,
                backup_path,
            } => (
                state,
                Some(StartupWarning::SavedStateMigrated {
                    from_version,
                    to_version,
                    backup_path,
                }),
                true,
            ),
            layout::SavedStateLoadOutcome::Recovered {
                state,
                warnings,
                backup_path,
            } => (
                state,
                Some(StartupWarning::SavedStateRecovered {
                    warnings,
                    backup_path,
                }),
                true,
            ),
            layout::SavedStateLoadOutcome::Corrupt {
                error,
                original_path,
                backup_path,
            } => (
                layout::SavedState::default(),
                Some(StartupWarning::SavedStateCorrupt {
                    error,
                    original_path,
                    backup_path,
                }),
                false,
            ),
        };

        let (main_window_id, open_main_window) = {
            let (position, size) = saved_state.window();
            let config = window::Settings {
                size,
                position,
                exit_on_close_request: false,
                ..window::settings()
            };
            window::open(config)
        };

        let handles = exchange::adapter::AdapterHandles::spawn_venues(
            exchange::adapter::Venue::ALL,
            saved_state.proxy_cfg.as_ref(),
        );

        let (sidebar, launch_sidebar) = dashboard::Sidebar::new(&saved_state, handles.clone());

        let (audio_stream, audio_init_err) = AudioStream::new(saved_state.audio_cfg);

        let windowing_mode = WindowingMode::platform_default();
        log::info!(
            "WINDOW Mode | mode={windowing_mode} reason={reason}",
            reason = windowing_mode.reason()
        );

        let market_store = Arc::new(market_service::MarketStore::new());
        let market_diagnostics = market_service::MarketDiagnostics::new(market_store.clone());
        log::info!("MARKET ServiceStarted | runtime=dedicated");

        // Initialize Windows power guard if on Windows
        #[cfg(target_os = "windows")]
        {
            power_guard::windows_power::init();
        }

        let mut state = Self {
            main_window: window::Window::new(main_window_id),
            layout_manager: saved_state.layout_manager,
            theme_editor: ThemeEditor::new(saved_state.custom_theme),
            audio_stream,
            sidebar,
            handles,
            confirm_dialog: None,
            startup_warning,
            save_state_enabled,
            timezone: saved_state.timezone,
            ui_scale_factor: saved_state.scale_factor,
            volume_size_unit: saved_state.volume_size_unit,
            theme: saved_state.theme,
            notifications: Notifications::new(),
            network: NetworkManager::new(saved_state.proxy_cfg),
            windowing_mode,
            market_store,
            market_diagnostics,
            dirty_flag: render_scheduler::DirtyFlag::new(),
            debug_terminal_enabled: saved_state.debug_terminal_enabled,
            debug_terminal_window: None,
            debug_terminal_embedded: false,
            debug_terminal_logs: logger::debug_terminal_snapshot(),
            debug_terminal_level_filter: DebugLevelFilter::DEFAULT,
            debug_terminal_category_filter: DebugLogCategory::All,
            debug_terminal_search: String::new(),
            debug_terminal_auto_scroll: true,
            debug_terminal_app_only: true,
            debug_terminal_compact_mode: true,
        };

        if let Some(err) = audio_init_err {
            state
                .notifications
                .push(Toast::error(format!("Audio disabled: {err}")));
        }

        match &state.startup_warning {
            Some(StartupWarning::SavedStateMigrated {
                from_version,
                to_version,
                backup_path,
            }) => state.notifications.push(Toast::info(format!(
                "Saved layout migrated from version {from_version} to {to_version}. Backup: {}",
                backup_path
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "none".to_string())
            ))),
            Some(StartupWarning::SavedStateRecovered {
                warnings,
                backup_path,
            }) => state.notifications.push(Toast::warn(format!(
                "Saved layout was repaired: {} Backup: {}",
                warnings.join("; "),
                backup_path
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "none".to_string())
            ))),
            Some(StartupWarning::SavedStateCorrupt { .. }) | None => {}
        }

        if state.layout_manager.layouts.is_empty() {
            log::error!("No layouts available after loading state; creating a default layout");
            state.layout_manager = LayoutManager::new();
        }

        let active_layout_id = state
            .layout_manager
            .active_layout_id()
            .or_else(|| {
                state
                    .layout_manager
                    .layouts
                    .first()
                    .map(|layout| &layout.id)
            })
            .map(|layout| layout.unique);

        let load_layout = active_layout_id
            .map(|uid| state.load_layout(uid, main_window_id))
            .unwrap_or_else(|| {
                log::error!("No active layout could be selected at startup");
                Task::none()
            });

        let debug_terminal = if state.debug_terminal_enabled {
            state.open_debug_terminal()
        } else {
            Task::none()
        };

        (
            state,
            open_main_window
                .discard()
                .chain(load_layout)
                .chain(launch_sidebar.map(Message::Sidebar))
                .chain(debug_terminal),
        )
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::MarketWsEvent(event) => {
                // Record WS event in market store for diagnostics
                self.market_store.record_ws_event();
                self.market_store.enqueue_event();
                self.market_diagnostics.maybe_log();

                // Mark UI dirty when market data arrives
                self.dirty_flag.mark_dirty();

                let main_window_id = self.main_window.id;
                let dashboard = self.active_dashboard_mut();

                match event {
                    exchange::Event::Connected(streams) => {
                        self.market_store.set_streams_connected(true);
                        log::info!("WS Connected | streams={}", streams.len());
                        for (idx, stream) in streams.iter().enumerate() {
                            log::debug!(
                                "WS ConnectedStream | idx={idx} stream={}",
                                crate::connector::fetcher::format_stream(stream)
                            );
                        }
                        // Compute real offline gap and enqueue backfill if needed.
                        // At disconnect time the gap was tiny; now we have the
                        // full offline duration (last_seen → reconnect_time).
                        let main_window_id = self.main_window.id;
                        let handles = self.handles.clone();
                        let dashboard = self.active_dashboard_mut();
                        let reconnect_time = exchange::UnixMs::now();
                        return dashboard
                            .execute_reconnect_backfill(&handles, main_window_id, reconnect_time)
                            .map(move |msg| Message::Dashboard {
                                layout_id: None,
                                event: msg,
                            });
                    }
                    exchange::Event::Disconnected(streams, reason) => {
                        self.market_store.set_streams_connected(false);
                        let now = exchange::UnixMs::now();
                        log::info!(
                            "WS Disconnected | reason={reason:?} streams={} now={}",
                            streams.len(),
                            crate::connector::fetcher::format_time_short(now)
                        );
                        for (idx, stream) in streams.iter().enumerate() {
                            log::debug!(
                                "WS DisconnectedStream | idx={idx} stream={}",
                                crate::connector::fetcher::format_stream(stream)
                            );
                        }

                        // Defer backfill until reconnect — the gap at disconnect
                        // time is tiny (last_seen → disconnect ≈ 87ms), but the
                        // real offline gap is last_seen → reconnect_time.
                        let dashboard = self.active_dashboard_mut();
                        dashboard.record_pending_disconnect_gaps(&streams, now);
                        return Task::none();
                    }
                    exchange::Event::DepthReceived(stream, update_t, depth) => {
                        log::trace!(
                            "WS DepthReceived | stream={} update_t={} routed=true",
                            crate::connector::fetcher::format_stream(&stream),
                            crate::connector::fetcher::format_time_short(update_t)
                        );
                        let task = dashboard
                            .ingest_depth(&stream, update_t, &depth, main_window_id)
                            .map(move |msg| Message::Dashboard {
                                layout_id: None,
                                event: msg,
                            });

                        return task;
                    }
                    exchange::Event::TradesReceived(stream, update_t, buffer) => {
                        let now = exchange::UnixMs::now();
                        let first_trade_t = buffer.first().map(|trade| trade.time);
                        let last_trade_t = buffer.last().map(|trade| trade.time);
                        log::trace!(
                            "WS TradesReceived | stream={} update_t={} batch_len={} first_trade_t={} last_trade_t={} lag_ms={}",
                            crate::connector::fetcher::format_stream(&stream),
                            crate::connector::fetcher::format_time_short(update_t),
                            buffer.len(),
                            crate::connector::fetcher::format_optional_time(first_trade_t),
                            crate::connector::fetcher::format_optional_time(last_trade_t),
                            now.saturating_diff(last_trade_t.unwrap_or(update_t))
                        );
                        let task = dashboard
                            .ingest_trades(&stream, &buffer, update_t, main_window_id)
                            .map(move |msg| Message::Dashboard {
                                layout_id: None,
                                event: msg,
                            });

                        if let Some(msg) = self.audio_stream.try_play_sound(&stream, &buffer) {
                            self.notifications.push(Toast::error(msg));
                        }

                        return task;
                    }
                    exchange::Event::KlineReceived(stream, kline) => {
                        let now = exchange::UnixMs::now();
                        log::trace!(
                            "WS KlineReceived | stream={} kline_t={} open={:?} high={:?} low={:?} close={:?} volume={:?} lag_ms={}",
                            crate::connector::fetcher::format_stream(&stream),
                            crate::connector::fetcher::format_time_short(kline.time),
                            kline.open,
                            kline.high,
                            kline.low,
                            kline.close,
                            kline.volume,
                            now.saturating_diff(kline.time)
                        );
                        return dashboard
                            .update_latest_klines(&stream, &kline, main_window_id)
                            .map(move |msg| Message::Dashboard {
                                layout_id: None,
                                event: msg,
                            });
                    }
                }
            }
            Message::Tick(now) => {
                // Throttled tick debug logging (once every 2 seconds)
                if DEBUG_WINDOW_DIAGNOSTICS {
                    static LAST_TICK_LOG: std::sync::Mutex<Option<std::time::Instant>> =
                        std::sync::Mutex::new(None);
                    if let Ok(mut last) = LAST_TICK_LOG.lock()
                        && last.is_none_or(|t| t.elapsed() > Duration::from_secs(2))
                    {
                        let popout_count = self.active_dashboard().popout.len();
                        log::trace!(
                            "[tick] main={:?}, debug_term={:?}, popouts={}",
                            self.main_window.id,
                            self.debug_terminal_window,
                            popout_count
                        );
                        *last = Some(now);
                    }
                }

                // Drain market events and log UI lag diagnostics
                let drained = self.market_store.drain_events();
                if drained > 0 {
                    self.market_diagnostics.log_ui_lag(drained, 0);
                }
                self.market_diagnostics.maybe_log();

                let main_window_id = self.main_window.id;
                let handles = self.handles.clone();

                return self
                    .active_dashboard_mut()
                    .tick(&handles, now, main_window_id)
                    .map(move |msg| Message::Dashboard {
                        layout_id: None,
                        event: msg,
                    });
            }
            Message::WindowEvent(event) => match event {
                window::Event::CloseRequested(window) => {
                    if self.debug_terminal_window == Some(window) {
                        self.debug_terminal_window = None;
                        self.debug_terminal_enabled = false;
                        return window::close(window);
                    }

                    let main_window = self.main_window.id;
                    let dashboard = self.active_dashboard_mut();

                    if window != main_window {
                        dashboard.popout.remove(&window);
                        return window::close(window);
                    }

                    let mut active_windows = dashboard
                        .popout
                        .keys()
                        .copied()
                        .collect::<Vec<window::Id>>();
                    active_windows.push(main_window);

                    return window::collect_window_specs(active_windows, Message::ExitRequested);
                }
                window::Event::Focused(id) => {
                    self.market_store.set_ui_focused(true);
                    if DEBUG_WINDOW_DIAGNOSTICS {
                        log::debug!(
                            "[window] Focused: id={:?} ({})",
                            id,
                            self.debug_window_label(id)
                        );
                    }
                }
                window::Event::Unfocused(id) => {
                    self.market_store.set_ui_focused(false);
                    if DEBUG_WINDOW_DIAGNOSTICS {
                        log::debug!(
                            "[window] Unfocused: id={:?} ({})",
                            id,
                            self.debug_window_label(id)
                        );
                    }
                }
            },
            Message::ExitRequested(windows) => {
                if self.save_state_enabled {
                    self.save_state_to_disk(&windows);
                } else {
                    log::warn!(
                        "SAVED_STATE SaveSkipped | reason=awaiting_corrupt_state_confirmation"
                    );
                }
                power_guard::windows_power::cleanup();
                return iced::exit();
            }
            Message::SaveStateRequested(windows) => {
                if self.save_state_enabled {
                    self.save_state_to_disk(&windows);
                } else {
                    log::warn!(
                        "SAVED_STATE SaveSkipped | reason=awaiting_corrupt_state_confirmation"
                    );
                }
            }
            Message::RestartRequested(Some(windows)) => {
                if self.save_state_enabled {
                    self.save_state_to_disk(&windows);
                } else {
                    log::warn!(
                        "SAVED_STATE SaveSkipped | reason=awaiting_corrupt_state_confirmation"
                    );
                }
                return self.restart();
            }
            Message::RestartRequested(None) => {
                self.confirm_dialog = None;

                let mut active_windows = self
                    .active_dashboard()
                    .popout
                    .keys()
                    .copied()
                    .collect::<Vec<window::Id>>();
                active_windows.push(self.main_window.id);

                return window::collect_window_specs(active_windows, |windows| {
                    Message::RestartRequested(Some(windows))
                });
            }
            Message::GoBack => {
                let main_window = self.main_window.id;

                if self.confirm_dialog.is_some() {
                    self.confirm_dialog = None;
                } else if self.sidebar.active_menu().is_some() {
                    self.sidebar.set_menu(None);
                } else {
                    let dashboard = self.active_dashboard_mut();

                    if dashboard.go_back(main_window) {
                        return Task::none();
                    } else if dashboard.focus.is_some() {
                        dashboard.focus = None;
                    } else {
                        self.sidebar.hide_tickers_table();
                    }
                }
            }
            Message::ThemeSelected(theme) => {
                self.theme = data::Theme(theme.clone());

                let main_window = self.main_window.id;
                self.active_dashboard_mut()
                    .theme_updated(main_window, &theme);
            }
            Message::Dashboard {
                layout_id: id,
                event: msg,
            } => {
                let Some(active_layout) = self.layout_manager.active_layout_id() else {
                    log::error!("No active layout to handle dashboard message");
                    return Task::none();
                };

                let main_window = self.main_window;
                let layout_id = id.unwrap_or(active_layout.unique);
                let handles = self.handles.clone();

                if let Some(dashboard) = self.layout_manager.mut_dashboard(layout_id) {
                    let (main_task, event) = dashboard.update(
                        &handles,
                        msg,
                        &main_window,
                        &layout_id,
                        self.windowing_mode,
                    );

                    let additional_task = match event {
                        Some(dashboard::Event::DistributeFetchedData {
                            layout_id,
                            pane_id,
                            data,
                            stream,
                        }) => dashboard
                            .distribute_fetched_data(main_window.id, pane_id, data, stream, false)
                            .map(move |msg| Message::Dashboard {
                                layout_id: Some(layout_id),
                                event: msg,
                            }),
                        Some(dashboard::Event::Notification(toast)) => {
                            self.notifications.push(toast);
                            Task::none()
                        }
                        Some(dashboard::Event::ResolveStreams { pane_id, streams }) => {
                            let tickers_info = self.sidebar.tickers_info();

                            let resolved_streams =
                                streams.into_iter().try_fold(vec![], |mut acc, persist| {
                                    let resolver = |t: &exchange::Ticker| {
                                        tickers_info.get(t).and_then(|opt| *opt)
                                    };

                                    match persist.into_stream_kinds(resolver) {
                                        Ok(mut resolved) => {
                                            acc.append(&mut resolved);
                                            Ok(acc)
                                        }
                                        Err(err) => Err(err),
                                    }
                                });

                            match resolved_streams {
                                Ok(resolved) => {
                                    if resolved.is_empty() {
                                        Task::none()
                                    } else {
                                        dashboard
                                            .resolve_streams(main_window.id, pane_id, resolved)
                                            .map(move |msg| Message::Dashboard {
                                                layout_id: None,
                                                event: msg,
                                            })
                                    }
                                }
                                Err(err) => {
                                    if self.sidebar.is_metadata_loading() {
                                        // Metadata fetches are still in flight
                                        log::debug!(
                                            "Deferring stream resolution for pane {pane_id}: metadata still loading ({err})"
                                        );
                                    } else {
                                        log::debug!("Blocking streams for pane {pane_id}: {err}");
                                        dashboard.block_streams(
                                            main_window.id,
                                            pane_id,
                                            format!("Metadata not available: {err}"),
                                        );
                                    }
                                    Task::none()
                                }
                            }
                        }
                        Some(dashboard::Event::RequestPalette) => {
                            let theme = self.theme.0.clone();

                            let main_window = self.main_window.id;
                            self.active_dashboard_mut()
                                .theme_updated(main_window, &theme);

                            Task::none()
                        }
                        None => Task::none(),
                    };

                    return main_task
                        .map(move |msg| Message::Dashboard {
                            layout_id: Some(layout_id),
                            event: msg,
                        })
                        .chain(additional_task);
                }
            }
            Message::RemoveNotification(index) => {
                self.notifications.remove(index);
            }
            Message::StartupContinueWithDefault => {
                self.save_state_enabled = true;
                self.startup_warning = None;
                self.notifications.push(Toast::warn(
                    "Default layout is active. The next save will overwrite saved-state.json; the backup remains available.",
                ));
            }
            Message::StartupExitWithoutOverwrite => {
                self.save_state_enabled = false;
                power_guard::windows_power::cleanup();
                return iced::exit();
            }
            Message::StartupWarningNoop => {}
            Message::SetTimezone(tz) => {
                self.timezone = tz;
            }
            Message::ScaleFactorChanged(value) => {
                self.ui_scale_factor = value;
            }
            Message::ToggleTradeFetch(checked) => {
                self.layout_manager
                    .iter_dashboards_mut()
                    .for_each(|dashboard| {
                        dashboard.toggle_trade_fetch(checked, &self.main_window);
                    });

                if checked {
                    self.confirm_dialog = None;
                }
            }
            Message::ToggleDebugTerminal(enabled) => {
                self.debug_terminal_enabled = enabled;

                if enabled {
                    self.debug_terminal_logs = logger::debug_terminal_snapshot();
                    return self.open_debug_terminal();
                } else {
                    if let Some(window) = self.debug_terminal_window.take() {
                        return window::close(window);
                    }
                    self.debug_terminal_embedded = false;
                }
            }
            Message::DebugTerminalOpened(window) => {
                self.debug_terminal_window = Some(window);
                self.debug_terminal_logs = logger::debug_terminal_snapshot();
                if self.debug_terminal_auto_scroll {
                    return self.scroll_debug_terminal_to_bottom();
                }
            }
            Message::DebugTerminalRefresh => {
                if self.debug_terminal_enabled || self.debug_terminal_window.is_some() {
                    self.debug_terminal_logs = logger::debug_terminal_snapshot();
                    if self.debug_terminal_auto_scroll {
                        return self.scroll_debug_terminal_to_bottom();
                    }
                }
            }
            Message::DebugTerminalClear => {
                logger::clear_debug_terminal();
                self.debug_terminal_logs.clear();
            }
            Message::DebugTerminalCopyAll => {
                return iced::clipboard::write(self.debug_terminal_logs.join("\n"));
            }
            Message::DebugTerminalCopyVisible => {
                let visible: Vec<String> = self
                    .filtered_debug_terminal_entries()
                    .into_iter()
                    .map(|e| e.raw)
                    .collect();
                return iced::clipboard::write(visible.join("\n"));
            }
            Message::DebugTerminalSearchChanged(value) => {
                self.debug_terminal_search = value;
            }
            Message::DebugTerminalToggleLevel(level, enabled) => {
                self.debug_terminal_level_filter.toggle(level, enabled);
            }
            Message::DebugTerminalToggleAutoScroll(enabled) => {
                self.debug_terminal_auto_scroll = enabled;
                if enabled {
                    return self.scroll_debug_terminal_to_bottom();
                }
            }
            Message::DebugTerminalCategoryFilterChanged(category) => {
                self.debug_terminal_category_filter = category;
            }
            Message::DebugTerminalToggleAppOnly(app_only) => {
                self.debug_terminal_app_only = app_only;
            }
            Message::DebugTerminalToggleCompactMode(compact) => {
                self.debug_terminal_compact_mode = compact;
            }
            Message::ToggleDialogModal(dialog) => {
                self.confirm_dialog = dialog;
            }
            Message::Layouts(message) => {
                let action = self.layout_manager.update(message);

                match action {
                    Some(modal::layout_manager::Action::Select(layout)) => {
                        let active_popout_keys = self
                            .active_dashboard()
                            .popout
                            .keys()
                            .copied()
                            .collect::<Vec<_>>();

                        let window_tasks = Task::batch(
                            active_popout_keys
                                .iter()
                                .map(|&popout_id| window::close::<window::Id>(popout_id))
                                .collect::<Vec<_>>(),
                        )
                        .discard();

                        let old_layout_id = self
                            .layout_manager
                            .active_layout_id()
                            .as_ref()
                            .map(|layout| layout.unique);

                        return window::collect_window_specs(
                            active_popout_keys,
                            dashboard::Message::SavePopoutSpecs,
                        )
                        .map(move |msg| Message::Dashboard {
                            layout_id: old_layout_id,
                            event: msg,
                        })
                        .chain(window_tasks)
                        .chain(self.load_layout(layout, self.main_window.id));
                    }
                    Some(modal::layout_manager::Action::Clone(id)) => {
                        let manager = &mut self.layout_manager;

                        let source_data = manager.get(id).map(|layout| {
                            (
                                layout.id.name.clone(),
                                layout.id.unique,
                                data::Dashboard::from(&layout.dashboard),
                            )
                        });

                        if let Some((name, old_id, ser_dashboard)) = source_data {
                            let new_uid = uuid::Uuid::new_v4();
                            let new_layout = LayoutId {
                                unique: new_uid,
                                name: manager.ensure_unique_name(&name, new_uid),
                            };

                            let mut popout_windows = Vec::new();

                            for (pane, window_spec) in &ser_dashboard.popout {
                                let configuration = configuration(pane.clone());
                                popout_windows.push((configuration, *window_spec));
                            }

                            let dashboard = Dashboard::from_config(
                                configuration(ser_dashboard.pane.clone()),
                                popout_windows,
                                old_id,
                            );

                            manager.insert_layout(new_layout.clone(), dashboard);
                        }
                    }
                    None => {}
                }
            }
            Message::AudioStream(message) => {
                if let Some(event) = self.audio_stream.update(message) {
                    match event {
                        modal::audio::UpdateEvent::RetryFailed(err) => {
                            self.notifications
                                .push(Toast::error(format!("Audio still unavailable: {err}")));
                        }
                        modal::audio::UpdateEvent::RetrySucceeded => {
                            self.notifications.push(Toast::info(
                                "Audio output re-initialized successfully".to_string(),
                            ));
                        }
                    }
                }
            }
            Message::DataFolderRequested => {
                if let Err(err) = data::open_data_folder() {
                    self.notifications
                        .push(Toast::error(format!("Failed to open data folder: {err}")));
                }
            }
            Message::OpenUrlRequested(url) => {
                if let Err(err) = data::open_url(url.as_ref()) {
                    self.notifications
                        .push(Toast::error(format!("Failed to open link: {err}")));
                }
            }
            Message::ThemeEditor(msg) => {
                let action = self.theme_editor.update(msg, &self.theme.clone().into());

                match action {
                    Some(modal::theme_editor::Action::Exit) => {
                        self.sidebar.set_menu(Some(sidebar::Menu::Settings));
                    }
                    Some(modal::theme_editor::Action::UpdateTheme(theme)) => {
                        self.theme = data::Theme(theme.clone());

                        let main_window = self.main_window.id;
                        self.active_dashboard_mut()
                            .theme_updated(main_window, &theme);
                    }
                    None => {}
                }
            }
            Message::NetworkManager(msg) => {
                let action = self.network.update(msg);

                match action {
                    Some(network_manager::Action::ApplyProxy) => {
                        if let Some(proxy) = self.network.proxy_cfg() {
                            data::config::proxy::save_proxy_auth(&proxy);
                        }

                        self.confirm_dialog = Some(
                            screen::ConfirmDialog::new(
                                "Proxy changes saved. Restart now to apply?".to_string(),
                                Box::new(Message::RestartRequested(None)),
                            )
                            .with_confirm_btn_text("Restart now".to_string()),
                        );

                        let main_window = self.main_window.id;
                        let dashboard = self.active_dashboard_mut();

                        let mut active_windows = dashboard
                            .popout
                            .keys()
                            .copied()
                            .collect::<Vec<window::Id>>();
                        active_windows.push(main_window);

                        return window::collect_window_specs(
                            active_windows,
                            Message::SaveStateRequested,
                        );
                    }
                    Some(network_manager::Action::Exit) => {
                        self.sidebar.set_menu(Some(sidebar::Menu::Settings));
                    }
                    None => {}
                }
            }
            Message::Sidebar(message) => {
                let (task, action) = self.sidebar.update(message);

                match action {
                    Some(dashboard::sidebar::Action::TickerSelected(ticker_info, content)) => {
                        let main_window_id = self.main_window.id;
                        let handles = self.handles.clone();

                        let task = {
                            if let Some(kind) = content {
                                self.active_dashboard_mut().init_focused_pane(
                                    &handles,
                                    main_window_id,
                                    ticker_info,
                                    kind,
                                )
                            } else {
                                self.active_dashboard_mut().switch_tickers_in_group(
                                    &handles,
                                    main_window_id,
                                    ticker_info,
                                )
                            }
                        };

                        return task.map(move |msg| Message::Dashboard {
                            layout_id: None,
                            event: msg,
                        });
                    }
                    Some(dashboard::sidebar::Action::ErrorOccurred(err)) => {
                        self.notifications.push(Toast::error(err.to_string()));
                    }
                    None => {}
                }

                return task.map(Message::Sidebar);
            }
            Message::ApplyVolumeSizeUnit(pref) => {
                self.volume_size_unit = pref;
                self.confirm_dialog = None;

                let mut active_windows: Vec<window::Id> =
                    self.active_dashboard().popout.keys().copied().collect();
                active_windows.push(self.main_window.id);

                return window::collect_window_specs(active_windows, |windows| {
                    Message::RestartRequested(Some(windows))
                });
            }
        }
        Task::none()
    }

    fn view(&self, id: window::Id) -> Element<'_, Message> {
        if self.debug_terminal_window == Some(id) {
            return self.debug_terminal_view();
        }

        let dashboard = self.active_dashboard();
        let sidebar_pos = self.sidebar.position();

        let tickers_table = &self.sidebar.tickers_table;

        let content = if id == self.main_window.id {
            let sidebar_view = self
                .sidebar
                .view(self.audio_stream.volume())
                .map(Message::Sidebar);

            let dashboard_view = dashboard
                .view(
                    &self.main_window,
                    tickers_table,
                    self.timezone,
                    self.windowing_mode.allows_native_popout(),
                )
                .map(move |msg| Message::Dashboard {
                    layout_id: None,
                    event: msg,
                });

            let header_title = {
                #[cfg(target_os = "macos")]
                {
                    iced::widget::center(
                        text("FLOWSURFACE")
                            .font(iced::Font {
                                weight: iced::font::Weight::Bold,
                                ..Default::default()
                            })
                            .size(crate::style::text_size::TITLE)
                            .style(style::title_text),
                    )
                    .height(20)
                    .align_y(Alignment::Center)
                    .padding(padding::top(4))
                }
                #[cfg(not(target_os = "macos"))]
                {
                    column![]
                }
            };

            let base = column![
                header_title,
                match sidebar_pos {
                    sidebar::Position::Left => row![sidebar_view, dashboard_view,],
                    sidebar::Position::Right => row![dashboard_view, sidebar_view],
                }
                .spacing(4)
                .padding(8),
            ];

            // In embedded mode, show debug terminal as a docked bottom panel
            let base_with_debug = if self.debug_terminal_embedded
                && self.debug_terminal_enabled
                && self.debug_terminal_window.is_none()
            {
                let debug_panel = container(self.debug_terminal_view())
                    .height(Length::FillPortion(2))
                    .width(Length::Fill);
                column![
                    container(base).height(Length::FillPortion(5)),
                    iced::widget::rule::horizontal(2).style(style::split_ruler),
                    debug_panel,
                ]
                .into()
            } else {
                base.into()
            };

            if let Some(menu) = self.sidebar.active_menu() {
                self.view_with_modal(base_with_debug, dashboard, menu)
            } else {
                base_with_debug
            }
        } else {
            container(
                dashboard
                    .view_window(
                        id,
                        &self.main_window,
                        tickers_table,
                        self.timezone,
                        self.windowing_mode.allows_native_popout(),
                    )
                    .map(move |msg| Message::Dashboard {
                        layout_id: None,
                        event: msg,
                    }),
            )
            .padding(padding::top(style::TITLE_PADDING_TOP))
            .into()
        };

        let content = if let Some(StartupWarning::SavedStateCorrupt { .. }) = &self.startup_warning
        {
            main_dialog_modal(
                content,
                self.startup_warning_modal(),
                Message::StartupWarningNoop,
            )
        } else {
            content
        };

        toast::Manager::new(
            content,
            self.notifications.toasts(),
            match sidebar_pos {
                sidebar::Position::Left => Alignment::Start,
                sidebar::Position::Right => Alignment::End,
            },
            Message::RemoveNotification,
        )
        .into()
    }

    fn startup_warning_modal(&self) -> Element<'_, Message> {
        let Some(StartupWarning::SavedStateCorrupt {
            error,
            original_path,
            backup_path,
        }) = &self.startup_warning
        else {
            return container(column![]).into();
        };

        let backup_text = backup_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "Backup could not be created.".to_string());

        let body = format!(
            "FlowSurface could not load your saved layout.\n\nOriginal file:\n{}\n\nBackup:\n{}\n\nError:\n{}\n\nYou can continue with a default layout. If you continue, the next save will overwrite saved-state.json. Your backup will remain available.",
            original_path.display(),
            backup_text,
            error
        );

        container(
            column![
                text("Saved layout corrupted").size(crate::style::text_size::TITLE),
                text(body)
                    .wrapping(iced::widget::text::Wrapping::Word)
                    .width(Length::Fill),
                row![
                    button(text("Open backup folder")).on_press(Message::DataFolderRequested),
                    button(text("Exit without overwriting"))
                        .style(|theme, status| style::button::transparent(theme, status, false))
                        .on_press(Message::StartupExitWithoutOverwrite),
                    button(text("Continue with default layout"))
                        .on_press(Message::StartupContinueWithDefault),
                ]
                .spacing(8)
                .align_y(Alignment::Center),
            ]
            .spacing(16)
            .width(Length::Fill),
        )
        .width(Length::Fixed(620.0))
        .padding(24)
        .style(style::dashboard_modal)
        .into()
    }

    fn theme(&self, _window: window::Id) -> iced_core::Theme {
        self.theme.clone().into()
    }

    fn title(&self, _window: window::Id) -> String {
        if self.debug_terminal_window == Some(_window) {
            return "Flowsurface Debug Terminal".to_string();
        }

        if let Some(id) = self.layout_manager.active_layout_id() {
            format!("Flowsurface [{}]", id.name)
        } else {
            "Flowsurface".to_string()
        }
    }

    fn scale_factor(&self, _window: window::Id) -> f32 {
        self.ui_scale_factor.into()
    }

    fn subscription(&self) -> Subscription<Message> {
        let window_events = window::events().map(Message::WindowEvent);
        let sidebar = self.sidebar.subscription().map(Message::Sidebar);

        let exchange_streams = self
            .active_dashboard()
            .market_subscriptions(&self.handles)
            .map(Message::MarketWsEvent);

        let tick = iced::time::every(Duration::from_millis(16)).map(Message::Tick);
        let debug_terminal = if self.debug_terminal_enabled || self.debug_terminal_window.is_some()
        {
            iced::time::every(Duration::from_millis(500)).map(|_| Message::DebugTerminalRefresh)
        } else {
            Subscription::none()
        };

        let hotkeys = keyboard::listen().filter_map(|event| {
            let keyboard::Event::KeyPressed { key, .. } = event else {
                return None;
            };
            match key {
                keyboard::Key::Named(keyboard::key::Named::Escape) => Some(Message::GoBack),
                _ => None,
            }
        });

        Subscription::batch(vec![
            exchange_streams,
            sidebar,
            window_events,
            tick,
            debug_terminal,
            hotkeys,
        ])
    }

    fn debug_window_label(&self, id: window::Id) -> &'static str {
        if id == self.main_window.id {
            "main"
        } else if self.debug_terminal_window == Some(id) {
            "debug_terminal"
        } else if self.active_dashboard().popout.contains_key(&id) {
            "popout"
        } else {
            "unknown"
        }
    }

    fn open_debug_terminal(&mut self) -> Task<Message> {
        if self.debug_terminal_window.is_some() || self.debug_terminal_embedded {
            return Task::none();
        }

        if self.windowing_mode.allows_native_popout() {
            let config = window::Settings {
                size: iced::Size::new(920.0, 520.0),
                position: window::Position::Centered,
                exit_on_close_request: false,
                min_size: Some(iced::Size::new(560.0, 320.0)),
                ..Default::default()
            };

            let (id, open) = window::open(config);
            open.map(move |_| Message::DebugTerminalOpened(id))
        } else {
            log::info!(
                "WINDOW DebugTerminalEmbedded | reason={reason}",
                reason = self.windowing_mode.reason()
            );
            self.debug_terminal_embedded = true;
            self.debug_terminal_logs = logger::debug_terminal_snapshot();
            if self.debug_terminal_auto_scroll {
                return self.scroll_debug_terminal_to_bottom();
            }
            Task::none()
        }
    }

    fn debug_terminal_view(&self) -> Element<'_, Message> {
        let filtered = self.filtered_debug_terminal_entries();
        let total = self.debug_terminal_logs.len();
        let visible = filtered.len();
        let error_count = filtered
            .iter()
            .filter(|e| e.level == Some(DebugLogLevel::Error))
            .count();
        let warn_count = filtered
            .iter()
            .filter(|e| e.level == Some(DebugLogLevel::Warn))
            .count();

        // Top row: title + stats
        let header = row![
            text("Debug terminal")
                .size(crate::style::text_size::SECTION)
                .width(Length::Fill),
            text(format!("{visible} visible / {total} total")).size(crate::style::text_size::SMALL),
            if error_count > 0 {
                text(format!(" {error_count} errors"))
                    .size(crate::style::text_size::SMALL)
                    .style(|theme: &iced::Theme| iced::widget::text::Style {
                        color: Some(theme.extended_palette().danger.base.color),
                    })
            } else {
                text("")
            },
            if warn_count > 0 {
                text(format!(" {warn_count} warnings"))
                    .size(crate::style::text_size::SMALL)
                    .style(|theme: &iced::Theme| iced::widget::text::Style {
                        color: Some(theme.extended_palette().primary.strong.color),
                    })
            } else {
                text("")
            },
        ]
        .align_y(Alignment::Center)
        .spacing(12);

        // Toolbar row
        let toolbar = row![
            button(text("Clear")).on_press(Message::DebugTerminalClear),
            button(text("Refresh")).on_press(Message::DebugTerminalRefresh),
            button(text("Copy all")).on_press(Message::DebugTerminalCopyAll),
            button(text("Copy visible")).on_press(Message::DebugTerminalCopyVisible),
            button(text("Open data folder")).on_press(Message::DataFolderRequested),
            iced::widget::checkbox(self.debug_terminal_auto_scroll)
                .label("Auto-scroll")
                .on_toggle(Message::DebugTerminalToggleAutoScroll),
            iced::widget::checkbox(self.debug_terminal_app_only)
                .label("App only")
                .on_toggle(Message::DebugTerminalToggleAppOnly),
            iced::widget::checkbox(self.debug_terminal_compact_mode)
                .label("Compact")
                .on_toggle(Message::DebugTerminalToggleCompactMode),
        ]
        .align_y(Alignment::Center)
        .spacing(8);

        // Filter row
        let level_checkboxes = row![
            iced::widget::checkbox(self.debug_terminal_level_filter.error)
                .label("Error")
                .on_toggle(|on| Message::DebugTerminalToggleLevel(DebugLogLevel::Error, on)),
            iced::widget::checkbox(self.debug_terminal_level_filter.warn)
                .label("Warn")
                .on_toggle(|on| Message::DebugTerminalToggleLevel(DebugLogLevel::Warn, on)),
            iced::widget::checkbox(self.debug_terminal_level_filter.info)
                .label("Info")
                .on_toggle(|on| Message::DebugTerminalToggleLevel(DebugLogLevel::Info, on)),
            iced::widget::checkbox(self.debug_terminal_level_filter.debug)
                .label("Debug")
                .on_toggle(|on| Message::DebugTerminalToggleLevel(DebugLogLevel::Debug, on)),
            iced::widget::checkbox(self.debug_terminal_level_filter.trace)
                .label("Trace")
                .on_toggle(|on| Message::DebugTerminalToggleLevel(DebugLogLevel::Trace, on)),
        ]
        .align_y(Alignment::Center)
        .spacing(8);

        let filters = row![
            text_input("Search logs...", &self.debug_terminal_search)
                .on_input(Message::DebugTerminalSearchChanged)
                .width(Length::Fill),
            level_checkboxes,
            pick_list(
                DebugLogCategory::ALL,
                Some(self.debug_terminal_category_filter),
                Message::DebugTerminalCategoryFilterChanged,
            )
            .width(110),
        ]
        .align_y(Alignment::Center)
        .spacing(8);

        // Log body
        let log_body: Element<'static, Message> = if filtered.is_empty() {
            text("No logs captured yet")
                .size(crate::style::text_size::SMALL)
                .font(iced::Font::MONOSPACE)
                .into()
        } else if self.debug_terminal_compact_mode {
            // Compact mode: structured rows
            let mut log_rows = column![].spacing(1);
            for entry in filtered {
                log_rows = log_rows.push(compact_log_row(entry));
            }
            log_rows.into()
        } else {
            // Raw mode: full lines
            let mut log_lines = column![].spacing(1);
            for entry in filtered {
                log_lines = log_lines.push(
                    text(entry.raw)
                        .size(crate::style::text_size::SMALL)
                        .font(iced::Font::MONOSPACE)
                        .wrapping(iced::widget::text::Wrapping::None)
                        .style(debug_log_text_style(entry.level)),
                );
            }
            log_lines.into()
        };

        // Horizontal scrollable wraps the log body
        let h_scroll = scrollable::Scrollable::with_direction(
            container(log_body).width(Length::Shrink).padding(12),
            scrollable::Direction::Horizontal(
                scrollable::Scrollbar::new().width(8).scroller_width(6),
            ),
        )
        .id(DEBUG_TERMINAL_HSCROLL_ID);

        // Vertical scrollable wraps the horizontal one
        let v_scroll = scrollable::Scrollable::with_direction(
            h_scroll,
            scrollable::Direction::Vertical(
                scrollable::Scrollbar::new().width(8).scroller_width(6),
            ),
        )
        .id(DEBUG_TERMINAL_VSCROLL_ID);

        container(column![header, toolbar, filters, v_scroll].spacing(8))
            .width(Length::Fill)
            .height(Length::Fill)
            .padding(16)
            .style(style::dashboard_modal)
            .into()
    }

    fn filtered_debug_terminal_entries(&self) -> Vec<DebugLogEntry> {
        let search = self.debug_terminal_search.trim().to_lowercase();

        self.debug_terminal_logs
            .iter()
            .filter(|line| self.debug_terminal_level_filter.matches(line))
            .filter(|line| {
                if self.debug_terminal_app_only {
                    let entry = parse_debug_log_entry(line);
                    is_app_target(entry.target.as_deref())
                } else {
                    true
                }
            })
            .filter(|line| {
                if self.debug_terminal_category_filter != DebugLogCategory::All {
                    let entry = parse_debug_log_entry(line);
                    entry.category == self.debug_terminal_category_filter
                } else {
                    true
                }
            })
            .filter(|line| {
                if search.is_empty() {
                    true
                } else {
                    let entry = parse_debug_log_entry(line);
                    entry.raw.to_lowercase().contains(&search)
                        || entry.summary.to_lowercase().contains(&search)
                        || entry.event.to_lowercase().contains(&search)
                        || entry
                            .target
                            .as_deref()
                            .unwrap_or("")
                            .to_lowercase()
                            .contains(&search)
                        || format!("{}", entry.category)
                            .to_lowercase()
                            .contains(&search)
                }
            })
            .map(|line| parse_debug_log_entry(line))
            .collect()
    }

    fn scroll_debug_terminal_to_bottom(&self) -> Task<Message> {
        iced::widget::operation::snap_to(
            DEBUG_TERMINAL_VSCROLL_ID,
            iced::widget::scrollable::RelativeOffset { x: 0.0, y: 1.0 },
        )
    }

    fn active_dashboard(&self) -> &Dashboard {
        let active_layout = self
            .layout_manager
            .active_layout_id()
            .expect("No active layout");
        self.layout_manager
            .get(active_layout.unique)
            .map(|layout| &layout.dashboard)
            .expect("No active dashboard")
    }

    fn active_dashboard_mut(&mut self) -> &mut Dashboard {
        let active_layout = self
            .layout_manager
            .active_layout_id()
            .expect("No active layout");
        self.layout_manager
            .get_mut(active_layout.unique)
            .map(|layout| &mut layout.dashboard)
            .expect("No active dashboard")
    }

    fn load_layout(&mut self, layout_uid: uuid::Uuid, main_window: window::Id) -> Task<Message> {
        if let Err(err) = self.layout_manager.set_active_layout(layout_uid) {
            log::error!("Failed to set active layout: {}", err);
            return Task::none();
        }

        self.layout_manager
            .park_inactive_layouts(layout_uid, main_window);

        let windowing_mode = self.windowing_mode;
        self.layout_manager
            .get_mut(layout_uid)
            .map(|layout| {
                layout
                    .dashboard
                    .load_layout(main_window, windowing_mode)
                    .map(move |msg| Message::Dashboard {
                        layout_id: Some(layout_uid),
                        event: msg,
                    })
            })
            .unwrap_or_else(|| {
                log::error!("Active layout missing after selection: {}", layout_uid);
                Task::none()
            })
    }

    fn view_with_modal<'a>(
        &'a self,
        base: Element<'a, Message>,
        dashboard: &'a Dashboard,
        menu: sidebar::Menu,
    ) -> Element<'a, Message> {
        let sidebar_pos = self.sidebar.position();

        match menu {
            sidebar::Menu::Settings => {
                let settings_modal = {
                    let theme_picklist = {
                        let mut themes: Vec<iced::Theme> = iced_core::Theme::ALL.to_vec();

                        let default_theme = iced_core::Theme::Custom(default_theme().into());
                        themes.push(default_theme);

                        if let Some(custom_theme) = &self.theme_editor.custom_theme {
                            themes.push(custom_theme.clone());
                        }

                        pick_list(themes, Some(self.theme.0.clone()), |theme| {
                            Message::ThemeSelected(theme)
                        })
                    };

                    let toggle_theme_editor = button(text("Theme editor")).on_press(
                        Message::Sidebar(dashboard::sidebar::Message::ToggleSidebarMenu(Some(
                            sidebar::Menu::ThemeEditor,
                        ))),
                    );

                    let toggle_network_editor = button(text("Network")).on_press(Message::Sidebar(
                        dashboard::sidebar::Message::ToggleSidebarMenu(Some(
                            sidebar::Menu::Network,
                        )),
                    ));

                    let timezone_picklist = pick_list(
                        [data::UserTimezone::Utc, data::UserTimezone::Local],
                        Some(self.timezone),
                        Message::SetTimezone,
                    );

                    let size_in_quote_currency_checkbox = {
                        let is_active = match self.volume_size_unit {
                            exchange::SizeUnit::Quote => true,
                            exchange::SizeUnit::Base => false,
                        };

                        let checkbox = iced::widget::checkbox(is_active)
                            .label("Size in quote currency")
                            .on_toggle(|checked| {
                                let on_dialog_confirm = Message::ApplyVolumeSizeUnit(if checked {
                                    exchange::SizeUnit::Quote
                                } else {
                                    exchange::SizeUnit::Base
                                });

                                let confirm_dialog = screen::ConfirmDialog::new(
                                    "Changing size display currency requires application restart"
                                        .to_string(),
                                    Box::new(on_dialog_confirm.clone()),
                                )
                                .with_confirm_btn_text("Restart now".to_string());

                                Message::ToggleDialogModal(Some(confirm_dialog))
                            });

                        tooltip(
                            checkbox,
                            Some(
                                "Display sizes/volumes in quote currency (USD)\nHas no effect on inverse perps or open interest",
                            ),
                            TooltipPosition::Top,
                        )
                    };

                    let sidebar_pos_picklist = pick_list(
                        [sidebar::Position::Left, sidebar::Position::Right],
                        Some(sidebar_pos),
                        |pos| {
                            Message::Sidebar(dashboard::sidebar::Message::SetSidebarPosition(pos))
                        },
                    );

                    let scale_factor = {
                        let current_value: f32 = self.ui_scale_factor.into();

                        let decrease_btn = if current_value > data::config::MIN_SCALE {
                            button(text("-"))
                                .on_press(Message::ScaleFactorChanged((current_value - 0.1).into()))
                        } else {
                            button(text("-"))
                        };

                        let increase_btn = if current_value < data::config::MAX_SCALE {
                            button(text("+"))
                                .on_press(Message::ScaleFactorChanged((current_value + 0.1).into()))
                        } else {
                            button(text("+"))
                        };

                        container(
                            row![
                                decrease_btn,
                                text(format!("{:.0}%", current_value * 100.0))
                                    .size(crate::style::text_size::SECTION),
                                increase_btn,
                            ]
                            .align_y(Alignment::Center)
                            .spacing(8)
                            .padding(4),
                        )
                        .style(style::modal_container)
                    };

                    let trade_fetch_checkbox = {
                        let is_active = connector::fetcher::is_trade_fetch_enabled();

                        let checkbox = iced::widget::checkbox(is_active)
                            .label("Fetch trades (Binance)")
                            .on_toggle(|checked| {
                                if checked {
                                    let confirm_dialog = screen::ConfirmDialog::new(
                                        "This might be unreliable and take some time to complete. Proceed?"
                                            .to_string(),
                                        Box::new(Message::ToggleTradeFetch(true)),
                                    );
                                    Message::ToggleDialogModal(Some(confirm_dialog))
                                } else {
                                    Message::ToggleTradeFetch(false)
                                }
                            });

                        tooltip(
                            checkbox,
                            Some("Try to fetch trades for footprint charts"),
                            TooltipPosition::Top,
                        )
                    };

                    let debug_terminal_checkbox = {
                        let checkbox = iced::widget::checkbox(self.debug_terminal_enabled)
                            .label("Debug terminal")
                            .on_toggle(Message::ToggleDebugTerminal);

                        tooltip(
                            checkbox,
                            Some("Open a popup terminal with detailed application logs"),
                            TooltipPosition::Top,
                        )
                    };

                    let open_data_folder = {
                        let button =
                            button(text("Open data folder")).on_press(Message::DataFolderRequested);

                        tooltip(
                            button,
                            Some("Open the folder where the data & config is stored"),
                            TooltipPosition::Top,
                        )
                    };

                    let version_info = {
                        let (version_label, commit_label) = version::app_build_version_parts();

                        let github_link_button =
                            button(text(version_label).size(crate::style::text_size::EMPHASIS))
                                .padding(0)
                                .style(style::button::text_link)
                                .on_press(Message::OpenUrlRequested(Cow::Borrowed(
                                    version::GITHUB_REPOSITORY_URL,
                                )));

                        let github_button: Element<'_, Message> = iced::widget::tooltip(
                            github_link_button,
                            container(
                                row![
                                    text("GitHub"),
                                    style::icon_text(style::Icon::ExternalLink, 12),
                                ]
                                .spacing(4)
                                .align_y(Alignment::Center),
                            )
                            .style(style::tooltip)
                            .padding(8),
                            TooltipPosition::Top,
                        )
                        .into();

                        if let (Some(commit_label), Some(commit_url)) =
                            (commit_label, version::build_commit_url())
                        {
                            let commit_button =
                                button(text(commit_label).size(crate::style::text_size::SMALL))
                                    .padding(0)
                                    .style(style::button::text_link_secondary)
                                    .on_press(Message::OpenUrlRequested(Cow::Owned(commit_url)));

                            column![github_button, commit_button]
                                .spacing(2)
                                .align_x(Alignment::End)
                                .into()
                        } else {
                            github_button
                        }
                    };

                    let footer = column![
                        container(version_info)
                            .width(iced::Length::Fill)
                            .align_x(Alignment::End),
                    ]
                    .spacing(8);

                    let column_content = split_column![
                        column![open_data_folder,].spacing(8),
                        column![text("Sidebar position").size(crate::style::text_size::SECTION), sidebar_pos_picklist,].spacing(12),
                        column![text("Time zone").size(crate::style::text_size::SECTION), timezone_picklist,].spacing(12),
                        column![text("Market data").size(crate::style::text_size::SECTION), size_in_quote_currency_checkbox,].spacing(12),
                        column![text("Theme").size(crate::style::text_size::SECTION), theme_picklist,].spacing(12),
                        column![text("Interface scale").size(crate::style::text_size::SECTION), scale_factor,].spacing(12),
                        column![
                            text("Experimental").size(crate::style::text_size::SECTION),
                            column![
                                trade_fetch_checkbox,
                                debug_terminal_checkbox,
                                toggle_theme_editor,
                                toggle_network_editor
                            ]
                            .spacing(8),
                        ]
                        .spacing(12),
                        footer,
                        ; spacing = 16, align_x = Alignment::Start
                    ];

                    let content = scrollable::Scrollable::with_direction(
                        column_content,
                        scrollable::Direction::Vertical(
                            scrollable::Scrollbar::new().width(8).scroller_width(6),
                        ),
                    );

                    container(content)
                        .align_x(Alignment::Start)
                        .max_width(240)
                        .padding(24)
                        .style(style::dashboard_modal)
                };

                let (align_x, padding) = match sidebar_pos {
                    sidebar::Position::Left => (Alignment::Start, padding::left(44).bottom(4)),
                    sidebar::Position::Right => (Alignment::End, padding::right(44).bottom(4)),
                };

                let base_content = dashboard_modal(
                    base,
                    settings_modal,
                    Message::Sidebar(dashboard::sidebar::Message::ToggleSidebarMenu(None)),
                    padding,
                    Alignment::End,
                    align_x,
                );

                if let Some(dialog) = &self.confirm_dialog {
                    let dialog_content =
                        confirm_dialog_container(dialog.clone(), Message::ToggleDialogModal(None));

                    main_dialog_modal(
                        base_content,
                        dialog_content,
                        Message::ToggleDialogModal(None),
                    )
                } else {
                    base_content
                }
            }
            sidebar::Menu::Layout => {
                let main_window = self.main_window.id;

                let manage_pane = if let Some((window_id, pane_id)) = dashboard.focus {
                    let selected_pane_str =
                        if let Some(state) = dashboard.get_pane(main_window, window_id, pane_id) {
                            let link_group_name: String =
                                state.link_group.as_ref().map_or_else(String::new, |g| {
                                    " - Group ".to_string() + &g.to_string()
                                });

                            state.content.to_string() + &link_group_name
                        } else {
                            "".to_string()
                        };

                    let is_main_window = window_id == main_window;

                    let reset_pane_button = {
                        let btn = button(text("Reset").align_x(Alignment::Center))
                            .width(iced::Length::Fill);
                        if is_main_window {
                            let dashboard_msg = Message::Dashboard {
                                layout_id: None,
                                event: dashboard::Message::Pane(
                                    main_window,
                                    dashboard::pane::Message::ReplacePane(pane_id),
                                ),
                            };

                            btn.on_press(dashboard_msg)
                        } else {
                            btn
                        }
                    };
                    let split_pane_button = {
                        let btn = button(text("Split").align_x(Alignment::Center))
                            .width(iced::Length::Fill);
                        if is_main_window {
                            let dashboard_msg = Message::Dashboard {
                                layout_id: None,
                                event: dashboard::Message::Pane(
                                    main_window,
                                    dashboard::pane::Message::SplitPane(
                                        pane_grid::Axis::Horizontal,
                                        pane_id,
                                    ),
                                ),
                            };
                            btn.on_press(dashboard_msg)
                        } else {
                            btn
                        }
                    };

                    column![
                        text(selected_pane_str),
                        row![
                            tooltip(
                                reset_pane_button,
                                if is_main_window {
                                    Some("Reset selected pane")
                                } else {
                                    None
                                },
                                TooltipPosition::Top,
                            ),
                            tooltip(
                                split_pane_button,
                                if is_main_window {
                                    Some("Split selected pane horizontally")
                                } else {
                                    None
                                },
                                TooltipPosition::Top,
                            ),
                        ]
                        .spacing(8)
                    ]
                    .spacing(8)
                } else {
                    let reset_pane_button =
                        button(text("Reset").align_x(Alignment::Center)).width(iced::Length::Fill);
                    let split_pane_button =
                        button(text("Split").align_x(Alignment::Center)).width(iced::Length::Fill);

                    column![
                        text("No pane selected"),
                        row![
                            tooltip(reset_pane_button, None, TooltipPosition::Top),
                            tooltip(split_pane_button, None, TooltipPosition::Top),
                        ]
                        .spacing(8)
                    ]
                    .spacing(8)
                };

                let manage_layout_modal = {
                    let col = column![
                        manage_pane,
                        rule::horizontal(1.0).style(style::split_ruler),
                        self.layout_manager.view().map(Message::Layouts)
                    ];

                    container(col.align_x(Alignment::Center).spacing(20))
                        .width(260)
                        .padding(24)
                        .style(style::dashboard_modal)
                };

                let (align_x, padding) = match sidebar_pos {
                    sidebar::Position::Left => (Alignment::Start, padding::left(44).top(40)),
                    sidebar::Position::Right => (Alignment::End, padding::right(44).top(40)),
                };

                dashboard_modal(
                    base,
                    manage_layout_modal,
                    Message::Sidebar(dashboard::sidebar::Message::ToggleSidebarMenu(None)),
                    padding,
                    Alignment::Start,
                    align_x,
                )
            }
            sidebar::Menu::Audio => {
                let (align_x, padding) = match sidebar_pos {
                    sidebar::Position::Left => (Alignment::Start, padding::left(44).top(76)),
                    sidebar::Position::Right => (Alignment::End, padding::right(44).top(76)),
                };

                let trade_streams_list = dashboard.streams.trade_streams(None);

                dashboard_modal(
                    base,
                    self.audio_stream
                        .view(trade_streams_list)
                        .map(Message::AudioStream),
                    Message::Sidebar(dashboard::sidebar::Message::ToggleSidebarMenu(None)),
                    padding,
                    Alignment::Start,
                    align_x,
                )
            }
            sidebar::Menu::ThemeEditor => {
                let (align_x, padding) = match sidebar_pos {
                    sidebar::Position::Left => (Alignment::Start, padding::left(44).bottom(4)),
                    sidebar::Position::Right => (Alignment::End, padding::right(44).bottom(4)),
                };

                dashboard_modal(
                    base,
                    self.theme_editor
                        .view(&self.theme.0)
                        .map(Message::ThemeEditor),
                    Message::Sidebar(dashboard::sidebar::Message::ToggleSidebarMenu(None)),
                    padding,
                    Alignment::End,
                    align_x,
                )
            }
            sidebar::Menu::Network => {
                let (align_x, padding) = match sidebar_pos {
                    sidebar::Position::Left => (Alignment::Start, padding::left(44).bottom(4)),
                    sidebar::Position::Right => (Alignment::End, padding::right(44).bottom(4)),
                };

                let base_content = dashboard_modal(
                    base,
                    self.network.view().map(Message::NetworkManager),
                    Message::Sidebar(dashboard::sidebar::Message::ToggleSidebarMenu(None)),
                    padding,
                    Alignment::End,
                    align_x,
                );

                if let Some(dialog) = &self.confirm_dialog {
                    let dialog_content =
                        confirm_dialog_container(dialog.clone(), Message::ToggleDialogModal(None));

                    main_dialog_modal(
                        base_content,
                        dialog_content,
                        Message::ToggleDialogModal(None),
                    )
                } else {
                    base_content
                }
            }
        }
    }

    fn save_state_to_disk(&mut self, windows: &HashMap<window::Id, WindowSpec>) {
        self.active_dashboard_mut()
            .popout
            .iter_mut()
            .for_each(|(id, (_, window_spec))| {
                if let Some(new_window_spec) = windows.get(id) {
                    *window_spec = *new_window_spec;
                }
            });

        self.sidebar.sync_tickers_table_settings();

        let mut ser_layouts = vec![];
        for layout in &self.layout_manager.layouts {
            if let Some(layout) = self.layout_manager.get(layout.id.unique) {
                let serialized_dashboard = data::Dashboard::from(&layout.dashboard);
                ser_layouts.push(data::Layout {
                    name: layout.id.name.clone(),
                    dashboard: serialized_dashboard,
                });
            }
        }

        let layouts = data::Layouts {
            layouts: ser_layouts,
            active_layout: self
                .layout_manager
                .active_layout_id()
                .map(|layout| layout.name.to_string())
                .clone(),
        };

        let main_window_spec = windows
            .iter()
            .find(|(id, _)| **id == self.main_window.id)
            .map(|(_, spec)| *spec);

        let audio_cfg = data::AudioStream::from(&self.audio_stream);

        let proxy_cfg_persisted = self.network.proxy_cfg().map(|p| p.without_auth());

        let state = data::State::from_parts(
            layouts,
            self.theme.clone(),
            self.theme_editor.custom_theme.clone().map(data::Theme),
            main_window_spec,
            self.timezone,
            self.sidebar.state.clone(),
            self.ui_scale_factor,
            audio_cfg,
            connector::fetcher::is_trade_fetch_enabled(),
            self.volume_size_unit,
            proxy_cfg_persisted,
            self.debug_terminal_enabled,
        );

        match data::save_saved_state_atomic(&state) {
            Ok(()) => {
                log::info!("Persisted state to {}", data::SAVED_STATE_PATH);
            }
            Err(e) => {
                log::error!("SAVED_STATE SaveFailed | error={e}");
            }
        }
    }

    fn restart(&mut self) -> Task<Message> {
        let mut windows_to_close: Vec<window::Id> =
            self.active_dashboard().popout.keys().copied().collect();
        windows_to_close.push(self.main_window.id);

        let close_windows = Task::batch(
            windows_to_close
                .into_iter()
                .map(window::close)
                .collect::<Vec<_>>(),
        );

        let (new_state, init_task) = Flowsurface::new();
        *self = new_state;

        close_windows.chain(init_task)
    }
}

fn compact_log_row(entry: DebugLogEntry) -> Element<'static, Message> {
    let time_text = entry
        .timestamp
        .as_deref()
        .and_then(|ts| ts.split_whitespace().last())
        .unwrap_or("")
        .to_string();

    let level_str = match entry.level {
        Some(DebugLogLevel::Error) => "ERR",
        Some(DebugLogLevel::Warn) => "WRN",
        Some(DebugLogLevel::Info) => "INF",
        Some(DebugLogLevel::Debug) => "DBG",
        Some(DebugLogLevel::Trace) => "TRC",
        None => "---",
    };

    let level = entry.level;
    let category = entry.category;
    let cat_str = format!("{}", category);
    let event_str = if entry.event.is_empty() {
        "-".to_string()
    } else {
        entry.event
    };
    let summary_str = entry.summary;

    row![
        text(time_text)
            .size(crate::style::text_size::SMALL)
            .font(iced::Font::MONOSPACE)
            .width(Length::Fixed(100.0)),
        text(level_str)
            .size(crate::style::text_size::SMALL)
            .font(iced::Font::MONOSPACE)
            .width(Length::Fixed(32.0))
            .style(debug_log_text_style(level)),
        text(cat_str)
            .size(crate::style::text_size::SMALL)
            .font(iced::Font::MONOSPACE)
            .width(Length::Fixed(72.0))
            .style(move |theme: &iced::Theme| {
                let palette = theme.extended_palette();
                let color = match category {
                    DebugLogCategory::Fetch => Some(palette.primary.strong.color),
                    DebugLogCategory::Cache => Some(palette.secondary.strong.color),
                    DebugLogCategory::Ws => Some(palette.warning.strong.color),
                    DebugLogCategory::Chart => Some(palette.success.strong.color),
                    DebugLogCategory::Data => Some(palette.primary.base.color),
                    DebugLogCategory::ThirdParty => Some(palette.background.strongest.color),
                    _ => None,
                };
                iced::widget::text::Style { color }
            }),
        text(event_str)
            .size(crate::style::text_size::SMALL)
            .font(iced::Font::MONOSPACE)
            .width(Length::Fixed(80.0)),
        text(summary_str)
            .size(crate::style::text_size::SMALL)
            .font(iced::Font::MONOSPACE)
            .wrapping(iced::widget::text::Wrapping::None),
    ]
    .align_y(Alignment::Center)
    .spacing(8)
    .into()
}
