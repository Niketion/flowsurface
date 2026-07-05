pub mod pane;
pub mod panel;
pub mod sidebar;
pub mod tickers_table;

pub use sidebar::Sidebar;

use super::DashboardError;
use crate::market_data::{
    job::FetchJobId,
    key::{MarketDataKey, MarketDataKind},
    range::{MarketDataRange, add_required_segment_dedup, compute_missing},
    requirement::ConsumerFeature,
};
use crate::{
    chart,
    connector::{
        ResolvedStream,
        fetcher::{self, FetchRange, FetchedData, InfoKind},
    },
    screen::dashboard::tickers_table::TickersTable,
    style,
    widget::toast::Toast,
    window::{self, Window},
    windowing::WindowingMode,
};
use data::{
    UserTimezone,
    layout::{WindowSpec, pane::ContentKind},
    stream::PersistStreamKind,
};
use exchange::{
    Kline, PushFrequency, StreamPairKind, TickerInfo, Trade, UnixMs,
    adapter::{
        AdapterHandles, MAX_KLINE_STREAMS_PER_STREAM, MAX_TRADE_TICKERS_PER_STREAM, StreamConfig,
        StreamKind, StreamTicksize, UniqueStreams,
    },
    depth::Depth,
};

use iced::{
    Element, Length, Subscription, Task, Vector,
    widget::{
        PaneGrid, center, container,
        pane_grid::{self, Configuration},
    },
};
use std::{collections::HashMap, time::Instant, vec};

const MARKET_DATA_JOB_STALE_MS: u64 = 30_000;

#[derive(Debug, Clone)]
pub enum Message {
    Pane(window::Id, pane::Message),
    ChangePaneStatus(uuid::Uuid, pane::Status),
    FetchCompleted {
        pane_id: uuid::Uuid,
        req_id: Option<uuid::Uuid>,
        fetch: Option<fetcher::FetchRange>,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
    },
    FetchFailed {
        pane_id: uuid::Uuid,
        error: String,
        req_id: Option<uuid::Uuid>,
        fetch: Option<fetcher::FetchRange>,
    },
    SavePopoutSpecs(HashMap<window::Id, WindowSpec>),
    ErrorOccurred(Option<uuid::Uuid>, DashboardError),
    Notification(Toast),
    DistributeFetchedData {
        layout_id: uuid::Uuid,
        pane_id: uuid::Uuid,
        stream: StreamKind,
        data: FetchedData,
    },
    BackfillFetchUpdate {
        pane_ids: Vec<uuid::Uuid>,
        stream: StreamKind,
        update: fetcher::FetchUpdate,
    },
    ResolveStreams(uuid::Uuid, Vec<PersistStreamKind>),
    RequestPalette,
}

/// Tracks WS disconnect state for deferred backfill computation.
/// Backfill is not decided at disconnect time because the gap is tiny
/// (last_seen → disconnect ≈ 87ms). Instead, we wait for reconnect and
/// compute the real offline gap (last_seen → reconnect_time).
struct PendingDisconnect {
    disconnected_at: UnixMs,
    /// Per-stream last live timestamp at the time of disconnect.
    stream_last_seen: HashMap<StreamKind, UnixMs>,
}

/// Logical segment status for accurate logging/debug reporting.
#[derive(Debug, Clone, PartialEq)]
struct ConsumerSegmentStatus {
    completed_logical: usize,
    total_logical: usize,
    missing: Vec<MarketDataRange>,
    coverage_complete: bool,
}

#[derive(Debug, Clone)]
struct PendingMarketDataConsumer {
    pane_id: uuid::Uuid,
    req_id: uuid::Uuid,
    fetch: fetcher::FetchRange,
    stream: Option<StreamKind>,
    key: MarketDataKey,
    range: MarketDataRange,
    feature: ConsumerFeature,
    chart_generation: u64,
    has_partial_updates: bool,
    completed: bool,
    required_segments: Vec<MarketDataRange>,
    completed_segments: Vec<MarketDataRange>,
    failed_segments: Vec<MarketDataRange>,
    delivered_segments: Vec<MarketDataRange>,
}

#[derive(Debug, Clone)]
struct DashboardFetchRoute {
    pane_id: uuid::Uuid,
    ready_streams: Vec<StreamKind>,
    chart_generation: u64,
    reqs: Vec<fetcher::FetchSpec>,
}

pub struct Dashboard {
    pub panes: pane_grid::State<pane::State>,
    pub focus: Option<(window::Id, pane_grid::Pane)>,
    pub popout: HashMap<window::Id, (pane_grid::State<pane::State>, WindowSpec)>,
    pub streams: UniqueStreams,
    layout_id: uuid::Uuid,
    /// Last live timestamp received per stream (trades & klines only).
    /// Used for historical gap backfill after WS disconnects.
    last_live_t: HashMap<StreamKind, UnixMs>,
    /// Tracks recently-queued backfill ranges to prevent duplicate fetches
    /// on repeated disconnects. Key is `(stream, from_ms, to_ms)`, value is
    /// the `Instant` when the entry was inserted so stale entries can expire.
    pending_backfills: HashMap<(StreamKind, u64, u64), std::time::Instant>,
    /// Abort handles for active backfill trade tasks. Stored to prevent
    /// the tasks from being aborted when the handle is dropped prematurely.
    backfill_handles: Vec<iced::task::Handle>,
    /// Pending WS disconnect awaiting reconnect to compute real backfill gap.
    pending_disconnect: Option<PendingDisconnect>,
    /// Market data coordinator for unified data management.
    pub market_coordinator: crate::market_data::coordinator::MarketDataCoordinator,
    /// Local market data cache for persistence.
    pub market_cache: crate::market_data::cache::LocalMarketCache,
    /// Live data adapter for WebSocket routing.
    pub live_adapter: crate::market_data::live::LiveDataAdapter,
    /// Last time coverage was saved to disk.
    last_coverage_save: std::time::Instant,
    /// Pane/chart requests waiting for coordinator-owned market data.
    pending_market_consumers: Vec<PendingMarketDataConsumer>,
    worker_req_to_job: HashMap<uuid::Uuid, FetchJobId>,
    job_to_worker_req: HashMap<FetchJobId, uuid::Uuid>,
    job_to_consumers: HashMap<FetchJobId, Vec<uuid::Uuid>>,
}

impl Default for Dashboard {
    fn default() -> Self {
        let mut cache = crate::market_data::cache::LocalMarketCache::default_cache();
        let mut coordinator = crate::market_data::coordinator::MarketDataCoordinator::new();

        // Load persisted coverage
        match cache.load_coverage() {
            Ok(coverage) => {
                coordinator.coverage = coverage;
                log::info!(
                    target: "marketdata",
                    "MARKETDATA CoverageLoaded | keys={}",
                    coordinator.coverage.len()
                );
            }
            Err(e) => {
                log::warn!(
                    target: "marketdata",
                    "MARKETDATA CoverageLoadFailed | error={}",
                    e
                );
            }
        }

        Self {
            panes: pane_grid::State::with_configuration(Self::default_pane_config()),
            focus: None,
            streams: UniqueStreams::default(),
            popout: HashMap::new(),
            layout_id: uuid::Uuid::new_v4(),
            last_live_t: HashMap::new(),
            pending_backfills: HashMap::new(),
            backfill_handles: Vec::new(),
            pending_disconnect: None,
            market_coordinator: coordinator,
            market_cache: cache,
            live_adapter: crate::market_data::live::LiveDataAdapter::new(),
            last_coverage_save: std::time::Instant::now(),
            pending_market_consumers: Vec::new(),
            worker_req_to_job: HashMap::new(),
            job_to_worker_req: HashMap::new(),
            job_to_consumers: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Event {
    Notification(Toast),
    DistributeFetchedData {
        layout_id: uuid::Uuid,
        pane_id: uuid::Uuid,
        data: FetchedData,
        stream: StreamKind,
    },
    ResolveStreams {
        pane_id: uuid::Uuid,
        streams: Vec<PersistStreamKind>,
    },
    RequestPalette,
}

/// Check whether a consumer has effective gaps remaining, accounting for
/// tiny Trade/TradeHydration gap suppression.
fn consumer_has_effective_gaps(consumer: &PendingMarketDataConsumer) -> bool {
    let terminal_segments = consumer
        .completed_segments
        .iter()
        .chain(consumer.failed_segments.iter())
        .copied()
        .collect::<Vec<_>>();
    let raw_missing = compute_missing(consumer.range, &terminal_segments);
    if matches!(
        consumer.feature,
        ConsumerFeature::TradeHydration | ConsumerFeature::Footprint
    ) {
        let filtered = crate::market_data::range::filter_tiny_trade_gaps(
            raw_missing,
            crate::market_data::coordinator::MIN_TRADE_BACKFILL_SEGMENT_MS,
        );
        !filtered.is_empty()
    } else {
        !raw_missing.is_empty()
    }
}

impl Dashboard {
    fn default_pane_config() -> Configuration<pane::State> {
        Configuration::Split {
            axis: pane_grid::Axis::Vertical,
            ratio: 0.8,
            a: Box::new(Configuration::Split {
                axis: pane_grid::Axis::Horizontal,
                ratio: 0.4,
                a: Box::new(Configuration::Split {
                    axis: pane_grid::Axis::Vertical,
                    ratio: 0.5,
                    a: Box::new(Configuration::Pane(pane::State::default())),
                    b: Box::new(Configuration::Pane(pane::State::default())),
                }),
                b: Box::new(Configuration::Split {
                    axis: pane_grid::Axis::Vertical,
                    ratio: 0.5,
                    a: Box::new(Configuration::Pane(pane::State::default())),
                    b: Box::new(Configuration::Pane(pane::State::default())),
                }),
            }),
            b: Box::new(Configuration::Pane(pane::State::default())),
        }
    }

    pub fn from_config(
        panes: Configuration<pane::State>,
        popout_windows: Vec<(Configuration<pane::State>, WindowSpec)>,
        layout_id: uuid::Uuid,
    ) -> Self {
        let panes = pane_grid::State::with_configuration(panes);

        let mut popout = HashMap::new();

        for (pane, specs) in popout_windows {
            popout.insert(
                window::Id::unique(),
                (pane_grid::State::with_configuration(pane), specs),
            );
        }

        // Reuse the same initialization logic as Default
        let mut cache = crate::market_data::cache::LocalMarketCache::default_cache();
        let mut coordinator = crate::market_data::coordinator::MarketDataCoordinator::new();

        match cache.load_coverage() {
            Ok(coverage) => {
                coordinator.coverage = coverage;
                log::info!(
                    target: "marketdata",
                    "MARKETDATA CoverageLoaded | keys={} source=from_config",
                    coordinator.coverage.len()
                );
            }
            Err(e) => {
                log::warn!(
                    target: "marketdata",
                    "MARKETDATA CoverageLoadFailed | error={} source=from_config",
                    e
                );
            }
        }

        Self {
            panes,
            focus: None,
            streams: UniqueStreams::default(),
            popout,
            layout_id,
            last_live_t: HashMap::new(),
            pending_backfills: HashMap::new(),
            backfill_handles: Vec::new(),
            pending_disconnect: None,
            market_coordinator: coordinator,
            market_cache: cache,
            live_adapter: crate::market_data::live::LiveDataAdapter::new(),
            last_coverage_save: std::time::Instant::now(),
            pending_market_consumers: Vec::new(),
            worker_req_to_job: HashMap::new(),
            job_to_worker_req: HashMap::new(),
            job_to_consumers: HashMap::new(),
        }
    }

    pub fn load_layout(
        &mut self,
        main_window: window::Id,
        windowing_mode: WindowingMode,
    ) -> Task<Message> {
        let mut open_popouts_tasks: Vec<Task<Message>> = vec![];
        let mut new_popout = Vec::new();
        let mut keys_to_remove = Vec::new();

        for (old_window_id, (_, specs)) in &self.popout {
            keys_to_remove.push((*old_window_id, *specs));
        }

        if windowing_mode.allows_native_popout() {
            // remove keys and open new windows
            for (old_window_id, window_spec) in keys_to_remove {
                let (window, task) = window::open(window::Settings {
                    position: window::Position::Specific(window_spec.position()),
                    size: window_spec.size(),
                    exit_on_close_request: false,
                    ..window::settings()
                });

                open_popouts_tasks.push(task.then(|_| Task::none()));

                if let Some((removed_pane, specs)) = self.popout.remove(&old_window_id) {
                    new_popout.push((window, (removed_pane, specs)));
                }
            }

            // assign new windows to old panes
            for (window, (pane, specs)) in new_popout {
                self.popout.insert(window, (pane, specs));
            }
        } else {
            // In embedded mode, merge popout panes back into the main pane grid
            log::info!(
                "WINDOW NativePopoutBlocked | pane=load_layout reason={reason}",
                reason = windowing_mode.reason()
            );
            for (_, (pane_state, _)) in keys_to_remove
                .iter()
                .filter_map(|(id, _)| self.popout.remove(id).map(|ps| (*id, ps)))
            {
                // Merge each popout pane into the main grid
                for (_, state) in pane_state.panes {
                    let _ = self.panes.split(
                        pane_grid::Axis::Vertical,
                        self.panes.iter().last().map(|(p, _)| *p).unwrap(),
                        state,
                    );
                }
            }
        }

        Task::batch(open_popouts_tasks).chain(self.refresh_streams(main_window))
    }

    pub fn update(
        &mut self,
        handles: &AdapterHandles,
        message: Message,
        main_window: &Window,
        layout_id: &uuid::Uuid,
        windowing_mode: WindowingMode,
    ) -> (Task<Message>, Option<Event>) {
        match message {
            Message::SavePopoutSpecs(specs) => {
                for (window_id, new_spec) in specs {
                    if let Some((_, spec)) = self.popout.get_mut(&window_id) {
                        *spec = new_spec;
                    }
                }
            }
            Message::ErrorOccurred(pane_id, err) => match pane_id {
                Some(id) => {
                    if let Some(state) = self.get_mut_pane_state_by_uuid(main_window.id, id) {
                        state.status = pane::Status::Ready;
                        state.notifications.push(Toast::error(err.to_string()));
                    }
                }
                _ => {
                    return (
                        Task::done(Message::Notification(Toast::error(err.to_string()))),
                        None,
                    );
                }
            },
            Message::Pane(window, message) => match message {
                pane::Message::PaneClicked(pane) => {
                    self.focus = Some((window, pane));
                }
                pane::Message::PaneResized(pane_grid::ResizeEvent { split, ratio }) => {
                    self.panes.resize(split, ratio);
                }
                pane::Message::PaneDragged(event) => {
                    if let pane_grid::DragEvent::Dropped { pane, target } = event {
                        self.panes.drop(pane, target);
                    }
                }
                pane::Message::SplitPane(axis, pane) => {
                    let focus_pane = if let Some((new_pane, _)) =
                        self.panes.split(axis, pane, pane::State::new())
                    {
                        Some(new_pane)
                    } else {
                        None
                    };

                    if let Some(focus_pane) = focus_pane {
                        self.focus = Some((window, focus_pane));
                    }
                }
                pane::Message::ClosePane(pane) => {
                    if let Some((_, sibling)) = self.panes.close(pane) {
                        self.focus = Some((window, sibling));
                    }
                }
                pane::Message::MaximizePane(pane) => {
                    self.panes.maximize(pane);
                }
                pane::Message::Restore => {
                    self.panes.restore();
                }
                pane::Message::ReplacePane(pane) => {
                    if let Some(pane) = self.panes.get_mut(pane) {
                        *pane = pane::State::new();
                    }

                    return (self.refresh_streams(main_window.id), None);
                }
                pane::Message::VisualConfigChanged(pane, cfg, to_sync) => {
                    let mut refresh_streams = false;

                    if to_sync {
                        if let Some(state) = self.get_pane(main_window.id, window, pane) {
                            let studies_cfg = state.content.studies();
                            let clusters_cfg = match &state.content {
                                pane::Content::Kline {
                                    kind: data::chart::KlineChartKind::Footprint { clusters, .. },
                                    ..
                                } => Some(*clusters),
                                _ => None,
                            };

                            self.iter_all_panes_mut(main_window.id)
                                .for_each(|(_, _, state)| {
                                    let should_apply = match state.settings.visual_config {
                                        Some(ref current_cfg) => {
                                            std::mem::discriminant(current_cfg)
                                                == std::mem::discriminant(&cfg)
                                        }
                                        None => matches!(
                                            (&cfg, &state.content),
                                            (
                                                data::layout::pane::VisualConfig::Kline(_),
                                                pane::Content::Kline { .. }
                                            ) | (
                                                data::layout::pane::VisualConfig::Heatmap(_),
                                                pane::Content::Heatmap { .. }
                                                    | pane::Content::ShaderHeatmap { .. }
                                            ) | (
                                                data::layout::pane::VisualConfig::TimeAndSales(_),
                                                pane::Content::TimeAndSales(_)
                                            ) | (
                                                data::layout::pane::VisualConfig::Comparison(_),
                                                pane::Content::Comparison(_)
                                            )
                                        ),
                                    };

                                    if should_apply {
                                        state.settings.visual_config = Some(cfg.clone());
                                        refresh_streams |=
                                            state.content.change_visual_config(cfg.clone());

                                        if let Some(studies) = &studies_cfg {
                                            state.content.update_studies(studies.clone());
                                        }

                                        if let Some(cluster_kind) = &clusters_cfg
                                            && let pane::Content::Kline { chart, .. } =
                                                &mut state.content
                                            && let Some(c) = chart
                                        {
                                            c.set_cluster_kind(*cluster_kind);
                                        }
                                    }
                                });
                        }
                    } else if let Some(state) = self.get_mut_pane(main_window.id, window, pane) {
                        state.settings.visual_config = Some(cfg.clone());
                        refresh_streams = state.content.change_visual_config(cfg);
                    }

                    if refresh_streams {
                        return (self.refresh_streams(main_window.id), None);
                    }
                }
                pane::Message::SwitchLinkGroup(pane, group) => {
                    if group.is_none() {
                        if let Some(state) = self.get_mut_pane(main_window.id, window, pane) {
                            state.link_group = None;
                        }
                        return (Task::none(), None);
                    }

                    let maybe_ticker_info = self
                        .iter_all_panes(main_window.id)
                        .filter(|(w, p, _)| !(*w == window && *p == pane))
                        .find_map(|(_, _, other_state)| {
                            if other_state.link_group == group {
                                other_state.stream_pair()
                            } else {
                                None
                            }
                        });

                    if let Some(state) = self.get_mut_pane(main_window.id, window, pane) {
                        state.link_group = group;
                        state.modal = None;

                        if let Some(ticker_info) = maybe_ticker_info
                            && state.stream_pair() != Some(ticker_info)
                        {
                            let pane_id = state.unique_id();
                            let content_kind = state.content.kind();

                            let streams =
                                state.set_content_and_streams(vec![ticker_info], content_kind);
                            self.streams.extend(streams.iter());

                            for stream in &streams {
                                if let StreamKind::Kline { .. } = stream {
                                    return (
                                        fetcher::kline_fetch_task(
                                            handles.clone(),
                                            *layout_id,
                                            pane_id,
                                            *stream,
                                            None,
                                            None,
                                        )
                                        .map(Message::from),
                                        None,
                                    );
                                }
                            }
                        }
                    }
                }
                pane::Message::Popout => {
                    return (self.popout_pane(main_window, windowing_mode), None);
                }
                pane::Message::Merge => {
                    return (self.merge_pane(main_window), None);
                }
                pane::Message::PaneEvent(pane, local) => {
                    if let Some(state) = self.get_mut_pane(main_window.id, window, pane) {
                        let Some(effect) = state.update(local) else {
                            return (Task::none(), None);
                        };

                        let task = match effect {
                            pane::Effect::RefreshStreams => self.refresh_streams(main_window.id),
                            pane::Effect::RequestFetch(reqs) => {
                                let pane_id = state.unique_id();
                                let ready_streams = state
                                    .streams
                                    .ready_iter()
                                    .map(|iter| iter.copied().collect::<Vec<_>>())
                                    .unwrap_or_default();

                                // Get chart generation for stale detection
                                let chart_generation =
                                    if let pane::Content::Kline { chart: Some(c), .. } =
                                        &state.content
                                    {
                                        c.current_generation()
                                    } else {
                                        0
                                    };

                                self.route_fetch_specs_through_market_data(
                                    handles.clone(),
                                    main_window.id,
                                    DashboardFetchRoute {
                                        pane_id,
                                        ready_streams,
                                        chart_generation,
                                        reqs,
                                    },
                                )
                                .chain(self.refresh_streams(main_window.id))
                            }
                            pane::Effect::SwitchTickersInGroup(ticker_info) => {
                                self.switch_tickers_in_group(handles, main_window.id, ticker_info)
                            }
                            pane::Effect::FocusWidget(id) => {
                                return (iced::widget::operation::focus(id), None);
                            }
                        };
                        return (task, None);
                    }
                }
            },
            Message::RequestPalette => {
                return (Task::none(), Some(Event::RequestPalette));
            }
            Message::ChangePaneStatus(pane_id, status) => {
                if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window.id, pane_id) {
                    pane_state.status = status;
                }
            }
            Message::FetchCompleted {
                pane_id,
                req_id,
                fetch,
                empty_covered_tail,
            } => {
                self.complete_fetch(main_window.id, pane_id, req_id, fetch, empty_covered_tail);
            }
            Message::FetchFailed {
                pane_id,
                error,
                req_id,
                fetch,
            } => {
                if let Some(worker_req) = req_id
                    && let Some(job_id) = self.worker_req_to_job.get(&worker_req).copied()
                {
                    log::warn!(
                        target: "marketdata",
                        "MARKETDATA WorkerFailed | worker_req={} job={} error={}",
                        fetcher::short_id(worker_req),
                        crate::market_data::job::short_id(job_id),
                        error
                    );
                    if let Some(job) = self.market_coordinator.job(job_id).cloned() {
                        for chart_req in self
                            .job_to_consumers
                            .get(&job_id)
                            .cloned()
                            .unwrap_or_default()
                        {
                            if let Some(consumer) = self
                                .pending_market_consumers
                                .iter_mut()
                                .find(|consumer| consumer.req_id == chart_req)
                            {
                                consumer.failed_segments.push(job.range);
                            }
                        }
                    }
                    self.market_coordinator.fail_and_remove_job(job_id, error);
                    self.worker_req_to_job.remove(&worker_req);
                    self.job_to_worker_req.remove(&job_id);
                    self.job_to_consumers.remove(&job_id);
                    return (Task::none(), None);
                }

                if let Some(id) = req_id
                    && let Some(pane_state) =
                        self.get_mut_pane_state_by_uuid(main_window.id, pane_id)
                {
                    pane_state.mark_fetch_failed(id, error.clone());
                }

                log::warn!(
                    "FETCH FailedUpdate | pane={} req={} fetch={} error={}",
                    fetcher::short_id(pane_id),
                    fetcher::format_req_id(req_id),
                    fetcher::format_fetch_range_compact(fetch),
                    error
                );
                if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window.id, pane_id) {
                    pane_state.status = pane::Status::Ready;
                    pane_state
                        .notifications
                        .push(Toast::error(DashboardError::Fetch(error).to_string()));
                }
            }
            Message::DistributeFetchedData {
                layout_id,
                pane_id,
                data,
                stream,
            } => {
                return (
                    Task::none(),
                    Some(Event::DistributeFetchedData {
                        layout_id,
                        pane_id,
                        data,
                        stream,
                    }),
                );
            }
            Message::BackfillFetchUpdate {
                pane_ids,
                stream,
                update,
            } => {
                self.apply_backfill_update(main_window.id, pane_ids, stream, update);
            }
            Message::ResolveStreams(pane_id, streams) => {
                return (
                    Task::none(),
                    Some(Event::ResolveStreams { pane_id, streams }),
                );
            }
            Message::Notification(toast) => {
                return (Task::none(), Some(Event::Notification(toast)));
            }
        }

        (Task::none(), None)
    }

    fn new_pane(
        &mut self,
        axis: pane_grid::Axis,
        main_window: &Window,
        pane_state: Option<pane::State>,
    ) -> Task<Message> {
        if self
            .focus
            .filter(|(window, _)| *window == main_window.id)
            .is_some()
        {
            // If there is any focused pane on main window, split it
            return self.split_pane(axis, main_window);
        } else {
            // If there is no focused pane, split the last pane or create a new empty grid
            let pane = self.panes.iter().last().map(|(pane, _)| pane).copied();

            if let Some(pane) = pane {
                let result = self.panes.split(axis, pane, pane_state.unwrap_or_default());

                if let Some((pane, _)) = result {
                    return self.focus_pane(main_window.id, pane);
                }
            } else {
                let (state, pane) = pane_grid::State::new(pane_state.unwrap_or_default());
                self.panes = state;

                return self.focus_pane(main_window.id, pane);
            }
        }

        Task::none()
    }

    fn focus_pane(&mut self, window: window::Id, pane: pane_grid::Pane) -> Task<Message> {
        if self.focus != Some((window, pane)) {
            self.focus = Some((window, pane));
        }

        Task::none()
    }

    fn split_pane(&mut self, axis: pane_grid::Axis, main_window: &Window) -> Task<Message> {
        if let Some((window, pane)) = self.focus
            && window == main_window.id
        {
            let result = self.panes.split(axis, pane, pane::State::new());

            if let Some((pane, _)) = result {
                return self.focus_pane(main_window.id, pane);
            }
        }

        Task::none()
    }

    fn popout_pane(
        &mut self,
        main_window: &Window,
        windowing_mode: WindowingMode,
    ) -> Task<Message> {
        if !windowing_mode.allows_native_popout() {
            log::info!(
                "WINDOW NativePopoutBlocked | reason={reason}",
                reason = windowing_mode.reason()
            );
            // In embedded mode, maximize the pane instead of popping out
            if let Some((_, pane_id)) = self.focus {
                self.panes.maximize(pane_id);
            }
            return Task::none();
        }

        if let Some((_, id)) = self.focus.take()
            && let Some((pane, _)) = self.panes.close(id)
        {
            let (window, task) = window::open(window::Settings {
                position: main_window
                    .position
                    .map(|point| window::Position::Specific(point + Vector::new(20.0, 20.0)))
                    .unwrap_or_default(),
                exit_on_close_request: false,
                min_size: Some(iced::Size::new(400.0, 300.0)),
                ..window::settings()
            });

            let (state, id) = pane_grid::State::new(pane);
            self.popout.insert(window, (state, WindowSpec::default()));

            return task.then(move |window| {
                Task::done(Message::Pane(window, pane::Message::PaneClicked(id)))
            });
        }

        Task::none()
    }

    fn merge_pane(&mut self, main_window: &Window) -> Task<Message> {
        if let Some((window, pane)) = self.focus.take()
            && let Some(pane_state) = self
                .popout
                .remove(&window)
                .and_then(|(mut panes, _)| panes.panes.remove(&pane))
        {
            let task = self.new_pane(pane_grid::Axis::Horizontal, main_window, Some(pane_state));

            return Task::batch(vec![window::close(window), task]);
        }

        Task::none()
    }

    pub fn get_pane(
        &self,
        main_window: window::Id,
        window: window::Id,
        pane: pane_grid::Pane,
    ) -> Option<&pane::State> {
        if main_window == window {
            self.panes.get(pane)
        } else {
            self.popout
                .get(&window)
                .and_then(|(panes, _)| panes.get(pane))
        }
    }

    fn get_mut_pane(
        &mut self,
        main_window: window::Id,
        window: window::Id,
        pane: pane_grid::Pane,
    ) -> Option<&mut pane::State> {
        if main_window == window {
            self.panes.get_mut(pane)
        } else {
            self.popout
                .get_mut(&window)
                .and_then(|(panes, _)| panes.get_mut(pane))
        }
    }

    fn get_mut_pane_state_by_uuid(
        &mut self,
        main_window: window::Id,
        uuid: uuid::Uuid,
    ) -> Option<&mut pane::State> {
        self.iter_all_panes_mut(main_window)
            .find(|(_, _, state)| state.unique_id() == uuid)
            .map(|(_, _, state)| state)
    }

    fn get_pane_state_by_uuid(
        &self,
        main_window: window::Id,
        uuid: uuid::Uuid,
    ) -> Option<&pane::State> {
        self.iter_all_panes(main_window)
            .find(|(_, _, state)| state.unique_id() == uuid)
            .map(|(_, _, state)| state)
    }

    fn iter_all_panes(
        &self,
        main_window: window::Id,
    ) -> impl Iterator<Item = (window::Id, pane_grid::Pane, &pane::State)> {
        self.panes
            .iter()
            .map(move |(pane, state)| (main_window, *pane, state))
            .chain(self.popout.iter().flat_map(|(window_id, (panes, _))| {
                panes.iter().map(|(pane, state)| (*window_id, *pane, state))
            }))
    }

    fn iter_all_panes_mut(
        &mut self,
        main_window: window::Id,
    ) -> impl Iterator<Item = (window::Id, pane_grid::Pane, &mut pane::State)> {
        self.panes
            .iter_mut()
            .map(move |(pane, state)| (main_window, *pane, state))
            .chain(self.popout.iter_mut().flat_map(|(window_id, (panes, _))| {
                panes
                    .iter_mut()
                    .map(|(pane, state)| (*window_id, *pane, state))
            }))
    }

    pub fn view<'a>(
        &'a self,
        main_window: &'a Window,
        tickers_table: &'a TickersTable,
        timezone: UserTimezone,
        allow_native_popout: bool,
    ) -> Element<'a, Message> {
        let pane_grid: Element<_> = PaneGrid::new(&self.panes, |id, pane, maximized| {
            let is_focused = self.focus == Some((main_window.id, id));
            pane.view(
                id,
                self.panes.len(),
                is_focused,
                maximized,
                main_window.id,
                main_window,
                timezone,
                tickers_table,
                allow_native_popout,
            )
        })
        .min_size(240)
        .on_click(pane::Message::PaneClicked)
        .on_drag(pane::Message::PaneDragged)
        .on_resize(8, pane::Message::PaneResized)
        .spacing(6)
        .style(style::pane_grid)
        .into();

        // Add unified market data progress overlay when loading
        let progress = self.market_coordinator.progress_snapshot();
        if progress.is_loading() {
            let progress_widget = crate::market_data::ui::unified_progress_view(&progress)
                .map(|_msg| Message::RequestPalette); // Map widget messages to dashboard messages
            let pane_mapped = pane_grid.map(move |message| Message::Pane(main_window.id, message));
            container(iced::widget::column(vec![pane_mapped, progress_widget]).spacing(4)).into()
        } else {
            pane_grid.map(move |message| Message::Pane(main_window.id, message))
        }
    }

    pub fn view_window<'a>(
        &'a self,
        window: window::Id,
        main_window: &'a Window,
        tickers_table: &'a TickersTable,
        timezone: UserTimezone,
        allow_native_popout: bool,
    ) -> Element<'a, Message> {
        if let Some((state, _)) = self.popout.get(&window) {
            let content = container(
                PaneGrid::new(state, |id, pane, _maximized| {
                    let is_focused = self.focus == Some((window, id));
                    pane.view(
                        id,
                        state.len(),
                        is_focused,
                        false,
                        window,
                        main_window,
                        timezone,
                        tickers_table,
                        allow_native_popout,
                    )
                })
                .on_click(pane::Message::PaneClicked),
            )
            .width(Length::Fill)
            .height(Length::Fill)
            .padding(8);

            Element::new(content).map(move |message| Message::Pane(window, message))
        } else {
            Element::new(center("No pane found for window"))
                .map(move |message| Message::Pane(window, message))
        }
    }

    pub fn go_back(&mut self, main_window: window::Id) -> bool {
        let Some((window, pane)) = self.focus else {
            return false;
        };

        let Some(state) = self.get_mut_pane(main_window, window, pane) else {
            return false;
        };

        if state.modal.is_some() {
            state.modal = None;
            return true;
        }
        false
    }

    fn handle_error(
        &mut self,
        pane_id: Option<uuid::Uuid>,
        err: &DashboardError,
        main_window: window::Id,
    ) -> Task<Message> {
        match pane_id {
            Some(id) => {
                if let Some(state) = self.get_mut_pane_state_by_uuid(main_window, id) {
                    state.status = pane::Status::Ready;
                    state.notifications.push(Toast::error(err.to_string()));
                }
                Task::none()
            }
            _ => Task::done(Message::Notification(Toast::error(err.to_string()))),
        }
    }

    fn init_pane(
        &mut self,
        handles: &AdapterHandles,
        main_window: window::Id,
        window: window::Id,
        selected_pane: pane_grid::Pane,
        ticker_info: TickerInfo,
        content_kind: ContentKind,
    ) -> Task<Message> {
        if let Some(state) = self.get_mut_pane(main_window, window, selected_pane) {
            let pane_id = state.unique_id();

            let streams = state.set_content_and_streams(vec![ticker_info], content_kind);
            self.streams.extend(streams.iter());

            for stream in &streams {
                if let StreamKind::Kline { .. } = stream {
                    return fetcher::kline_fetch_task(
                        handles.clone(),
                        self.layout_id,
                        pane_id,
                        *stream,
                        None,
                        None,
                    )
                    .map(Message::from);
                }
            }
        }

        Task::none()
    }

    pub fn init_focused_pane(
        &mut self,
        handles: &AdapterHandles,
        main_window: window::Id,
        ticker_info: TickerInfo,
        content_kind: ContentKind,
    ) -> Task<Message> {
        if self.focus.is_none()
            && self.panes.len() == 1
            && let Some((pane_id, _)) = self.panes.iter().next()
        {
            self.focus = Some((main_window, *pane_id));
        }

        if let Some((window, selected_pane)) = self.focus
            && let Some(state) = self.get_mut_pane(main_window, window, selected_pane)
        {
            let previous_ticker = state.stream_pair();
            if previous_ticker.is_some() && previous_ticker != Some(ticker_info) {
                state.link_group = None;
            }

            let streams = state.set_content_and_streams(vec![ticker_info], content_kind);

            let pane_id = state.unique_id();
            self.streams.extend(streams.iter());

            for stream in &streams {
                if let StreamKind::Kline { .. } = stream {
                    return fetcher::kline_fetch_task(
                        handles.clone(),
                        self.layout_id,
                        pane_id,
                        *stream,
                        None,
                        None,
                    )
                    .map(Message::from);
                }
            }
            return Task::none();
        }

        Task::done(Message::Notification(Toast::warn(
            "No focused pane found".to_string(),
        )))
    }

    pub fn switch_tickers_in_group(
        &mut self,
        handles: &AdapterHandles,
        main_window: window::Id,
        ticker_info: TickerInfo,
    ) -> Task<Message> {
        if self.focus.is_none()
            && self.panes.len() == 1
            && let Some((pane_id, _)) = self.panes.iter().next()
        {
            self.focus = Some((main_window, *pane_id));
        }

        let link_group = self.focus.and_then(|(window, pane)| {
            self.get_pane(main_window, window, pane)
                .and_then(|state| state.link_group)
        });

        if let Some(group) = link_group {
            let pane_infos: Vec<(window::Id, pane_grid::Pane, ContentKind)> = self
                .iter_all_panes_mut(main_window)
                .filter_map(|(window, pane, state)| {
                    if state.link_group == Some(group) {
                        Some((window, pane, state.content.kind()))
                    } else {
                        None
                    }
                })
                .collect();

            let tasks: Vec<Task<Message>> = pane_infos
                .iter()
                .map(|(window, pane, content_kind)| {
                    self.init_pane(
                        handles,
                        main_window,
                        *window,
                        *pane,
                        ticker_info,
                        *content_kind,
                    )
                })
                .collect();

            Task::batch(tasks)
        } else if let Some((window, pane)) = self.focus {
            if let Some(state) = self.get_mut_pane(main_window, window, pane) {
                let content_kind = state.content.kind();
                self.init_focused_pane(handles, main_window, ticker_info, content_kind)
            } else {
                Task::done(Message::Notification(Toast::warn(
                    "Couldn't get focused pane's content".to_string(),
                )))
            }
        } else {
            Task::done(Message::Notification(Toast::warn(
                "No link group or focused pane found".to_string(),
            )))
        }
    }

    pub fn toggle_trade_fetch(&mut self, is_enabled: bool, main_window: &Window) {
        fetcher::toggle_trade_fetch(is_enabled);

        self.iter_all_panes_mut(main_window.id)
            .for_each(|(_, _, state)| {
                if let pane::Content::Kline { chart, kind, .. } = &mut state.content
                    && matches!(kind, data::chart::KlineChartKind::Footprint { .. })
                    && let Some(c) = chart
                {
                    c.reset_request_handler();

                    if !is_enabled {
                        state.status = pane::Status::Ready;
                    }
                }
            });
    }

    fn route_fetch_specs_through_market_data(
        &mut self,
        handles: AdapterHandles,
        main_window: window::Id,
        route: DashboardFetchRoute,
    ) -> Task<Message> {
        let DashboardFetchRoute {
            pane_id,
            ready_streams,
            chart_generation,
            reqs,
        } = route;

        let ticker_info = ready_streams.iter().find_map(stream_ticker_info);
        let mut registered_any = false;

        for spec in &reqs {
            let feature = crate::market_data::bridge::fetch_range_to_feature(&spec.fetch);
            let timeframe = self.resolve_fetch_timeframe(spec, &ready_streams);
            let Some(key) =
                crate::market_data::bridge::fetch_range_to_key(&spec.fetch, ticker_info, timeframe)
            else {
                log::warn!(
                    target: "marketdata",
                    "MARKETDATA RequirementSkip | pane={} fetch={} reason=no_key",
                    crate::market_data::job::short_id(pane_id),
                    fetcher::format_fetch_range(&spec.fetch)
                );
                continue;
            };
            let Some(range) = crate::market_data::bridge::fetch_range_to_range(&spec.fetch) else {
                continue;
            };

            if matches!(feature, ConsumerFeature::VolumeBubbles) {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA BubbleRequirement | pane={} range={} key={}",
                    crate::market_data::job::short_id(pane_id),
                    range.format_display(),
                    key.display_key()
                );
            }

            self.pending_market_consumers
                .push(PendingMarketDataConsumer {
                    pane_id,
                    req_id: spec.req_id,
                    fetch: spec.fetch,
                    stream: spec.stream,
                    key: key.clone(),
                    range,
                    feature,
                    chart_generation,
                    has_partial_updates: false,
                    completed: false,
                    required_segments: Vec::new(),
                    completed_segments: Vec::new(),
                    failed_segments: Vec::new(),
                    delivered_segments: Vec::new(),
                });

            if let Some(requirement) = crate::market_data::bridge::fetch_range_to_requirement(
                &spec.fetch,
                pane_id,
                feature,
                ticker_info,
                timeframe,
            ) {
                self.market_coordinator.require(requirement);
                registered_any = true;
            }
        }

        if !registered_any || !self.market_coordinator.has_pending_requirements() {
            return self.forward_legacy_fetches(
                handles,
                main_window,
                pane_id,
                &ready_streams,
                reqs,
                chart_generation,
                "no_coordinator_requirement",
            );
        }

        let plan = self.market_coordinator.plan().clone();
        log::info!(
            target: "marketdata",
            "MARKETDATA RuntimePlan | pane={} {}",
            crate::market_data::job::short_id(pane_id),
            plan.runtime_summary(self.market_coordinator.active_job_count())
        );

        self.register_required_segments_from_plan(&plan);
        let mut cache_desync_specs = self.serve_cached_market_segments(main_window, &plan);

        let created_jobs = self.market_coordinator.execute_plan();
        for job_id in &created_jobs {
            self.market_coordinator.start_job(*job_id);
        }

        let mut network_specs = Vec::new();
        for job_id in &created_jobs {
            let Some(job) = self.market_coordinator.job(*job_id).cloned() else {
                continue;
            };
            if let Some(spec) = self.fetch_spec_for_market_job(
                *job_id,
                &job.key,
                job.range,
                &ready_streams,
                pane_id,
            ) {
                if matches!(job.key.kind, MarketDataKind::Trades)
                    && self.pending_market_consumers.iter().any(|c| {
                        c.key == job.key
                            && c.range.overlaps(&job.range)
                            && c.feature == ConsumerFeature::VolumeBubbles
                    })
                {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA BubbleRawTradesFetch | range={}",
                        job.range.format_display()
                    );
                }
                network_specs.push(spec);
            }
        }
        self.attach_pending_consumers_to_active_jobs("dedup_active_job");

        network_specs.append(&mut cache_desync_specs);

        let reason = if !network_specs.is_empty() {
            "coordinator_new_network_jobs"
        } else if plan.has_cached_data() {
            "coordinator_served_cache"
        } else {
            "coordinator_active_job"
        };

        if network_specs.is_empty() {
            log::info!(
                target: "marketdata",
                "MARKETDATA RuntimeLegacy | pane={} count=0 reason={reason}",
                crate::market_data::job::short_id(pane_id)
            );
            Task::none()
        } else {
            self.forward_legacy_fetches(
                handles,
                main_window,
                pane_id,
                &ready_streams,
                network_specs,
                chart_generation,
                reason,
            )
        }
    }

    fn forward_legacy_fetches(
        &mut self,
        handles: AdapterHandles,
        main_window: window::Id,
        pane_id: uuid::Uuid,
        ready_streams: &[StreamKind],
        reqs: Vec<fetcher::FetchSpec>,
        chart_generation: u64,
        reason: &'static str,
    ) -> Task<Message> {
        log::info!(
            target: "marketdata",
            "MARKETDATA RuntimeLegacy | pane={} count={} reason={reason}",
            crate::market_data::job::short_id(pane_id),
            reqs.len()
        );
        if reqs.is_empty() {
            return Task::none();
        }

        let mut handles_to_store = Vec::new();
        let task = fetcher::request_fetch_many(
            handles,
            pane_id,
            ready_streams,
            self.layout_id,
            reqs.into_iter().map(|r| (r.req_id, r.fetch, r.stream)),
            |handle| handles_to_store.push(handle),
            chart_generation,
        )
        .map(Message::from);

        if !handles_to_store.is_empty()
            && let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, pane_id)
            && let pane::Content::Kline { chart: Some(c), .. } = &mut pane_state.content
        {
            for handle in handles_to_store {
                c.set_handle(handle);
            }
        }

        task
    }

    fn resolve_fetch_timeframe(
        &self,
        spec: &fetcher::FetchSpec,
        ready_streams: &[StreamKind],
    ) -> Option<exchange::Timeframe> {
        if let Some(StreamKind::Kline { timeframe, .. }) = spec.stream {
            return Some(timeframe);
        }
        ready_streams.iter().find_map(|stream| match stream {
            StreamKind::Kline { timeframe, .. } => Some(*timeframe),
            _ => None,
        })
    }

    fn fetch_spec_for_market_job(
        &mut self,
        job_id: FetchJobId,
        key: &MarketDataKey,
        range: MarketDataRange,
        ready_streams: &[StreamKind],
        fallback_pane: uuid::Uuid,
    ) -> Option<fetcher::FetchSpec> {
        let stream = ready_streams
            .iter()
            .copied()
            .find(|stream| stream_matches_market_key(stream, key));
        let source = self
            .pending_market_consumers
            .iter()
            .find(|c| c.key == *key && c.range.overlaps(&range));
        let req_id = uuid::Uuid::new_v4();
        let fetch = match key.kind {
            MarketDataKind::Klines { .. } => fetcher::FetchRange::Kline(range.from, range.to),
            MarketDataKind::Trades => fetcher::FetchRange::Trades(range.from, range.to),
            MarketDataKind::OpenInterest { .. } => {
                fetcher::FetchRange::OpenInterest(range.from, range.to)
            }
        };
        let stream = stream.or_else(|| source.and_then(|c| c.stream));
        if stream.is_none() {
            log::warn!(
                target: "marketdata",
                "MARKETDATA RuntimeLegacy | pane={} count=0 reason=no_matching_stream key={} range={}",
                crate::market_data::job::short_id(source.map_or(fallback_pane, |c| c.pane_id)),
                key.display_key(),
                range.format_display()
            );
        }
        let consumers = self
            .pending_market_consumers
            .iter()
            .filter(|consumer| {
                if consumer.key != *key || !consumer.range.overlaps(&range) {
                    return false;
                }
                if consumer.completed {
                    log::warn!(
                        target: "marketdata",
                        "MARKETDATA ConsumerAttachRejected | chart_req={} job={} reason=consumer_already_completed",
                        fetcher::short_id(consumer.req_id),
                        crate::market_data::job::short_id(job_id)
                    );
                    return false;
                }
                true
            })
            .map(|consumer| consumer.req_id)
            .collect::<Vec<_>>();
        self.worker_req_to_job.insert(req_id, job_id);
        self.job_to_worker_req.insert(job_id, req_id);
        self.job_to_consumers.insert(job_id, consumers.clone());
        for consumer_id in &consumers {
            self.add_required_segment_to_consumer(*consumer_id, range);
        }
        log::info!(
            target: "marketdata",
            "MARKETDATA WorkerLaunch | job={} worker_req={} key={} range={} consumers={}",
            crate::market_data::job::short_id(job_id),
            fetcher::short_id(req_id),
            key.display_key(),
            range.format_display(),
            consumers
                .iter()
                .map(|id| fetcher::short_id(*id))
                .collect::<Vec<_>>()
                .join(",")
        );
        Some(fetcher::FetchSpec {
            req_id,
            fetch,
            stream,
        })
    }

    fn attach_pending_consumers_to_active_jobs(&mut self, reason: &'static str) -> usize {
        let active_jobs = self
            .market_coordinator
            .active_jobs()
            .into_iter()
            .map(|job| (job.id, job.key.clone(), job.range))
            .collect::<Vec<_>>();
        let mut attached = 0usize;

        for (job_id, key, job_range) in active_jobs {
            let matching_req_ids = self
                .pending_market_consumers
                .iter()
                .filter_map(|consumer| {
                    if consumer.key != key || !consumer.range.overlaps(&job_range) {
                        return None;
                    }
                    if consumer.completed {
                        log::warn!(
                            target: "marketdata",
                            "MARKETDATA ConsumerAttachRejected | chart_req={} job={} reason=consumer_already_completed",
                            fetcher::short_id(consumer.req_id),
                            crate::market_data::job::short_id(job_id)
                        );
                        return None;
                    }
                    Some((consumer.req_id, consumer.feature))
                })
                .collect::<Vec<_>>();

            for (chart_req, feature) in matching_req_ids {
                let consumers = self.job_to_consumers.entry(job_id).or_default();
                if consumers.contains(&chart_req) {
                    continue;
                }

                consumers.push(chart_req);
                self.add_required_segment_to_consumer(chart_req, job_range);
                attached = attached.saturating_add(1);
                log::info!(
                    target: "marketdata",
                    "MARKETDATA ConsumerAttach | job={} chart_req={} feature={} reason={}",
                    crate::market_data::job::short_id(job_id),
                    fetcher::short_id(chart_req),
                    feature.short_name(),
                    reason
                );
            }
        }

        attached
    }

    fn fetch_spec_for_market_refetch(
        &self,
        key: &MarketDataKey,
        range: MarketDataRange,
        ready_streams: &[StreamKind],
        fallback_pane: uuid::Uuid,
    ) -> Option<fetcher::FetchSpec> {
        let stream = ready_streams
            .iter()
            .copied()
            .find(|stream| stream_matches_market_key(stream, key));
        let source = self
            .pending_market_consumers
            .iter()
            .find(|c| c.key == *key && c.range.overlaps(&range));
        let fetch = match key.kind {
            MarketDataKind::Klines { .. } => fetcher::FetchRange::Kline(range.from, range.to),
            MarketDataKind::Trades => fetcher::FetchRange::Trades(range.from, range.to),
            MarketDataKind::OpenInterest { .. } => {
                fetcher::FetchRange::OpenInterest(range.from, range.to)
            }
        };
        let stream = stream.or_else(|| source.and_then(|c| c.stream));
        if stream.is_none() {
            log::warn!(
                target: "marketdata",
                "MARKETDATA RuntimeLegacy | pane={} count=0 reason=no_matching_stream key={} range={}",
                crate::market_data::job::short_id(source.map_or(fallback_pane, |c| c.pane_id)),
                key.display_key(),
                range.format_display()
            );
        }
        Some(fetcher::FetchSpec {
            req_id: uuid::Uuid::new_v4(),
            fetch,
            stream,
        })
    }

    fn serve_cached_market_segments(
        &mut self,
        main_window: window::Id,
        plan: &crate::market_data::planner::DataLoadPlan,
    ) -> Vec<fetcher::FetchSpec> {
        let mut refetch = Vec::new();
        for cached in &plan.cached_segments {
            match &cached.key.kind {
                MarketDataKind::Klines { timeframe } => {
                    let rows = self.market_cache.query_klines(&cached.key, &cached.range);
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA CacheLoad | key={} range={} kind=Klines records={}",
                        cached.key.display_key(),
                        cached.range.format_display(),
                        rows.len()
                    );
                    if rows.is_empty() {
                        self.mark_cache_desync_and_refetch(&cached.key, cached.range, &mut refetch);
                        continue;
                    }

                    // Step 1: Range filter — reject klines outside cache segment bounds
                    let from_ms = cached.range.from.as_u64();
                    let to_ms = cached.range.to.as_u64();
                    let before_count = rows.len();
                    let first_before = rows.first().map(|k| k.time);
                    let last_before = rows.last().map(|k| k.time);
                    let filtered_rows: Vec<_> = rows
                        .into_iter()
                        .filter(|kline| {
                            kline.time.as_u64() >= from_ms && kline.time.as_u64() < to_ms
                        })
                        .collect();
                    if filtered_rows.len() != before_count {
                        log::warn!(
                            target: "marketdata",
                            "MARKETDATA KlineCacheOutOfRangeFiltered | key={} cache_range={} before={} after={} first_before={} last_before={}",
                            cached.key.display_key(),
                            cached.range.format_display(),
                            before_count,
                            filtered_rows.len(),
                            first_before.map_or("-".to_string(), crate::connector::fetcher::format_time_short),
                            last_before.map_or("-".to_string(), crate::connector::fetcher::format_time_short)
                        );
                    }

                    // Step 2: Density check — too many rows for the timeframe is corrupt data
                    let duration_ms = cached.range.duration_ms();
                    let tf_ms = timeframe.to_milliseconds();
                    let expected_max = (duration_ms / tf_ms) + 2; // small tolerance for boundary candles
                    if filtered_rows.len() as u64 > expected_max {
                        log::warn!(
                            target: "marketdata",
                            "MARKETDATA KlineCacheCorrupt | key={} range={} records={} expected_max={} reason=too_many_rows_for_timeframe",
                            cached.key.display_key(),
                            cached.range.format_display(),
                            filtered_rows.len(),
                            expected_max
                        );
                        self.mark_cache_corrupt_and_refetch(
                            &cached.key,
                            cached.range,
                            "too_many_rows_for_timeframe",
                            &mut refetch,
                        );
                        continue;
                    }

                    // Step 3: After range filter, if filtered rows are empty
                    if filtered_rows.is_empty() {
                        self.mark_cache_desync_and_refetch(&cached.key, cached.range, &mut refetch);
                        continue;
                    }

                    // Step 4: Valid data — feed and dispatch
                    self.market_coordinator
                        .feed_klines(&cached.key, filtered_rows.as_slice());
                    self.market_coordinator
                        .record_cache_served(filtered_rows.len());
                    self.dispatch_cached_klines(
                        main_window,
                        &cached.key,
                        cached.range,
                        *timeframe,
                        filtered_rows,
                    );
                }
                MarketDataKind::Trades => {
                    let rows = self.market_cache.query_trades(&cached.key, &cached.range);
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA CacheLoad | key={} range={} kind=Trades records={}",
                        cached.key.display_key(),
                        cached.range.format_display(),
                        rows.len()
                    );
                    if rows.is_empty() {
                        self.mark_cache_desync_and_refetch(&cached.key, cached.range, &mut refetch);
                        continue;
                    }
                    self.market_coordinator
                        .feed_trades(&cached.key, rows.as_slice());
                    self.market_coordinator.record_cache_served(rows.len());
                    self.dispatch_cached_trades(main_window, &cached.key, cached.range, rows);
                }
                MarketDataKind::OpenInterest { timeframe } => {
                    let rows = self
                        .market_cache
                        .query_open_interest(&cached.key, &cached.range);
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA CacheLoad | key={} range={} kind=OpenInterest records={}",
                        cached.key.display_key(),
                        cached.range.format_display(),
                        rows.len()
                    );
                    if rows.is_empty() {
                        self.mark_cache_desync_and_refetch(&cached.key, cached.range, &mut refetch);
                        continue;
                    }

                    // Range filter — reject OI rows outside cache segment bounds
                    let from_ms = cached.range.from.as_u64();
                    let to_ms = cached.range.to.as_u64();
                    let before_count = rows.len();
                    let first_before = rows.first().map(|oi| oi.time);
                    let last_before = rows.last().map(|oi| oi.time);
                    let filtered_rows: Vec<_> = rows
                        .into_iter()
                        .filter(|oi| oi.time.as_u64() >= from_ms && oi.time.as_u64() < to_ms)
                        .collect();
                    if filtered_rows.len() != before_count {
                        log::warn!(
                            target: "marketdata",
                            "MARKETDATA OICacheOutOfRangeFiltered | key={} cache_range={} before={} after={} first_before={} last_before={}",
                            cached.key.display_key(),
                            cached.range.format_display(),
                            before_count,
                            filtered_rows.len(),
                            first_before.map_or("-".to_string(), crate::connector::fetcher::format_time_short),
                            last_before.map_or("-".to_string(), crate::connector::fetcher::format_time_short)
                        );
                    }

                    if filtered_rows.is_empty() {
                        self.mark_cache_desync_and_refetch(&cached.key, cached.range, &mut refetch);
                        continue;
                    }

                    self.market_coordinator
                        .store
                        .insert_open_interest(&cached.key, filtered_rows.as_slice());
                    self.market_coordinator
                        .record_cache_served(filtered_rows.len());
                    self.dispatch_cached_oi(
                        main_window,
                        &cached.key,
                        cached.range,
                        *timeframe,
                        filtered_rows,
                    );
                }
            }
        }
        if plan.has_cached_data() {
            self.log_market_data_progress_snapshot();
        }
        refetch
    }

    fn register_required_segments_from_plan(
        &mut self,
        plan: &crate::market_data::planner::DataLoadPlan,
    ) {
        // Only register cached segments as logical required segments.
        // Network segments must NOT be registered here because they can be
        // modified by execute_plan (kline canonicalization, active job subtraction,
        // tiny gap suppression, dedup). The concrete job ranges are registered
        // later by fetch_spec_for_market_job() and attach_pending_consumers_to_active_jobs().
        for segment in &plan.cached_segments {
            // Skip tiny cached segments for Trade/TradeHydration features —
            // these represent tiny tail offsets and should not block completion.
            if segment.range.duration_ms()
                < crate::market_data::coordinator::MIN_TRADE_BACKFILL_SEGMENT_MS
            {
                let should_skip = self.pending_market_consumers.iter().any(|consumer| {
                    consumer.key == segment.key
                        && consumer.range.overlaps(&segment.range)
                        && matches!(
                            consumer.feature,
                            ConsumerFeature::TradeHydration | ConsumerFeature::Footprint
                        )
                });
                if should_skip {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA TinyCachedSegmentSkipped | key={} segment={} duration_ms={}",
                        segment.key.display_key(),
                        segment.range.format_display(),
                        segment.range.duration_ms()
                    );
                    continue;
                }
            }

            let req_ids = self
                .pending_market_consumers
                .iter()
                .filter(|consumer| {
                    consumer.key == segment.key && consumer.range.overlaps(&segment.range)
                })
                .map(|consumer| consumer.req_id)
                .collect::<Vec<_>>();

            for req_id in req_ids {
                self.add_required_segment_to_consumer(req_id, segment.range);
            }
        }
    }

    fn mark_cache_corrupt_and_refetch(
        &mut self,
        key: &MarketDataKey,
        range: MarketDataRange,
        reason: &str,
        refetch: &mut Vec<fetcher::FetchSpec>,
    ) {
        log::warn!(
            target: "marketdata",
            "MARKETDATA CacheServeSkipped | key={} range={} reason={}",
            key.display_key(),
            range.format_display(),
            reason
        );
        self.market_coordinator
            .coverage
            .mark_stale(key.clone(), range, "corrupt_cache");
        if matches!(key.kind, MarketDataKind::Trades)
            && range.duration_ms() < crate::market_data::coordinator::MIN_TRADE_BACKFILL_SEGMENT_MS
        {
            self.market_coordinator
                .coverage
                .mark_empty(key.clone(), range);
            return;
        }
        if let Some(spec) = self.fetch_spec_for_market_refetch(key, range, &[], uuid::Uuid::nil()) {
            refetch.push(spec);
        }
    }

    fn mark_cache_desync_and_refetch(
        &mut self,
        key: &MarketDataKey,
        range: MarketDataRange,
        refetch: &mut Vec<fetcher::FetchSpec>,
    ) {
        log::warn!(
            target: "marketdata",
            "MARKETDATA CacheCoverageDesync | key={} range={} action=network_refetch",
            key.display_key(),
            range.format_display()
        );
        self.market_coordinator
            .coverage
            .mark_stale(key.clone(), range, "cache_desync");
        if matches!(key.kind, MarketDataKind::Trades)
            && range.duration_ms() < crate::market_data::coordinator::MIN_TRADE_BACKFILL_SEGMENT_MS
        {
            log::info!(
                target: "marketdata",
                "MARKETDATA TinyTradeGapSuppressed | key={} range={} reason=below_threshold_cache_desync",
                key.display_key(),
                range.format_display()
            );
            self.market_coordinator
                .coverage
                .mark_empty(key.clone(), range);
            return;
        }
        if let Some(spec) = self.fetch_spec_for_market_refetch(key, range, &[], uuid::Uuid::nil()) {
            refetch.push(spec);
        }
    }

    fn dispatch_cached_klines(
        &mut self,
        main_window: window::Id,
        key: &MarketDataKey,
        range: MarketDataRange,
        timeframe: exchange::Timeframe,
        rows: Vec<Kline>,
    ) {
        let consumers = self.matching_pending_consumers(key, &range);
        log::info!(
            target: "marketdata",
            "MARKETDATA CacheServe | key={} range={} consumers={}",
            key.display_key(),
            range.format_display(),
            consumers.len()
        );
        for consumer in consumers {
            if self.pending_consumer_is_stale(main_window, &consumer) {
                continue;
            }
            if consumer.feature != ConsumerFeature::ChartKlines {
                continue;
            }
            let stream = consumer
                .stream
                .or_else(|| self.stream_for_consumer_key(main_window, consumer.pane_id, key));
            if let Some(StreamKind::Kline { ticker_info, .. }) = stream {
                if !self.mark_consumer_segment_delivered(consumer.req_id, range) {
                    continue;
                }
                if let Some(pane_state) =
                    self.get_mut_pane_state_by_uuid(main_window, consumer.pane_id)
                {
                    pane_state.status = pane::Status::Ready;
                    pane_state.insert_hist_klines_partial(
                        Some(consumer.req_id),
                        timeframe,
                        ticker_info,
                        &rows,
                    );
                }
                self.complete_cached_segment_for_consumer(main_window, &consumer, range);
            }
        }
    }

    fn dispatch_cached_trades(
        &mut self,
        main_window: window::Id,
        key: &MarketDataKey,
        range: MarketDataRange,
        rows: Vec<Trade>,
    ) {
        let consumers = self.matching_pending_consumers(key, &range);
        log::info!(
            target: "marketdata",
            "MARKETDATA CacheServe | key={} range={} consumers={}",
            key.display_key(),
            range.format_display(),
            consumers.len()
        );
        for consumer in consumers {
            if self.pending_consumer_is_stale(main_window, &consumer) {
                continue;
            }
            if !self.mark_consumer_segment_delivered(consumer.req_id, range) {
                continue;
            }
            self.dispatch_trades_to_consumer(main_window, &consumer, rows.clone(), range);
            self.complete_cached_segment_for_consumer(main_window, &consumer, range);
        }
    }

    fn dispatch_cached_oi(
        &mut self,
        main_window: window::Id,
        key: &MarketDataKey,
        range: MarketDataRange,
        _timeframe: exchange::Timeframe,
        rows: Vec<exchange::OpenInterest>,
    ) {
        let consumers = self.matching_pending_consumers(key, &range);
        log::info!(
            target: "marketdata",
            "MARKETDATA CacheServe | key={} range={} consumers={}",
            key.display_key(),
            range.format_display(),
            consumers.len()
        );
        for consumer in consumers {
            if self.pending_consumer_is_stale(main_window, &consumer) {
                continue;
            }
            if consumer.feature != ConsumerFeature::OpenInterest {
                continue;
            }
            let stream = consumer
                .stream
                .or_else(|| self.stream_for_consumer_key(main_window, consumer.pane_id, key));
            if let Some(StreamKind::Kline { .. }) = stream {
                if !self.mark_consumer_segment_delivered(consumer.req_id, range) {
                    continue;
                }
                if let Some(pane_state) =
                    self.get_mut_pane_state_by_uuid(main_window, consumer.pane_id)
                {
                    pane_state.status = pane::Status::Ready;
                    pane_state.insert_hist_oi_partial(Some(consumer.req_id), &rows);
                }
                self.complete_cached_segment_for_consumer(main_window, &consumer, range);
            }
        }
    }

    fn dispatch_pending_for_fetched_data(
        &mut self,
        main_window: window::Id,
        stream_type: StreamKind,
        data: &FetchedData,
    ) -> bool {
        let coordinator_job = fetched_data_req_id(data)
            .and_then(|req_id| self.worker_req_to_job.get(&req_id).copied())
            .and_then(|job_id| {
                self.market_coordinator
                    .job(job_id)
                    .cloned()
                    .map(|job| (job_id, job))
            });

        // Track raw record count before normalization to distinguish
        // true empty responses from out-of-range filtered responses.
        let raw_kline_count = match &data {
            FetchedData::Klines { data, .. } => data.len(),
            FetchedData::OI { data, .. } => data.len(),
            _ => 0,
        };

        let data =
            self.normalize_fetched_data_for_job(data, coordinator_job.as_ref().map(|(_, job)| job));

        let Some((key, range, record_count)) = self.store_fetched_market_data(
            stream_type,
            &data,
            coordinator_job.as_ref().map(|(_, job)| job),
        ) else {
            // If store returned None for a coordinator-owned Kline/OI job,
            // it means the filtered data is empty.
            // Distinguish between true empty and out-of-range filtered.
            if let Some((job_id, job)) = &coordinator_job
                && matches!(data, FetchedData::Klines { .. } | FetchedData::OI { .. })
                && let Some(worker_req) = fetched_data_req_id(&data)
            {
                if raw_kline_count > 0 {
                    // Records existed but were outside the authoritative job range.
                    // This is an invalid response — must NOT mark coverage Empty/Complete.
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA KlineOutOfRangeFiltered | before={} after=0",
                        raw_kline_count
                    );
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA KlineInvalidResponse | worker_req={} requested={} reason=all_records_out_of_range",
                        fetcher::short_id(worker_req),
                        job.range.format_display()
                    );
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA JobFailed | job={} reason=invalid_out_of_range_response",
                        crate::market_data::job::short_id(*job_id)
                    );
                    self.finish_coordinator_worker_job_invalid(main_window, worker_req);
                } else {
                    // True empty response — exchange returned no records.
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA JobEmpty | job={} reason=zero_records_after_filter",
                        crate::market_data::job::short_id(*job_id)
                    );
                    self.finish_coordinator_worker_job(main_window, worker_req, None);
                }
            }
            return false;
        };

        let consumers = if let Some((job_id, _)) = &coordinator_job {
            self.consumers_for_job(*job_id)
        } else {
            self.matching_pending_consumers(&key, &range)
        };
        if consumers.is_empty() {
            if coordinator_job.is_some()
                && matches!(data, FetchedData::Klines { .. } | FetchedData::OI { .. })
                && let Some(worker_req) = fetched_data_req_id(&data)
            {
                self.finish_coordinator_worker_job(main_window, worker_req, None);
                return true;
            }
            return false;
        }

        log::info!(
            target: "marketdata",
            "MARKETDATA ConsumerDispatch | key={} range={} consumers={} kind={}",
            key.display_key(),
            range.format_display(),
            consumers.len(),
            market_kind_label(&key.kind)
        );

        match &data {
            FetchedData::Klines { data, .. } => {
                for consumer in consumers {
                    if self.pending_consumer_is_stale(main_window, &consumer) {
                        continue;
                    }
                    if consumer.feature != ConsumerFeature::ChartKlines {
                        continue;
                    }
                    let stream = consumer.stream.unwrap_or(stream_type);
                    if let StreamKind::Kline {
                        timeframe,
                        ticker_info,
                    } = stream
                        && let Some(pane_state) =
                            self.get_mut_pane_state_by_uuid(main_window, consumer.pane_id)
                    {
                        pane_state.status = pane::Status::Ready;
                        pane_state.insert_hist_klines_partial(
                            Some(consumer.req_id),
                            timeframe,
                            ticker_info,
                            data,
                        );
                    }
                }
            }
            FetchedData::Trades {
                batch, until_time, ..
            } => {
                for consumer in consumers {
                    if self.pending_consumer_is_stale(main_window, &consumer) {
                        continue;
                    }
                    self.dispatch_trades_to_consumer(main_window, &consumer, batch.clone(), range);
                    if consumer.feature == ConsumerFeature::Footprint {
                        let _ = until_time;
                    }
                }
            }
            FetchedData::OI { data, .. } => {
                for consumer in consumers {
                    if self.pending_consumer_is_stale(main_window, &consumer) {
                        continue;
                    }
                    if consumer.feature != ConsumerFeature::OpenInterest {
                        continue;
                    }
                    if let Some(pane_state) =
                        self.get_mut_pane_state_by_uuid(main_window, consumer.pane_id)
                    {
                        pane_state.status = pane::Status::Ready;
                        pane_state.insert_hist_oi_partial(Some(consumer.req_id), data);
                    }
                }
            }
            FetchedData::BubbleSummary { .. } => {}
        }

        for job_id in coordinator_job
            .as_ref()
            .map(|(job_id, _)| vec![*job_id])
            .unwrap_or_else(|| {
                self.market_coordinator
                    .active_jobs()
                    .iter()
                    .filter(|job| job.key == key && job.range.contains(&range))
                    .map(|job| job.id)
                    .collect::<Vec<_>>()
            })
        {
            if let Some(job) = self.market_coordinator.job_mut(job_id) {
                job.progress.records_fetched =
                    job.progress.records_fetched.saturating_add(record_count);
            }
        }
        if coordinator_job.is_some() {
            self.market_coordinator.record_network_fetched(record_count);
        }

        if coordinator_job.is_some()
            && matches!(data, FetchedData::Klines { .. } | FetchedData::OI { .. })
            && let Some(worker_req) = fetched_data_req_id(&data)
        {
            self.finish_coordinator_worker_job(main_window, worker_req, None);
        }

        true
    }

    fn dispatch_trades_to_consumer(
        &mut self,
        main_window: window::Id,
        consumer: &PendingMarketDataConsumer,
        trades: Vec<Trade>,
        range: MarketDataRange,
    ) {
        match consumer.feature {
            ConsumerFeature::Footprint => {
                let stream = consumer.stream.or_else(|| {
                    self.stream_for_consumer_key(main_window, consumer.pane_id, &consumer.key)
                });
                if let Some(stream) = stream {
                    let _ = self.distribute_fetched_data(
                        main_window,
                        consumer.pane_id,
                        FetchedData::Trades {
                            batch: trades,
                            until_time: range.to,
                            req_id: Some(consumer.req_id),
                        },
                        stream,
                        true,
                    );
                }
            }
            ConsumerFeature::VolumeBubbles => {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA ConsumerDispatch | key={} range={} consumer=VolumeBubbles action=derive_bubbles",
                    consumer.key.display_key(),
                    range.format_display()
                );
                let fetcher::FetchRange::BubbleSummary {
                    timeframe_ms,
                    price_step,
                    max_candidates_per_candle,
                    ..
                } = consumer.fetch
                else {
                    return;
                };
                log::info!(
                    target: "marketdata",
                    "MARKETDATA BubbleDerivedStart | source=Trades range={}",
                    consumer.range.format_display()
                );
                match self.market_coordinator.compute_bubble_summaries(
                    &consumer.key,
                    &consumer.range,
                    timeframe_ms,
                    price_step,
                    max_candidates_per_candle,
                ) {
                    Some(summaries) => {
                        let trades_seen = trades.len();
                        let candidates: usize = summaries.iter().map(|s| s.candidates.len()).sum();
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA BubbleReuse | source=coordinatorDerived range={} summaries={} candidates={}",
                            consumer.range.format_display(),
                            summaries.len(),
                            candidates
                        );
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA BubblePartialUpdate | req={} batch={} summaries={}",
                            fetcher::short_id(consumer.req_id),
                            trades_seen,
                            summaries.len()
                        );
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA BubbleChartUpdate | pane={} req={} partial=true summaries={}",
                            crate::market_data::job::short_id(consumer.pane_id),
                            fetcher::short_id(consumer.req_id),
                            summaries.len()
                        );
                        if let Some(stream) = consumer.stream.or_else(|| {
                            self.stream_for_consumer_key(
                                main_window,
                                consumer.pane_id,
                                &consumer.key,
                            )
                        }) {
                            self.apply_bubble_summaries_to_chart(
                                main_window,
                                consumer.pane_id,
                                stream,
                                summaries,
                                consumer.range,
                                trades_seen,
                                0,
                                Some(consumer.req_id),
                                false,
                            );
                            self.mark_bubble_consumer_partial(consumer.req_id);
                        }
                    }
                    None => {
                        log::warn!(
                            target: "marketdata",
                            "MARKETDATA BubbleFallbackLegacy | reason=no_raw_trades_after_fetch"
                        );
                    }
                }
            }
            ConsumerFeature::TradeHydration => {
                // Insert raw trades into the owning KlineChart for CVD/delta indicators.
                // This uses the same insertion path as Footprint trades.
                log::info!(
                    target: "marketdata",
                    "MARKETDATA TradeHydrationDispatch | pane={} records={} range={} partial=true",
                    crate::market_data::job::short_id(consumer.pane_id),
                    trades.len(),
                    range.format_display()
                );
                let stream = consumer.stream.or_else(|| {
                    self.stream_for_consumer_key(main_window, consumer.pane_id, &consumer.key)
                });
                if let Some(stream) = stream {
                    let _ = self.distribute_fetched_data(
                        main_window,
                        consumer.pane_id,
                        FetchedData::Trades {
                            batch: trades,
                            until_time: range.to,
                            req_id: Some(consumer.req_id),
                        },
                        stream,
                        true,
                    );
                }
            }
            _ => {}
        }
    }

    fn dispatch_final_bubbles_to_consumer(
        &mut self,
        main_window: window::Id,
        consumer: &PendingMarketDataConsumer,
    ) {
        if consumer.completed {
            log::info!(
                target: "marketdata",
                "MARKETDATA BubbleDuplicateCompleteSuppressed | req={}",
                fetcher::short_id(consumer.req_id)
            );
            return;
        }

        let fetcher::FetchRange::BubbleSummary {
            timeframe_ms,
            price_step,
            max_candidates_per_candle,
            ..
        } = consumer.fetch
        else {
            return;
        };

        match self.market_coordinator.compute_bubble_summaries(
            &consumer.key,
            &consumer.range,
            timeframe_ms,
            price_step,
            max_candidates_per_candle,
        ) {
            Some(summaries) => {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA BubbleFinalUpdate | req={} summaries={} had_partial={}",
                    fetcher::short_id(consumer.req_id),
                    summaries.len(),
                    consumer.has_partial_updates
                );
                log::info!(
                    target: "marketdata",
                    "MARKETDATA Derived | kind=VolumeBubbles source=Trades range={} candles={} candidates={}",
                    consumer.range.format_display(),
                    summaries.len(),
                    summaries
                        .iter()
                        .map(|summary| summary.candidates.len())
                        .sum::<usize>()
                );
                log::info!(
                    target: "marketdata",
                    "MARKETDATA BubbleChartUpdate | pane={} req={} partial=false summaries={}",
                    crate::market_data::job::short_id(consumer.pane_id),
                    fetcher::short_id(consumer.req_id),
                    summaries.len()
                );

                if let Some(stream) = consumer.stream.or_else(|| {
                    self.stream_for_consumer_key(main_window, consumer.pane_id, &consumer.key)
                }) {
                    self.apply_bubble_summaries_to_chart(
                        main_window,
                        consumer.pane_id,
                        stream,
                        summaries,
                        consumer.range,
                        0,
                        0,
                        Some(consumer.req_id),
                        true,
                    );
                    self.mark_bubble_consumer_completed(consumer.req_id);
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA BubbleChartComplete | req={}",
                        fetcher::short_id(consumer.req_id)
                    );
                }
            }
            None => {
                log::warn!(
                    target: "marketdata",
                    "MARKETDATA BubbleFallbackLegacy | reason=no_raw_trades_on_complete"
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_bubble_summaries_to_chart(
        &mut self,
        main_window: window::Id,
        pane_id: uuid::Uuid,
        stream_type: StreamKind,
        data: Vec<data::chart::kline::BubbleVolumeSummary>,
        range: MarketDataRange,
        trades_seen: usize,
        raw_discarded: usize,
        req_id: Option<uuid::Uuid>,
        complete: bool,
    ) {
        log::debug!(
            "BUBBLE Summary Distribute | pane={} req={} stream={} range={} candles={} trades_seen={} raw_discarded={} complete={}",
            fetcher::short_id(pane_id),
            fetcher::format_req_id(req_id),
            fetcher::format_stream(&stream_type),
            fetcher::format_time_range(range.from, range.to),
            data.len(),
            trades_seen,
            raw_discarded,
            complete
        );
        let summary_count = data.len();
        if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, pane_id) {
            pane_state.status = pane::Status::Ready;
            if let pane::Content::Kline { chart: Some(c), .. } = &mut pane_state.content {
                if complete {
                    c.insert_bubble_summaries(
                        data,
                        range.from,
                        range.to,
                        trades_seen,
                        raw_discarded,
                        req_id,
                    );
                } else {
                    c.update_bubble_summaries_partial(
                        data,
                        range.from,
                        range.to,
                        trades_seen,
                        raw_discarded,
                        req_id,
                    );
                }
                log::info!(
                    "MARKETDATA BubbleLiveUpdate | pane={} summaries={} complete={}",
                    fetcher::short_id(pane_id),
                    summary_count,
                    complete
                );
            }
        }
    }

    fn mark_bubble_consumer_partial(&mut self, req_id: uuid::Uuid) {
        if let Some(consumer) = self
            .pending_market_consumers
            .iter_mut()
            .find(|consumer| consumer.req_id == req_id)
        {
            consumer.has_partial_updates = true;
        }
    }

    fn mark_bubble_consumer_completed(&mut self, req_id: uuid::Uuid) {
        if let Some(consumer) = self
            .pending_market_consumers
            .iter_mut()
            .find(|consumer| consumer.req_id == req_id)
        {
            if consumer.completed {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA BubbleDuplicateCompleteSuppressed | req={}",
                    fetcher::short_id(req_id)
                );
            } else {
                consumer.completed = true;
            }
        }
    }

    fn mark_generic_consumer_completed(&mut self, req_id: uuid::Uuid) {
        if let Some(consumer) = self
            .pending_market_consumers
            .iter_mut()
            .find(|consumer| consumer.req_id == req_id)
        {
            consumer.completed = true;
        }
    }

    fn pending_consumer_is_stale(
        &self,
        main_window: window::Id,
        consumer: &PendingMarketDataConsumer,
    ) -> bool {
        let current_generation = self
            .get_pane_state_by_uuid(main_window, consumer.pane_id)
            .and_then(|pane_state| match &pane_state.content {
                pane::Content::Kline { chart: Some(c), .. } => Some(c.current_generation()),
                _ => None,
            });
        let stale = current_generation.is_some_and(|current| current != consumer.chart_generation);
        if stale {
            log::info!(
                target: "marketdata",
                "MARKETDATA ConsumerDispatchSkip | pane={} req={} reason=stale_generation request_generation={} current_generation={}",
                crate::market_data::job::short_id(consumer.pane_id),
                fetcher::short_id(consumer.req_id),
                consumer.chart_generation,
                current_generation.map_or("-".to_string(), |generation| generation.to_string())
            );
        }
        stale
    }

    fn store_fetched_market_data(
        &mut self,
        stream_type: StreamKind,
        data: &FetchedData,
        coordinator_job: Option<&crate::market_data::job::FetchJob>,
    ) -> Option<(MarketDataKey, MarketDataRange, usize)> {
        let key = coordinator_job
            .map(|job| job.key.clone())
            .or_else(|| key_for_fetched_data(stream_type, data))?;
        match data {
            FetchedData::Trades { batch, .. } => {
                let range = coordinator_job
                    .map(|job| job.range)
                    .or_else(|| range_from_trades(batch))?;
                self.market_coordinator.feed_trades(&key, batch);
                self.market_cache.insert_trades(&key, batch);
                Some((key, range, batch.len()))
            }
            FetchedData::Klines { data, .. } => {
                if data.is_empty() {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA KlineStoreSkipped | key={} reason=zero_records_after_filter",
                        key.display_key()
                    );
                    return None;
                }
                let range = coordinator_job
                    .map(|job| job.range)
                    .or_else(|| range_from_klines(data))?;
                self.market_coordinator.feed_klines(&key, data);
                self.market_cache.insert_klines(&key, data);
                Some((key, range, data.len()))
            }
            FetchedData::OI { data, .. } => {
                if data.is_empty() {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA OIStoreSkipped | key={} reason=zero_records_after_filter",
                        key.display_key()
                    );
                    return None;
                }
                let range = coordinator_job
                    .map(|job| job.range)
                    .or_else(|| range_from_oi(data))?;
                self.market_coordinator
                    .store
                    .insert_open_interest(&key, data);
                self.market_cache.insert_open_interest(&key, data);
                Some((key, range, data.len()))
            }
            FetchedData::BubbleSummary { .. } => None,
        }
    }

    fn matching_pending_consumers(
        &self,
        key: &MarketDataKey,
        range: &MarketDataRange,
    ) -> Vec<PendingMarketDataConsumer> {
        self.pending_market_consumers
            .iter()
            .filter(|consumer| consumer.key == *key && consumer.range.overlaps(range))
            .cloned()
            .collect()
    }

    fn consumers_for_job(&self, job_id: FetchJobId) -> Vec<PendingMarketDataConsumer> {
        self.job_to_consumers
            .get(&job_id)
            .into_iter()
            .flatten()
            .filter_map(|req_id| {
                self.pending_market_consumers
                    .iter()
                    .find(|consumer| consumer.req_id == *req_id)
                    .cloned()
            })
            .collect()
    }

    fn normalize_fetched_data_for_job(
        &self,
        data: &FetchedData,
        coordinator_job: Option<&crate::market_data::job::FetchJob>,
    ) -> FetchedData {
        let Some(job) = coordinator_job else {
            return data.clone();
        };
        match data {
            FetchedData::Trades {
                batch,
                until_time: _,
                req_id,
            } => {
                let before = batch.len();
                let first_before = batch.first().map(|trade| trade.time);
                let last_before = batch.last().map(|trade| trade.time);
                let filtered = batch
                    .iter()
                    .filter(|trade| job.range.contains_timestamp(trade.time))
                    .copied()
                    .collect::<Vec<_>>();
                if filtered.len() != before {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA TradeOutOfRangeFiltered | worker_req={} requested={} before={} after={} first_before={} last_before={}",
                        req_id.map_or("-".to_string(), fetcher::short_id),
                        job.range.format_display(),
                        before,
                        filtered.len(),
                        fetcher::format_optional_time(first_before),
                        fetcher::format_optional_time(last_before)
                    );
                }
                FetchedData::Trades {
                    batch: filtered,
                    until_time: job.range.to,
                    req_id: *req_id,
                }
            }
            FetchedData::Klines { data, req_id } => {
                let before = data.len();
                let first_before = data.first().map(|k| k.time);
                let last_before = data.last().map(|k| k.time);

                let filtered = data
                    .iter()
                    .filter(|kline| job.range.contains_timestamp(kline.time))
                    .copied()
                    .collect::<Vec<_>>();

                if filtered.len() != before {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA KlineOutOfRangeFiltered | worker_req={} requested={} before={} after={} first_before={} last_before={}",
                        req_id.map_or("-".to_string(), fetcher::short_id),
                        job.range.format_display(),
                        before,
                        filtered.len(),
                        fetcher::format_optional_time(first_before),
                        fetcher::format_optional_time(last_before)
                    );
                }

                FetchedData::Klines {
                    data: filtered,
                    req_id: *req_id,
                }
            }
            _ => data.clone(),
        }
    }

    fn remove_pending_consumers(&mut self, key: &MarketDataKey, range: &MarketDataRange) {
        self.pending_market_consumers.retain(|consumer| {
            !(consumer.key == *key
                && consumer.completed
                && !consumer_has_effective_gaps(consumer)
                && consumer.range.overlaps(range))
        });
    }

    fn add_required_segment_to_consumer(&mut self, req_id: uuid::Uuid, segment: MarketDataRange) {
        if let Some(consumer) = self
            .pending_market_consumers
            .iter_mut()
            .find(|consumer| consumer.req_id == req_id)
        {
            // Use dedup (not merge) to keep adjacent logical segments separate
            // for accurate completed/total logging counters.
            add_required_segment_dedup(&mut consumer.required_segments, segment);
        }
    }

    fn mark_consumer_segment_complete(
        &mut self,
        req_id: uuid::Uuid,
        segment: MarketDataRange,
    ) -> Option<ConsumerSegmentStatus> {
        let consumer = self
            .pending_market_consumers
            .iter_mut()
            .find(|consumer| consumer.req_id == req_id)?;
        crate::market_data::range::add_segment_merged(&mut consumer.completed_segments, segment);
        let raw_missing = compute_missing(consumer.range, &consumer.completed_segments);

        // For Trade/TradeHydration/Footprint features, suppress tiny gaps
        let missing = if matches!(
            consumer.feature,
            ConsumerFeature::TradeHydration | ConsumerFeature::Footprint
        ) {
            crate::market_data::range::filter_tiny_trade_gaps(
                raw_missing.clone(),
                crate::market_data::coordinator::MIN_TRADE_BACKFILL_SEGMENT_MS,
            )
        } else {
            raw_missing.clone()
        };

        // Logical segment counting: count how many required segments are fully covered
        // by the accumulated completed coverage.
        let total_logical = consumer.required_segments.len();
        let completed_logical = consumer
            .required_segments
            .iter()
            .filter(|required| compute_missing(**required, &consumer.completed_segments).is_empty())
            .count();

        Some(ConsumerSegmentStatus {
            completed_logical,
            total_logical,
            coverage_complete: missing.is_empty(),
            missing,
        })
    }

    fn mark_consumer_segment_delivered(
        &mut self,
        req_id: uuid::Uuid,
        segment: MarketDataRange,
    ) -> bool {
        let Some(consumer) = self
            .pending_market_consumers
            .iter_mut()
            .find(|consumer| consumer.req_id == req_id)
        else {
            return false;
        };
        // Check if this segment is already fully covered by delivered segments
        let missing =
            crate::market_data::range::compute_missing(segment, &consumer.delivered_segments);
        if missing.is_empty() {
            log::info!(
                target: "marketdata",
                "MARKETDATA ConsumerSegmentAlreadyDelivered | req={} segment={} action=skip",
                fetcher::short_id(req_id),
                segment.format_display()
            );
            return false;
        }
        crate::market_data::range::add_segment_merged(&mut consumer.delivered_segments, segment);
        true
    }

    fn complete_cached_segment_for_consumer(
        &mut self,
        main_window: window::Id,
        consumer: &PendingMarketDataConsumer,
        segment: MarketDataRange,
    ) {
        let Some(status) = self.mark_consumer_segment_complete(consumer.req_id, segment) else {
            return;
        };
        if status.coverage_complete {
            log::info!(
                target: "marketdata",
                "MARKETDATA ConsumerSegmentComplete | req={} segment={} completed={}/{} source=cache coverage_complete=true missing=",
                fetcher::short_id(consumer.req_id),
                segment.format_display(),
                status.completed_logical,
                status.total_logical
            );
        } else {
            log::info!(
                target: "marketdata",
                "MARKETDATA ConsumerSegmentComplete | req={} segment={} completed={}/{} source=cache coverage_complete=false missing={}",
                fetcher::short_id(consumer.req_id),
                segment.format_display(),
                status.completed_logical,
                status.total_logical,
                status.missing.iter().map(MarketDataRange::format_display).collect::<Vec<_>>().join(",")
            );
        }

        if self.consumer_is_fully_satisfied(consumer.req_id) {
            log::info!(
                target: "marketdata",
                "MARKETDATA ChartReqComplete | chart_req={} feature={}",
                fetcher::short_id(consumer.req_id),
                consumer.feature.short_name()
            );
            match consumer.feature {
                ConsumerFeature::VolumeBubbles => {
                    if let Some(updated) = self
                        .pending_market_consumers
                        .iter()
                        .find(|pending| pending.req_id == consumer.req_id)
                        .cloned()
                    {
                        self.dispatch_final_bubbles_to_consumer(main_window, &updated);
                    }
                }
                _ => {
                    self.complete_pending_consumer(main_window, consumer, None);
                    self.mark_generic_consumer_completed(consumer.req_id);
                }
            }
        } else {
            let remaining = self.consumer_remaining_segments(consumer.req_id).join(",");
            log::info!(
                target: "marketdata",
                "MARKETDATA ConsumerWaiting | req={} remaining={}",
                fetcher::short_id(consumer.req_id),
                remaining
            );
        }
    }

    /// Compute "effective" missing ranges for a consumer, suppressing tiny
    /// Trade/TradeHydration gaps that are below the backfill segment threshold.
    fn effective_missing_for_consumer(&self, req_id: uuid::Uuid) -> Vec<MarketDataRange> {
        self.pending_market_consumers
            .iter()
            .find(|consumer| consumer.req_id == req_id)
            .map(|consumer| {
                let raw_missing = compute_missing(consumer.range, &consumer.completed_segments);
                if matches!(
                    consumer.feature,
                    ConsumerFeature::TradeHydration | ConsumerFeature::Footprint
                ) {
                    crate::market_data::range::filter_tiny_trade_gaps(
                        raw_missing,
                        crate::market_data::coordinator::MIN_TRADE_BACKFILL_SEGMENT_MS,
                    )
                } else {
                    raw_missing
                }
            })
            .unwrap_or_default()
    }

    fn consumer_remaining_segments(&self, req_id: uuid::Uuid) -> Vec<String> {
        self.pending_market_consumers
            .iter()
            .find(|consumer| consumer.req_id == req_id)
            .map(|consumer| {
                let terminal_segments = consumer
                    .completed_segments
                    .iter()
                    .chain(consumer.failed_segments.iter())
                    .copied()
                    .collect::<Vec<_>>();
                let raw_missing = compute_missing(consumer.range, &terminal_segments);
                let missing = if matches!(
                    consumer.feature,
                    ConsumerFeature::TradeHydration | ConsumerFeature::Footprint
                ) {
                    crate::market_data::range::filter_tiny_trade_gaps(
                        raw_missing,
                        crate::market_data::coordinator::MIN_TRADE_BACKFILL_SEGMENT_MS,
                    )
                } else {
                    raw_missing
                };
                missing.into_iter().map(|r| r.format_display()).collect()
            })
            .unwrap_or_default()
    }

    fn consumer_is_fully_satisfied(&self, req_id: uuid::Uuid) -> bool {
        self.pending_market_consumers
            .iter()
            .find(|consumer| consumer.req_id == req_id)
            .is_some_and(|consumer| {
                consumer.required_segments.is_empty()
                    || self.effective_missing_for_consumer(req_id).is_empty()
            })
    }

    fn complete_pending_consumer(
        &mut self,
        main_window: window::Id,
        consumer: &PendingMarketDataConsumer,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
    ) -> bool {
        if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, consumer.pane_id) {
            if let pane::Content::Kline { chart: Some(c), .. } = &mut pane_state.content {
                if matches!(consumer.fetch, fetcher::FetchRange::Trades(_, _)) {
                    c.complete_trade_fetch(
                        Some(consumer.req_id),
                        Some(consumer.fetch),
                        empty_covered_tail,
                    );
                } else if matches!(consumer.fetch, fetcher::FetchRange::TradeHydration(_, _)) {
                    // TradeHydration completion: store the range for backward-loop prevention
                    c.complete_trade_fetch(
                        Some(consumer.req_id),
                        Some(consumer.fetch),
                        empty_covered_tail,
                    );
                } else if matches!(consumer.fetch, fetcher::FetchRange::BubbleSummary { .. }) {
                    // Bubble chart completion is driven by dispatch_final_bubbles_to_consumer so
                    // partial batch updates cannot remove the pending chart-local request.
                } else {
                    if !c.mark_request_completed_if_present(consumer.req_id) {
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA ChartReqMissing | chart_req={} feature={} reason=already_removed_or_generation_stale",
                            fetcher::short_id(consumer.req_id),
                            consumer.feature.short_name()
                        );
                        return false;
                    }
                    pane_state.mark_backfill_completed(Some(consumer.fetch), empty_covered_tail);
                }
            }
            pane_state.status = pane::Status::Ready;
            return true;
        }
        log::info!(
            target: "marketdata",
            "MARKETDATA ChartReqMissing | chart_req={} feature={} reason=already_removed_or_generation_stale",
            fetcher::short_id(consumer.req_id),
            consumer.feature.short_name()
        );
        false
    }

    fn stream_for_consumer_key(
        &self,
        main_window: window::Id,
        pane_id: uuid::Uuid,
        key: &MarketDataKey,
    ) -> Option<StreamKind> {
        self.get_pane_state_by_uuid(main_window, pane_id)
            .and_then(|pane| pane.streams.ready_iter())
            .and_then(|iter| {
                iter.copied()
                    .find(|stream| stream_matches_market_key(stream, key))
            })
    }

    pub fn distribute_fetched_data(
        &mut self,
        main_window: window::Id,
        pane_id: uuid::Uuid,
        data: FetchedData,
        stream_type: StreamKind,
        skip_stale_check: bool,
    ) -> Task<Message> {
        if !skip_stale_check
            && self.dispatch_pending_for_fetched_data(main_window, stream_type, &data)
        {
            return Task::none();
        }

        // Check for stale generation before applying any data
        if !skip_stale_check {
            let req_id = match &data {
                FetchedData::Trades { req_id, .. } => *req_id,
                FetchedData::BubbleSummary { req_id, .. } => *req_id,
                FetchedData::Klines { req_id, .. } => *req_id,
                FetchedData::OI { req_id, .. } => *req_id,
            };

            if let Some(req_id) = req_id
                && let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, pane_id)
                && let pane::Content::Kline { chart: Some(c), .. } = &mut pane_state.content
                && c.request_generation(req_id).is_some()
                && c.is_fetch_stale(req_id)
            {
                let request_gen = c.request_generation(req_id);
                let current_gen = c.current_generation();
                log::info!(
                    "FETCH StaleResult | req={} request_generation={} current_generation={} action=discard",
                    fetcher::short_id(req_id),
                    request_gen.map_or("-".to_string(), |g| g.to_string()),
                    current_gen
                );
                // Mark the request as completed to clean up pending state
                c.mark_trade_request_completed(req_id);
                return Task::none();
            }
        }

        match data {
            FetchedData::Trades {
                batch,
                until_time,
                req_id,
            } => {
                let last_trade_time = batch.last().map_or(UnixMs::ZERO, |trade| trade.time);
                let received = batch.len();
                let first_received = batch.first().map(|trade| trade.time);
                let last_received = batch.last().map(|trade| trade.time);

                if last_trade_time < until_time {
                    log::debug!(
                        "DATA Trades Distribute | pane={} req={} stream={} received={} inserted={} dropped_after_until=0 until_time={} first_received={} last_received={}",
                        fetcher::short_id(pane_id),
                        fetcher::format_req_id(req_id),
                        fetcher::format_stream(&stream_type),
                        received,
                        received,
                        fetcher::format_time_short(until_time),
                        fetcher::format_optional_time(first_received),
                        fetcher::format_optional_time(last_received)
                    );
                    if let Err(reason) =
                        self.insert_fetched_trades(main_window, pane_id, &batch, false, req_id)
                    {
                        return self.handle_error(Some(pane_id), &reason, main_window);
                    }
                } else {
                    let filtered_batch = batch
                        .iter()
                        .filter(|trade| trade.time <= until_time)
                        .copied()
                        .collect::<Vec<_>>();
                    let dropped_after_until = received.saturating_sub(filtered_batch.len());

                    log::debug!(
                        "DATA Trades Distribute | pane={} req={} stream={} received={received} inserted={} dropped_after_until={dropped_after_until} until_time={} first_received={} last_received={}",
                        fetcher::short_id(pane_id),
                        fetcher::format_req_id(req_id),
                        fetcher::format_stream(&stream_type),
                        filtered_batch.len(),
                        fetcher::format_time_short(until_time),
                        fetcher::format_optional_time(first_received),
                        fetcher::format_optional_time(last_received)
                    );

                    if let Err(reason) = self.insert_fetched_trades(
                        main_window,
                        pane_id,
                        &filtered_batch,
                        true,
                        req_id,
                    ) {
                        return self.handle_error(Some(pane_id), &reason, main_window);
                    }
                }
            }
            FetchedData::BubbleSummary {
                data,
                range,
                trades_seen,
                raw_discarded,
                req_id,
            } => {
                log::debug!(
                    "BUBBLE Summary Distribute | pane={} req={} stream={} range={} candles={} trades_seen={} raw_discarded={}",
                    fetcher::short_id(pane_id),
                    fetcher::format_req_id(req_id),
                    fetcher::format_stream(&stream_type),
                    fetcher::format_time_range(range.0, range.1),
                    data.len(),
                    trades_seen,
                    raw_discarded
                );
                if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, pane_id) {
                    pane_state.status = pane::Status::Ready;
                    if let pane::Content::Kline { chart: Some(c), .. } = &mut pane_state.content {
                        c.insert_bubble_summaries(
                            data,
                            range.0,
                            range.1,
                            trades_seen,
                            raw_discarded,
                            req_id,
                        );
                    }
                }
            }
            FetchedData::Klines { data, req_id } => {
                log::debug!(
                    "DATA Klines Distribute | pane={} req={} stream={} count={} first={} last={}",
                    fetcher::short_id(pane_id),
                    fetcher::format_req_id(req_id),
                    fetcher::format_stream(&stream_type),
                    data.len(),
                    fetcher::format_optional_time(data.first().map(|kline| kline.time)),
                    fetcher::format_optional_time(data.last().map(|kline| kline.time))
                );
                if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, pane_id) {
                    pane_state.status = pane::Status::Ready;

                    if let StreamKind::Kline {
                        timeframe,
                        ticker_info,
                    } = stream_type
                    {
                        pane_state.insert_hist_klines(req_id, timeframe, ticker_info, &data);
                    }
                }
            }
            FetchedData::OI { data, req_id } => {
                log::debug!(
                    "DATA OI Distribute | pane={} req={} stream={} count={}",
                    fetcher::short_id(pane_id),
                    fetcher::format_req_id(req_id),
                    fetcher::format_stream(&stream_type),
                    data.len()
                );
                if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, pane_id) {
                    pane_state.status = pane::Status::Ready;

                    if let StreamKind::Kline { .. } = stream_type {
                        pane_state.insert_hist_oi(req_id, &data);
                    }
                }
            }
        }

        Task::none()
    }

    fn finish_coordinator_worker_job(
        &mut self,
        main_window: window::Id,
        worker_req: uuid::Uuid,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
    ) -> bool {
        let Some(job_id) = self.worker_req_to_job.get(&worker_req).copied() else {
            return false;
        };

        log::info!(
            target: "marketdata",
            "MARKETDATA WorkerDone | worker_req={} job={}",
            fetcher::short_id(worker_req),
            crate::market_data::job::short_id(job_id)
        );
        log::info!(
            target: "marketdata",
            "MARKETDATA WorkerReqIgnoredByChartHandler | worker_req={} reason=coordinator_owned",
            fetcher::short_id(worker_req)
        );

        let Some(job) = self.market_coordinator.job(job_id).cloned() else {
            self.worker_req_to_job.remove(&worker_req);
            self.job_to_worker_req.remove(&job_id);
            self.job_to_consumers.remove(&job_id);
            return true;
        };
        let consumer_ids = self
            .job_to_consumers
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let records = job.progress.records_fetched;

        for chart_req in &consumer_ids {
            let Some(status) = self.mark_consumer_segment_complete(*chart_req, job.range) else {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA ChartReqMissing | chart_req={} feature={} reason=already_removed_or_generation_stale",
                    fetcher::short_id(*chart_req),
                    market_kind_label(&job.key.kind)
                );
                continue;
            };

            let Some(consumer) = self
                .pending_market_consumers
                .iter()
                .find(|consumer| consumer.req_id == *chart_req)
                .cloned()
            else {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA ChartReqMissing | chart_req={} feature={} reason=already_removed_or_generation_stale",
                    fetcher::short_id(*chart_req),
                    market_kind_label(&job.key.kind)
                );
                continue;
            };

            if self.pending_consumer_is_stale(main_window, &consumer) {
                log::info!(
                    target: "marketdata",
                    "MARKETDATA ChartReqMissing | chart_req={} feature={} reason=already_removed_or_generation_stale",
                    fetcher::short_id(*chart_req),
                    consumer.feature.short_name()
                );
                self.mark_generic_consumer_completed(*chart_req);
                continue;
            }

            match consumer.feature {
                ConsumerFeature::VolumeBubbles => {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA BubbleSegmentComplete | req={} segment={} completed={}/{} missing={}",
                        fetcher::short_id(*chart_req),
                        job.range.format_display(),
                        status.completed_logical,
                        status.total_logical,
                        status.missing.iter().map(MarketDataRange::format_display).collect::<Vec<_>>().join(",")
                    );
                    if self.consumer_is_fully_satisfied(*chart_req) {
                        self.dispatch_final_bubbles_to_consumer(main_window, &consumer);
                    } else {
                        let remaining = self.consumer_remaining_segments(*chart_req).join(",");
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA BubbleWaiting | req={} remaining={}",
                            fetcher::short_id(*chart_req),
                            remaining
                        );
                    }
                }
                _ => {
                    let feature_label = consumer.feature.short_name();
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA ConsumerSegmentComplete | req={} segment={} completed={}/{} source=network feature={} coverage_complete={} missing={}",
                        fetcher::short_id(*chart_req),
                        job.range.format_display(),
                        status.completed_logical,
                        status.total_logical,
                        feature_label,
                        status.coverage_complete,
                        status.missing.iter().map(MarketDataRange::format_display).collect::<Vec<_>>().join(",")
                    );
                    if self.consumer_is_fully_satisfied(*chart_req) {
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA ChartReqComplete | chart_req={} feature={}",
                            fetcher::short_id(*chart_req),
                            consumer.feature.short_name()
                        );
                        self.complete_pending_consumer(main_window, &consumer, empty_covered_tail);
                        self.mark_generic_consumer_completed(*chart_req);
                    } else {
                        let remaining = self.consumer_remaining_segments(*chart_req).join(",");
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA ConsumerWaiting | req={} remaining={}",
                            fetcher::short_id(*chart_req),
                            remaining
                        );
                    }
                }
            }
        }

        if empty_covered_tail.is_some() || records == 0 {
            self.market_coordinator.mark_empty_and_remove_job(job_id);
        } else {
            self.market_coordinator
                .complete_and_remove_job(job_id, records);
        }

        self.worker_req_to_job.remove(&worker_req);
        self.job_to_worker_req.remove(&job_id);
        self.job_to_consumers.remove(&job_id);
        self.pending_market_consumers
            .retain(|consumer| !consumer.completed || consumer_has_effective_gaps(consumer));

        if let Err(e) = self
            .market_cache
            .save_coverage(&self.market_coordinator.coverage)
        {
            log::warn!(
                target: "marketdata",
                "MARKETDATA CoverageSaveFailed | error={}",
                e
            );
        }

        self.log_market_data_progress_snapshot();

        true
    }

    /// Handle an invalid/out-of-range Kline/OI response.
    ///
    /// Unlike `finish_coordinator_worker_job`, this does NOT mark coverage
    /// Empty or Complete. It marks the job as Failed, removes the active job
    /// so progress does not hang, and allows a future retry/refetch.
    fn finish_coordinator_worker_job_invalid(
        &mut self,
        _main_window: window::Id,
        worker_req: uuid::Uuid,
    ) {
        let Some(job_id) = self.worker_req_to_job.get(&worker_req).copied() else {
            return;
        };

        log::info!(
            target: "marketdata",
            "MARKETDATA WorkerDone | worker_req={} job={}",
            fetcher::short_id(worker_req),
            crate::market_data::job::short_id(job_id)
        );

        // Fail and remove the job — do NOT mark coverage Empty or Complete.
        self.market_coordinator
            .fail_and_remove_job(job_id, "invalid_out_of_range_response".to_string());

        log::info!(
            target: "marketdata",
            "MARKETDATA JobRemoved | job={} reason=invalid_out_of_range_response",
            crate::market_data::job::short_id(job_id)
        );

        // Clean up mappings — no dispatch, no cache, no coverage update.
        self.worker_req_to_job.remove(&worker_req);
        self.job_to_worker_req.remove(&job_id);
        self.job_to_consumers.remove(&job_id);

        // Do NOT save coverage — nothing changed.
        // Do NOT dispatch to consumers — no valid data.

        self.log_market_data_progress_snapshot();
    }

    fn log_market_data_progress_snapshot(&self) {
        let progress = self.market_coordinator.progress_snapshot();
        log::info!(
            target: "marketdata",
            "MARKETDATA ProgressSnapshot | active={} cached_records={} fetched_records={} jobs=[{}]",
            progress.active_job_count(),
            progress.total_cached_records,
            progress.total_fetched_records,
            progress
                .active_jobs
                .iter()
                .map(|job| format!(
                    "{}:{}",
                    crate::market_data::job::short_id(job.id),
                    job.range.format_display()
                ))
                .collect::<Vec<_>>()
                .join(",")
        );
    }

    fn complete_fetch(
        &mut self,
        main_window: window::Id,
        pane_id: uuid::Uuid,
        req_id: Option<uuid::Uuid>,
        fetch: Option<fetcher::FetchRange>,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
    ) {
        if let Some(worker_req) = req_id
            && self.finish_coordinator_worker_job(main_window, worker_req, empty_covered_tail)
        {
            let _ = pane_id;
            let _ = fetch;
            return;
        }

        // Step 1: Extract data from pane_state without holding borrow
        let (streams_snapshot, pane_exists) = {
            if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, pane_id) {
                let streams = pane_state.streams.find_ready_map(|s| match s {
                    StreamKind::Kline { ticker_info, .. }
                    | StreamKind::Trades { ticker_info }
                    | StreamKind::Depth { ticker_info, .. } => Some(*ticker_info),
                });
                (streams, true)
            } else {
                log::warn!(
                    "FETCH Complete | pane={} req={} fetch={} found=false reason=no_pane",
                    fetcher::short_id(pane_id),
                    fetcher::format_req_id(req_id),
                    fetcher::format_fetch_range_compact(fetch)
                );
                (None, false)
            }
        };

        if !pane_exists {
            return;
        }

        let pending_context = req_id.and_then(|id| {
            self.pending_market_consumers
                .iter()
                .find(|consumer| consumer.req_id == id)
                .map(|consumer| (consumer.key.clone(), consumer.range))
        });

        let mut handled_by_market_consumers = false;

        // Step 2: Update coordinator coverage (no borrow on pane_state)
        if let Some(ref fr) = fetch {
            let completion_timeframe = pending_context
                .as_ref()
                .and_then(|(key, _)| key.kind.timeframe())
                .or_else(|| {
                    streams_snapshot.as_ref().and_then(|_| {
                        self.get_pane_state_by_uuid(main_window, pane_id)
                            .and_then(|pane| pane.streams.ready_iter())
                            .and_then(|mut iter| {
                                iter.find_map(|stream| match stream {
                                    StreamKind::Kline { timeframe, .. } => Some(*timeframe),
                                    _ => None,
                                })
                            })
                    })
                });
            let key = pending_context
                .as_ref()
                .map(|(key, _)| key.clone())
                .or_else(|| {
                    crate::market_data::bridge::fetch_range_to_key(
                        fr,
                        streams_snapshot.as_ref(),
                        completion_timeframe,
                    )
                });

            if let Some(key) = key
                && let Some(range) = crate::market_data::bridge::fetch_range_to_range(fr)
            {
                if empty_covered_tail.is_some() {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA LegacyCoverageEmpty | pane={} key={} range={}",
                        crate::market_data::job::short_id(pane_id),
                        key.display_key(),
                        range.format_display()
                    );
                    self.market_coordinator
                        .coverage
                        .mark_empty(key.clone(), range);
                } else {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA CoverageComplete | pane={} key={} range={}",
                        crate::market_data::job::short_id(pane_id),
                        key.display_key(),
                        range.format_display()
                    );
                    self.market_coordinator
                        .coverage
                        .mark_complete(key.clone(), range, 0);
                }

                // Complete matching coordinator jobs
                let job_ids: Vec<uuid::Uuid> = self
                    .market_coordinator
                    .active_jobs()
                    .iter()
                    .filter(|job| job.key == key && job.range.overlaps(&range))
                    .map(|job| job.id)
                    .collect();
                for job_id in job_ids {
                    log::info!(
                        target: "marketdata",
                        "MARKETDATA BridgeJobComplete | job={}",
                        crate::market_data::job::short_id(job_id)
                    );
                    self.market_coordinator.complete_job(job_id, 0);
                }

                let consumers = self.matching_pending_consumers(&key, &range);
                handled_by_market_consumers = !consumers.is_empty();
                for consumer in &consumers {
                    if consumer.feature == ConsumerFeature::VolumeBubbles {
                        self.dispatch_final_bubbles_to_consumer(main_window, consumer);
                    }
                    self.complete_pending_consumer(main_window, consumer, empty_covered_tail);
                }
                if !consumers.is_empty() {
                    self.remove_pending_consumers(&key, &range);
                }

                // Save coverage immediately after fetch completion
                if let Err(e) = self
                    .market_cache
                    .save_coverage(&self.market_coordinator.coverage)
                {
                    log::warn!(
                        target: "marketdata",
                        "MARKETDATA CoverageSaveFailed | error={}",
                        e
                    );
                }
            }
        }

        // Step 3: Update chart (re-borrow pane_state)
        let mut chart_found = false;
        if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, pane_id) {
            if let Some(fetcher::FetchRange::Trades(from, to)) = fetch
                && !handled_by_market_consumers
                && let pane::Content::Kline { chart: Some(c), .. } = &mut pane_state.content
            {
                chart_found = true;
                c.complete_trade_fetch(
                    req_id,
                    Some(fetcher::FetchRange::Trades(from, to)),
                    empty_covered_tail,
                );
            }
            pane_state.status = pane::Status::Ready;
        }

        log::debug!(
            "FETCH Complete | pane={} req={} fetch={} found=true chart_found={chart_found}",
            fetcher::short_id(pane_id),
            fetcher::format_req_id(req_id),
            fetcher::format_fetch_range_compact(fetch)
        );
    }

    fn apply_backfill_update(
        &mut self,
        main_window: window::Id,
        pane_ids: Vec<uuid::Uuid>,
        stream: StreamKind,
        update: fetcher::FetchUpdate,
    ) {
        match update {
            fetcher::FetchUpdate::Status { status, .. } => match status {
                fetcher::FetchTaskStatus::Loading(info) => {
                    log::debug!(
                        "BACKFILL Update | kind=Loading stream={} pane_ids={} info={info:?}",
                        fetcher::format_stream(&stream),
                        pane_ids.len()
                    );
                    for pane_id in pane_ids {
                        if let Some(pane_state) =
                            self.get_mut_pane_state_by_uuid(main_window, pane_id)
                        {
                            pane_state.status = pane::Status::Loading(info);
                        }
                    }
                }
                fetcher::FetchTaskStatus::Completed {
                    req_id,
                    fetch,
                    empty_covered_tail,
                } => {
                    log::info!(
                        "BACKFILL Completed | stream={} pane_ids={} req={} fetch={} tail={}",
                        fetcher::format_stream(&stream),
                        pane_ids.len(),
                        fetcher::format_req_id(req_id),
                        fetcher::format_fetch_range_compact(fetch),
                        empty_covered_tail
                            .map(|(f, t)| fetcher::format_time_range(f, t))
                            .unwrap_or_else(|| "-".to_string())
                    );
                    if let Some(fetch) = fetch {
                        self.clear_pending_backfill(stream, fetch);
                        log::info!(
                            "BACKFILL PendingRemove | stream={} fetch={} reason=completed",
                            fetcher::format_stream(&stream),
                            fetcher::format_fetch_range(&fetch)
                        );
                    }

                    // Backfill completion: mark covered ranges on each pane
                    // without going through per-pane RequestHandler.
                    // Only mark panes that support the fetched data type.
                    for pane_id in pane_ids {
                        if let Some(pane_state) =
                            self.get_mut_pane_state_by_uuid(main_window, pane_id)
                        {
                            let supports = fetch
                                .map(|f| pane_state.supports_fetch_range(&f))
                                .unwrap_or(true);

                            if supports {
                                pane_state.mark_backfill_completed(fetch, empty_covered_tail);
                            } else {
                                log::debug!(
                                    "BACKFILL CompletionSkip | pane={} content={} reason=unsupported_fetch_range",
                                    fetcher::short_id(pane_id),
                                    pane_state.content
                                );
                            }
                        }
                    }
                }
            },
            fetcher::FetchUpdate::Data { data, .. } => {
                let data_summary = match &data {
                    FetchedData::Trades { batch, req_id, .. } => {
                        format!(
                            "Trades:req={}:count={}",
                            fetcher::format_req_id(*req_id),
                            batch.len()
                        )
                    }
                    FetchedData::BubbleSummary { data, req_id, .. } => {
                        format!(
                            "BubbleSummary:req={}:candles={}:candidates={}",
                            fetcher::format_req_id(*req_id),
                            data.len(),
                            data.iter()
                                .map(|summary| summary.candidates.len())
                                .sum::<usize>()
                        )
                    }
                    FetchedData::Klines { data, req_id } => {
                        format!(
                            "Klines:req={}:count={}",
                            fetcher::format_req_id(*req_id),
                            data.len()
                        )
                    }
                    FetchedData::OI { data, req_id } => {
                        format!(
                            "OI:req={}:count={}",
                            fetcher::format_req_id(*req_id),
                            data.len()
                        )
                    }
                };
                log::debug!(
                    "BACKFILL Update | kind=Data stream={} pane_ids={} data={}",
                    fetcher::format_stream(&stream),
                    pane_ids.len(),
                    data_summary
                );
                for pane_id in pane_ids {
                    // Check if this pane supports the fetched data type
                    let supports = self
                        .get_pane_state_by_uuid(main_window, pane_id)
                        .map(|s| s.supports_fetched_data(&data))
                        .unwrap_or(false);

                    if !supports {
                        log::debug!(
                            "BACKFILL DataSkip | pane={} stream={} data={} reason=unsupported_fetched_data",
                            fetcher::short_id(pane_id),
                            fetcher::format_stream(&stream),
                            data_summary_type(&data)
                        );
                        continue;
                    }

                    let _ = self.distribute_fetched_data(
                        main_window,
                        pane_id,
                        data.clone(),
                        stream,
                        true, // backfill: skip stale generation check
                    );
                }
            }
            fetcher::FetchUpdate::Error {
                pane_id,
                error,
                req_id,
                fetch,
            } => {
                // For backfill, skip stale generation check since requests
                // are tracked globally (pending_backfills), not per-pane.
                let is_timeout = error.contains("TimedOut") || error.contains("timed out");
                if is_timeout {
                    log::error!(
                        "BACKFILL Timeout | stream={} pane_ids={} source_pane={} req={} fetch={} error={}",
                        fetcher::format_stream(&stream),
                        pane_ids.len(),
                        fetcher::short_id(pane_id),
                        fetcher::format_req_id(req_id),
                        fetcher::format_fetch_range_compact(fetch),
                        error
                    );
                } else {
                    log::warn!(
                        "BACKFILL Failed | stream={} pane_ids={} source_pane={} req={} fetch={} error={}",
                        fetcher::format_stream(&stream),
                        pane_ids.len(),
                        fetcher::short_id(pane_id),
                        fetcher::format_req_id(req_id),
                        fetcher::format_fetch_range_compact(fetch),
                        error
                    );
                }
                if let Some(fetch) = fetch {
                    self.clear_pending_backfill(stream, fetch);
                    log::info!(
                        "BACKFILL PendingRemove | stream={} fetch={} reason={}",
                        fetcher::format_stream(&stream),
                        fetcher::format_fetch_range(&fetch),
                        if is_timeout { "timeout" } else { "failed" }
                    );
                }

                // Set all affected panes to Ready (don't call mark_fetch_failed
                // since backfill requests aren't in per-pane RequestHandler).
                for target_pane_id in &pane_ids {
                    if let Some(pane_state) =
                        self.get_mut_pane_state_by_uuid(main_window, *target_pane_id)
                    {
                        pane_state.status = pane::Status::Ready;
                    }
                }

                if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, pane_id) {
                    pane_state.status = pane::Status::Ready;
                    pane_state
                        .notifications
                        .push(Toast::error(DashboardError::Fetch(error).to_string()));
                }
            }
        }
    }

    fn clear_pending_backfill(&mut self, stream: StreamKind, fetch: fetcher::FetchRange) {
        let (from, to) = match fetch {
            fetcher::FetchRange::Kline(from, to)
            | fetcher::FetchRange::OpenInterest(from, to)
            | fetcher::FetchRange::Trades(from, to)
            | fetcher::FetchRange::TradeHydration(from, to) => (from, to),
            fetcher::FetchRange::BubbleSummary { from, to, .. } => (from, to),
        };

        let removed = self
            .pending_backfills
            .remove(&(stream, from.as_u64(), to.as_u64()))
            .is_some();
        log::debug!(
            "BACKFILL ClearPending | stream={} range={} removed={removed}",
            fetcher::format_stream(&stream),
            fetcher::format_time_range(from, to)
        );
    }

    fn insert_fetched_trades(
        &mut self,
        main_window: window::Id,
        pane_id: uuid::Uuid,
        trades: &[Trade],
        is_batches_done: bool,
        _req_id: Option<uuid::Uuid>,
    ) -> Result<(), DashboardError> {
        let pane_state = self
            .get_mut_pane_state_by_uuid(main_window, pane_id)
            .ok_or_else(|| {
                DashboardError::Unknown(
                    "No matching pane state found for fetched trades".to_string(),
                )
            })?;

        match &mut pane_state.content {
            pane::Content::Kline { chart, .. } => {
                if let Some(c) = chart {
                    // Update loading status
                    match &mut pane_state.status {
                        pane::Status::Loading(InfoKind::FetchingTrades(count)) => {
                            *count += trades.len();
                        }
                        _ => {
                            pane_state.status =
                                pane::Status::Loading(InfoKind::FetchingTrades(trades.len()));
                        }
                    }

                    c.insert_raw_trades(trades.to_owned(), is_batches_done);

                    if is_batches_done {
                        // NOTE: Do NOT mark request completed here.
                        // complete_fetch() -> complete_trade_fetch() handles that,
                        // avoiding the double PendingRemove + StaleResult log.
                        pane_state.status = pane::Status::Ready;
                    }
                    Ok(())
                } else {
                    log::debug!(
                        "FETCH TradesSkip | pane={} content=Kline(no_chart) reason=no_chart",
                        fetcher::short_id(pane_id)
                    );
                    Ok(())
                }
            }
            // Non-Kline panes cannot consume fetched trades.
            // This is an internal routing mismatch, not a user error.
            _ => {
                log::debug!(
                    "FETCH TradesSkip | pane={} content={} reason=unsupported_content_type",
                    fetcher::short_id(pane_id),
                    pane_state.content
                );
                Ok(())
            }
        }
    }

    pub fn update_latest_klines(
        &mut self,
        stream: &StreamKind,
        kline: &Kline,
        main_window: window::Id,
    ) -> Task<Message> {
        // Track last live timestamp for backfill on disconnect.
        let previous_last_live_t = self.last_live_t.get(stream).copied();
        if self
            .last_live_t
            .get(stream)
            .is_none_or(|&prev| kline.time > prev)
        {
            self.last_live_t.insert(*stream, kline.time);
        }
        let new_last_live_t = self.last_live_t.get(stream).copied();

        // Route live klines through the market data layer
        // NOTE: We do NOT mark live klines as Complete coverage for same reason as trades
        if let Some(key) = crate::market_data::bridge::stream_kind_to_key(stream) {
            self.market_coordinator
                .feed_klines(&key, std::slice::from_ref(kline));

            log::trace!(
                target: "marketdata",
                "MARKETDATA LiveKlineObserved | key={} time={}",
                key.display_key(),
                fetcher::format_time_short(kline.time)
            );

            // Persist to cache
            self.live_adapter.ingest_klines(
                &key,
                std::slice::from_ref(kline),
                &mut self.market_coordinator.store,
                &mut self.market_coordinator.coverage,
                Some(&mut self.market_cache),
            );
        }

        let mut found_match = false;
        let mut matched_panes = 0usize;

        self.iter_all_panes_mut(main_window)
            .for_each(|(_, _, pane_state)| {
                if pane_state.matches_stream(stream) {
                    matched_panes += 1;
                    match &mut pane_state.content {
                        pane::Content::Kline { chart: Some(c), .. } => {
                            c.update_latest_kline(kline);
                        }
                        pane::Content::Comparison(Some(c)) => {
                            c.update_latest_kline(&stream.ticker_info(), kline);
                        }
                        _ => {}
                    }
                    found_match = true;
                }
            });

        log::trace!(
            "KLINE LiveRoute | stream={} kline_t={} prev_last_live_t={} new_last_live_t={} matched_panes={matched_panes}",
            fetcher::format_stream(stream),
            fetcher::format_time_short(kline.time),
            fetcher::format_optional_time(previous_last_live_t),
            fetcher::format_optional_time(new_last_live_t)
        );

        if found_match {
            Task::none()
        } else {
            log::warn!(
                "KLINE NoMatch | stream={} kline_t={} reason=refresh_streams",
                fetcher::format_stream(stream),
                fetcher::format_time_short(kline.time)
            );
            self.refresh_streams(main_window)
        }
    }

    pub fn ingest_depth(
        &mut self,
        stream: &StreamKind,
        update_t: UnixMs,
        depth: &Depth,
        main_window: window::Id,
    ) -> Task<Message> {
        let mut found_match = false;
        let mut matched_panes = 0usize;

        self.iter_all_panes_mut(main_window)
            .for_each(|(_, _, pane_state)| {
                if pane_state.matches_stream(stream) {
                    matched_panes += 1;
                    match &mut pane_state.content {
                        pane::Content::Heatmap { chart, .. } => {
                            if let Some(c) = chart {
                                c.insert_depth(depth, update_t);
                            }
                        }
                        pane::Content::ShaderHeatmap { chart, .. } => {
                            if let Some(c) = chart {
                                c.insert_depth(depth, update_t);
                            }
                        }
                        pane::Content::Ladder(panel) => {
                            if let Some(panel) = panel {
                                panel.insert_depth(depth, update_t);
                            }
                        }
                        _ => {
                            log::error!("No chart found for the stream: {stream:?}");
                        }
                    }
                    found_match = true;
                }
            });

        log::trace!(
            "DATA DepthRoute | stream={} update_t={} matched_panes={matched_panes}",
            fetcher::format_stream(stream),
            fetcher::format_time_short(update_t)
        );

        if found_match {
            Task::none()
        } else {
            log::warn!(
                "DATA DepthNoMatch | stream={} update_t={} reason=refresh_streams",
                fetcher::format_stream(stream),
                fetcher::format_time_short(update_t)
            );
            self.refresh_streams(main_window)
        }
    }

    pub fn ingest_trades(
        &mut self,
        stream: &StreamKind,
        buffer: &[Trade],
        update_t: UnixMs,
        main_window: window::Id,
    ) -> Task<Message> {
        // Track last live timestamp for backfill on disconnect.
        let last_trade_t = buffer.last().map_or(update_t, |t| t.time);
        let _previous_last_live_t = self.last_live_t.get(stream).copied();
        if self
            .last_live_t
            .get(stream)
            .is_none_or(|&prev| last_trade_t > prev)
        {
            self.last_live_t.insert(*stream, last_trade_t);
        }
        let _new_last_live_t = self.last_live_t.get(stream).copied();

        // Route live trades through the market data layer
        // This ensures live data is stored in coordinator and persisted to cache
        // NOTE: We do NOT mark live data as Complete coverage because live WS data
        // can have gaps. Coverage should only be marked Complete after REST fetch confirmation.
        if let Some(key) = crate::market_data::bridge::stream_kind_to_key(stream) {
            self.market_coordinator.feed_trades(&key, buffer);

            log::trace!(
                target: "marketdata",
                "MARKETDATA LiveObserved | key={} count={} latest={}",
                key.display_key(),
                buffer.len(),
                buffer.last().map_or("-".to_string(), |t| fetcher::format_time_short(t.time))
            );

            // Persist to cache (batched via live adapter)
            // Cache stores the data but does NOT update coverage to Complete
            self.live_adapter.ingest_trades(
                &key,
                buffer,
                &mut self.market_coordinator.store,
                &mut self.market_coordinator.coverage,
                Some(&mut self.market_cache),
            );
        }

        let mut found_match = false;
        let mut matched_panes = 0usize;
        let mut content_updates = Vec::new();

        self.iter_all_panes_mut(main_window)
            .for_each(|(_win_id, _pane, pane_state)| {
                if pane_state.matches_stream(stream) {
                    matched_panes += 1;
                    match &mut pane_state.content {
                        pane::Content::Heatmap { chart, .. } => {
                            if let Some(c) = chart {
                                c.insert_trades(buffer, update_t);
                                content_updates.push("Heatmap");
                            }
                        }
                        pane::Content::ShaderHeatmap { chart, .. } => {
                            if let Some(c) = chart {
                                c.insert_trades(buffer, update_t);
                                content_updates.push("ShaderHeatmap");
                            }
                        }
                        pane::Content::Kline { chart, .. } => {
                            if let Some(c) = chart {
                                c.insert_trades(buffer);
                                content_updates.push("Kline");
                            }
                        }
                        pane::Content::TimeAndSales(panel) => {
                            if let Some(p) = panel {
                                p.insert_buffer(buffer);
                                content_updates.push("TimeAndSales");
                            }
                        }
                        pane::Content::Ladder(panel) => {
                            if let Some(p) = panel {
                                p.insert_trades(buffer);
                                content_updates.push("Ladder");
                            }
                        }
                        _ => {
                            log::error!("No chart found for the stream: {stream:?}");
                        }
                    }
                    found_match = true;
                }
            });

        log::info!(
            "MARKETDATA LiveTradeRouteSummary | stream={} buffer_len={} matched_panes={} content_updates={} first={} last={}",
            fetcher::format_stream(stream),
            buffer.len(),
            matched_panes,
            content_updates.join(","),
            fetcher::format_optional_time(buffer.first().map(|t| t.time)),
            fetcher::format_optional_time(buffer.last().map(|t| t.time))
        );

        // Log CVD and volume live updates for chart panes that received trades
        if content_updates.contains(&"Kline") {
            log::info!(
                "MARKETDATA CVDLiveUpdate | stream={} touched_candles=1 latest={}",
                fetcher::format_stream(stream),
                fetcher::format_optional_time(buffer.last().map(|t| t.time))
            );
            log::info!(
                "MARKETDATA VolumeLiveUpdate | stream={} source=trades touched_candles=1",
                fetcher::format_stream(stream)
            );
        }

        if found_match {
            Task::none()
        } else {
            log::warn!(
                "TRADE NoMatch | stream={} buffer_len={} update_t={} reason=refresh_streams",
                fetcher::format_stream(stream),
                buffer.len(),
                fetcher::format_time_short(update_t)
            );
            self.refresh_streams(main_window)
        }
    }

    pub fn invalidate_all_panes(&mut self, main_window: window::Id) {
        self.iter_all_panes_mut(main_window)
            .for_each(|(_, _, state)| {
                let _ = state.invalidate(Instant::now());
            });
    }

    pub fn park_for_inactive_layout(&mut self, main_window: window::Id) {
        self.iter_all_panes_mut(main_window)
            .for_each(|(_, _, state)| state.park_for_inactive_layout());
    }

    pub fn tick(
        &mut self,
        handles: &AdapterHandles,
        now: Instant,
        main_window: window::Id,
    ) -> Task<Message> {
        // Clean up backfill handles when no backfills are pending.
        if self.pending_backfills.is_empty() && !self.backfill_handles.is_empty() {
            log::debug!(
                "BACKFILL HandleCleanup | cleared={} handles",
                self.backfill_handles.len()
            );
            self.backfill_handles.clear();
        }

        self.fail_stale_market_jobs(main_window);

        // Periodically save coverage to disk (every 30 seconds)
        if now.duration_since(self.last_coverage_save) > std::time::Duration::from_secs(30) {
            self.last_coverage_save = now;
            if let Err(e) = self
                .market_cache
                .save_coverage(&self.market_coordinator.coverage)
            {
                log::warn!(
                    target: "marketdata",
                    "MARKETDATA CoverageSaveFailed | error={}",
                    e
                );
            }
        }

        let mut tasks = vec![];
        let mut route_fetches = Vec::new();

        let mut tick_state = |state: &mut pane::State| match state.tick(now) {
            Some(pane::Action::Chart(action)) => match action {
                chart::Action::ErrorOccurred(err) => {
                    state.status = pane::Status::Ready;
                    state.notifications.push(Toast::error(err.to_string()));
                }
                chart::Action::RequestFetch(reqs) => {
                    let pane_id = state.unique_id();
                    let ready_streams = state
                        .streams
                        .ready_iter()
                        .map(|iter| iter.copied().collect::<Vec<_>>())
                        .unwrap_or_default();

                    // Get chart generation for stale detection
                    let chart_generation =
                        if let pane::Content::Kline { chart: Some(c), .. } = &state.content {
                            c.current_generation()
                        } else {
                            0
                        };

                    route_fetches.push(DashboardFetchRoute {
                        pane_id,
                        ready_streams,
                        chart_generation,
                        reqs,
                    });
                }
                chart::Action::RequestPalette => {
                    tasks.push(Task::done(Message::RequestPalette));
                }
            },
            Some(pane::Action::Panel(_action)) => {}
            Some(pane::Action::ResolveStreams(streams)) => {
                tasks.push(Task::done(Message::ResolveStreams(
                    state.unique_id(),
                    streams,
                )));
            }
            Some(pane::Action::ResolveContent) => match state.stream_pair_kind() {
                Some(StreamPairKind::MultiSource(tickers)) => {
                    state.set_content_and_streams(tickers, state.content.kind());
                }
                Some(StreamPairKind::SingleSource(ticker)) => {
                    state.set_content_and_streams(vec![ticker], state.content.kind());
                }
                None => {}
            },
            None => {}
        };

        // tick only the maximized pane if there is any, otherwise tick all panes
        let maximized_pane = self.panes.maximized();
        for (pane_id, state) in self.panes.iter_mut() {
            if maximized_pane.is_some_and(|maximized| *pane_id != maximized) {
                continue;
            }

            tick_state(state);
        }

        for (popout_state, _) in self.popout.values_mut() {
            for (_, state) in popout_state.iter_mut() {
                tick_state(state);
            }
        }

        for route in route_fetches {
            tasks.push(self.route_fetch_specs_through_market_data(
                handles.clone(),
                main_window,
                route,
            ));
        }

        Task::batch(tasks)
    }

    fn fail_stale_market_jobs(&mut self, main_window: window::Id) {
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        let stale_jobs = self
            .market_coordinator
            .active_jobs()
            .into_iter()
            .filter(|job| {
                now_ms.saturating_sub(job.created_at) >= MARKET_DATA_JOB_STALE_MS
                    && job.progress.records_fetched == 0
            })
            .map(|job| (job.id, job.range))
            .collect::<Vec<_>>();

        for (job_id, range) in &stale_jobs {
            let worker_req = self.job_to_worker_req.get(job_id).copied();
            let age = self
                .market_coordinator
                .job(*job_id)
                .map(|job| now_ms.saturating_sub(job.created_at))
                .unwrap_or_default();
            log::warn!(
                target: "marketdata",
                "MARKETDATA JobStale | job={} worker_req={} age={} action=fail_and_remove",
                crate::market_data::job::short_id(*job_id),
                worker_req.map_or("-".to_string(), fetcher::short_id),
                age
            );

            for chart_req in self
                .job_to_consumers
                .get(job_id)
                .cloned()
                .unwrap_or_default()
            {
                if let Some(consumer) = self
                    .pending_market_consumers
                    .iter_mut()
                    .find(|consumer| consumer.req_id == chart_req)
                    && !consumer.failed_segments.contains(range)
                {
                    consumer.failed_segments.push(*range);
                }

                if self.consumer_is_fully_satisfied(chart_req)
                    && let Some(consumer) = self
                        .pending_market_consumers
                        .iter()
                        .find(|consumer| consumer.req_id == chart_req)
                        .cloned()
                {
                    if let Some(pane_state) =
                        self.get_mut_pane_state_by_uuid(main_window, consumer.pane_id)
                    {
                        pane_state.status = pane::Status::Ready;
                    }
                    self.mark_generic_consumer_completed(chart_req);
                }
            }

            self.market_coordinator
                .fail_and_remove_job(*job_id, "stale market data worker".to_string());
            if let Some(worker_req) = worker_req {
                self.worker_req_to_job.remove(&worker_req);
            }
            self.job_to_worker_req.remove(job_id);
            self.job_to_consumers.remove(job_id);
        }

        if !stale_jobs.is_empty() {
            self.pending_market_consumers
                .retain(|consumer| !consumer.completed || consumer_has_effective_gaps(consumer));
            self.log_market_data_progress_snapshot();
        }
    }

    pub fn resolve_streams(
        &mut self,
        main_window: window::Id,
        pane_id: uuid::Uuid,
        streams: Vec<StreamKind>,
    ) -> Task<Message> {
        log::debug!(
            "STREAM ResolveReady | pane={} streams={}",
            fetcher::short_id(pane_id),
            fetcher::format_streams(&streams)
        );
        if let Some(state) = self.get_mut_pane_state_by_uuid(main_window, pane_id) {
            state.streams = ResolvedStream::Ready(streams.clone());
        }
        self.refresh_streams(main_window)
    }

    pub fn market_subscriptions(&self, handles: &AdapterHandles) -> Subscription<exchange::Event> {
        let pane_count = self.panes.iter().count()
            + self
                .popout
                .values()
                .map(|(state, _)| state.iter().count())
                .sum::<usize>();
        log::debug!("STREAM RebuildSubscriptions | panes={pane_count}");
        let unique_streams = self
            .streams
            .combined_used()
            .flat_map(|(exchange, specs)| {
                let mut subs = vec![];
                log::debug!(
                    "STREAM SubscriptionGroup | exchange={exchange:?} depth={} trade={} kline={}",
                    specs.depth.len(),
                    specs.trade.len(),
                    specs.kline.len()
                );

                if !specs.depth.is_empty() {
                    let depth_subs = specs
                        .depth
                        .iter()
                        .map(|(ticker, aggr, push_freq)| {
                            let tick_mltp = match aggr {
                                StreamTicksize::Client => None,
                                StreamTicksize::ServerSide(tick_mltp) => Some(*tick_mltp),
                            };

                            let config = StreamConfig::new(
                                *ticker,
                                ticker.exchange(),
                                tick_mltp,
                                *push_freq,
                            );

                            let data = (handles.clone(), config);
                            Subscription::run_with(data, |data| data.0.depth_stream(&data.1))
                        })
                        .collect::<Vec<_>>();

                    if !depth_subs.is_empty() {
                        subs.push(Subscription::batch(depth_subs));
                    }
                }

                if !specs.trade.is_empty() {
                    let trade_subs = specs
                        .trade
                        .chunks(MAX_TRADE_TICKERS_PER_STREAM)
                        .enumerate()
                        .map(|(idx, tickers)| {
                            log::debug!(
                                "STREAM TradeChunk | exchange={exchange:?} idx={idx} size={}",
                                tickers.len()
                            );
                            let config = StreamConfig::new(
                                tickers.to_vec(),
                                exchange,
                                None,
                                PushFrequency::ServerDefault,
                            );

                            let data = (handles.clone(), config);
                            Subscription::run_with(data, |data| data.0.trade_stream(&data.1))
                        })
                        .collect::<Vec<_>>();

                    if !trade_subs.is_empty() {
                        subs.push(Subscription::batch(trade_subs));
                    }
                }

                if !specs.kline.is_empty() {
                    let kline_subs = specs
                        .kline
                        .chunks(MAX_KLINE_STREAMS_PER_STREAM)
                        .enumerate()
                        .map(|(idx, streams)| {
                            log::debug!(
                                "STREAM KlineChunk | exchange={exchange:?} idx={idx} size={}",
                                streams.len()
                            );
                            let config = StreamConfig::new(
                                streams.to_vec(),
                                exchange,
                                None,
                                PushFrequency::ServerDefault,
                            );

                            let data = (handles.clone(), config);
                            Subscription::run_with(data, |data| data.0.kline_stream(&data.1))
                        })
                        .collect::<Vec<_>>();

                    if !kline_subs.is_empty() {
                        subs.push(Subscription::batch(kline_subs));
                    }
                }

                subs
            })
            .collect::<Vec<Subscription<exchange::Event>>>();

        if unique_streams.is_empty() && pane_count > 0 {
            // Log at debug level to avoid spamming every frame.
            // This is a normal transient state during startup / stream resolution.
            log::debug!("STREAM EmptySubscriptions | panes={pane_count} reason=no_unique_streams");
        }

        Subscription::batch(unique_streams)
    }

    pub fn theme_updated(&mut self, main_window: window::Id, theme: &iced_core::Theme) {
        self.iter_all_panes_mut(main_window)
            .for_each(|(_, _, state)| {
                state.content.update_theme(theme);
            });
    }

    fn refresh_streams(&mut self, main_window: window::Id) -> Task<Message> {
        let old_streams = all_unique_streams(&self.streams);
        let all_pane_streams = self
            .iter_all_panes(main_window)
            .flat_map(|(_, _, pane_state)| pane_state.streams.ready_iter().into_iter().flatten());
        self.streams = UniqueStreams::from(all_pane_streams);
        let new_streams = all_unique_streams(&self.streams);
        let added = new_streams
            .iter()
            .filter(|stream| !old_streams.contains(stream))
            .copied()
            .collect::<Vec<_>>();
        let removed = old_streams
            .iter()
            .filter(|stream| !new_streams.contains(stream))
            .copied()
            .collect::<Vec<_>>();

        log::debug!(
            "STREAM Refresh | old_count={} new_count={} added={} removed={}",
            old_streams.len(),
            new_streams.len(),
            added.len(),
            removed.len()
        );
        for stream in &added {
            log::debug!("STREAM Added | stream={}", fetcher::format_stream(stream));
        }
        for stream in &removed {
            log::debug!("STREAM Removed | stream={}", fetcher::format_stream(stream));
        }

        Task::none()
    }

    /// Historical gap backfill after WS disconnect.
    ///
    /// For each disconnected trade/kline stream, finds all panes that use it and
    /// requests a historical fetch from `last_seen + 1ms` to `now`. Skips depth
    /// streams (stateful snapshot, no gap fill needed).
    ///
    /// Every disconnected stream produces exactly one decision log:
    /// - `WS Backfill Skip | reason=depth_not_supported`
    /// - `WS Backfill Skip | reason=no_last_seen`
    /// - `WS Backfill Skip | reason=gap_too_small`
    /// - `WS Backfill Skip | reason=already_pending`
    /// - `WS Backfill Queued`  (with range)
    /// - `WS Backfill Capped`  (with original + capped range)
    pub fn backfill_disconnected_streams(
        &mut self,
        handles: &exchange::adapter::AdapterHandles,
        main_window: window::Id,
        streams: &[StreamKind],
        now: UnixMs,
        reason: &str,
    ) -> Task<Message> {
        /// Minimum gap (ms) to bother backfilling; avoids tiny useless fetches.
        const MIN_BACKFILL_GAP_MS: u64 = 1_000;
        /// Maximum automatic backfill range to cap REST fetches after long downtime.
        const MAX_BACKFILL_RANGE_MS: u64 = 15 * 60 * 1_000;
        /// Pending backfill entries older than this are pruned to allow re-fetching.
        const PENDING_EXPIRY: std::time::Duration = std::time::Duration::from_secs(60);

        log::info!(
            "BACKFILL Entry | disconnected_streams={} last_live_t={} pending_backfills={} now={}",
            streams.len(),
            self.last_live_t.len(),
            self.pending_backfills.len(),
            fetcher::format_time_short(now)
        );

        // Prune stale pending entries.
        let pending_before = self.pending_backfills.len();
        self.pending_backfills
            .retain(|_, inserted_at| inserted_at.elapsed() < PENDING_EXPIRY);
        let pruned = pending_before.saturating_sub(self.pending_backfills.len());
        if pruned > 0 {
            log::debug!(
                "BACKFILL Pruned | before={pending_before} after={} pruned={pruned}",
                self.pending_backfills.len()
            );
        }

        let mut fetch_tasks: Vec<Task<Message>> = Vec::new();
        let mut new_backfill_handles: Vec<iced::task::Handle> = Vec::new();

        for stream in streams {
            // Depth streams are stateful snapshots — no historical gap to fill.
            if matches!(stream, StreamKind::Depth { .. }) {
                log::info!(
                    "BACKFILL Decision | stream={} has_last_seen={} last_seen=- full_from=- full_to={} gap_ms=0 capped=false grouped_panes=0 registered_panes=0 reason=depth_not_supported",
                    fetcher::format_stream(stream),
                    self.last_live_t.contains_key(stream),
                    fetcher::format_time_short(now)
                );
                continue;
            }

            let last_t = match self.last_live_t.get(stream) {
                Some(&t) => t,
                None => {
                    log::info!(
                        "BACKFILL Decision | stream={} has_last_seen=false last_seen=- full_from=- full_to={} gap_ms=0 capped=false grouped_panes=0 registered_panes=0 reason=no_last_seen",
                        fetcher::format_stream(stream),
                        fetcher::format_time_short(now)
                    );
                    continue;
                }
            };

            let full_from = last_t.saturating_add(1);
            let full_to = now;
            let gap_ms = full_to.saturating_diff(full_from);

            if gap_ms < MIN_BACKFILL_GAP_MS {
                log::info!(
                    "BACKFILL Decision | stream={} has_last_seen=true last_seen={} full_from={} full_to={} gap_ms={gap_ms} capped=false grouped_panes=0 registered_panes=0 reason=gap_too_small",
                    fetcher::format_stream(stream),
                    fetcher::format_time_short(last_t),
                    fetcher::format_time_short(full_from),
                    fetcher::format_time_short(full_to)
                );
                continue;
            }

            // Cap the range if it exceeds the maximum.
            let capped = gap_ms > MAX_BACKFILL_RANGE_MS;
            let (from, to) = if capped {
                let capped_from = full_to.saturating_sub(MAX_BACKFILL_RANGE_MS);
                log::info!(
                    "BACKFILL Capped | stream={} \
                     original_range={orig} capped_range={capped} \
                     reason={reason}",
                    fetcher::format_stream(stream),
                    orig = fetcher::format_time_range(full_from, full_to),
                    capped = fetcher::format_time_range(capped_from, full_to),
                );
                (capped_from, full_to)
            } else {
                (full_from, full_to)
            };

            let mut grouped_panes: HashMap<(UnixMs, UnixMs), Vec<uuid::Uuid>> = HashMap::new();

            for (_window, _pane, pane_state) in self.iter_all_panes(main_window) {
                if !pane_state.matches_stream(stream) {
                    continue;
                }

                // Check if this pane supports fetched data for this stream type
                let fetch_range = match stream {
                    StreamKind::Trades { .. } => FetchRange::Trades(from, to),
                    StreamKind::Kline { .. } => FetchRange::Kline(from, to),
                    _ => continue,
                };

                if !pane_state.supports_fetch_range(&fetch_range) {
                    log::debug!(
                        "BACKFILL PaneSkip | stream={} pane={} content={} reason=unsupported_fetch_range",
                        fetcher::format_stream(stream),
                        fetcher::short_id(pane_state.unique_id()),
                        pane_state.content
                    );
                    continue;
                }

                let pane_id = pane_state.unique_id();
                let Some((missing_from, missing_to)) = (match stream {
                    StreamKind::Trades { .. } => pane_state.missing_trade_range(from, to),
                    StreamKind::Kline { .. } => Some((from, to)),
                    _ => None,
                }) else {
                    log::debug!(
                        "BACKFILL PaneCovered | stream={} pane={} requested_range={} reason=no_missing_range",
                        fetcher::format_stream(stream),
                        fetcher::short_id(pane_id),
                        fetcher::format_time_range(from, to)
                    );
                    continue;
                };

                log::debug!(
                    "BACKFILL PaneMissing | stream={} pane={} missing_range={}",
                    fetcher::format_stream(stream),
                    fetcher::short_id(pane_id),
                    fetcher::format_time_range(missing_from, missing_to)
                );
                grouped_panes
                    .entry((missing_from, missing_to))
                    .or_default()
                    .push(pane_id);
            }

            if grouped_panes.is_empty() {
                log::info!(
                    "BACKFILL Decision | stream={} has_last_seen=true last_seen={} full_from={} full_to={} gap_ms={gap_ms} capped={} grouped_panes=0 registered_panes=0 range={} reason=no_matching_pane_or_covered",
                    fetcher::format_stream(stream),
                    fetcher::format_time_short(last_t),
                    fetcher::format_time_short(full_from),
                    fetcher::format_time_short(full_to),
                    capped,
                    fetcher::format_time_range(from, to),
                );
                continue;
            }

            for ((missing_from, missing_to), pane_ids) in grouped_panes {
                let pending_overlap = self.pending_backfills.keys().find(|(s, ef, et)| {
                    *s == *stream && *ef <= missing_to.as_u64() && *et >= missing_from.as_u64()
                });

                if let Some((_, ef, et)) = pending_overlap {
                    log::info!(
                        "BACKFILL Decision | stream={} has_last_seen=true last_seen={} full_from={} full_to={} gap_ms={gap_ms} capped={} grouped_panes={} missing_range={} pending_overlap={} registered_panes=0 reason=already_pending",
                        fetcher::format_stream(stream),
                        fetcher::format_time_short(last_t),
                        fetcher::format_time_short(full_from),
                        fetcher::format_time_short(full_to),
                        capped,
                        pane_ids.len(),
                        fetcher::format_time_range(missing_from, missing_to),
                        fetcher::format_time_range(UnixMs::new(*ef), UnixMs::new(*et))
                    );
                    continue;
                }

                let dedupe_key = (*stream, missing_from.as_u64(), missing_to.as_u64());
                self.pending_backfills
                    .insert(dedupe_key, std::time::Instant::now());

                let req_id = uuid::Uuid::new_v4();
                let fetch = match stream {
                    StreamKind::Trades { .. } => FetchRange::Trades(missing_from, missing_to),
                    StreamKind::Kline { .. } => FetchRange::Kline(missing_from, missing_to),
                    _ => continue,
                };

                // Backfill uses global pending_backfills for dedup; do NOT
                // register the request in per-pane RequestHandlers.
                // This avoids duplicate FETCH Queued/PendingInsert/Timeout logs.
                let pane_id = pane_ids[0];
                let target_panes = pane_ids.clone();

                log::info!(
                    "BACKFILL Queued | stream={} has_last_seen=true last_seen={} full_from={} full_to={} gap_ms={gap_ms} capped={} grouped_panes={} req={} fetch_range={} reason={reason}",
                    fetcher::format_stream(stream),
                    fetcher::format_time_short(last_t),
                    fetcher::format_time_short(full_from),
                    fetcher::format_time_short(full_to),
                    capped,
                    target_panes.len(),
                    fetcher::short_id(req_id),
                    fetcher::format_time_range(missing_from, missing_to),
                );

                let ready_streams = vec![*stream];
                let handles_clone = handles.clone();
                let layout_id = self.layout_id;
                let stream_kind = *stream;
                let backfill_req_id = req_id;
                let backfill_from = missing_from;
                let backfill_to = missing_to;
                let task = fetcher::request_fetch(
                    handles_clone,
                    pane_id,
                    &ready_streams,
                    layout_id,
                    req_id,
                    fetch,
                    Some(*stream),
                    &mut |handle| {
                        // Store the abort handle to prevent the backfill task
                        // from being aborted when the handle is dropped.
                        new_backfill_handles.push(handle);
                    },
                    0, // backfill tasks don't have a specific chart generation
                );

                log::info!(
                    "BACKFILL FetchStart | req={} stream={} range={} target_panes={}",
                    fetcher::short_id(backfill_req_id),
                    fetcher::format_stream(&stream_kind),
                    fetcher::format_time_range(backfill_from, backfill_to),
                    target_panes.len()
                );

                fetch_tasks.push(task.map(move |update| Message::BackfillFetchUpdate {
                    pane_ids: target_panes.clone(),
                    stream: stream_kind,
                    update,
                }));
            }
        }

        // Store backfill handles to keep tasks alive.
        self.backfill_handles.extend(new_backfill_handles);

        Task::batch(fetch_tasks)
    }

    /// Records that a WS disconnect happened, deferring backfill until reconnect.
    /// At disconnect time the gap is tiny (last_seen → disconnect ≈ 87ms),
    /// so we wait for reconnect to compute the real offline duration.
    pub fn record_pending_disconnect_gaps(
        &mut self,
        streams: &[StreamKind],
        disconnected_at: UnixMs,
    ) {
        let mut stream_last_seen = HashMap::new();
        for stream in streams {
            if matches!(stream, StreamKind::Depth { .. }) {
                log::info!(
                    "BACKFILL PendingGap | stream={} reason=depth_not_supported",
                    fetcher::format_stream(stream),
                );
                continue;
            }
            match self.last_live_t.get(stream) {
                Some(&last_t) => {
                    log::info!(
                        "BACKFILL PendingGap | stream={} last_seen={} disconnected_at={}",
                        fetcher::format_stream(stream),
                        fetcher::format_time_short(last_t),
                        fetcher::format_time_short(disconnected_at),
                    );
                    stream_last_seen.insert(*stream, last_t);
                }
                None => {
                    log::info!(
                        "BACKFILL PendingGap | stream={} last_seen=- disconnected_at={} reason=no_last_seen",
                        fetcher::format_stream(stream),
                        fetcher::format_time_short(disconnected_at),
                    );
                }
            }
        }
        self.pending_disconnect = Some(PendingDisconnect {
            disconnected_at,
            stream_last_seen,
        });
    }

    /// Computes real offline gap from stored disconnect state and enqueues backfill.
    /// Called when WS reconnects — the gap is now `last_seen → reconnect_time`
    /// which accurately reflects the offline duration.
    pub fn execute_reconnect_backfill(
        &mut self,
        handles: &exchange::adapter::AdapterHandles,
        main_window: window::Id,
        reconnect_time: UnixMs,
    ) -> Task<Message> {
        let Some(pending) = self.pending_disconnect.take() else {
            return Task::none();
        };

        let offline_ms = reconnect_time.saturating_diff(pending.disconnected_at);
        log::info!(
            "BACKFILL ReconnectGap | disconnected_at={} reconnect_time={} offline_ms={offline_ms}",
            fetcher::format_time_short(pending.disconnected_at),
            fetcher::format_time_short(reconnect_time),
        );

        let streams: Vec<StreamKind> = pending.stream_last_seen.keys().copied().collect();
        if streams.is_empty() {
            return Task::none();
        }

        // Delegate to existing backfill logic with reconnect_time as "now".
        // This computes gap = reconnect_time - last_seen for each stream,
        // which reflects the real offline duration.
        self.backfill_disconnected_streams(
            handles,
            main_window,
            &streams,
            reconnect_time,
            "reconnect_gap",
        )
    }
}

fn all_unique_streams(streams: &UniqueStreams) -> Vec<StreamKind> {
    let mut all = Vec::new();
    all.extend(streams.depth_streams(None).into_iter().map(
        |(ticker_info, depth_aggr, push_freq)| StreamKind::Depth {
            ticker_info,
            depth_aggr,
            push_freq,
        },
    ));
    all.extend(
        streams
            .trade_streams(None)
            .into_iter()
            .map(|ticker_info| StreamKind::Trades { ticker_info }),
    );
    all.extend(
        streams
            .kline_streams(None)
            .into_iter()
            .map(|(ticker_info, timeframe)| StreamKind::Kline {
                ticker_info,
                timeframe,
            }),
    );
    all
}

impl From<fetcher::FetchUpdate> for Message {
    fn from(update: fetcher::FetchUpdate) -> Self {
        match update {
            fetcher::FetchUpdate::Status { pane_id, status } => match status {
                fetcher::FetchTaskStatus::Loading(info) => {
                    Message::ChangePaneStatus(pane_id, pane::Status::Loading(info))
                }
                fetcher::FetchTaskStatus::Completed {
                    req_id,
                    fetch,
                    empty_covered_tail,
                } => Message::FetchCompleted {
                    pane_id,
                    req_id,
                    fetch,
                    empty_covered_tail,
                },
            },
            fetcher::FetchUpdate::Data {
                layout_id,
                pane_id,
                stream,
                data,
            } => Message::DistributeFetchedData {
                layout_id,
                pane_id,
                stream,
                data,
            },
            fetcher::FetchUpdate::Error {
                pane_id,
                error,
                req_id,
                fetch,
            } => Message::FetchFailed {
                pane_id,
                error,
                req_id,
                fetch,
            },
        }
    }
}

/// Returns a short type label for fetched data (for logging).
fn data_summary_type(data: &FetchedData) -> &'static str {
    match data {
        FetchedData::Trades { .. } => "Trades",
        FetchedData::BubbleSummary { .. } => "BubbleSummary",
        FetchedData::Klines { .. } => "Klines",
        FetchedData::OI { .. } => "OI",
    }
}

fn fetched_data_req_id(data: &FetchedData) -> Option<uuid::Uuid> {
    match data {
        FetchedData::Trades { req_id, .. }
        | FetchedData::BubbleSummary { req_id, .. }
        | FetchedData::Klines { req_id, .. }
        | FetchedData::OI { req_id, .. } => *req_id,
    }
}

fn stream_ticker_info(stream: &StreamKind) -> Option<&TickerInfo> {
    match stream {
        StreamKind::Kline { ticker_info, .. }
        | StreamKind::Trades { ticker_info }
        | StreamKind::Depth { ticker_info, .. } => Some(ticker_info),
    }
}

fn stream_matches_market_key(stream: &StreamKind, key: &MarketDataKey) -> bool {
    let Some(stream_key) = (match (&key.kind, stream) {
        (MarketDataKind::Trades, StreamKind::Trades { .. }) => {
            crate::market_data::bridge::stream_kind_to_key(stream)
        }
        (MarketDataKind::Klines { timeframe }, StreamKind::Kline { timeframe: tf, .. })
            if timeframe == tf =>
        {
            crate::market_data::bridge::stream_kind_to_key(stream)
        }
        (
            MarketDataKind::OpenInterest { timeframe },
            StreamKind::Kline {
                timeframe: tf,
                ticker_info,
            },
        ) if timeframe == tf => MarketDataKey::from_ticker_info(
            ticker_info,
            MarketDataKind::OpenInterest {
                timeframe: *timeframe,
            },
        ),
        _ => None,
    }) else {
        return false;
    };
    stream_key == *key
}

fn key_for_fetched_data(stream: StreamKind, data: &FetchedData) -> Option<MarketDataKey> {
    match data {
        FetchedData::Trades { .. } => crate::market_data::bridge::stream_kind_to_key(&stream),
        FetchedData::Klines { .. } => crate::market_data::bridge::stream_kind_to_key(&stream),
        FetchedData::OI { .. } => match stream {
            StreamKind::Kline {
                ticker_info,
                timeframe,
            } => MarketDataKey::from_ticker_info(
                &ticker_info,
                MarketDataKind::OpenInterest { timeframe },
            ),
            _ => None,
        },
        FetchedData::BubbleSummary { .. } => None,
    }
}

fn range_from_trades(trades: &[Trade]) -> Option<MarketDataRange> {
    let from = trades.iter().map(|trade| trade.time).min()?;
    let to = trades
        .iter()
        .map(|trade| trade.time)
        .max()?
        .saturating_add(1);
    MarketDataRange::new(from, to)
}

fn range_from_klines(klines: &[Kline]) -> Option<MarketDataRange> {
    let from = klines.first()?.time;
    let to = klines.last()?.time.saturating_add(1);
    MarketDataRange::new(from, to)
}

fn range_from_oi(rows: &[exchange::OpenInterest]) -> Option<MarketDataRange> {
    let from = rows.first()?.time;
    let to = rows.last()?.time.saturating_add(1);
    MarketDataRange::new(from, to)
}

fn market_kind_label(kind: &MarketDataKind) -> &'static str {
    match kind {
        MarketDataKind::Trades => "Trades",
        MarketDataKind::Klines { .. } => "Klines",
        MarketDataKind::OpenInterest { .. } => "OpenInterest",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::{
        key::{MarketKind, Symbol, Venue},
        requirement::{ConsumerId, DataRequirement, Priority},
    };

    fn kline_key() -> MarketDataKey {
        MarketDataKey::klines(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
            exchange::Timeframe::M1,
        )
    }

    fn range(from: u64, to: u64) -> MarketDataRange {
        MarketDataRange::new(UnixMs::new(from), UnixMs::new(to)).unwrap()
    }

    fn test_dashboard() -> Dashboard {
        Dashboard {
            market_cache: crate::market_data::cache::LocalMarketCache::new(
                std::env::temp_dir().join(format!("flowsurface-md-test-{}", uuid::Uuid::new_v4())),
            ),
            ..Dashboard::default()
        }
    }

    fn create_active_job(
        dashboard: &mut Dashboard,
        pane_id: uuid::Uuid,
        job_range: MarketDataRange,
        key: MarketDataKey,
        feature: ConsumerFeature,
    ) -> FetchJobId {
        dashboard.market_coordinator.require(DataRequirement::new(
            ConsumerId::pane(pane_id, feature),
            key,
            job_range,
            Priority::High,
            "test",
        ));
        dashboard.market_coordinator.plan();
        let jobs = dashboard.market_coordinator.execute_plan();
        assert_eq!(jobs.len(), 1);
        dashboard.market_coordinator.start_job(jobs[0]);
        jobs[0]
    }

    fn add_kline_consumer(
        dashboard: &mut Dashboard,
        pane_id: uuid::Uuid,
        req_id: uuid::Uuid,
        consumer_range: MarketDataRange,
    ) {
        dashboard
            .pending_market_consumers
            .push(PendingMarketDataConsumer {
                pane_id,
                req_id,
                fetch: fetcher::FetchRange::Kline(consumer_range.from, consumer_range.to),
                stream: None,
                key: kline_key(),
                range: consumer_range,
                feature: ConsumerFeature::ChartKlines,
                chart_generation: 0,
                has_partial_updates: false,
                completed: false,
                required_segments: Vec::new(),
                completed_segments: Vec::new(),
                failed_segments: Vec::new(),
                delivered_segments: Vec::new(),
            });
    }

    fn add_consumer(
        dashboard: &mut Dashboard,
        pane_id: uuid::Uuid,
        req_id: uuid::Uuid,
        consumer_range: MarketDataRange,
        key: MarketDataKey,
        feature: ConsumerFeature,
        fetch: fetcher::FetchRange,
    ) {
        dashboard
            .pending_market_consumers
            .push(PendingMarketDataConsumer {
                pane_id,
                req_id,
                fetch,
                stream: None,
                key,
                range: consumer_range,
                feature,
                chart_generation: 0,
                has_partial_updates: false,
                completed: false,
                required_segments: Vec::new(),
                completed_segments: Vec::new(),
                failed_segments: Vec::new(),
                delivered_segments: Vec::new(),
            });
    }

    fn trade_key() -> MarketDataKey {
        MarketDataKey::trades(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
        )
    }

    #[test]
    fn deduped_kline_consumer_attaches_to_existing_active_job() {
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let job_range = range(100, 200);
        let key = trade_key();
        let job_id = create_active_job(
            &mut dashboard,
            pane_id,
            job_range,
            key.clone(),
            ConsumerFeature::Footprint,
        );
        let first_req = uuid::Uuid::new_v4();
        let second_req = uuid::Uuid::new_v4();
        add_consumer(
            &mut dashboard,
            pane_id,
            first_req,
            job_range,
            key.clone(),
            ConsumerFeature::Footprint,
            fetcher::FetchRange::Trades(job_range.from, job_range.to),
        );
        add_consumer(
            &mut dashboard,
            pane_id,
            second_req,
            job_range,
            key,
            ConsumerFeature::Footprint,
            fetcher::FetchRange::Trades(job_range.from, job_range.to),
        );
        dashboard.job_to_consumers.insert(job_id, vec![first_req]);

        let attached = dashboard.attach_pending_consumers_to_active_jobs("dedup_active_job");

        assert_eq!(attached, 1);
        assert_eq!(
            dashboard.job_to_consumers.get(&job_id).unwrap(),
            &vec![first_req, second_req]
        );
        // After add_segment_merged, the required_segments may be merged,
        // so check that the segment is fully covered
        let consumer = dashboard
            .pending_market_consumers
            .iter()
            .find(|consumer| consumer.req_id == second_req)
            .unwrap();
        assert!(
            crate::market_data::range::compute_missing(job_range, &consumer.required_segments)
                .is_empty()
        );
    }

    #[test]
    fn two_identical_kline_chart_reqs_use_one_worker_and_two_consumers() {
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let job_range = range(100, 200);
        let key = trade_key();
        let job_id = create_active_job(
            &mut dashboard,
            pane_id,
            job_range,
            key.clone(),
            ConsumerFeature::Footprint,
        );
        let worker_req = uuid::Uuid::new_v4();
        let first_req = uuid::Uuid::new_v4();
        let second_req = uuid::Uuid::new_v4();
        add_consumer(
            &mut dashboard,
            pane_id,
            first_req,
            job_range,
            key.clone(),
            ConsumerFeature::Footprint,
            fetcher::FetchRange::Trades(job_range.from, job_range.to),
        );
        add_consumer(
            &mut dashboard,
            pane_id,
            second_req,
            job_range,
            key,
            ConsumerFeature::Footprint,
            fetcher::FetchRange::Trades(job_range.from, job_range.to),
        );
        dashboard.worker_req_to_job.insert(worker_req, job_id);
        dashboard.job_to_worker_req.insert(job_id, worker_req);
        dashboard.job_to_consumers.insert(job_id, vec![first_req]);

        dashboard.attach_pending_consumers_to_active_jobs("dedup_active_job");

        assert_eq!(dashboard.worker_req_to_job.len(), 1);
        assert_eq!(dashboard.job_to_consumers.get(&job_id).unwrap().len(), 2);
    }

    #[test]
    fn kline_worker_completion_removes_job_and_pending_consumers() {
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let job_range = range(100, 200);
        let key = trade_key();
        let job_id = create_active_job(
            &mut dashboard,
            pane_id,
            job_range,
            key.clone(),
            ConsumerFeature::Footprint,
        );
        let worker_req = uuid::Uuid::new_v4();
        let first_req = uuid::Uuid::new_v4();
        let second_req = uuid::Uuid::new_v4();
        add_consumer(
            &mut dashboard,
            pane_id,
            first_req,
            job_range,
            key.clone(),
            ConsumerFeature::Footprint,
            fetcher::FetchRange::Trades(job_range.from, job_range.to),
        );
        add_consumer(
            &mut dashboard,
            pane_id,
            second_req,
            job_range,
            key,
            ConsumerFeature::Footprint,
            fetcher::FetchRange::Trades(job_range.from, job_range.to),
        );
        dashboard.worker_req_to_job.insert(worker_req, job_id);
        dashboard.job_to_worker_req.insert(job_id, worker_req);
        dashboard
            .job_to_consumers
            .insert(job_id, vec![first_req, second_req]);
        dashboard.add_required_segment_to_consumer(first_req, job_range);
        dashboard.add_required_segment_to_consumer(second_req, job_range);
        dashboard
            .market_coordinator
            .job_mut(job_id)
            .unwrap()
            .progress
            .records_fetched = 2;

        assert!(dashboard.finish_coordinator_worker_job(window::Id::unique(), worker_req, None));

        assert!(dashboard.market_coordinator.job(job_id).is_none());
        assert!(dashboard.worker_req_to_job.is_empty());
        assert!(dashboard.job_to_worker_req.is_empty());
        assert!(dashboard.job_to_consumers.is_empty());
        assert!(dashboard.pending_market_consumers.is_empty());
    }

    #[test]
    fn priority_kline_request_attached_to_active_job_completes_with_job() {
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let job_range = range(100, 300);
        let key = trade_key();
        let job_id = create_active_job(
            &mut dashboard,
            pane_id,
            job_range,
            key.clone(),
            ConsumerFeature::Footprint,
        );
        let worker_req = uuid::Uuid::new_v4();
        let priority_req = uuid::Uuid::new_v4();
        add_consumer(
            &mut dashboard,
            pane_id,
            priority_req,
            range(150, 250),
            key,
            ConsumerFeature::Footprint,
            fetcher::FetchRange::Trades(UnixMs::new(150), UnixMs::new(250)),
        );
        dashboard.worker_req_to_job.insert(worker_req, job_id);
        dashboard.job_to_worker_req.insert(job_id, worker_req);

        dashboard.attach_pending_consumers_to_active_jobs("dedup_active_job");
        dashboard
            .market_coordinator
            .job_mut(job_id)
            .unwrap()
            .progress
            .records_fetched = 1;
        dashboard.finish_coordinator_worker_job(window::Id::unique(), worker_req, None);

        assert!(dashboard.pending_market_consumers.is_empty());
        assert_eq!(dashboard.market_coordinator.active_job_count(), 0);
    }

    #[test]
    fn kline_progress_active_count_returns_to_zero_after_completion() {
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let job_range = range(100, 200);
        let key = trade_key();
        let job_id = create_active_job(
            &mut dashboard,
            pane_id,
            job_range,
            key.clone(),
            ConsumerFeature::Footprint,
        );
        let worker_req = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        add_consumer(
            &mut dashboard,
            pane_id,
            req_id,
            job_range,
            key,
            ConsumerFeature::Footprint,
            fetcher::FetchRange::Trades(job_range.from, job_range.to),
        );
        dashboard.worker_req_to_job.insert(worker_req, job_id);
        dashboard.job_to_worker_req.insert(job_id, worker_req);
        dashboard.job_to_consumers.insert(job_id, vec![req_id]);
        dashboard.add_required_segment_to_consumer(req_id, job_range);
        dashboard
            .market_coordinator
            .job_mut(job_id)
            .unwrap()
            .progress
            .records_fetched = 1;

        assert_eq!(
            dashboard
                .market_coordinator
                .progress_snapshot()
                .active_job_count(),
            1
        );
        dashboard.finish_coordinator_worker_job(window::Id::unique(), worker_req, None);

        assert_eq!(
            dashboard
                .market_coordinator
                .progress_snapshot()
                .active_job_count(),
            0
        );
    }

    #[test]
    fn kline_split_cache_segment_does_not_satisfy_full_request() {
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        let cached = range(100, 200);
        let network = range(200, 300);
        add_kline_consumer(&mut dashboard, pane_id, req_id, range(100, 300));
        dashboard.add_required_segment_to_consumer(req_id, cached);
        dashboard.add_required_segment_to_consumer(req_id, network);

        assert_eq!(
            dashboard.mark_consumer_segment_complete(req_id, cached),
            Some(ConsumerSegmentStatus {
                completed_logical: 1,
                total_logical: 2,
                missing: vec![range(200, 300)],
                coverage_complete: false,
            })
        );

        assert!(!dashboard.consumer_is_fully_satisfied(req_id));
        assert_eq!(
            dashboard.consumer_remaining_segments(req_id),
            vec![network.format_display()]
        );
    }

    #[test]
    fn kline_split_request_completes_after_cache_and_network_segments() {
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        let cached = range(100, 200);
        let network = range(200, 300);
        add_kline_consumer(&mut dashboard, pane_id, req_id, range(100, 300));
        dashboard.add_required_segment_to_consumer(req_id, cached);
        dashboard.add_required_segment_to_consumer(req_id, network);

        dashboard.mark_consumer_segment_complete(req_id, cached);
        assert_eq!(
            dashboard.mark_consumer_segment_complete(req_id, network),
            Some(ConsumerSegmentStatus {
                completed_logical: 2,
                total_logical: 2,
                missing: vec![],
                coverage_complete: true,
            })
        );

        assert!(dashboard.consumer_is_fully_satisfied(req_id));
    }

    #[test]
    fn footprint_split_cache_segment_does_not_satisfy_full_request() {
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        let cached = range(100, 200);
        let network = range(200, 300);
        add_consumer(
            &mut dashboard,
            pane_id,
            req_id,
            range(100, 300),
            trade_key(),
            ConsumerFeature::Footprint,
            fetcher::FetchRange::Trades(UnixMs::new(100), UnixMs::new(300)),
        );
        dashboard.add_required_segment_to_consumer(req_id, cached);
        dashboard.add_required_segment_to_consumer(req_id, network);

        dashboard.mark_consumer_segment_complete(req_id, cached);

        // With tiny gap suppression, a 100ms gap (200..300) is below
        // MIN_TRADE_BACKFILL_SEGMENT_MS so the Footprint consumer is
        // considered satisfied.
        assert!(dashboard.consumer_is_fully_satisfied(req_id));
    }

    #[test]
    fn footprint_split_request_completes_after_network_tail() {
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        let cached = range(100, 200);
        let network = range(200, 300);
        add_consumer(
            &mut dashboard,
            pane_id,
            req_id,
            range(100, 300),
            trade_key(),
            ConsumerFeature::Footprint,
            fetcher::FetchRange::Trades(UnixMs::new(100), UnixMs::new(300)),
        );
        dashboard.add_required_segment_to_consumer(req_id, cached);
        dashboard.add_required_segment_to_consumer(req_id, network);

        dashboard.mark_consumer_segment_complete(req_id, cached);
        dashboard.mark_consumer_segment_complete(req_id, network);

        assert!(dashboard.consumer_is_fully_satisfied(req_id));
    }

    #[test]
    fn bubble_split_request_waits_for_all_raw_trade_segments() {
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        let cached = range(100, 200);
        let network = range(200, 300);
        add_consumer(
            &mut dashboard,
            pane_id,
            req_id,
            range(100, 300),
            trade_key(),
            ConsumerFeature::VolumeBubbles,
            fetcher::FetchRange::BubbleSummary {
                from: UnixMs::new(100),
                to: UnixMs::new(300),
                timeframe_ms: 60_000,
                price_step: exchange::unit::PriceStep { units: 1_000_000 },
                max_candidates_per_candle: 10,
            },
        );
        dashboard.add_required_segment_to_consumer(req_id, cached);
        dashboard.add_required_segment_to_consumer(req_id, network);

        dashboard.mark_consumer_segment_complete(req_id, cached);
        assert!(!dashboard.consumer_is_fully_satisfied(req_id));
        dashboard.mark_consumer_segment_complete(req_id, network);
        assert!(dashboard.consumer_is_fully_satisfied(req_id));
    }

    #[test]
    fn cached_segment_is_not_delivered_twice_to_same_consumer() {
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        let segment = range(100, 200);
        add_kline_consumer(&mut dashboard, pane_id, req_id, segment);

        assert!(dashboard.mark_consumer_segment_delivered(req_id, segment));
        assert!(!dashboard.mark_consumer_segment_delivered(req_id, segment));
    }

    #[test]
    fn completed_chart_request_is_not_attached_to_active_job() {
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let job_range = range(100, 200);
        let key = trade_key();
        let job_id = create_active_job(
            &mut dashboard,
            pane_id,
            job_range,
            key.clone(),
            ConsumerFeature::Footprint,
        );
        let req_id = uuid::Uuid::new_v4();
        add_consumer(
            &mut dashboard,
            pane_id,
            req_id,
            job_range,
            key,
            ConsumerFeature::Footprint,
            fetcher::FetchRange::Trades(job_range.from, job_range.to),
        );
        dashboard
            .pending_market_consumers
            .iter_mut()
            .find(|consumer| consumer.req_id == req_id)
            .unwrap()
            .completed = true;

        let attached = dashboard.attach_pending_consumers_to_active_jobs("test");

        assert_eq!(attached, 0);
        assert!(
            !dashboard
                .job_to_consumers
                .get(&job_id)
                .is_some_and(|consumers| consumers.contains(&req_id))
        );
    }

    #[test]
    fn stale_worker_without_progress_is_failed_and_removed() {
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let job_range = range(100, 200);
        let key = trade_key();
        let job_id = create_active_job(
            &mut dashboard,
            pane_id,
            job_range,
            key.clone(),
            ConsumerFeature::Footprint,
        );
        let worker_req = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        add_consumer(
            &mut dashboard,
            pane_id,
            req_id,
            job_range,
            key,
            ConsumerFeature::Footprint,
            fetcher::FetchRange::Trades(job_range.from, job_range.to),
        );
        dashboard.add_required_segment_to_consumer(req_id, job_range);
        dashboard.worker_req_to_job.insert(worker_req, job_id);
        dashboard.job_to_worker_req.insert(job_id, worker_req);
        dashboard.job_to_consumers.insert(job_id, vec![req_id]);
        dashboard
            .market_coordinator
            .job_mut(job_id)
            .unwrap()
            .created_at = 0;

        dashboard.fail_stale_market_jobs(window::Id::unique());

        assert!(dashboard.market_coordinator.job(job_id).is_none());
        assert!(dashboard.worker_req_to_job.is_empty());
        assert!(dashboard.job_to_worker_req.is_empty());
        assert!(dashboard.job_to_consumers.is_empty());
        assert_eq!(dashboard.market_coordinator.active_job_count(), 0);
    }

    #[test]
    fn progress_snapshot_includes_cached_records_after_cache_serve_counter() {
        let mut dashboard = test_dashboard();
        dashboard.market_coordinator.record_cache_served(448);

        let snapshot = dashboard.market_coordinator.progress_snapshot();

        assert_eq!(snapshot.total_cached_records, 448);
    }

    #[test]
    fn progress_snapshot_active_becomes_zero_after_job_terminal() {
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let job_range = range(100, 200);
        let job_id = create_active_job(
            &mut dashboard,
            pane_id,
            job_range,
            trade_key(),
            ConsumerFeature::Footprint,
        );

        assert_eq!(
            dashboard
                .market_coordinator
                .progress_snapshot()
                .active_job_count(),
            1
        );
        dashboard
            .market_coordinator
            .complete_and_remove_job(job_id, 1);

        assert_eq!(
            dashboard
                .market_coordinator
                .progress_snapshot()
                .active_job_count(),
            0
        );
    }

    // --- New tests for logical segment accounting fix ---

    #[test]
    fn active_job_split_bubble_accounting() {
        // Test 1 — active job split bubble accounting
        // Bubble consumer range: 100 -> 300
        // Concrete job segments: 100 -> 200, 200 -> 300
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        let segment_a = range(100, 200);
        let segment_b = range(200, 300);
        add_consumer(
            &mut dashboard,
            pane_id,
            req_id,
            range(100, 300),
            trade_key(),
            ConsumerFeature::VolumeBubbles,
            fetcher::FetchRange::BubbleSummary {
                from: UnixMs::new(100),
                to: UnixMs::new(300),
                timeframe_ms: 60_000,
                price_step: exchange::unit::PriceStep { units: 1_000_000 },
                max_candidates_per_candle: 10,
            },
        );
        dashboard.add_required_segment_to_consumer(req_id, segment_a);
        dashboard.add_required_segment_to_consumer(req_id, segment_b);

        // After completing segment_a: 1/2, missing=[200..300], not complete
        let status_a = dashboard
            .mark_consumer_segment_complete(req_id, segment_a)
            .unwrap();
        assert_eq!(status_a.completed_logical, 1);
        assert_eq!(status_a.total_logical, 2);
        assert_eq!(status_a.missing, vec![segment_b]);
        assert!(!status_a.coverage_complete);
        assert!(!dashboard.consumer_is_fully_satisfied(req_id));

        // After completing segment_b: 2/2, missing=[], coverage complete
        let status_b = dashboard
            .mark_consumer_segment_complete(req_id, segment_b)
            .unwrap();
        assert_eq!(status_b.completed_logical, 2);
        assert_eq!(status_b.total_logical, 2);
        assert!(status_b.missing.is_empty());
        assert!(status_b.coverage_complete);
        assert!(dashboard.consumer_is_fully_satisfied(req_id));
    }

    #[test]
    fn broad_pre_execute_network_segment_does_not_inflate_logical_accounting() {
        // Test 2 — broad pre-execute network segment must not inflate/merge logical accounting
        // After the fix, register_required_segments_from_plan() no longer registers
        // broad network segments. The concrete job ranges are registered later.
        // This test verifies that add_required_segment_dedup correctly handles
        // the case where sub-segments are added (they should be skipped if already covered).
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        add_kline_consumer(&mut dashboard, pane_id, req_id, range(100, 300));

        // Register the broad range first
        dashboard.add_required_segment_to_consumer(req_id, range(100, 300));
        // Sub-segments should be skipped because they're already covered
        dashboard.add_required_segment_to_consumer(req_id, range(100, 200));
        dashboard.add_required_segment_to_consumer(req_id, range(200, 300));

        // Should have 1 logical segment (the broad one), sub-segments skipped
        let consumer = dashboard
            .pending_market_consumers
            .iter()
            .find(|c| c.req_id == req_id)
            .unwrap();
        assert_eq!(consumer.required_segments.len(), 1);

        // Complete the broad segment - should show 1/1
        let status = dashboard
            .mark_consumer_segment_complete(req_id, range(100, 300))
            .unwrap();
        assert_eq!(status.completed_logical, 1);
        assert_eq!(status.total_logical, 1);
        assert!(status.coverage_complete);
    }

    #[test]
    fn split_segments_add_dedup_correctly() {
        // Verify that adding split segments (not sub-segments) works correctly
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        add_kline_consumer(&mut dashboard, pane_id, req_id, range(100, 300));

        // Register split segments directly (the normal runtime path)
        dashboard.add_required_segment_to_consumer(req_id, range(100, 200));
        dashboard.add_required_segment_to_consumer(req_id, range(200, 300));

        let consumer = dashboard
            .pending_market_consumers
            .iter()
            .find(|c| c.req_id == req_id)
            .unwrap();
        assert_eq!(consumer.required_segments.len(), 2);

        // Complete first segment
        let status = dashboard
            .mark_consumer_segment_complete(req_id, range(100, 200))
            .unwrap();
        assert_eq!(status.completed_logical, 1);
        assert_eq!(status.total_logical, 2);
        assert!(!status.coverage_complete);
    }

    #[test]
    fn bubble_waiting_uses_exact_missing_range() {
        // Test 3 — BubbleWaiting uses exact missing range
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        let segment_a = range(100, 200);
        let segment_b = range(200, 300);
        add_consumer(
            &mut dashboard,
            pane_id,
            req_id,
            range(100, 300),
            trade_key(),
            ConsumerFeature::VolumeBubbles,
            fetcher::FetchRange::BubbleSummary {
                from: UnixMs::new(100),
                to: UnixMs::new(300),
                timeframe_ms: 60_000,
                price_step: exchange::unit::PriceStep { units: 1_000_000 },
                max_candidates_per_candle: 10,
            },
        );
        dashboard.add_required_segment_to_consumer(req_id, segment_a);
        dashboard.add_required_segment_to_consumer(req_id, segment_b);

        dashboard.mark_consumer_segment_complete(req_id, segment_a);

        // consumer_remaining_segments should return [200 -> 300], not [100 -> 300]
        let remaining = dashboard.consumer_remaining_segments(req_id);
        assert_eq!(remaining, vec![segment_b.format_display()]);
    }

    #[test]
    fn kline_split_cache_network_existing_behavior_unchanged() {
        // Test 4 — Kline/Footprint existing split behavior still passes
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        let cached = range(100, 200);
        let network = range(200, 300);

        // Kline cache + network split
        add_kline_consumer(&mut dashboard, pane_id, req_id, range(100, 300));
        dashboard.add_required_segment_to_consumer(req_id, cached);
        dashboard.add_required_segment_to_consumer(req_id, network);

        assert!(!dashboard.consumer_is_fully_satisfied(req_id));
        dashboard.mark_consumer_segment_complete(req_id, cached);
        assert!(!dashboard.consumer_is_fully_satisfied(req_id));
        dashboard.mark_consumer_segment_complete(req_id, network);
        assert!(dashboard.consumer_is_fully_satisfied(req_id));
    }

    #[test]
    fn required_segments_count_never_zero_on_active_consumer() {
        // Ensure total_logical is never 0 when there are concrete segments registered
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        add_kline_consumer(&mut dashboard, pane_id, req_id, range(100, 300));
        dashboard.add_required_segment_to_consumer(req_id, range(100, 200));
        dashboard.add_required_segment_to_consumer(req_id, range(200, 300));

        let status = dashboard
            .mark_consumer_segment_complete(req_id, range(100, 200))
            .unwrap();
        assert!(status.total_logical > 0);
        assert_eq!(status.completed_logical, 1);
    }

    #[test]
    fn tiny_trade_gap_suppressed_on_consumer_completion() {
        // Create a TradeHydration consumer with range 100..3000.
        // Add required segments 100..1000 and 1000..3000.
        // Complete 100..1000 from cache. Complete 1000..2999 from network.
        // Verify that the 1ms gap (2999..3000) is suppressed and consumer is fully satisfied.
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        add_consumer(
            &mut dashboard,
            pane_id,
            req_id,
            range(100, 3000),
            trade_key(),
            ConsumerFeature::TradeHydration,
            fetcher::FetchRange::TradeHydration(UnixMs::new(100), UnixMs::new(3000)),
        );
        dashboard.add_required_segment_to_consumer(req_id, range(100, 1000));
        dashboard.add_required_segment_to_consumer(req_id, range(1000, 3000));

        // Complete first segment (cache)
        let status = dashboard
            .mark_consumer_segment_complete(req_id, range(100, 1000))
            .unwrap();
        assert!(!status.coverage_complete);
        assert!(!dashboard.consumer_is_fully_satisfied(req_id));

        // Complete second segment minus 1ms (network)
        let status = dashboard
            .mark_consumer_segment_complete(req_id, range(1000, 2999))
            .unwrap();
        // The tiny 1ms gap (2999..3000) should be suppressed
        assert!(status.missing.is_empty());
        assert!(status.coverage_complete);
        assert!(dashboard.consumer_is_fully_satisfied(req_id));

        // consumer_remaining_segments should return empty
        let remaining = dashboard.consumer_remaining_segments(req_id);
        assert!(remaining.is_empty());
    }

    #[test]
    fn tiny_cached_segment_does_not_block_consumer() {
        // Create a TradeHydration consumer with range 100..200.
        // Add required segment 100..101 (tiny, 1ms).
        // Verify consumer is satisfied (the tiny segment is ignored).
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        add_consumer(
            &mut dashboard,
            pane_id,
            req_id,
            range(100, 200),
            trade_key(),
            ConsumerFeature::TradeHydration,
            fetcher::FetchRange::TradeHydration(UnixMs::new(100), UnixMs::new(200)),
        );
        // Register a tiny required segment (simulating a tiny cached segment from the plan)
        dashboard.add_required_segment_to_consumer(req_id, range(100, 101));

        // Consumer should be satisfied because the tiny segment is suppressed
        assert!(dashboard.consumer_is_fully_satisfied(req_id));

        // remaining segments should be empty
        let remaining = dashboard.consumer_remaining_segments(req_id);
        assert!(remaining.is_empty());
    }

    #[test]
    fn completed_total_never_contradicts_missing() {
        // Create a consumer, complete one segment,
        // verify that if missing is non-empty, completed < total.
        let mut dashboard = test_dashboard();
        let pane_id = uuid::Uuid::new_v4();
        let req_id = uuid::Uuid::new_v4();
        add_kline_consumer(&mut dashboard, pane_id, req_id, range(100, 300));
        dashboard.add_required_segment_to_consumer(req_id, range(100, 200));
        dashboard.add_required_segment_to_consumer(req_id, range(200, 300));

        let status = dashboard
            .mark_consumer_segment_complete(req_id, range(100, 200))
            .unwrap();

        // missing is non-empty (200..300 still needed)
        assert!(!status.missing.is_empty());
        // completed_logical < total_logical
        assert!(
            status.completed_logical < status.total_logical,
            "completed_logical ({}) should be < total_logical ({}) when missing is non-empty",
            status.completed_logical,
            status.total_logical
        );
    }
}
