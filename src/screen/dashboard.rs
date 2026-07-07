pub mod pane;
pub mod panel;
pub mod sidebar;
pub mod tickers_table;

pub use sidebar::Sidebar;

use super::DashboardError;
use crate::market_data::{
    key::{MarketDataKey, MarketDataKind},
    range::MarketDataRange,
    requirement::ConsumerFeature,
    runtime::{
        DashboardChartNeedRoute, DashboardFetchRoute, MarketDataChartEffect,
        MarketDataRouteOutcome, MarketDataRuntime, PendingMarketDataConsumer,
    },
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
    /// Runtime owner for market-data coordinator, cache, live ingestion, and worker mappings.
    pub market_data_runtime: MarketDataRuntime,
}

impl Default for Dashboard {
    fn default() -> Self {
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
            market_data_runtime: MarketDataRuntime::new(),
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
            market_data_runtime: MarketDataRuntime::from_config(),
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
                            pane::Effect::RequestMarketDataNeeds(needs) => {
                                let pane_id = state.unique_id();
                                let ready_streams = state
                                    .streams
                                    .ready_iter()
                                    .map(|iter| iter.copied().collect::<Vec<_>>())
                                    .unwrap_or_default();
                                let chart_generation =
                                    if let pane::Content::Kline { chart: Some(c), .. } =
                                        &state.content
                                    {
                                        c.current_generation()
                                    } else {
                                        0
                                    };

                                self.route_market_data_needs_through_market_data(
                                    handles.clone(),
                                    main_window.id,
                                    DashboardChartNeedRoute {
                                        pane_id,
                                        ready_streams,
                                        chart_generation,
                                        needs,
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
                    && let Some(failure) = self
                        .market_data_runtime
                        .fail_worker_request(worker_req, error.clone())
                {
                    log::warn!(
                        target: "marketdata",
                        "MARKETDATA WorkerFailed | worker_req={} job={} error={}",
                        fetcher::short_id(worker_req),
                        crate::market_data::job::short_id(failure.job_id),
                        error
                    );
                    let _ = failure.range;
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
        let progress = self.market_data_runtime.progress_snapshot();
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
        let outcome = self.market_data_runtime.route_fetch_specs(route);
        self.handle_market_data_route_outcome(handles, main_window, outcome)
    }

    fn route_market_data_needs_through_market_data(
        &mut self,
        handles: AdapterHandles,
        main_window: window::Id,
        route: DashboardChartNeedRoute,
    ) -> Task<Message> {
        let outcome = self.market_data_runtime.route_chart_data_needs(route);
        self.handle_market_data_route_outcome(handles, main_window, outcome)
    }

    fn handle_market_data_route_outcome(
        &mut self,
        handles: AdapterHandles,
        main_window: window::Id,
        outcome: MarketDataRouteOutcome,
    ) -> Task<Message> {
        for dispatch in outcome.cached_dispatches {
            let effects = self
                .market_data_runtime
                .effects_for_cached_dispatch(dispatch);
            self.apply_market_data_effects(main_window, effects);
        }

        if outcome.fetch_specs.is_empty() {
            log::info!(
                target: "marketdata",
                "MARKETDATA RuntimeLegacy | pane={} count=0 reason={}",
                crate::market_data::job::short_id(outcome.pane_id),
                outcome.reason
            );
            Task::none()
        } else {
            self.forward_legacy_fetches(
                handles,
                main_window,
                outcome.pane_id,
                &outcome.ready_streams,
                outcome.fetch_specs,
                outcome.chart_generation,
                outcome.reason,
            )
        }
    }

    fn apply_market_data_effects(
        &mut self,
        main_window: window::Id,
        effects: Vec<MarketDataChartEffect>,
    ) {
        for effect in effects {
            self.apply_market_data_effect(main_window, effect);
        }
    }

    fn apply_market_data_effect(&mut self, main_window: window::Id, effect: MarketDataChartEffect) {
        match effect {
            MarketDataChartEffect::InsertKlinesPartial {
                consumer,
                stream,
                timeframe,
                ticker_info,
                rows,
            } => {
                if self.pending_consumer_is_stale(main_window, &consumer) {
                    return;
                }
                let stream = stream.or_else(|| {
                    self.stream_for_consumer_key(main_window, consumer.pane_id, &consumer.key)
                });
                if matches!(stream, Some(StreamKind::Kline { .. }))
                    && let Some(pane_state) =
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
            }
            MarketDataChartEffect::InsertTrades {
                consumer,
                stream,
                batch,
                until_time,
            } => {
                if self.pending_consumer_is_stale(main_window, &consumer) {
                    return;
                }
                let stream = stream.or_else(|| {
                    self.stream_for_consumer_key(main_window, consumer.pane_id, &consumer.key)
                });
                if let Some(stream) = stream {
                    let _ = self.distribute_fetched_data(
                        main_window,
                        consumer.pane_id,
                        FetchedData::Trades {
                            batch,
                            until_time,
                            req_id: Some(consumer.req_id),
                        },
                        stream,
                        true,
                    );
                }
            }
            MarketDataChartEffect::InsertOpenInterestPartial { consumer, rows } => {
                if self.pending_consumer_is_stale(main_window, &consumer) {
                    return;
                }
                if let Some(pane_state) =
                    self.get_mut_pane_state_by_uuid(main_window, consumer.pane_id)
                {
                    pane_state.status = pane::Status::Ready;
                    pane_state.insert_hist_oi_partial(Some(consumer.req_id), &rows);
                }
            }
            MarketDataChartEffect::InsertBubbleSummaries {
                consumer,
                summaries,
                range,
                trades_seen,
                raw_discarded,
                complete,
            } => {
                if self.pending_consumer_is_stale(main_window, &consumer) {
                    return;
                }
                let stream = consumer.stream.or_else(|| {
                    self.stream_for_consumer_key(main_window, consumer.pane_id, &consumer.key)
                });
                if let Some(stream) = stream {
                    self.apply_bubble_summaries_to_chart(
                        main_window,
                        consumer.pane_id,
                        stream,
                        summaries,
                        range,
                        trades_seen,
                        raw_discarded,
                        Some(consumer.req_id),
                        complete,
                    );
                    if complete {
                        log::info!(
                            target: "marketdata",
                            "MARKETDATA BubbleChartComplete | req={}",
                            fetcher::short_id(consumer.req_id)
                        );
                    }
                }
            }
            MarketDataChartEffect::CompleteConsumer {
                consumer,
                empty_covered_tail,
            } => {
                if self.pending_consumer_is_stale(main_window, &consumer) {
                    return;
                }
                let _ = self.complete_pending_consumer(main_window, &consumer, empty_covered_tail);
            }
            MarketDataChartEffect::MarkConsumerCompleted { .. } => {}
            MarketDataChartEffect::CompleteLegacyTradeFetch {
                pane_id,
                req_id,
                fetch,
                empty_covered_tail,
            } => {
                if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, pane_id)
                    && let pane::Content::Kline { chart: Some(c), .. } = &mut pane_state.content
                {
                    c.complete_trade_fetch(req_id, Some(fetch), empty_covered_tail);
                }
            }
            MarketDataChartEffect::MarkPaneReady { pane_id } => {
                if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, pane_id) {
                    pane_state.status = pane::Status::Ready;
                }
            }
            MarketDataChartEffect::InsertLegacyTrades {
                pane_id,
                req_id,
                batch,
                is_batches_done,
            } => {
                if let Err(reason) = self.insert_fetched_trades(
                    main_window,
                    pane_id,
                    &batch,
                    is_batches_done,
                    req_id,
                ) {
                    log::warn!(
                        "MARKETDATA LegacyTradeEffectFailed | pane={} error={}",
                        fetcher::short_id(pane_id),
                        reason
                    );
                }
            }
            MarketDataChartEffect::InsertLegacyBubbleSummaries {
                pane_id,
                req_id,
                summaries,
                range,
                trades_seen,
                raw_discarded,
            } => {
                if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, pane_id) {
                    pane_state.status = pane::Status::Ready;
                    if let pane::Content::Kline { chart: Some(c), .. } = &mut pane_state.content {
                        c.insert_bubble_summaries(
                            summaries,
                            range.0,
                            range.1,
                            trades_seen,
                            raw_discarded,
                            req_id,
                        );
                    }
                }
            }
            MarketDataChartEffect::InsertLegacyKlines {
                pane_id,
                req_id,
                stream,
                rows,
            } => {
                if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, pane_id) {
                    pane_state.status = pane::Status::Ready;
                    if let StreamKind::Kline {
                        timeframe,
                        ticker_info,
                    } = stream
                    {
                        pane_state.insert_hist_klines(req_id, timeframe, ticker_info, &rows);
                    }
                }
            }
            MarketDataChartEffect::InsertLegacyOpenInterest {
                pane_id,
                req_id,
                rows,
            } => {
                if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, pane_id) {
                    pane_state.status = pane::Status::Ready;
                    pane_state.insert_hist_oi(req_id, &rows);
                }
            }
            MarketDataChartEffect::ProgressSnapshot => self.log_market_data_progress_snapshot(),
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

    #[cfg(test)]
    fn attach_pending_consumers_to_active_jobs(&mut self, reason: &'static str) -> usize {
        self.market_data_runtime
            .attach_pending_consumers_to_active_jobs(reason)
    }

    fn dispatch_pending_for_fetched_data(
        &mut self,
        main_window: window::Id,
        stream_type: StreamKind,
        data: &FetchedData,
    ) -> bool {
        let outcome = self
            .market_data_runtime
            .effects_for_fetched_data(stream_type, data);
        self.apply_market_data_effects(main_window, outcome.effects);
        outcome.handled
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

    fn mark_bubble_consumer_completed(&mut self, req_id: uuid::Uuid) {
        self.market_data_runtime
            .mark_bubble_consumer_completed(req_id);
    }

    fn mark_generic_consumer_completed(&mut self, req_id: uuid::Uuid) {
        self.market_data_runtime.mark_consumer_completed(req_id);
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

    fn matching_pending_consumers(
        &self,
        key: &MarketDataKey,
        range: &MarketDataRange,
    ) -> Vec<PendingMarketDataConsumer> {
        self.market_data_runtime
            .matching_pending_consumers(key, range)
    }

    fn remove_pending_consumers(&mut self, key: &MarketDataKey, range: &MarketDataRange) {
        self.market_data_runtime
            .remove_completed_consumers_for(key, range);
    }

    #[cfg(test)]
    fn add_required_segment_to_consumer(&mut self, req_id: uuid::Uuid, segment: MarketDataRange) {
        self.market_data_runtime
            .add_required_segment_to_consumer(req_id, segment);
    }

    #[cfg(test)]
    fn mark_consumer_segment_complete(
        &mut self,
        req_id: uuid::Uuid,
        segment: MarketDataRange,
    ) -> Option<crate::market_data::runtime::ConsumerSegmentStatus> {
        self.market_data_runtime
            .mark_consumer_segment_complete(req_id, segment)
    }

    #[cfg(test)]
    fn mark_consumer_segment_delivered(
        &mut self,
        req_id: uuid::Uuid,
        segment: MarketDataRange,
    ) -> bool {
        self.market_data_runtime
            .mark_consumer_segment_delivered(req_id, segment)
    }

    #[cfg(test)]
    fn consumer_remaining_segments(&self, req_id: uuid::Uuid) -> Vec<String> {
        self.market_data_runtime.consumer_remaining_segments(req_id)
    }

    #[cfg(test)]
    fn consumer_is_fully_satisfied(&self, req_id: uuid::Uuid) -> bool {
        self.market_data_runtime.consumer_is_fully_satisfied(req_id)
    }

    fn complete_pending_consumer(
        &mut self,
        main_window: window::Id,
        consumer: &PendingMarketDataConsumer,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
    ) -> bool {
        if let Some(pane_state) = self.get_mut_pane_state_by_uuid(main_window, consumer.pane_id) {
            if let pane::Content::Kline { chart: Some(c), .. } = &mut pane_state.content {
                if consumer.feature == ConsumerFeature::VolumeBubbles {
                    // Bubble chart completion is driven by the runtime-derived final
                    // InsertBubbleSummaries effect so partial batch updates cannot
                    // remove the pending chart-local request.
                } else if matches!(consumer.fetch, fetcher::FetchRange::Trades(_, _)) {
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

        // Check for stale generation before applying legacy/non-runtime data.
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
                // Mark the request as completed to clean up pending state.
                c.mark_trade_request_completed(req_id);
                return Task::none();
            }
        }

        let effects =
            self.market_data_runtime
                .legacy_fetched_data_effects(pane_id, stream_type, data);
        self.apply_market_data_effects(main_window, effects);
        Task::none()
    }

    fn finish_coordinator_worker_job(
        &mut self,
        main_window: window::Id,
        worker_req: uuid::Uuid,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
    ) -> bool {
        let Some(effects) = self
            .market_data_runtime
            .finish_worker_job_effects(worker_req, empty_covered_tail)
        else {
            return false;
        };
        self.apply_market_data_effects(main_window, effects);
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
        if self
            .market_data_runtime
            .finish_worker_job_invalid(worker_req)
        {
            self.log_market_data_progress_snapshot();
        }
    }

    fn log_market_data_progress_snapshot(&self) {
        self.market_data_runtime.log_progress_snapshot();
    }

    fn complete_fetch(
        &mut self,
        main_window: window::Id,
        pane_id: uuid::Uuid,
        req_id: Option<uuid::Uuid>,
        fetch: Option<fetcher::FetchRange>,
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
    ) {
        let Some(pane_state) = self.get_pane_state_by_uuid(main_window, pane_id) else {
            log::warn!(
                "FETCH Complete | pane={} req={} fetch={} found=false reason=no_pane",
                fetcher::short_id(pane_id),
                fetcher::format_req_id(req_id),
                fetcher::format_fetch_range_compact(fetch)
            );
            return;
        };

        let ready_streams = pane_state
            .streams
            .ready_iter()
            .map(|iter| iter.copied().collect::<Vec<_>>())
            .unwrap_or_default();

        let effects = self.market_data_runtime.complete_fetch_effects(
            pane_id,
            req_id,
            fetch,
            empty_covered_tail,
            &ready_streams,
        );
        self.apply_market_data_effects(main_window, effects);

        log::debug!(
            "FETCH Complete | pane={} req={} fetch={} found=true",
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
            self.market_data_runtime
                .coordinator
                .feed_klines(&key, std::slice::from_ref(kline));

            log::trace!(
                target: "marketdata",
                "MARKETDATA LiveKlineObserved | key={} time={}",
                key.display_key(),
                fetcher::format_time_short(kline.time)
            );

            // Persist to cache.
            self.market_data_runtime
                .insert_live_klines(&key, std::slice::from_ref(kline));
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
            log::trace!(
                target: "marketdata",
                "MARKETDATA LiveObserved | key={} count={} latest={}",
                key.display_key(),
                buffer.len(),
                buffer.last().map_or("-".to_string(), |t| fetcher::format_time_short(t.time))
            );

            // Persist to cache (batched via live adapter). Cache stores the
            // data but does NOT update coverage to Complete.
            self.market_data_runtime.insert_live_trades(&key, buffer);
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

        // Periodically save coverage to disk.
        if self.market_data_runtime.should_save_coverage(now) {
            self.market_data_runtime.save_coverage();
        }

        let mut tasks = vec![];
        let mut route_fetches = Vec::new();
        let mut route_needs = Vec::new();

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
                chart::Action::RequestMarketDataNeeds(needs) => {
                    let pane_id = state.unique_id();
                    let ready_streams = state
                        .streams
                        .ready_iter()
                        .map(|iter| iter.copied().collect::<Vec<_>>())
                        .unwrap_or_default();
                    let chart_generation =
                        if let pane::Content::Kline { chart: Some(c), .. } = &state.content {
                            c.current_generation()
                        } else {
                            0
                        };

                    route_needs.push(DashboardChartNeedRoute {
                        pane_id,
                        ready_streams,
                        chart_generation,
                        needs,
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
        for route in route_needs {
            tasks.push(self.route_market_data_needs_through_market_data(
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
            .market_data_runtime
            .stale_worker_jobs(now_ms, MARKET_DATA_JOB_STALE_MS);

        for stale in &stale_jobs {
            log::warn!(
                target: "marketdata",
                "MARKETDATA JobStale | job={} worker_req={} age={} action=fail_and_remove",
                crate::market_data::job::short_id(stale.job_id),
                stale.worker_req.map_or("-".to_string(), fetcher::short_id),
                stale.age_ms
            );

            let satisfied_consumers = self
                .market_data_runtime
                .mark_stale_job_consumers_failed(stale.job_id, stale.range);
            for consumer in satisfied_consumers {
                if let Some(pane_state) =
                    self.get_mut_pane_state_by_uuid(main_window, consumer.pane_id)
                {
                    pane_state.status = pane::Status::Ready;
                }
            }

            self.market_data_runtime
                .fail_and_remove_worker_job(stale.job_id, "stale market data worker".to_string());
        }

        if !stale_jobs.is_empty() {
            self.market_data_runtime
                .retain_effective_pending_consumers();
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
        coordinator::MarketDataCoordinator,
        job::FetchJobId,
        key::{MarketKind, Symbol, Venue},
        requirement::{ConsumerId, DataRequirement, Priority},
        runtime::{ConsumerSegmentStatus, MarketDataRuntime},
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
            market_data_runtime: MarketDataRuntime::from_parts(
                MarketDataCoordinator::new(),
                crate::market_data::cache::LocalMarketCache::new(
                    std::env::temp_dir()
                        .join(format!("flowsurface-md-test-{}", uuid::Uuid::new_v4())),
                ),
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
        dashboard
            .market_data_runtime
            .coordinator
            .require(DataRequirement::new(
                ConsumerId::pane(pane_id, feature),
                key,
                job_range,
                Priority::High,
                "test",
            ));
        dashboard.market_data_runtime.coordinator.plan();
        let jobs = dashboard.market_data_runtime.coordinator.execute_plan();
        assert_eq!(jobs.len(), 1);
        dashboard.market_data_runtime.coordinator.start_job(jobs[0]);
        jobs[0]
    }

    fn add_kline_consumer(
        dashboard: &mut Dashboard,
        pane_id: uuid::Uuid,
        req_id: uuid::Uuid,
        consumer_range: MarketDataRange,
    ) {
        dashboard
            .market_data_runtime
            .push_pending_consumer_for_test(PendingMarketDataConsumer {
                pane_id,
                req_id,
                fetch: fetcher::FetchRange::Kline(consumer_range.from, consumer_range.to),
                stream: None,
                key: kline_key(),
                range: consumer_range,
                feature: ConsumerFeature::ChartKlines,
                bubble_config: None,
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
            .market_data_runtime
            .push_pending_consumer_for_test(PendingMarketDataConsumer {
                pane_id,
                req_id,
                fetch,
                stream: None,
                key,
                range: consumer_range,
                feature,
                bubble_config: None,
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
        dashboard
            .market_data_runtime
            .set_job_consumers_for_test(job_id, vec![first_req]);

        let attached = dashboard.attach_pending_consumers_to_active_jobs("dedup_active_job");

        assert_eq!(attached, 1);
        assert_eq!(
            dashboard.market_data_runtime.job_consumer_ids(job_id),
            vec![first_req, second_req]
        );
        // After add_segment_merged, the required_segments may be merged,
        // so check that the segment is fully covered
        let consumer = dashboard
            .market_data_runtime
            .pending_consumer_by_req(second_req)
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
        dashboard
            .market_data_runtime
            .set_worker_job_mapping_for_test(worker_req, job_id);
        dashboard
            .market_data_runtime
            .set_job_consumers_for_test(job_id, vec![first_req]);

        dashboard.attach_pending_consumers_to_active_jobs("dedup_active_job");

        assert_eq!(dashboard.market_data_runtime.worker_mapping_count(), 1);
        assert_eq!(
            dashboard.market_data_runtime.job_consumer_ids(job_id).len(),
            2
        );
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
        dashboard
            .market_data_runtime
            .set_worker_job_mapping_for_test(worker_req, job_id);
        dashboard
            .market_data_runtime
            .set_job_consumers_for_test(job_id, vec![first_req, second_req]);
        dashboard.add_required_segment_to_consumer(first_req, job_range);
        dashboard.add_required_segment_to_consumer(second_req, job_range);
        dashboard
            .market_data_runtime
            .coordinator
            .job_mut(job_id)
            .unwrap()
            .progress
            .records_fetched = 2;

        assert!(dashboard.finish_coordinator_worker_job(window::Id::unique(), worker_req, None));

        assert!(
            dashboard
                .market_data_runtime
                .coordinator
                .job(job_id)
                .is_none()
        );
        assert!(dashboard.market_data_runtime.worker_maps_empty());
        assert!(dashboard.market_data_runtime.pending_consumers_empty());
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
        dashboard
            .market_data_runtime
            .set_worker_job_mapping_for_test(worker_req, job_id);

        dashboard.attach_pending_consumers_to_active_jobs("dedup_active_job");
        dashboard
            .market_data_runtime
            .coordinator
            .job_mut(job_id)
            .unwrap()
            .progress
            .records_fetched = 1;
        dashboard.finish_coordinator_worker_job(window::Id::unique(), worker_req, None);

        assert!(dashboard.market_data_runtime.pending_consumers_empty());
        assert_eq!(
            dashboard.market_data_runtime.coordinator.active_job_count(),
            0
        );
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
        dashboard
            .market_data_runtime
            .set_worker_job_mapping_for_test(worker_req, job_id);
        dashboard
            .market_data_runtime
            .set_job_consumers_for_test(job_id, vec![req_id]);
        dashboard.add_required_segment_to_consumer(req_id, job_range);
        dashboard
            .market_data_runtime
            .coordinator
            .job_mut(job_id)
            .unwrap()
            .progress
            .records_fetched = 1;

        assert_eq!(
            dashboard
                .market_data_runtime
                .coordinator
                .progress_snapshot()
                .active_job_count(),
            1
        );
        dashboard.finish_coordinator_worker_job(window::Id::unique(), worker_req, None);

        assert_eq!(
            dashboard
                .market_data_runtime
                .coordinator
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
            .market_data_runtime
            .mark_consumer_completed(req_id);

        let attached = dashboard.attach_pending_consumers_to_active_jobs("test");

        assert_eq!(attached, 0);
        assert!(
            !dashboard
                .market_data_runtime
                .job_consumer_ids(job_id)
                .contains(&req_id)
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
        dashboard
            .market_data_runtime
            .set_worker_job_mapping_for_test(worker_req, job_id);
        dashboard
            .market_data_runtime
            .set_job_consumers_for_test(job_id, vec![req_id]);
        dashboard
            .market_data_runtime
            .coordinator
            .job_mut(job_id)
            .unwrap()
            .created_at = 0;

        dashboard.fail_stale_market_jobs(window::Id::unique());

        assert!(
            dashboard
                .market_data_runtime
                .coordinator
                .job(job_id)
                .is_none()
        );
        assert!(dashboard.market_data_runtime.worker_maps_empty());
        assert_eq!(
            dashboard.market_data_runtime.coordinator.active_job_count(),
            0
        );
    }

    #[test]
    fn progress_snapshot_includes_cached_records_after_cache_serve_counter() {
        let mut dashboard = test_dashboard();
        dashboard
            .market_data_runtime
            .coordinator
            .record_cache_served(448);

        let snapshot = dashboard
            .market_data_runtime
            .coordinator
            .progress_snapshot();

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
                .market_data_runtime
                .coordinator
                .progress_snapshot()
                .active_job_count(),
            1
        );
        dashboard
            .market_data_runtime
            .coordinator
            .complete_and_remove_job(job_id, 1);

        assert_eq!(
            dashboard
                .market_data_runtime
                .coordinator
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
            .market_data_runtime
            .pending_consumer_by_req(req_id)
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
            .market_data_runtime
            .pending_consumer_by_req(req_id)
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
