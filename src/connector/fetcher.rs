use exchange::adapter::{AdapterError, AdapterHandles, Exchange, StreamKind};
use exchange::{Kline, OpenInterest, TickerInfo, Trade, UnixMs};
use iced::{
    Task,
    task::{Handle, Straw, sipper},
};
use rustc_hash::FxHashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use uuid::Uuid;

// ── Human-readable log helpers ──────────────────────────────────────────────

/// Format a ticker symbol for display (e.g., "BTCUSDT")
pub fn format_symbol(ticker_info: &TickerInfo) -> String {
    ticker_info
        .ticker
        .display_symbol()
        .unwrap_or(&ticker_info.ticker.to_string())
        .to_string()
}

/// Format an exchange venue for display (e.g., "BinanceLinear")
pub fn format_venue(ticker_info: &TickerInfo) -> String {
    format!("{:?}", ticker_info.exchange())
}

/// Format a UnixMs timestamp as HH:MM:SS.mmm
pub fn format_time_short(ms: UnixMs) -> String {
    let dt = chrono::DateTime::from_timestamp_millis(ms.as_u64() as i64)
        .unwrap_or_default()
        .with_timezone(&chrono::Local);
    dt.format("%H:%M:%S%.3f").to_string()
}

/// Format a time range as HH:MM:SS.mmm → HH:MM:SS.mmm
pub fn format_time_range(from: UnixMs, to: UnixMs) -> String {
    format!("{} → {}", format_time_short(from), format_time_short(to))
}

/// Format a duration in milliseconds as human-readable (e.g., "3.7s" or "625ms")
pub fn format_duration_ms(ms: u64) -> String {
    if ms >= 1000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}ms")
    }
}

/// Shorten a UUID to first 8 characters
pub fn short_id(id: Uuid) -> String {
    let s = id.to_string();
    s[..8.min(s.len())].to_string()
}

/// Format a FetchRange as a human-readable string
pub fn format_fetch_range(fetch: &FetchRange) -> String {
    match fetch {
        FetchRange::Kline(from, to) => {
            format!("Kline {}", format_time_range(*from, *to))
        }
        FetchRange::OpenInterest(from, to) => {
            format!("OI {}", format_time_range(*from, *to))
        }
        FetchRange::Trades(from, to) => {
            format!("Trades {}", format_time_range(*from, *to))
        }
    }
}

/// Check if a trade range is fully contained within another
fn trades_contained(
    inner_from: UnixMs,
    inner_to: UnixMs,
    outer_from: UnixMs,
    outer_to: UnixMs,
) -> bool {
    inner_from >= outer_from && inner_to <= outer_to
}

static TRADE_FETCH_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn toggle_trade_fetch(value: bool) {
    TRADE_FETCH_ENABLED.store(value, Ordering::Relaxed);
}

pub fn is_trade_fetch_enabled() -> bool {
    TRADE_FETCH_ENABLED.load(Ordering::Relaxed)
}

#[derive(Debug, Clone)]
pub enum FetchedData {
    Trades {
        batch: Vec<Trade>,
        until_time: UnixMs,
        req_id: Option<uuid::Uuid>,
    },
    Klines {
        data: Vec<Kline>,
        req_id: Option<uuid::Uuid>,
    },
    OI {
        data: Vec<OpenInterest>,
        req_id: Option<uuid::Uuid>,
    },
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum ReqError {
    #[error("Request is already failed: {0}")]
    Failed(String),
}

#[derive(PartialEq, Debug)]
enum RequestStatus {
    Pending,
    Completed(u64),
    Failed(String),
}

#[derive(Default)]
pub struct RequestHandler {
    requests: FxHashMap<Uuid, FetchRequest>,
}

impl RequestHandler {
    pub fn add_request(&mut self, fetch: FetchRange) -> Result<Option<Uuid>, ReqError> {
        let request = FetchRequest::new(fetch);
        let id = Uuid::new_v4();
        let now = chrono::Utc::now().timestamp_millis() as u64;

        if let FetchRange::Trades(new_from, new_to) = fetch {
            for (existing_id, existing_req) in &self.requests {
                let FetchRange::Trades(exist_from, exist_to) = existing_req.fetch_type else {
                    continue;
                };

                let exact = new_from == exist_from && new_to == exist_to;
                let contained = trades_contained(new_from, new_to, exist_from, exist_to);
                let overlaps = new_from < exist_to && new_to > exist_from;

                match &existing_req.status {
                    RequestStatus::Pending if exact || contained || overlaps => {
                        let reason = if contained {
                            "contained_pending"
                        } else {
                            "overlap_pending"
                        };
                        log::debug!(
                            "FETCH Skipped | reason={reason} {} pending_req={} pending_range={}",
                            format_fetch_range(&fetch),
                            short_id(*existing_id),
                            format_time_range(exist_from, exist_to)
                        );
                        return Ok(None);
                    }
                    RequestStatus::Completed(ts) if (exact || contained) && now - ts <= 30_000 => {
                        log::debug!(
                            "FETCH Skipped | reason=already_completed {} completed_req={} completed_range={}",
                            format_fetch_range(&fetch),
                            short_id(*existing_id),
                            format_time_range(exist_from, exist_to)
                        );
                        return Ok(None);
                    }
                    RequestStatus::Failed(error_msg) if exact => {
                        log::warn!(
                            "CACHE Failed | {} req={} prev_error={}",
                            format_fetch_range(&fetch),
                            short_id(*existing_id),
                            error_msg
                        );
                        return Err(ReqError::Failed(error_msg.clone()));
                    }
                    _ => {}
                }
            }
        }

        if let Some((existing_id, existing_req)) = self.requests.iter().find_map(|(k, v)| {
            if v.same_with(&request) {
                Some((*k, v))
            } else {
                None
            }
        }) {
            return match &existing_req.status {
                RequestStatus::Failed(error_msg) => {
                    log::warn!(
                        "CACHE Failed | {} req={} prev_error={}",
                        format_fetch_range(&fetch),
                        short_id(existing_id),
                        error_msg
                    );
                    Err(ReqError::Failed(error_msg.clone()))
                }
                RequestStatus::Completed(ts) => {
                    if chrono::Utc::now().timestamp_millis() as u64 - ts > 30_000 {
                        log::info!(
                            "CACHE Expired | {} req={} retrying",
                            format_fetch_range(&fetch),
                            short_id(existing_id)
                        );
                        Ok(Some(existing_id))
                    } else {
                        log::debug!(
                            "CACHE Hit | {} req={} skipping (cooldown)",
                            format_fetch_range(&fetch),
                            short_id(existing_id)
                        );
                        Ok(None)
                    }
                }
                RequestStatus::Pending => {
                    log::debug!(
                        "FETCH Skipped | reason=overlap_pending {} req={} already in flight",
                        format_fetch_range(&fetch),
                        short_id(existing_id)
                    );
                    Ok(None)
                }
            };
        }

        log::info!(
            "FETCH Queued | {} req={}",
            format_fetch_range(&fetch),
            short_id(id)
        );
        self.requests.insert(id, request);
        Ok(Some(id))
    }

    pub fn mark_completed(&mut self, id: Uuid) {
        if let Some(request) = self.requests.get_mut(&id) {
            let timestamp = chrono::Utc::now().timestamp_millis() as u64;
            request.status = RequestStatus::Completed(timestamp);
            log::debug!("FETCH Completed | req={}", short_id(id));
        } else {
            log::warn!("FETCH NotFound | req={}", short_id(id));
        }
    }

    pub fn mark_failed(&mut self, id: Uuid, error: String) {
        if let Some(request) = self.requests.get_mut(&id) {
            log::warn!("FETCH Failed | req={} error={}", short_id(id), error);
            request.status = RequestStatus::Failed(error);
        } else {
            log::warn!("FETCH NotFound | req={}", short_id(id));
        }
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum FetchRange {
    Kline(UnixMs, UnixMs),
    OpenInterest(UnixMs, UnixMs),
    Trades(UnixMs, UnixMs),
}

#[derive(PartialEq, Debug)]
struct FetchRequest {
    fetch_type: FetchRange,
    status: RequestStatus,
}

impl FetchRequest {
    fn new(fetch_type: FetchRange) -> Self {
        FetchRequest {
            fetch_type,
            status: RequestStatus::Pending,
        }
    }

    fn same_with(&self, other: &FetchRequest) -> bool {
        match (&self.fetch_type, &other.fetch_type) {
            (FetchRange::Kline(s1, e1), FetchRange::Kline(s2, e2)) => e1 == e2 && s1 == s2,
            (FetchRange::OpenInterest(s1, e1), FetchRange::OpenInterest(s2, e2)) => {
                e1 == e2 && s1 == s2
            }
            (FetchRange::Trades(s1, e1), FetchRange::Trades(s2, e2)) => e1 == e2 && s1 == s2,
            _ => false,
        }
    }
}

pub struct FetchSpec {
    pub req_id: uuid::Uuid,
    pub fetch: FetchRange,
    pub stream: Option<StreamKind>,
}

impl From<(uuid::Uuid, FetchRange, Option<StreamKind>)> for FetchSpec {
    fn from(t: (uuid::Uuid, FetchRange, Option<StreamKind>)) -> Self {
        FetchSpec {
            req_id: t.0,
            fetch: t.1,
            stream: t.2,
        }
    }
}

impl std::fmt::Debug for FetchSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FetchSpec")
            .field("req_id", &self.req_id)
            .field("fetch", &self.fetch)
            .field("stream", &self.stream)
            .finish()
    }
}

impl Clone for FetchSpec {
    fn clone(&self) -> Self {
        FetchSpec {
            req_id: self.req_id,
            fetch: self.fetch,
            stream: self.stream,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InfoKind {
    FetchingKlines,
    FetchingTrades(usize),
    FetchingOI,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FetchTaskStatus {
    Loading(InfoKind),
    Completed {
        req_id: Option<Uuid>,
        fetch: Option<FetchRange>,
    },
}

#[derive(Debug, Clone)]
pub enum FetchUpdate {
    Status {
        pane_id: Uuid,
        status: FetchTaskStatus,
    },
    Data {
        layout_id: Uuid,
        pane_id: Uuid,
        stream: StreamKind,
        data: FetchedData,
    },
    Error {
        pane_id: Uuid,
        error: String,
    },
}

pub fn request_fetch(
    handles: AdapterHandles,
    pane_id: Uuid,
    ready_streams: &[StreamKind],
    layout_id: Uuid,
    req_id: Uuid,
    fetch: FetchRange,
    stream: Option<StreamKind>,
    on_trade_handle: &mut impl FnMut(Handle),
) -> Task<FetchUpdate> {
    log::debug!(
        "FETCH Dispatch | {} req={} pane={} streams={}",
        format_fetch_range(&fetch),
        short_id(req_id),
        short_id(pane_id),
        ready_streams.len()
    );

    match fetch {
        FetchRange::Kline(from, to) => {
            let kline_stream = if let Some(s) = stream {
                Some((s, pane_id))
            } else {
                ready_streams.iter().find_map(|stream| {
                    if let StreamKind::Kline { .. } = stream {
                        Some((*stream, pane_id))
                    } else {
                        None
                    }
                })
            };

            if let Some((stream, pane_uid)) = kline_stream {
                log::info!(
                    "KLINE Start | pane={} req={} stream={stream:?} range={}",
                    short_id(pane_uid),
                    short_id(req_id),
                    format_time_range(from, to)
                );
                return kline_fetch_task(
                    handles.clone(),
                    layout_id,
                    pane_uid,
                    stream,
                    Some(req_id),
                    Some((from, to)),
                );
            }

            log::debug!(
                "KLINE Skip | pane={} req={} reason=no_kline_stream",
                short_id(pane_id),
                short_id(req_id)
            );
        }
        FetchRange::OpenInterest(from, to) => {
            let kline_stream = if let Some(s) = stream {
                Some((s, pane_id))
            } else {
                ready_streams.iter().find_map(|stream| {
                    if let StreamKind::Kline { .. } = stream {
                        Some((*stream, pane_id))
                    } else {
                        None
                    }
                })
            };

            if let Some((stream, pane_uid)) = kline_stream {
                log::info!(
                    "OI Start | pane={} req={} stream={stream:?} range={}",
                    short_id(pane_uid),
                    short_id(req_id),
                    format_time_range(from, to)
                );
                return oi_fetch_task(
                    handles.clone(),
                    layout_id,
                    pane_uid,
                    stream,
                    Some(req_id),
                    Some((from, to)),
                );
            }

            log::debug!(
                "OI Skip | pane={} req={} reason=no_kline_stream",
                short_id(pane_id),
                short_id(req_id)
            );
        }
        FetchRange::Trades(from_time, to_time) => {
            let trade_info = ready_streams.iter().find_map(|stream| {
                if let StreamKind::Trades { ticker_info } = stream {
                    Some((*ticker_info, pane_id, *stream))
                } else {
                    None
                }
            });

            if let Some((ticker_info, pane_id, stream)) = trade_info {
                let is_binance = matches!(
                    ticker_info.exchange(),
                    Exchange::BinanceSpot | Exchange::BinanceLinear | Exchange::BinanceInverse
                );

                if is_binance {
                    let data_path = data::data_path(Some("market_data/binance/"));
                    log::info!(
                        "TRADE Start | {} {} {} req={} pane={} path={}",
                        format_venue(&ticker_info),
                        format_symbol(&ticker_info),
                        format_time_range(from_time, to_time),
                        short_id(req_id),
                        short_id(pane_id),
                        data_path.display()
                    );

                    let (task, handle) = Task::sip(
                        fetch_trades_batched(
                            handles.clone(),
                            ticker_info,
                            from_time,
                            to_time,
                            data_path,
                        ),
                        move |batch| {
                            log::debug!(
                                "TRADE Batch | {} {} trades={} req={}",
                                format_venue(&ticker_info),
                                format_symbol(&ticker_info),
                                batch.len(),
                                short_id(req_id)
                            );
                            let data = FetchedData::Trades {
                                batch,
                                until_time: to_time,
                                req_id: Some(req_id),
                            };

                            FetchUpdate::Data {
                                layout_id,
                                pane_id,
                                data,
                                stream,
                            }
                        },
                        move |result| match result {
                            Ok(()) => {
                                log::info!(
                                    "TRADE Done | {} {} req={}",
                                    format_venue(&ticker_info),
                                    format_symbol(&ticker_info),
                                    short_id(req_id)
                                );
                                FetchUpdate::Status {
                                    pane_id,
                                    status: FetchTaskStatus::Completed {
                                        req_id: Some(req_id),
                                        fetch: Some(FetchRange::Trades(from_time, to_time)),
                                    },
                                }
                            }
                            Err(err) => {
                                log::error!(
                                    "TRADE Failed | {} {} req={} error={err}",
                                    format_venue(&ticker_info),
                                    format_symbol(&ticker_info),
                                    short_id(req_id)
                                );
                                FetchUpdate::Error {
                                    pane_id,
                                    error: err.ui_message(),
                                }
                            }
                        },
                    )
                    .abortable();

                    on_trade_handle(handle.abort_on_drop());

                    return task;
                } else {
                    log::debug!(
                        "TRADE Skip | {} {} req={} reason=unsupported_exchange",
                        format_venue(&ticker_info),
                        format_symbol(&ticker_info),
                        short_id(req_id)
                    );
                }
            } else {
                log::debug!(
                    "TRADE Skip | pane={} req={} reason=no_trade_stream",
                    short_id(pane_id),
                    short_id(req_id)
                );
            }
        }
    }

    Task::none()
}

pub fn request_fetch_many(
    handles: AdapterHandles,
    pane_id: Uuid,
    ready_streams: &[StreamKind],
    layout_id: Uuid,
    reqs: impl IntoIterator<Item = (Uuid, FetchRange, Option<StreamKind>)>,
    mut on_trade_handle: impl FnMut(Handle),
) -> Task<FetchUpdate> {
    let mut tasks = Vec::new();

    for (req_id, fetch, stream) in reqs {
        tasks.push(request_fetch(
            handles.clone(),
            pane_id,
            ready_streams,
            layout_id,
            req_id,
            fetch,
            stream,
            &mut on_trade_handle,
        ));
    }

    Task::batch(tasks)
}

pub fn oi_fetch_task(
    handles: AdapterHandles,
    layout_id: Uuid,
    pane_id: Uuid,
    stream: StreamKind,
    req_id: Option<Uuid>,
    range: Option<(UnixMs, UnixMs)>,
) -> Task<FetchUpdate> {
    let update_status = Task::done(FetchUpdate::Status {
        pane_id,
        status: FetchTaskStatus::Loading(InfoKind::FetchingOI),
    });

    let fetch_task = match stream {
        StreamKind::Kline {
            ticker_info,
            timeframe,
        } => {
            let fetch = async move {
                let started = Instant::now();
                log::info!(
                    "OI Request | pane={} req={:?} venue={:?} symbol={:?} tf={timeframe:?} range={range:?}",
                    short_id(pane_id),
                    req_id.map(short_id),
                    ticker_info.exchange(),
                    ticker_info.ticker
                );

                let result = handles
                    .fetch_open_interest(ticker_info, timeframe, range)
                    .await;

                match &result {
                    Ok(oi) => log::info!(
                        "OI Done | pane={} req={:?} records={} duration={}",
                        short_id(pane_id),
                        req_id.map(short_id),
                        oi.len(),
                        format_duration_ms(started.elapsed().as_millis() as u64)
                    ),
                    Err(err) => log::error!(
                        "OI Error | pane={} req={:?} duration={} error={err}",
                        short_id(pane_id),
                        req_id.map(short_id),
                        format_duration_ms(started.elapsed().as_millis() as u64)
                    ),
                }

                result
            };

            Task::perform(
                iced::futures::TryFutureExt::map_err(fetch, |err| err.ui_message()),
                move |result| match result {
                    Ok(oi) => {
                        let data = FetchedData::OI { data: oi, req_id };
                        FetchUpdate::Data {
                            layout_id,
                            pane_id,
                            data,
                            stream,
                        }
                    }
                    Err(err) => FetchUpdate::Error {
                        pane_id,
                        error: err,
                    },
                },
            )
        }
        _ => {
            log::debug!(
                "Open interest fetch skipped: pane_id={pane_id}, req_id={req_id:?}, stream={stream:?}, reason=not_kline_stream"
            );
            Task::none()
        }
    };

    update_status.chain(fetch_task)
}

pub fn kline_fetch_task(
    handles: AdapterHandles,
    layout_id: Uuid,
    pane_id: Uuid,
    stream: StreamKind,
    req_id: Option<Uuid>,
    range: Option<(UnixMs, UnixMs)>,
) -> Task<FetchUpdate> {
    let update_status = Task::done(FetchUpdate::Status {
        pane_id,
        status: FetchTaskStatus::Loading(InfoKind::FetchingKlines),
    });

    let fetch_task = match stream {
        StreamKind::Kline {
            ticker_info,
            timeframe,
        } => {
            let fetch = async move {
                let started = Instant::now();
                log::info!(
                    "KLINE Request | pane={} req={:?} venue={:?} symbol={:?} tf={timeframe:?} range={range:?}",
                    short_id(pane_id),
                    req_id.map(short_id),
                    ticker_info.exchange(),
                    ticker_info.ticker
                );

                let result = handles.fetch_klines(ticker_info, timeframe, range).await;

                match &result {
                    Ok(klines) => log::info!(
                        "KLINE Done | pane={} req={:?} records={} duration={}",
                        short_id(pane_id),
                        req_id.map(short_id),
                        klines.len(),
                        format_duration_ms(started.elapsed().as_millis() as u64)
                    ),
                    Err(err) => log::error!(
                        "KLINE Error | pane={} req={:?} duration={} error={err}",
                        short_id(pane_id),
                        req_id.map(short_id),
                        format_duration_ms(started.elapsed().as_millis() as u64)
                    ),
                }

                result
            };

            Task::perform(
                iced::futures::TryFutureExt::map_err(fetch, |err| err.ui_message()),
                move |result| match result {
                    Ok(klines) => {
                        let data = FetchedData::Klines {
                            data: klines,
                            req_id,
                        };
                        FetchUpdate::Data {
                            layout_id,
                            pane_id,
                            data,
                            stream,
                        }
                    }
                    Err(err) => FetchUpdate::Error {
                        pane_id,
                        error: err,
                    },
                },
            )
        }
        _ => {
            log::debug!(
                "KLINE Skip | pane={} req={:?} reason=not_kline_stream",
                short_id(pane_id),
                req_id.map(short_id)
            );
            Task::none()
        }
    };

    update_status.chain(fetch_task)
}

pub fn fetch_trades_batched(
    handles: AdapterHandles,
    ticker_info: TickerInfo,
    from_time: UnixMs,
    to_time: UnixMs,
    data_path: PathBuf,
) -> impl Straw<(), Vec<Trade>, AdapterError> {
    let venue = format_venue(&ticker_info);
    let symbol = format_symbol(&ticker_info);

    sipper(async move |mut progress| {
        let mut latest_trade_t = from_time;
        let started = Instant::now();
        let mut total_trades = 0usize;
        let mut request_count = 0usize;
        let mut last_progress_log = Instant::now();

        log::info!(
            "TRADE Worker | {venue} {symbol} range={} path={}",
            format_time_range(from_time, to_time),
            data_path.display()
        );

        while latest_trade_t < to_time {
            request_count += 1;
            let request_started = Instant::now();
            log::debug!(
                "TRADE Request | {venue} {symbol} #{request_count} from={}",
                format_time_short(latest_trade_t)
            );

            match handles
                .fetch_trades(ticker_info, latest_trade_t, Some(data_path.clone()))
                .await
            {
                Ok(batch) => {
                    let elapsed = request_started.elapsed().as_millis() as u64;
                    log::debug!(
                        "TRADE Response | {venue} {symbol} #{request_count} trades={} duration={}",
                        batch.len(),
                        format_duration_ms(elapsed)
                    );

                    if batch.is_empty() {
                        log::info!(
                            "TRADE Stop | {venue} {symbol} reason=empty_batch requests={request_count} trades={total_trades} total_duration={}",
                            format_duration_ms(started.elapsed().as_millis() as u64)
                        );
                        break;
                    }

                    latest_trade_t = batch.last().map_or(latest_trade_t, |trade| trade.time);
                    total_trades += batch.len();

                    // Progress log every 5 seconds or every 10 requests
                    if last_progress_log.elapsed().as_secs() >= 5
                        || request_count.is_multiple_of(10)
                    {
                        log::info!(
                            "TRADE Progress | {venue} {symbol} #{request_count} trades={total_trades} latest={} elapsed={}",
                            format_time_short(latest_trade_t),
                            format_duration_ms(started.elapsed().as_millis() as u64)
                        );
                        last_progress_log = Instant::now();
                    }

                    let () = progress.send(batch).await;
                }
                Err(err) => {
                    log::error!(
                        "TRADE Error | {venue} {symbol} #{request_count} from={} duration={} error={err}",
                        format_time_short(latest_trade_t),
                        format_duration_ms(request_started.elapsed().as_millis() as u64)
                    );
                    return Err(err);
                }
            }
        }

        log::info!(
            "TRADE Worker Done | {venue} {symbol} range={} requests={request_count} returned_trades={total_trades} duration={}",
            format_time_range(from_time, to_time),
            format_duration_ms(started.elapsed().as_millis() as u64)
        );

        Ok(())
    })
}
