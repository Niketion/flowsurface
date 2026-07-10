use super::market_data::{DataRequirement, DataSet, RefreshPolicy, RequestKey, TimeRange};
use data::chart::kline::{BubbleCandidate, BubbleVolumeSummary};
use exchange::adapter::{AdapterError, AdapterHandles, Exchange, StreamKind};
use exchange::unit::{Price, PriceStep, Qty};
use exchange::{Kline, OpenInterest, TickerInfo, Trade, UnixMs};
use iced::{
    Task,
    task::{Handle, Straw, sipper},
};
use rustc_hash::FxHashMap;
use std::cell::Cell;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
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

/// Format a ticker with venue and symbol for structured diagnostics.
pub fn format_ticker(ticker_info: &TickerInfo) -> String {
    format!(
        "{}/{}",
        format_venue(ticker_info),
        format_symbol(ticker_info)
    )
}

/// Format a stream kind compactly while preserving the important route keys.
pub fn format_stream(stream: &StreamKind) -> String {
    match stream {
        StreamKind::Kline {
            ticker_info,
            timeframe,
        } => format!("Kline:{}:{timeframe:?}", format_ticker(ticker_info)),
        StreamKind::Depth {
            ticker_info,
            depth_aggr,
            push_freq,
        } => format!(
            "Depth:{}:aggr={depth_aggr:?}:freq={push_freq:?}",
            format_ticker(ticker_info)
        ),
        StreamKind::Trades { ticker_info } => {
            format!("Trades:{}", format_ticker(ticker_info))
        }
    }
}

/// Format a stream slice for bounded single-line diagnostics.
pub fn format_streams(streams: &[StreamKind]) -> String {
    streams
        .iter()
        .map(format_stream)
        .collect::<Vec<_>>()
        .join(",")
}

/// Format an optional request id.
pub fn format_req_id(req_id: Option<Uuid>) -> String {
    req_id.map_or("-".to_string(), short_id)
}

/// Format an optional timestamp.
pub fn format_optional_time(ms: Option<UnixMs>) -> String {
    ms.map_or("-".to_string(), format_time_short)
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
        FetchRange::BubbleSummary {
            from,
            to,
            timeframe_ms,
            ..
        } => {
            format!(
                "BubbleSummary {} tf_ms={timeframe_ms}",
                format_time_range(*from, *to)
            )
        }
    }
}

pub fn format_fetch_range_compact(fetch: Option<FetchRange>) -> String {
    fetch.map_or("-".to_string(), |fetch| format_fetch_range(&fetch))
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

const TRADE_REST_REQUEST_TIMEOUT: Duration = Duration::from_secs(35);

/// Overall wall-clock timeout for the entire trade worker (all REST batches).
/// Prevents the worker from hanging indefinitely if individual requests succeed
/// but cursor advancement is slow.
const TRADE_WORKER_TIMEOUT: Duration = Duration::from_secs(85);

/// Stop raw Trades fetch after this many consecutive no-progress batches.
const TRADE_NO_PROGRESS_MAX_CONSECUTIVE: usize = 3;

/// If the remaining gap to `target_to` is within this epsilon (ms) and trades
/// have already been collected, treat the fetch as complete rather than
/// retrying the same tiny tail indefinitely.
const NO_PROGRESS_REMAINING_EPSILON_MS: u64 = 1000;

/// How long to remember that a trade range returned empty.
const EMPTY_TRADE_FETCH_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub enum FetchedData {
    Trades {
        batch: Vec<Trade>,
        until_time: UnixMs,
        req_id: Option<uuid::Uuid>,
    },
    BubbleSummary {
        data: Vec<BubbleVolumeSummary>,
        range: (UnixMs, UnixMs),
        trades_seen: usize,
        raw_discarded: usize,
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
    Failed {
        error: String,
        failed_at: u64,
        retry_at: u64,
    },
    Superseded {
        reason: &'static str,
        superseded_at: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchSuppressionReason {
    AlreadyPending,
    CoveredByInflight,
    RecentFailed,
    NoStream,
    InvalidRange,
    Throttled,
}

impl FetchSuppressionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyPending => "already_pending",
            Self::CoveredByInflight => "covered_by_inflight",
            Self::RecentFailed => "recent_failed",
            Self::NoStream => "no_stream",
            Self::InvalidRange => "invalid_range",
            Self::Throttled => "throttled",
        }
    }
}

impl fmt::Display for FetchSuppressionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Default)]
pub struct RequestHandler {
    requests: FxHashMap<Uuid, FetchRequest>,
    last_suppression: Cell<Option<FetchSuppressionReason>>,
    generation_id: u64,
    /// Cache of recently empty trade fetch ranges to avoid re-fetching.
    /// Keyed by full stream identity + time range to avoid cross-symbol suppression.
    empty_trade_fetches: FxHashMap<EmptyTradeFetchKey, Instant>,
}

/// Full identity for the empty trade fetch cache.
/// Includes venue, symbol, market type, and time range so that
/// an empty result for one stream never suppresses a different stream.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EmptyTradeFetchKey {
    venue: String,
    symbol: String,
    market_type: String,
    from_ms: u64,
    to_ms: u64,
}

impl EmptyTradeFetchKey {
    fn from_ticker_info(ticker_info: &TickerInfo, from: UnixMs, to: UnixMs) -> Self {
        Self {
            venue: format_venue(ticker_info),
            symbol: format_symbol(ticker_info),
            market_type: format!("{:?}", ticker_info.exchange()),
            from_ms: from.as_u64(),
            to_ms: to.as_u64(),
        }
    }

    fn stream_display(&self) -> String {
        format!("{}/{}/{}", self.venue, self.symbol, self.market_type)
    }
}

impl RequestHandler {
    const COMPLETED_RETENTION_MS: u64 = 30_000;
    const FAILED_RETENTION_MS: u64 = 5 * 60_000;
    const PENDING_TIMEOUT_MS: u64 = 90_000;
    const SUPERSEDED_RETENTION_MS: u64 = 10_000;

    /// Get the current generation ID.
    pub fn generation_id(&self) -> u64 {
        self.generation_id
    }

    pub fn add_request(&mut self, fetch: FetchRange) -> Result<Option<Uuid>, ReqError> {
        self.add_request_with_id(Uuid::new_v4(), fetch, None)
    }

    /// Add a request with optional ticker_info for empty trade range checking.
    pub fn add_request_with_ticker(
        &mut self,
        fetch: FetchRange,
        ticker_info: Option<&TickerInfo>,
    ) -> Result<Option<Uuid>, ReqError> {
        self.add_request_with_id(Uuid::new_v4(), fetch, ticker_info)
    }

    pub fn last_suppression_reason(&self) -> Option<FetchSuppressionReason> {
        self.last_suppression.get()
    }

    pub fn cleanup_stale(&mut self) {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        self.cleanup_stale_at(now);
        self.cleanup_empty_trade_fetches();
    }

    pub fn has_pending_trade_requests(&self) -> bool {
        self.requests.values().any(|request| {
            matches!(request.fetch_type, FetchRange::Trades(_, _))
                && matches!(request.status, RequestStatus::Pending)
        })
    }

    fn cleanup_empty_trade_fetches(&mut self) {
        let now = Instant::now();
        self.empty_trade_fetches
            .retain(|_, marked_at| now.duration_since(*marked_at) < EMPTY_TRADE_FETCH_TTL);
    }

    /// Mark a trade range as empty (no trades in the tail).
    pub fn mark_empty_trade_range(&mut self, ticker_info: &TickerInfo, from: UnixMs, to: UnixMs) {
        let key = EmptyTradeFetchKey::from_ticker_info(ticker_info, from, to);
        log::info!(
            "FETCH EmptyCoveredInsert | stream={} range={} reason=no_progress_near_target ttl={:?}",
            key.stream_display(),
            format_time_range(from, to),
            EMPTY_TRADE_FETCH_TTL
        );
        self.empty_trade_fetches.insert(key, Instant::now());
    }

    /// Check if a trade range was recently marked as empty.
    pub fn is_empty_trade_range(&self, ticker_info: &TickerInfo, from: UnixMs, to: UnixMs) -> bool {
        let key = EmptyTradeFetchKey::from_ticker_info(ticker_info, from, to);
        self.empty_trade_fetches
            .get(&key)
            .is_some_and(|marked_at| marked_at.elapsed() < EMPTY_TRADE_FETCH_TTL)
    }

    fn retry_delay_ms(attempts: u32) -> u64 {
        let exp = attempts.saturating_sub(1).min(5);
        2_500u64.saturating_mul(1u64 << exp)
    }

    fn set_suppressed(
        &self,
        reason: FetchSuppressionReason,
        id: Uuid,
        fetch: &FetchRange,
        existing_range: Option<String>,
    ) {
        self.last_suppression.set(Some(reason));
        log::info!(
            "FETCH Suppressed | reason={} req={} {} existing_range={}",
            reason,
            short_id(id),
            format_fetch_range(fetch),
            existing_range.unwrap_or_else(|| "-".to_string())
        );
    }

    fn cleanup_stale_at(&mut self, now: u64) {
        let mut remove_ids = Vec::new();
        for (id, request) in &mut self.requests {
            match &request.status {
                RequestStatus::Pending
                    if now.saturating_sub(request.updated_at) >= Self::PENDING_TIMEOUT_MS =>
                {
                    let error = format!(
                        "request timed out after {}",
                        format_duration_ms(now.saturating_sub(request.updated_at))
                    );
                    request.attempts = request.attempts.saturating_add(1);
                    let retry_delay = Self::retry_delay_ms(request.attempts);
                    request.status = RequestStatus::Failed {
                        error,
                        failed_at: now,
                        retry_at: now.saturating_add(retry_delay),
                    };
                    request.updated_at = now;
                    log::warn!(
                        "FETCH Timeout | req={} {} age={} retry_after={}",
                        short_id(*id),
                        format_fetch_range(&request.fetch_type),
                        format_duration_ms(now.saturating_sub(request.created_at)),
                        format_duration_ms(retry_delay)
                    );
                    log::info!(
                        "FETCH PendingRemove | req={} {} reason=timeout",
                        short_id(*id),
                        format_fetch_range(&request.fetch_type)
                    );
                }
                RequestStatus::Completed(ts)
                    if now.saturating_sub(*ts) > Self::COMPLETED_RETENTION_MS =>
                {
                    remove_ids.push(*id);
                }
                RequestStatus::Failed { failed_at, .. }
                    if now.saturating_sub(*failed_at) > Self::FAILED_RETENTION_MS =>
                {
                    remove_ids.push(*id);
                }
                RequestStatus::Superseded { superseded_at, .. }
                    if now.saturating_sub(*superseded_at) > Self::SUPERSEDED_RETENTION_MS =>
                {
                    remove_ids.push(*id);
                }
                _ => {}
            }
        }

        for id in remove_ids {
            if let Some(request) = self.requests.remove(&id) {
                log::debug!(
                    "FETCH Cleanup | req={} {} reason=stale_terminal",
                    short_id(id),
                    format_fetch_range(&request.fetch_type)
                );
            }
        }
    }

    /// Supersede all pending requests. Increments generation ID.
    /// Returns the list of superseded request IDs for emitting terminal events.
    pub fn supersede_all_pending(&mut self, reason: &'static str) -> Vec<Uuid> {
        self.generation_id = self.generation_id.wrapping_add(1);
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let mut superseded_ids = Vec::new();

        for (id, request) in &mut self.requests {
            if request.status == RequestStatus::Pending {
                request.status = RequestStatus::Superseded {
                    reason,
                    superseded_at: now,
                };
                request.updated_at = now;
                superseded_ids.push(*id);
                log::info!(
                    "FETCH Superseded | req={} {} reason={} generation={}",
                    short_id(*id),
                    format_fetch_range(&request.fetch_type),
                    reason,
                    self.generation_id
                );
                log::info!(
                    "FETCH PendingRemove | req={} {} reason=superseded",
                    short_id(*id),
                    format_fetch_range(&request.fetch_type)
                );
            }
        }

        superseded_ids
    }

    /// Check if a request belongs to a stale generation AND is still active.
    /// Returns false for already-terminal requests (Completed, Failed) since
    /// their results have already been applied.
    pub fn is_stale_generation(&self, req_id: Uuid) -> bool {
        if let Some(request) = self.requests.get(&req_id) {
            // Already-terminal requests are not considered stale
            // (their result was already applied before the reset)
            match request.status {
                RequestStatus::Completed(_) | RequestStatus::Failed { .. } => false,
                _ => request.generation != self.generation_id,
            }
        } else {
            // Request not found - treat as stale
            true
        }
    }

    /// Get the generation ID of a specific request.
    pub fn request_generation(&self, req_id: Uuid) -> Option<u64> {
        self.requests.get(&req_id).map(|r| r.generation)
    }

    fn status_counts(&self) -> (usize, usize, usize, usize) {
        self.requests.values().fold(
            (0, 0, 0, 0),
            |(pending, completed, failed, superseded), request| match &request.status {
                RequestStatus::Pending => (pending + 1, completed, failed, superseded),
                RequestStatus::Completed(_) => (pending, completed + 1, failed, superseded),
                RequestStatus::Failed { .. } => (pending, completed, failed + 1, superseded),
                RequestStatus::Superseded { .. } => (pending, completed, failed, superseded + 1),
            },
        )
    }

    pub fn add_request_with_id(
        &mut self,
        id: Uuid,
        fetch: FetchRange,
        ticker_info: Option<&TickerInfo>,
    ) -> Result<Option<Uuid>, ReqError> {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        self.cleanup_stale_at(now);
        self.last_suppression.set(None);
        if !fetch.is_valid() {
            self.set_suppressed(FetchSuppressionReason::InvalidRange, id, &fetch, None);
            return Ok(None);
        }

        // Check if this trade range was recently marked as empty
        if let FetchRange::Trades(from, to) = fetch
            && let Some(ticker_info) = ticker_info
            && self.is_empty_trade_range(ticker_info, from, to)
        {
            log::info!(
                "FETCH Skip | reason=recent_empty_result stream={}/{}/{:?} range={} req={}",
                format_venue(ticker_info),
                format_symbol(ticker_info),
                ticker_info.exchange(),
                format_time_range(from, to),
                short_id(id)
            );
            self.last_suppression
                .set(Some(FetchSuppressionReason::Throttled));
            return Ok(None);
        }

        let request = FetchRequest::new(fetch, now, self.generation_id);
        let retryable_failed = self
            .requests
            .iter()
            .filter_map(|(existing_id, existing_req)| match &existing_req.status {
                RequestStatus::Failed {
                    error, retry_at, ..
                } if existing_req.same_with(&request) && now >= *retry_at => {
                    Some((*existing_id, error.clone()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        for (existing_id, error) in retryable_failed {
            if let Some(existing_req) = self.requests.remove(&existing_id) {
                log::info!(
                    "FETCH RetryAllowed | req={} previous_req={} {} previous_error={}",
                    short_id(id),
                    short_id(existing_id),
                    format_fetch_range(&existing_req.fetch_type),
                    error
                );
            }
        }
        let (pending, completed, failed, superseded) = self.status_counts();

        log::debug!(
            "FETCH AddRequest | req={} {} map_size={} pending={} completed={} failed={} superseded={}",
            short_id(id),
            format_fetch_range(&fetch),
            self.requests.len(),
            pending,
            completed,
            failed,
            superseded
        );

        if let FetchRange::Trades(new_from, new_to) = fetch {
            for (existing_id, existing_req) in &self.requests {
                let FetchRange::Trades(exist_from, exist_to) = existing_req.fetch_type else {
                    continue;
                };

                let exact = new_from == exist_from && new_to == exist_to;
                let contained = trades_contained(new_from, new_to, exist_from, exist_to);

                match &existing_req.status {
                    RequestStatus::Pending if exact || contained => {
                        let reason = if exact {
                            FetchSuppressionReason::AlreadyPending
                        } else {
                            FetchSuppressionReason::CoveredByInflight
                        };
                        self.set_suppressed(
                            reason,
                            id,
                            &fetch,
                            Some(format!(
                                "req={} {}",
                                short_id(*existing_id),
                                format_time_range(exist_from, exist_to)
                            )),
                        );
                        return Ok(None);
                    }
                    RequestStatus::Completed(ts)
                        if (exact || contained)
                            && now.saturating_sub(*ts) <= Self::COMPLETED_RETENTION_MS =>
                    {
                        log::debug!(
                            "FETCH Skipped | reason=completed_cooldown {} existing_req={} existing_range={} age_ms={}",
                            format_fetch_range(&fetch),
                            short_id(*existing_id),
                            format_time_range(exist_from, exist_to),
                            now.saturating_sub(*ts)
                        );
                        self.last_suppression
                            .set(Some(FetchSuppressionReason::Throttled));
                        return Ok(None);
                    }
                    RequestStatus::Failed {
                        error, retry_at, ..
                    } if exact => {
                        if now >= *retry_at {
                            log::info!(
                                "FETCH RetryAllowed | req={} previous_req={} {} previous_error={}",
                                short_id(id),
                                short_id(*existing_id),
                                format_fetch_range(&fetch),
                                error
                            );
                        } else {
                            self.set_suppressed(
                                FetchSuppressionReason::RecentFailed,
                                id,
                                &fetch,
                                Some(format!(
                                    "req={} retry_in={}",
                                    short_id(*existing_id),
                                    format_duration_ms(retry_at.saturating_sub(now))
                                )),
                            );
                            return Err(ReqError::Failed(error.clone()));
                        }
                    }
                    _ => {}
                }
            }
        }

        if let FetchRange::BubbleSummary {
            from: new_from,
            to: new_to,
            timeframe_ms: new_timeframe,
            ..
        } = fetch
        {
            for (existing_id, existing_req) in &self.requests {
                let FetchRange::BubbleSummary {
                    from: exist_from,
                    to: exist_to,
                    timeframe_ms: exist_timeframe,
                    ..
                } = existing_req.fetch_type
                else {
                    continue;
                };

                let exact = new_from == exist_from
                    && new_to == exist_to
                    && new_timeframe == exist_timeframe;
                let contained = new_timeframe == exist_timeframe
                    && trades_contained(new_from, new_to, exist_from, exist_to);

                match &existing_req.status {
                    RequestStatus::Pending if exact || contained => {
                        let reason = if exact {
                            FetchSuppressionReason::AlreadyPending
                        } else {
                            FetchSuppressionReason::CoveredByInflight
                        };
                        self.set_suppressed(
                            reason,
                            id,
                            &fetch,
                            Some(format!(
                                "req={} {}",
                                short_id(*existing_id),
                                format_time_range(exist_from, exist_to)
                            )),
                        );
                        return Ok(None);
                    }
                    RequestStatus::Completed(ts)
                        if (exact || contained)
                            && now.saturating_sub(*ts) <= Self::COMPLETED_RETENTION_MS =>
                    {
                        log::debug!(
                            "FETCH Skipped | reason=completed_cooldown {} existing_req={} existing_range={} age_ms={}",
                            format_fetch_range(&fetch),
                            short_id(*existing_id),
                            format_time_range(exist_from, exist_to),
                            now.saturating_sub(*ts)
                        );
                        self.last_suppression
                            .set(Some(FetchSuppressionReason::Throttled));
                        return Ok(None);
                    }
                    RequestStatus::Failed {
                        error, retry_at, ..
                    } if exact => {
                        if now >= *retry_at {
                            log::info!(
                                "FETCH RetryAllowed | req={} previous_req={} {} previous_error={}",
                                short_id(id),
                                short_id(*existing_id),
                                format_fetch_range(&fetch),
                                error
                            );
                        } else {
                            self.set_suppressed(
                                FetchSuppressionReason::RecentFailed,
                                id,
                                &fetch,
                                Some(format!(
                                    "req={} retry_in={}",
                                    short_id(*existing_id),
                                    format_duration_ms(retry_at.saturating_sub(now))
                                )),
                            );
                            return Err(ReqError::Failed(error.clone()));
                        }
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
                RequestStatus::Failed {
                    error, retry_at, ..
                } => {
                    if now >= *retry_at {
                        log::info!(
                            "FETCH RetryAllowed | req={} previous_req={} {} previous_error={}",
                            short_id(id),
                            short_id(existing_id),
                            format_fetch_range(&fetch),
                            error
                        );
                        Ok(Some(existing_id))
                    } else {
                        self.set_suppressed(
                            FetchSuppressionReason::RecentFailed,
                            id,
                            &fetch,
                            Some(format!(
                                "req={} retry_in={}",
                                short_id(existing_id),
                                format_duration_ms(retry_at.saturating_sub(now))
                            )),
                        );
                        Err(ReqError::Failed(error.clone()))
                    }
                }
                RequestStatus::Completed(ts) => {
                    if now.saturating_sub(*ts) > Self::COMPLETED_RETENTION_MS {
                        log::info!(
                            "FETCH Retry | reason=completed_cache_expired {} existing_req={}",
                            format_fetch_range(&fetch),
                            short_id(existing_id)
                        );
                        Ok(Some(existing_id))
                    } else {
                        log::debug!(
                            "FETCH Skipped | reason=completed_cooldown {} existing_req={} age_ms={}",
                            format_fetch_range(&fetch),
                            short_id(existing_id),
                            now.saturating_sub(*ts)
                        );
                        self.last_suppression
                            .set(Some(FetchSuppressionReason::Throttled));
                        Ok(None)
                    }
                }
                RequestStatus::Pending => {
                    self.set_suppressed(
                        FetchSuppressionReason::AlreadyPending,
                        id,
                        &fetch,
                        Some(format!("req={}", short_id(existing_id))),
                    );
                    Ok(None)
                }
                RequestStatus::Superseded { .. } => {
                    // Previous request was superseded, reactivate with new generation
                    if let Some(existing_req) = self.requests.get_mut(&existing_id) {
                        log::info!(
                            "FETCH Reactivate | req={} previous_status=Superseded new_generation={} {}",
                            short_id(existing_id),
                            self.generation_id,
                            format_fetch_range(&fetch)
                        );
                        existing_req.status = RequestStatus::Pending;
                        existing_req.generation = self.generation_id;
                        existing_req.updated_at = now;
                        existing_req.attempts = 0;
                    }
                    Ok(Some(existing_id))
                }
            };
        }

        log::info!(
            "FETCH Queued | {} req={} pending_after={} generation={}",
            format_fetch_range(&fetch),
            short_id(id),
            pending + 1,
            self.generation_id
        );
        self.requests.insert(id, request);
        log::info!(
            "FETCH PendingInsert | req={} {} pending_after={} generation={}",
            short_id(id),
            format_fetch_range(&fetch),
            pending + 1,
            self.generation_id
        );
        Ok(Some(id))
    }

    pub fn mark_completed(&mut self, id: Uuid) {
        if let Some(request) = self.requests.get_mut(&id) {
            // Do not overwrite a terminal status (Superseded, Failed, Completed)
            if matches!(request.status, RequestStatus::Superseded { .. }) {
                log::info!(
                    "FETCH StaleResult | req={} action=discard_already_superseded",
                    short_id(id)
                );
                return;
            }
            if matches!(request.status, RequestStatus::Failed { .. }) {
                log::info!(
                    "FETCH StaleResult | req={} action=discard_already_failed",
                    short_id(id)
                );
                return;
            }
            if matches!(request.status, RequestStatus::Completed(_)) {
                log::debug!(
                    "FETCH StaleResult | req={} action=discard_already_completed",
                    short_id(id)
                );
                return;
            }

            let timestamp = chrono::Utc::now().timestamp_millis() as u64;
            request.status = RequestStatus::Completed(timestamp);
            log::debug!(
                "FETCH Completed | req={} {}",
                short_id(id),
                format_fetch_range(&request.fetch_type)
            );
            log::info!(
                "FETCH PendingRemove | req={} {} reason=completed",
                short_id(id),
                format_fetch_range(&request.fetch_type)
            );
        } else {
            log::warn!("FETCH NotFound | req={}", short_id(id));
        }
    }

    pub fn mark_failed(&mut self, id: Uuid, error: String) {
        if let Some(request) = self.requests.get_mut(&id) {
            // Do not overwrite a terminal status (Superseded, Completed)
            if matches!(request.status, RequestStatus::Superseded { .. }) {
                log::info!(
                    "FETCH StaleResult | req={} action=discard_already_superseded",
                    short_id(id)
                );
                return;
            }
            if matches!(request.status, RequestStatus::Completed(_)) {
                log::info!(
                    "FETCH StaleResult | req={} action=discard_already_completed",
                    short_id(id)
                );
                return;
            }

            let now = chrono::Utc::now().timestamp_millis() as u64;
            request.attempts = request.attempts.saturating_add(1);
            let retry_delay = Self::retry_delay_ms(request.attempts);
            log::warn!(
                "FETCH Failed | req={} {} error={} retry_after={}",
                short_id(id),
                format_fetch_range(&request.fetch_type),
                error,
                format_duration_ms(retry_delay)
            );
            log::info!(
                "FETCH PendingRemove | req={} {} reason=failed",
                short_id(id),
                format_fetch_range(&request.fetch_type)
            );
            request.status = RequestStatus::Failed {
                error,
                failed_at: now,
                retry_at: now.saturating_add(retry_delay),
            };
            request.updated_at = now;
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
    BubbleSummary {
        from: UnixMs,
        to: UnixMs,
        timeframe_ms: u64,
        price_step: PriceStep,
        max_candidates_per_candle: usize,
    },
}

impl FetchRange {
    fn is_valid(&self) -> bool {
        match *self {
            FetchRange::Kline(from, to)
            | FetchRange::OpenInterest(from, to)
            | FetchRange::Trades(from, to) => from < to,
            FetchRange::BubbleSummary { from, to, .. } => from < to,
        }
    }

    /// Compatibility bridge used while legacy chart callers are migrated to
    /// declaring `DataRequirement` directly. Bubble summaries deliberately map
    /// to raw trades: aggregation is a derivation concern, not a transport.
    pub fn requirement(self, stream: StreamKind) -> Option<(RequestKey, DataRequirement)> {
        let (dataset, from, to) = match (self, stream) {
            (FetchRange::Kline(from, to), StreamKind::Kline { timeframe, .. }) => (
                DataSet::Klines {
                    timeframe_ms: timeframe.to_milliseconds(),
                },
                from,
                to,
            ),
            (FetchRange::OpenInterest(from, to), StreamKind::Kline { .. }) => {
                (DataSet::OpenInterest, from, to)
            }
            (FetchRange::Trades(from, to), StreamKind::Trades { .. }) => {
                (DataSet::Trades, from, to)
            }
            (FetchRange::BubbleSummary { from, to, .. }, StreamKind::Kline { ticker_info, .. }) => {
                return DataRequirement::trades(TimeRange::new(from, to)?)
                    .with_stream(StreamKind::Trades { ticker_info });
            }
            _ => return None,
        };
        let range = TimeRange::new(from, to)?;
        let key = RequestKey::new(stream, dataset)?;
        Some((
            key,
            DataRequirement {
                dataset,
                range,
                refresh: RefreshPolicy::Historical,
            },
        ))
    }
}

#[derive(PartialEq, Debug)]
struct FetchRequest {
    fetch_type: FetchRange,
    status: RequestStatus,
    created_at: u64,
    updated_at: u64,
    attempts: u32,
    generation: u64,
}

impl FetchRequest {
    fn new(fetch_type: FetchRange, now: u64, generation: u64) -> Self {
        FetchRequest {
            fetch_type,
            status: RequestStatus::Pending,
            created_at: now,
            updated_at: now,
            attempts: 0,
            generation,
        }
    }

    fn same_with(&self, other: &FetchRequest) -> bool {
        match (&self.fetch_type, &other.fetch_type) {
            (FetchRange::Kline(s1, e1), FetchRange::Kline(s2, e2)) => e1 == e2 && s1 == s2,
            (FetchRange::OpenInterest(s1, e1), FetchRange::OpenInterest(s2, e2)) => {
                e1 == e2 && s1 == s2
            }
            (FetchRange::Trades(s1, e1), FetchRange::Trades(s2, e2)) => e1 == e2 && s1 == s2,
            (
                FetchRange::BubbleSummary {
                    from: s1,
                    to: e1,
                    timeframe_ms: t1,
                    price_step: p1,
                    max_candidates_per_candle: m1,
                },
                FetchRange::BubbleSummary {
                    from: s2,
                    to: e2,
                    timeframe_ms: t2,
                    price_step: p2,
                    max_candidates_per_candle: m2,
                },
            ) => e1 == e2 && s1 == s2 && t1 == t2 && p1 == p2 && m1 == m2,
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
    FetchingBubbleSummaries,
    FetchingOI,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FetchTaskStatus {
    Loading(InfoKind),
    Completed {
        req_id: Option<Uuid>,
        fetch: Option<FetchRange>,
        /// When the worker stopped early due to no-progress near target,
        /// this carries the unfilled tail range that should be marked
        /// empty-covered in the negative cache.
        empty_covered_tail: Option<(UnixMs, UnixMs)>,
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
        req_id: Option<Uuid>,
        fetch: Option<FetchRange>,
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
    chart_generation: u64,
) -> Task<FetchUpdate> {
    log::info!(
        "FETCH Owner | req={} pane={} chart_generation={} {}",
        short_id(req_id),
        short_id(pane_id),
        chart_generation,
        format_fetch_range(&fetch)
    );
    log::debug!(
        "FETCH Dispatch | {} req={} pane={} layout={} ready_streams={} override_stream={}",
        format_fetch_range(&fetch),
        short_id(req_id),
        short_id(pane_id),
        short_id(layout_id),
        ready_streams.len(),
        stream.as_ref().map_or("-".to_string(), format_stream)
    );
    trace_ready_streams("FETCH DispatchStreams", ready_streams);

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
                    "KLINE Start | pane={} req={} stream={} range={}",
                    short_id(pane_uid),
                    short_id(req_id),
                    format_stream(&stream),
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

            log::warn!(
                "KLINE Skip | pane={} req={} reason=no_kline_stream ready_streams={}",
                short_id(pane_id),
                short_id(req_id),
                format_streams(ready_streams)
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
                    "OI Start | pane={} req={} stream={} range={}",
                    short_id(pane_uid),
                    short_id(req_id),
                    format_stream(&stream),
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

            log::warn!(
                "OI Skip | pane={} req={} reason=no_kline_stream ready_streams={}",
                short_id(pane_id),
                short_id(req_id),
                format_streams(ready_streams)
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
                        "TRADE Start | venue={} symbol={} range={} req={} pane={} stream={} path={}",
                        format_venue(&ticker_info),
                        format_symbol(&ticker_info),
                        format_time_range(from_time, to_time),
                        short_id(req_id),
                        short_id(pane_id),
                        format_stream(&stream),
                        data_path.display()
                    );

                    let (task, handle) = Task::sip(
                        fetch_trades_batched(
                            handles.clone(),
                            ticker_info,
                            from_time,
                            to_time,
                            data_path,
                            req_id,
                        ),
                        move |batch| {
                            log::debug!(
                                "TRADE Batch | venue={} symbol={} trades={} req={}",
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
                            Ok(empty_covered_tail) => {
                                log::info!(
                                    "TRADE Done | venue={} symbol={} req={} range={} tail={}",
                                    format_venue(&ticker_info),
                                    format_symbol(&ticker_info),
                                    short_id(req_id),
                                    format_time_range(from_time, to_time),
                                    empty_covered_tail
                                        .map(|(f, t)| format_time_range(f, t))
                                        .unwrap_or_else(|| "-".to_string())
                                );
                                FetchUpdate::Status {
                                    pane_id,
                                    status: FetchTaskStatus::Completed {
                                        req_id: Some(req_id),
                                        fetch: Some(FetchRange::Trades(from_time, to_time)),
                                        empty_covered_tail,
                                    },
                                }
                            }
                            Err(err) => {
                                let terminal = match &err {
                                    AdapterError::InvalidRequest(message)
                                        if message.starts_with("TimedOut:") =>
                                    {
                                        "TimedOut"
                                    }
                                    _ => "Failed",
                                };
                                log::error!(
                                    "TRADE {terminal} | venue={} symbol={} req={} range={} error={err}",
                                    format_venue(&ticker_info),
                                    format_symbol(&ticker_info),
                                    short_id(req_id),
                                    format_time_range(from_time, to_time)
                                );
                                FetchUpdate::Error {
                                    pane_id,
                                    error: err.ui_message(),
                                    req_id: Some(req_id),
                                    fetch: Some(FetchRange::Trades(from_time, to_time)),
                                }
                            }
                        },
                    )
                    .abortable();

                    on_trade_handle(handle.abort_on_drop());

                    return task;
                } else {
                    log::warn!(
                        "TRADE Skip | venue={} symbol={} req={} reason=unsupported_exchange range={}",
                        format_venue(&ticker_info),
                        format_symbol(&ticker_info),
                        short_id(req_id),
                        format_time_range(from_time, to_time)
                    );
                }
            } else {
                log::warn!(
                    "TRADE Skip | pane={} req={} reason=no_trade_stream ready_streams={}",
                    short_id(pane_id),
                    short_id(req_id),
                    format_streams(ready_streams)
                );
            }
        }
        FetchRange::BubbleSummary {
            from,
            to,
            timeframe_ms,
            price_step,
            max_candidates_per_candle,
        } => {
            let kline_info = if let Some(
                kline_stream @ StreamKind::Kline {
                    ticker_info,
                    timeframe,
                },
            ) = stream
            {
                Some((ticker_info, timeframe, pane_id, kline_stream))
            } else {
                ready_streams.iter().find_map(|stream| {
                    if let StreamKind::Kline {
                        ticker_info,
                        timeframe,
                    } = stream
                    {
                        Some((*ticker_info, *timeframe, pane_id, *stream))
                    } else {
                        None
                    }
                })
            };

            if let Some((ticker_info, timeframe, pane_id, stream)) = kline_info {
                let is_binance = matches!(
                    ticker_info.exchange(),
                    Exchange::BinanceSpot | Exchange::BinanceLinear | Exchange::BinanceInverse
                );

                if is_binance {
                    let data_path = data::data_path(Some("market_data/binance/"));
                    log::info!(
                        "BUBBLE Summary Request | venue={} symbol={} range={} tf={timeframe:?} timeframe_ms={} max_candidates={} req={} pane={}",
                        format_venue(&ticker_info),
                        format_symbol(&ticker_info),
                        format_time_range(from, to),
                        timeframe_ms,
                        max_candidates_per_candle,
                        short_id(req_id),
                        short_id(pane_id)
                    );

                    return bubble_summary_fetch_task(
                        handles.clone(),
                        layout_id,
                        pane_id,
                        stream,
                        req_id,
                        ticker_info,
                        from,
                        to,
                        timeframe_ms,
                        price_step,
                        max_candidates_per_candle,
                        data_path,
                    );
                }

                log::warn!(
                    "BUBBLE Summary Skip | venue={} symbol={} req={} reason=unsupported_exchange range={}",
                    format_venue(&ticker_info),
                    format_symbol(&ticker_info),
                    short_id(req_id),
                    format_time_range(from, to)
                );
            } else {
                log::warn!(
                    "BUBBLE Summary Skip | pane={} req={} reason=no_kline_stream ready_streams={}",
                    short_id(pane_id),
                    short_id(req_id),
                    format_streams(ready_streams)
                );
            }
        }
    }

    log::warn!(
        "FETCH Suppressed | reason={} req={} pane={} {}",
        FetchSuppressionReason::NoStream,
        short_id(req_id),
        short_id(pane_id),
        format_fetch_range(&fetch)
    );
    Task::done(FetchUpdate::Error {
        pane_id,
        error: "No compatible stream available for fetch".to_string(),
        req_id: Some(req_id),
        fetch: Some(fetch),
    })
}

#[allow(clippy::too_many_arguments)]
fn bubble_summary_fetch_task(
    handles: AdapterHandles,
    layout_id: Uuid,
    pane_id: Uuid,
    stream: StreamKind,
    req_id: Uuid,
    ticker_info: TickerInfo,
    from_time: UnixMs,
    to_time: UnixMs,
    timeframe_ms: u64,
    price_step: PriceStep,
    max_candidates_per_candle: usize,
    data_path: PathBuf,
) -> Task<FetchUpdate> {
    let update_status = Task::done(FetchUpdate::Status {
        pane_id,
        status: FetchTaskStatus::Loading(InfoKind::FetchingBubbleSummaries),
    });

    let fetch = async move {
        fetch_bubble_summaries(
            handles,
            ticker_info,
            from_time,
            to_time,
            timeframe_ms,
            price_step,
            max_candidates_per_candle,
            data_path,
        )
        .await
        .map_err(|err| err.ui_message())
    };

    update_status.chain(Task::perform(fetch, move |result| match result {
        Ok((data, trades_seen, raw_discarded)) => FetchUpdate::Data {
            layout_id,
            pane_id,
            stream,
            data: FetchedData::BubbleSummary {
                data,
                range: (from_time, to_time),
                trades_seen,
                raw_discarded,
                req_id: Some(req_id),
            },
        },
        Err(error) => FetchUpdate::Error {
            pane_id,
            error,
            req_id: Some(req_id),
            fetch: Some(FetchRange::BubbleSummary {
                from: from_time,
                to: to_time,
                timeframe_ms,
                price_step,
                max_candidates_per_candle,
            }),
        },
    }))
}

fn trace_ready_streams(prefix: &str, ready_streams: &[StreamKind]) {
    for (idx, stream) in ready_streams.iter().enumerate() {
        log::trace!("{prefix} | idx={idx} stream={}", format_stream(stream));
    }
}

pub fn request_fetch_many(
    handles: AdapterHandles,
    pane_id: Uuid,
    ready_streams: &[StreamKind],
    layout_id: Uuid,
    reqs: impl IntoIterator<Item = (Uuid, FetchRange, Option<StreamKind>)>,
    mut on_trade_handle: impl FnMut(Handle),
    chart_generation: u64,
) -> Task<FetchUpdate> {
    let mut tasks = Vec::new();
    let reqs = reqs.into_iter().collect::<Vec<_>>();

    log::debug!(
        "FETCH Many | pane={} layout={} requests={} ready_streams={} chart_generation={}",
        short_id(pane_id),
        short_id(layout_id),
        reqs.len(),
        ready_streams.len(),
        chart_generation
    );
    for (idx, (req_id, fetch, stream)) in reqs.iter().enumerate() {
        log::debug!(
            "FETCH Spec | idx={idx} req={} {} override_stream={}",
            short_id(*req_id),
            format_fetch_range(fetch),
            stream.as_ref().map_or("-".to_string(), format_stream)
        );
    }

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
            chart_generation,
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
                    "OI Request | pane={} req={} venue={} symbol={} tf={timeframe:?} range={}",
                    short_id(pane_id),
                    format_req_id(req_id),
                    format_venue(&ticker_info),
                    format_symbol(&ticker_info),
                    range.map_or("-".to_string(), |(from, to)| format_time_range(from, to))
                );

                let result = handles
                    .fetch_open_interest(ticker_info, timeframe, range)
                    .await;

                match &result {
                    Ok(oi) => {
                        if oi.is_empty() {
                            log::warn!(
                                "OI Empty | pane={} req={} range={} duration={}",
                                short_id(pane_id),
                                format_req_id(req_id),
                                range.map_or("-".to_string(), |(from, to)| format_time_range(
                                    from, to
                                )),
                                format_duration_ms(started.elapsed().as_millis() as u64)
                            );
                        }
                        log::info!(
                            "OI Done | pane={} req={} records={} first={} last={} duration={}",
                            short_id(pane_id),
                            format_req_id(req_id),
                            oi.len(),
                            oi.first()
                                .map_or("-".to_string(), |oi| format_time_short(oi.time)),
                            oi.last()
                                .map_or("-".to_string(), |oi| format_time_short(oi.time)),
                            format_duration_ms(started.elapsed().as_millis() as u64)
                        )
                    }
                    Err(err) => log::error!(
                        "OI Error | pane={} req={} duration={} error={err}",
                        short_id(pane_id),
                        format_req_id(req_id),
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
                        req_id,
                        fetch: range.map(|(from, to)| FetchRange::OpenInterest(from, to)),
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
                    "KLINE Request | pane={} req={} venue={} symbol={} tf={timeframe:?} range={}",
                    short_id(pane_id),
                    format_req_id(req_id),
                    format_venue(&ticker_info),
                    format_symbol(&ticker_info),
                    range.map_or("-".to_string(), |(from, to)| format_time_range(from, to))
                );

                let mut result = handles.fetch_klines(ticker_info, timeframe, range).await;
                let raw_count = result.as_ref().map_or(0, Vec::len);
                if let (Ok(klines), Some((from, to))) = (&mut result, range) {
                    let timeframe_ms = timeframe.to_milliseconds();
                    klines.retain(|kline| {
                        kline.time.saturating_add(timeframe_ms) > from && kline.time <= to
                    });
                }

                match &result {
                    Ok(klines) => {
                        if klines.is_empty() {
                            log::warn!(
                                "KLINE Empty | pane={} req={} range={} raw_records={} duration={}",
                                short_id(pane_id),
                                format_req_id(req_id),
                                range.map_or("-".to_string(), |(from, to)| format_time_range(
                                    from, to
                                )),
                                raw_count,
                                format_duration_ms(started.elapsed().as_millis() as u64)
                            );
                        }
                        log::info!(
                            "KLINE Done | pane={} req={} raw_records={} retained_records={} first={} last={} duration={}",
                            short_id(pane_id),
                            format_req_id(req_id),
                            raw_count,
                            klines.len(),
                            klines
                                .first()
                                .map_or("-".to_string(), |kline| format_time_short(kline.time)),
                            klines
                                .last()
                                .map_or("-".to_string(), |kline| format_time_short(kline.time)),
                            format_duration_ms(started.elapsed().as_millis() as u64)
                        )
                    }
                    Err(err) => log::error!(
                        "KLINE Error | pane={} req={} duration={} error={err}",
                        short_id(pane_id),
                        format_req_id(req_id),
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
                        req_id,
                        fetch: range.map(|(from, to)| FetchRange::Kline(from, to)),
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
    req_id: Uuid,
) -> impl Straw<Option<(UnixMs, UnixMs)>, Vec<Trade>, AdapterError> {
    let venue = format_venue(&ticker_info);
    let symbol = format_symbol(&ticker_info);

    sipper(async move |mut progress| {
        let mut latest_trade_t = from_time;
        let started = Instant::now();
        let mut total_trades = 0usize;
        let mut request_count = 0usize;
        let mut last_progress_log = Instant::now();
        let mut consecutive_no_progress: usize = 0;
        let mut prev_request_from: Option<UnixMs> = None;
        let mut prev_request_to: Option<UnixMs> = None;

        log::info!(
            "TRADE Worker | {venue} {symbol} req={} range={} path={}",
            short_id(req_id),
            format_time_range(from_time, to_time),
            data_path.display()
        );

        while latest_trade_t < to_time {
            // Overall worker timeout: bail out if we've been running too long.
            if started.elapsed() >= TRADE_WORKER_TIMEOUT {
                log::error!(
                    "TRADE Worker Timeout | venue={venue} symbol={symbol} req={} range={} elapsed={} requests={request_count} trades={total_trades} latest={} target_to={}",
                    short_id(req_id),
                    format_time_range(from_time, to_time),
                    format_duration_ms(started.elapsed().as_millis() as u64),
                    format_time_short(latest_trade_t),
                    format_time_short(to_time)
                );
                return Err(AdapterError::InvalidRequest(format!(
                    "TimedOut: worker exceeded {}",
                    format_duration_ms(TRADE_WORKER_TIMEOUT.as_millis() as u64)
                )));
            }

            request_count += 1;

            // Safety: stop if we're about to make the exact same request as
            // last time (same startTime/endTime).  This catches edge cases
            // where cursor advancement was skipped.
            if prev_request_from == Some(latest_trade_t) && prev_request_to == Some(to_time) {
                log::warn!(
                    "TRADE Stop | venue={venue} symbol={symbol} reason=duplicate_request_bounds from={} target_to={} requests={request_count} trades={total_trades} duration={}",
                    format_time_short(latest_trade_t),
                    format_time_short(to_time),
                    format_duration_ms(started.elapsed().as_millis() as u64)
                );
                break;
            }
            prev_request_from = Some(latest_trade_t);
            prev_request_to = Some(to_time);

            let request_started = Instant::now();
            log::debug!(
                "TRADE Request | venue={venue} symbol={symbol} req={} request_idx={request_count} from={} target_to={}",
                short_id(req_id),
                format_time_short(latest_trade_t),
                format_time_short(to_time)
            );

            let fetch_result = tokio::time::timeout(
                TRADE_REST_REQUEST_TIMEOUT,
                handles.fetch_trades(ticker_info, latest_trade_t, Some(data_path.clone())),
            )
            .await;

            match fetch_result {
                Err(_) => {
                    let elapsed = request_started.elapsed().as_millis() as u64;
                    log::error!(
                        "TRADE TimedOut | venue={venue} symbol={symbol} req={} request_idx={request_count} from={} target_to={} timeout={} duration={}",
                        short_id(req_id),
                        format_time_short(latest_trade_t),
                        format_time_short(to_time),
                        format_duration_ms(TRADE_REST_REQUEST_TIMEOUT.as_millis() as u64),
                        format_duration_ms(elapsed)
                    );
                    return Err(AdapterError::InvalidRequest(format!(
                        "TimedOut: REST trade fetch exceeded {}",
                        format_duration_ms(TRADE_REST_REQUEST_TIMEOUT.as_millis() as u64)
                    )));
                }
                Ok(Ok(batch)) => {
                    let elapsed = request_started.elapsed().as_millis() as u64;
                    let prev_latest_trade_t = latest_trade_t;
                    log::debug!(
                        "TRADE Response | venue={venue} symbol={symbol} req={} request_idx={request_count} trades={} first={} last={} duration={}",
                        short_id(req_id),
                        batch.len(),
                        format_optional_time(batch.first().map(|trade| trade.time)),
                        format_optional_time(batch.last().map(|trade| trade.time)),
                        format_duration_ms(elapsed)
                    );

                    if batch.is_empty() {
                        log::warn!(
                            "TRADE Stop | venue={venue} symbol={symbol} reason=empty_batch_before_target latest={} target_to={} remaining_ms={} requests={request_count} trades={total_trades} total_duration={}",
                            format_time_short(latest_trade_t),
                            format_time_short(to_time),
                            to_time.saturating_diff(latest_trade_t),
                            format_duration_ms(started.elapsed().as_millis() as u64)
                        );
                        break;
                    }

                    latest_trade_t = batch.last().map_or(latest_trade_t, |trade| trade.time);
                    if latest_trade_t <= prev_latest_trade_t {
                        consecutive_no_progress += 1;
                        let remaining_ms = to_time.saturating_diff(latest_trade_t);
                        log::warn!(
                            "TRADE NoProgress | venue={venue} symbol={symbol} request_idx={request_count} prev_latest={} new_latest={} target_to={} remaining_ms={remaining_ms} consecutive={consecutive_no_progress} reason=batch_last_not_after_latest",
                            format_time_short(prev_latest_trade_t),
                            format_time_short(latest_trade_t),
                            format_time_short(to_time)
                        );

                        // Near-target with tiny unreachable tail → complete as
                        // partial result instead of retrying forever.
                        if remaining_ms <= NO_PROGRESS_REMAINING_EPSILON_MS {
                            let tail_from = latest_trade_t.saturating_add(1);
                            log::info!(
                                "TRADE Stop | venue={venue} symbol={symbol} reason=no_progress_near_target latest={} target_to={} remaining_ms={remaining_ms} requests={request_count} trades={total_trades} duration={}",
                                format_time_short(latest_trade_t),
                                format_time_short(to_time),
                                format_duration_ms(started.elapsed().as_millis() as u64)
                            );
                            return Ok(Some((tail_from, to_time)));
                        }

                        // Consecutive no-progress threshold → stop early,
                        // long before the 90s watchdog.
                        if consecutive_no_progress >= TRADE_NO_PROGRESS_MAX_CONSECUTIVE {
                            let tail_from = latest_trade_t.saturating_add(1);
                            log::warn!(
                                "TRADE Stop | venue={venue} symbol={symbol} reason=no_progress latest={} target_to={} remaining_ms={remaining_ms} requests={request_count} trades={total_trades} duration={}",
                                format_time_short(latest_trade_t),
                                format_time_short(to_time),
                                format_duration_ms(started.elapsed().as_millis() as u64)
                            );
                            return Ok(Some((tail_from, to_time)));
                        }

                        // Advance cursor by 1ms so the next request uses
                        // different startTime and avoids re-fetching the
                        // identical batch.
                        let advanced = latest_trade_t.saturating_add(1);
                        if advanced >= to_time {
                            log::info!(
                                "TRADE Stop | venue={venue} symbol={symbol} reason=cursor_exceeds_target latest={} target_to={} requests={request_count} trades={total_trades}",
                                format_time_short(latest_trade_t),
                                format_time_short(to_time)
                            );
                            return Ok(Some((advanced, to_time)));
                        }
                        latest_trade_t = advanced;
                        prev_request_from = None; // bounds changed
                    } else {
                        consecutive_no_progress = 0;
                    }
                    total_trades += batch.len();

                    // Progress log every 5 seconds or every 10 requests
                    if last_progress_log.elapsed().as_secs() >= 5
                        || request_count.is_multiple_of(10)
                    {
                        log::info!(
                            "TRADE Progress | venue={venue} symbol={symbol} request_idx={request_count} trades={total_trades} latest={} target_to={} remaining_ms={} elapsed={}",
                            format_time_short(latest_trade_t),
                            format_time_short(to_time),
                            to_time.saturating_diff(latest_trade_t),
                            format_duration_ms(started.elapsed().as_millis() as u64)
                        );
                        last_progress_log = Instant::now();
                    }

                    let () = progress.send(batch).await;
                }
                Ok(Err(err)) => {
                    log::error!(
                        "TRADE Error | venue={venue} symbol={symbol} req={} request_idx={request_count} from={} duration={} error={err}",
                        short_id(req_id),
                        format_time_short(latest_trade_t),
                        format_duration_ms(request_started.elapsed().as_millis() as u64)
                    );
                    return Err(err);
                }
            }
        }

        log::info!(
            "TRADE Worker Done | venue={venue} symbol={symbol} req={} range={} requests={request_count} returned_trades={total_trades} final_latest={} duration={}",
            short_id(req_id),
            format_time_range(from_time, to_time),
            format_time_short(latest_trade_t),
            format_duration_ms(started.elapsed().as_millis() as u64)
        );

        Ok(None)
    })
}

#[derive(Debug, Clone, Copy, Default)]
struct BubbleBucketAccum {
    buy_qty: Qty,
    sell_qty: Qty,
    trade_count: usize,
    first_time: Option<UnixMs>,
    last_time: Option<UnixMs>,
}

impl BubbleBucketAccum {
    fn add_trade(&mut self, trade: &Trade) {
        if trade.is_sell {
            self.sell_qty += trade.qty;
        } else {
            self.buy_qty += trade.qty;
        }

        self.trade_count += 1;
        self.first_time = Some(
            self.first_time
                .map_or(trade.time, |first| first.min(trade.time)),
        );
        self.last_time = Some(
            self.last_time
                .map_or(trade.time, |last| last.max(trade.time)),
        );
    }

    fn total_qty(self) -> Qty {
        self.buy_qty + self.sell_qty
    }
}

async fn fetch_bubble_summaries(
    handles: AdapterHandles,
    ticker_info: TickerInfo,
    from_time: UnixMs,
    to_time: UnixMs,
    timeframe_ms: u64,
    price_step: PriceStep,
    max_candidates_per_candle: usize,
    data_path: PathBuf,
) -> Result<(Vec<BubbleVolumeSummary>, usize, usize), AdapterError> {
    let venue = format_venue(&ticker_info);
    let symbol = format_symbol(&ticker_info);
    let started = Instant::now();
    let mut latest_trade_t = from_time;
    let mut request_count = 0usize;
    let mut trades_seen = 0usize;
    let mut buckets: FxHashMap<(UnixMs, Price), BubbleBucketAccum> = FxHashMap::default();

    log::info!(
        "CHART Bubbles | action=fetch_summary venue={venue} symbol={symbol} range={} timeframe_ms={} max_candidates={max_candidates_per_candle}",
        format_time_range(from_time, to_time),
        timeframe_ms
    );

    while latest_trade_t < to_time {
        request_count += 1;
        let batch_started = Instant::now();
        let batch = handles
            .fetch_trades(ticker_info, latest_trade_t, Some(data_path.clone()))
            .await?;

        if batch.is_empty() {
            log::warn!(
                "BUBBLE Summary Batch | trades_seen={trades_seen} buckets={} retained_candidates={} reason=empty_batch",
                buckets.len(),
                retained_bubble_candidate_count(&buckets)
            );
            break;
        }

        let prev_latest_trade_t = latest_trade_t;
        for trade in &batch {
            if trade.time < from_time || trade.time > to_time {
                continue;
            }

            let candle_time =
                UnixMs::new(trade.time.as_u64() - (trade.time.as_u64() % timeframe_ms));
            let price = trade.price.round_to_step(price_step);
            buckets
                .entry((candle_time, price))
                .or_default()
                .add_trade(trade);
        }

        trades_seen += batch.len();
        latest_trade_t = batch.last().map_or(latest_trade_t, |trade| trade.time);
        retain_top_bubble_buckets(&mut buckets, max_candidates_per_candle);

        log::debug!(
            "BUBBLE Summary Batch | trades_seen={trades_seen} buckets={} retained_candidates={} request_idx={request_count} first={} last={} duration={}",
            buckets.len(),
            retained_bubble_candidate_count(&buckets),
            format_optional_time(batch.first().map(|trade| trade.time)),
            format_optional_time(batch.last().map(|trade| trade.time)),
            format_duration_ms(batch_started.elapsed().as_millis() as u64)
        );

        if latest_trade_t <= prev_latest_trade_t {
            log::warn!(
                "BUBBLE Summary Stop | reason=no_progress latest={} target_to={} requests={request_count}",
                format_time_short(latest_trade_t),
                format_time_short(to_time)
            );
            break;
        }
    }

    let summaries = bubble_summaries_from_buckets(buckets, max_candidates_per_candle);
    let candidate_count = summaries
        .iter()
        .map(|summary| summary.candidates.len())
        .sum::<usize>();

    log::info!(
        "BUBBLE Summary Done | range={} candles={} candidates={} raw_discarded={} trades_seen={} requests={request_count} duration={}",
        format_time_range(from_time, to_time),
        summaries.len(),
        candidate_count,
        trades_seen,
        trades_seen,
        format_duration_ms(started.elapsed().as_millis() as u64)
    );

    Ok((summaries, trades_seen, trades_seen))
}

fn retained_bubble_candidate_count(
    buckets: &FxHashMap<(UnixMs, Price), BubbleBucketAccum>,
) -> usize {
    buckets.len()
}

fn retain_top_bubble_buckets(
    buckets: &mut FxHashMap<(UnixMs, Price), BubbleBucketAccum>,
    max_candidates_per_candle: usize,
) {
    if max_candidates_per_candle == 0 {
        buckets.clear();
        return;
    }

    let mut by_candle: FxHashMap<UnixMs, Vec<(Price, Qty)>> = FxHashMap::default();
    for ((candle_time, price), bucket) in buckets.iter() {
        by_candle
            .entry(*candle_time)
            .or_default()
            .push((*price, bucket.total_qty()));
    }

    let mut keep: FxHashMap<(UnixMs, Price), ()> = FxHashMap::default();
    for (candle_time, mut entries) in by_candle {
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.1));
        entries.truncate(max_candidates_per_candle);
        for (price, _) in entries {
            keep.insert((candle_time, price), ());
        }
    }

    buckets.retain(|key, _| keep.contains_key(key));
}

fn bubble_summaries_from_buckets(
    buckets: FxHashMap<(UnixMs, Price), BubbleBucketAccum>,
    max_candidates_per_candle: usize,
) -> Vec<BubbleVolumeSummary> {
    let mut grouped: FxHashMap<UnixMs, Vec<BubbleCandidate>> = FxHashMap::default();

    for ((candle_time, price), bucket) in buckets {
        let total_qty = bucket.total_qty();
        let total = total_qty.to_f64();
        let delta_qty = bucket.buy_qty - bucket.sell_qty;
        let score = if total > 0.0 {
            total * (1.0 + (delta_qty.to_f64().abs() / total).clamp(0.0, 1.0))
        } else {
            0.0
        };

        grouped
            .entry(candle_time)
            .or_default()
            .push(BubbleCandidate {
                candle_time,
                price,
                total_qty,
                buy_qty: bucket.buy_qty,
                sell_qty: bucket.sell_qty,
                delta_qty,
                trade_count: bucket.trade_count,
                score,
                first_time: bucket.first_time,
                last_time: bucket.last_time,
            });
    }

    let mut summaries = grouped
        .into_iter()
        .map(|(candle_time, mut candidates)| {
            candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.total_qty));
            candidates.truncate(max_candidates_per_candle);
            BubbleVolumeSummary::new(candle_time, candidates)
        })
        .collect::<Vec<_>>();

    summaries.sort_by_key(|summary| summary.candle_time);
    summaries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generation_id_increments_on_supersede() {
        let mut handler = RequestHandler::default();
        assert_eq!(handler.generation_id(), 0);

        // Add a pending request
        let req_id = handler
            .add_request(FetchRange::Trades(UnixMs::new(1000), UnixMs::new(2000)))
            .unwrap()
            .unwrap();
        assert_eq!(handler.generation_id(), 0);

        // Supersede should increment generation
        let superseded = handler.supersede_all_pending("settings_changed");
        assert_eq!(superseded.len(), 1);
        assert_eq!(superseded[0], req_id);
        assert_eq!(handler.generation_id(), 1);

        // Second supersede increments again
        handler.supersede_all_pending("settings_changed");
        assert_eq!(handler.generation_id(), 2);
    }

    #[test]
    fn test_mark_completed_does_not_overwrite_superseded() {
        let mut handler = RequestHandler::default();

        let req_id = handler
            .add_request(FetchRange::Trades(UnixMs::new(1000), UnixMs::new(2000)))
            .unwrap()
            .unwrap();

        // Supersede the request
        handler.supersede_all_pending("settings_changed");

        // Try to mark as completed - should not overwrite Superseded
        handler.mark_completed(req_id);

        // Verify it's still Superseded, not Completed
        assert!(handler.is_stale_generation(req_id));
    }

    #[test]
    fn test_mark_failed_does_not_overwrite_superseded() {
        let mut handler = RequestHandler::default();

        let req_id = handler
            .add_request(FetchRange::Trades(UnixMs::new(1000), UnixMs::new(2000)))
            .unwrap()
            .unwrap();

        // Supersede the request
        handler.supersede_all_pending("settings_changed");

        // Try to mark as failed - should not overwrite Superseded
        handler.mark_failed(req_id, "some error".to_string());

        // Verify it's still stale (Superseded)
        assert!(handler.is_stale_generation(req_id));
    }

    #[test]
    fn test_stale_generation_detection() {
        let mut handler = RequestHandler::default();

        // Add request in generation 0
        let req_id = handler
            .add_request(FetchRange::Trades(UnixMs::new(1000), UnixMs::new(2000)))
            .unwrap()
            .unwrap();

        // Request is in current generation
        assert!(!handler.is_stale_generation(req_id));
        assert_eq!(handler.request_generation(req_id), Some(0));

        // Supersede increments generation to 1
        handler.supersede_all_pending("settings_changed");
        assert_eq!(handler.generation_id(), 1);

        // Request from generation 0 is now stale
        assert!(handler.is_stale_generation(req_id));
        assert_eq!(handler.request_generation(req_id), Some(0));
    }

    #[test]
    fn test_supersede_only_affects_pending() {
        let mut handler = RequestHandler::default();

        // Add two requests
        let req1 = handler
            .add_request(FetchRange::Trades(UnixMs::new(1000), UnixMs::new(2000)))
            .unwrap()
            .unwrap();
        let req2 = handler
            .add_request(FetchRange::Trades(UnixMs::new(3000), UnixMs::new(4000)))
            .unwrap()
            .unwrap();

        // Complete one request
        handler.mark_completed(req1);

        // Supersede should only affect pending requests
        let superseded = handler.supersede_all_pending("settings_changed");
        assert_eq!(superseded.len(), 1);
        assert_eq!(superseded[0], req2);

        // req1 should be Completed, not Superseded
        assert!(!handler.is_stale_generation(req1));
        // req2 should be Superseded (stale)
        assert!(handler.is_stale_generation(req2));
    }

    #[test]
    fn test_timeout_watchdog() {
        let mut handler = RequestHandler::default();

        // Add a request
        let _req_id = handler
            .add_request(FetchRange::Trades(UnixMs::new(1000), UnixMs::new(2000)))
            .unwrap()
            .unwrap();

        // Simulate time passing beyond timeout
        let future_time = chrono::Utc::now().timestamp_millis() as u64
            + RequestHandler::PENDING_TIMEOUT_MS
            + 1000;
        handler.cleanup_stale_at(future_time);

        // Request should now be Failed with timeout
        // Note: cleanup_stale_at doesn't remove immediately, just changes status
        // It will be removed on next cleanup after FAILED_RETENTION_MS
    }

    #[test]
    fn test_new_request_after_supersede() {
        let mut handler = RequestHandler::default();

        // Add request in generation 0
        let req1 = handler
            .add_request(FetchRange::Trades(UnixMs::new(1000), UnixMs::new(2000)))
            .unwrap()
            .unwrap();

        // Supersede
        handler.supersede_all_pending("settings_changed");
        assert!(handler.is_stale_generation(req1));

        // Add new request with same range - reactivates the old request
        let req2 = handler
            .add_request(FetchRange::Trades(UnixMs::new(1000), UnixMs::new(2000)))
            .unwrap()
            .unwrap();

        // req2 reuses req1 ID and reactivates it with new generation
        assert_eq!(req1, req2);
        assert!(!handler.is_stale_generation(req1));
        assert_eq!(handler.request_generation(req1), Some(1));

        // A different range gets a new ID in generation 1
        let req3 = handler
            .add_request(FetchRange::Trades(UnixMs::new(3000), UnixMs::new(4000)))
            .unwrap()
            .unwrap();
        assert_ne!(req1, req3);
        assert!(!handler.is_stale_generation(req3));
        assert_eq!(handler.request_generation(req3), Some(1));
    }

    #[test]
    fn test_not_found_request_is_stale() {
        let handler = RequestHandler::default();
        let random_id = Uuid::new_v4();

        // Unknown request should be treated as stale
        assert!(handler.is_stale_generation(random_id));
        assert_eq!(handler.request_generation(random_id), None);
    }
}
