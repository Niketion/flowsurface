//! Persistent, validated cache for every bounded historical market-data fetch.
//!
//! Data is stored in bounded time buckets so a later request can reuse an arbitrary
//! prefix/suffix and fetch only genuine coverage gaps. Every value has a schema
//! marker and checksum; decoded records are also validated semantically before
//! they are allowed back into the chart.

use data::chart::kline::{BubbleCandidate, BubbleVolumeSummary};
use exchange::{Kline, OpenInterest, TickerInfo, Timeframe, Trade, UnixMs, Volume};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

// v3 invalidates v2 coverage records that could mark future/empty trade ranges
// and discontinuous kline ranges as complete. Those records can suppress the
// footprint and reconnect backfill even though the underlying data is missing.
const CACHE_SCHEMA: u64 = 3;
const CACHE_MAGIC: &[u8; 8] = b"FSCACHE1";
const HEADER_LEN: usize = 8 + 8 + 8 + 4;
const HOURLY_BUCKET_MS: u64 = 60 * 60 * 1_000;
const TRADE_BUCKET_MS: u64 = 60 * 1_000;
const MAX_BLOB_BYTES: usize = 512 * 1024 * 1024;
const MAX_RECORDS_PER_BUCKET: usize = 5_000_000;
const SCHEMA_KEY: &str = "market-data-cache-schema";

const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("cache_meta_v1");
const COVERAGE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("cache_coverage_v1");
const KLINE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("klines_v1");
const TRADE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("trades_v1");
const OI_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("open_interest_v1");
// V1 stored candle/price bins. V2 stores temporal/spatial smart clusters and must never decode V1.
const BUBBLE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("bubble_summaries_v2");

static CACHE: OnceLock<Option<MarketDataCache>> = OnceLock::new();

/// Cached records plus the half-open intervals which still require the network.
#[derive(Debug)]
pub struct CacheSlice<T> {
    pub records: Vec<T>,
    pub gaps: Vec<(UnixMs, UnixMs)>,
}

impl<T> CacheSlice<T> {
    fn miss(from: UnixMs, to_exclusive: UnixMs) -> Self {
        Self {
            records: Vec::new(),
            gaps: if from < to_exclusive {
                vec![(from, to_exclusive)]
            } else {
                Vec::new()
            },
        }
    }

    pub fn is_complete(&self) -> bool {
        self.gaps.is_empty()
    }
}

#[derive(Debug, Clone, Copy)]
enum CacheKind {
    Kline,
    Trade,
    OpenInterest,
    BubbleSummary,
}

impl CacheKind {
    fn label(self) -> &'static str {
        match self {
            Self::Kline => "kline",
            Self::Trade => "trade",
            Self::OpenInterest => "open_interest",
            Self::BubbleSummary => "bubble_summary",
        }
    }

    fn table(self) -> TableDefinition<'static, &'static str, &'static [u8]> {
        match self {
            Self::Kline => KLINE_TABLE,
            Self::Trade => TRADE_TABLE,
            Self::OpenInterest => OI_TABLE,
            Self::BubbleSummary => BUBBLE_TABLE,
        }
    }

    fn bucket_ms(self) -> u64 {
        match self {
            // Raw trades are much denser than every other dataset. Minute
            // buckets avoid repeatedly rewriting a multi-million-record hour.
            Self::Trade => TRADE_BUCKET_MS,
            Self::Kline | Self::OpenInterest | Self::BubbleSummary => HOURLY_BUCKET_MS,
        }
    }
}

#[derive(Debug, Serialize, serde::Deserialize)]
struct StoredBucket<T> {
    schema: u64,
    dataset_key: String,
    bucket_start: u64,
    records: Vec<T>,
}

#[derive(Debug, Default, Serialize, serde::Deserialize)]
struct StoredCoverage {
    schema: u64,
    dataset_key: String,
    /// Sorted, merged, half-open intervals `[from, to)`.
    intervals: Vec<(u64, u64)>,
}

trait CacheRecord: Clone + Serialize + DeserializeOwned {
    fn timestamp(&self) -> UnixMs;
    fn is_semantically_valid(&self) -> bool;
    fn normalize(records: Vec<Self>) -> Vec<Self>;
}

impl CacheRecord for Kline {
    fn timestamp(&self) -> UnixMs {
        self.time
    }

    fn is_semantically_valid(&self) -> bool {
        let prices_are_valid = self.open.units > 0
            && self.high.units > 0
            && self.low.units > 0
            && self.close.units > 0
            && self.high >= self.low
            && self.high >= self.open
            && self.high >= self.close
            && self.low <= self.open
            && self.low <= self.close;
        let volume_is_valid = match self.volume {
            Volume::TotalOnly(total) => total.units >= 0,
            Volume::BuySell(buy, sell) => buy.units >= 0 && sell.units >= 0,
        };
        prices_are_valid && volume_is_valid
    }

    fn normalize(records: Vec<Self>) -> Vec<Self> {
        let mut by_time = BTreeMap::new();
        for record in records {
            by_time.insert(record.time, record);
        }
        by_time.into_values().collect()
    }
}

impl CacheRecord for Trade {
    fn timestamp(&self) -> UnixMs {
        self.time
    }

    fn is_semantically_valid(&self) -> bool {
        self.price.units > 0 && self.qty.units >= 0
    }

    fn normalize(mut records: Vec<Self>) -> Vec<Self> {
        records.sort_by_key(|trade| (trade.time, trade.id));
        let mut seen_ids = BTreeSet::new();
        records.retain(|trade| {
            // Never collapse id-less trades by value: two real executions can
            // legitimately have identical millisecond/side/price/quantity.
            trade.id.is_none_or(|id| seen_ids.insert(id))
        });
        records
    }
}

impl CacheRecord for OpenInterest {
    fn timestamp(&self) -> UnixMs {
        self.time
    }

    fn is_semantically_valid(&self) -> bool {
        self.value.is_finite() && self.value >= 0.0
    }

    fn normalize(records: Vec<Self>) -> Vec<Self> {
        let mut by_time = BTreeMap::new();
        for record in records {
            by_time.insert(record.time, record);
        }
        by_time.into_values().collect()
    }
}

impl CacheRecord for BubbleVolumeSummary {
    fn timestamp(&self) -> UnixMs {
        self.candle_time
    }

    fn is_semantically_valid(&self) -> bool {
        if self.candidates.len() > MAX_RECORDS_PER_BUCKET {
            return false;
        }
        if self.algorithm_version != data::chart::kline::BUBBLE_SUMMARY_ALGORITHM_VERSION {
            return false;
        }
        self.candidates.iter().all(|candidate| {
            candidate.candle_time == self.candle_time
                && candidate.vwap_price.units > 0
                && candidate.total_qty.units >= 0
                && candidate.buy_qty.units >= 0
                && candidate.sell_qty.units >= 0
                && candidate.total_qty == candidate.buy_qty + candidate.sell_qty
                && candidate.delta_qty == candidate.buy_qty - candidate.sell_qty
                && candidate.percentile_rank.is_finite()
                && candidate.importance_score.is_finite()
                && candidate.first_time <= candidate.last_time
        })
    }

    fn normalize(records: Vec<Self>) -> Vec<Self> {
        let mut by_time = BTreeMap::new();
        for record in records {
            // The newest calculation replaces a previous calculation of the
            // same candle. Callers merge partial Bubble ranges before storing.
            by_time.insert(record.candle_time, record);
        }
        by_time.into_values().collect()
    }
}

pub struct MarketDataCache {
    db: Database,
    path: PathBuf,
}

/// Lazily opens the process-wide cache. A cache failure never blocks fetching.
pub fn market_cache() -> Option<&'static MarketDataCache> {
    CACHE
        .get_or_init(|| match MarketDataCache::open() {
            Ok(cache) => {
                log::info!("CACHE Ready | path={}", cache.path.display());
                Some(cache)
            }
            Err(error) => {
                log::error!("CACHE Disabled | error={error}");
                None
            }
        })
        .as_ref()
}

impl MarketDataCache {
    fn open() -> Result<Self, String> {
        let path = data::data_path(Some("market_data/cache/market.redb"));
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "cannot create cache directory {}: {error}",
                    parent.display()
                )
            })?;
        }

        match open_initialized_database(&path) {
            Ok(db) => Ok(Self { db, path }),
            Err(first_error) => {
                if path.exists() {
                    quarantine_database(&path, &first_error)?;
                }
                let db = open_initialized_database(&path).map_err(|second_error| {
                    format!(
                        "cache recreation failed after {first_error}; second error: {second_error}"
                    )
                })?;
                Ok(Self { db, path })
            }
        }
    }

    pub fn read_klines(
        &self,
        ticker_info: TickerInfo,
        timeframe: Timeframe,
        from: UnixMs,
        to_exclusive: UnixMs,
    ) -> CacheSlice<Kline> {
        let key = dataset_key(
            ticker_info,
            &format!("kline|timeframe_ms={}", timeframe.to_milliseconds()),
        );
        let timeframe_ms = timeframe.to_milliseconds();
        let record_from = from.saturating_sub(timeframe_ms.saturating_sub(1));
        let mut slice: CacheSlice<Kline> =
            self.read_records(CacheKind::Kline, &key, from, to_exclusive, record_from);

        // Coverage is not sufficient proof of a continuous candle series. A
        // partial/truncated response may have persisted coverage around a
        // missing candle. Turn every internal discontinuity back into a network
        // gap so the fetcher can repair it.
        let mut gaps = slice
            .gaps
            .iter()
            .map(|(gap_from, gap_to)| (gap_from.as_u64(), gap_to.as_u64()))
            .collect::<Vec<_>>();
        let gaps_before = gaps.len();
        if timeframe_ms > 0 {
            for pair in slice.records.windows(2) {
                let expected = pair[0].time.saturating_add(timeframe_ms);
                let next = pair[1].time;
                if next > expected {
                    let gap_from = expected.max(from);
                    let gap_to = next.min(to_exclusive);
                    if gap_from < gap_to {
                        gaps.push((gap_from.as_u64(), gap_to.as_u64()));
                    }
                }
            }
        }
        gaps = merge_intervals(gaps);
        if gaps.len() > gaps_before {
            log::warn!(
                "CACHE KlineIntegrityGap | key={} records={} detected_gaps={} range={}..{}",
                key,
                slice.records.len(),
                gaps.len().saturating_sub(gaps_before),
                from.as_u64(),
                to_exclusive.as_u64()
            );
        }
        slice.gaps = gaps
            .into_iter()
            .map(|(gap_from, gap_to)| (UnixMs::new(gap_from), UnixMs::new(gap_to)))
            .collect();
        slice
    }

    pub fn store_klines(
        &self,
        ticker_info: TickerInfo,
        timeframe: Timeframe,
        from: UnixMs,
        to_exclusive: UnixMs,
        records: &[Kline],
    ) {
        let key = dataset_key(
            ticker_info,
            &format!("kline|timeframe_ms={}", timeframe.to_milliseconds()),
        );
        self.store_records(CacheKind::Kline, &key, from, to_exclusive, records);
    }

    pub fn read_trades(
        &self,
        ticker_info: TickerInfo,
        from: UnixMs,
        to_exclusive: UnixMs,
    ) -> CacheSlice<Trade> {
        let key = dataset_key(ticker_info, "trade");
        self.read_records(CacheKind::Trade, &key, from, to_exclusive, from)
    }

    pub fn store_trades(
        &self,
        ticker_info: TickerInfo,
        from: UnixMs,
        to_exclusive: UnixMs,
        records: &[Trade],
    ) {
        let key = dataset_key(ticker_info, "trade");
        self.store_records(CacheKind::Trade, &key, from, to_exclusive, records);
    }

    pub fn read_open_interest(
        &self,
        ticker_info: TickerInfo,
        timeframe: Timeframe,
        from: UnixMs,
        to_exclusive: UnixMs,
    ) -> CacheSlice<OpenInterest> {
        let key = open_interest_dataset_key(ticker_info, timeframe);
        self.read_records(CacheKind::OpenInterest, &key, from, to_exclusive, from)
    }

    pub fn store_open_interest(
        &self,
        ticker_info: TickerInfo,
        timeframe: Timeframe,
        from: UnixMs,
        to_exclusive: UnixMs,
        records: &[OpenInterest],
    ) {
        let key = open_interest_dataset_key(ticker_info, timeframe);
        self.store_records(CacheKind::OpenInterest, &key, from, to_exclusive, records);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_bubble_summaries(
        &self,
        ticker_info: TickerInfo,
        timeframe_ms: u64,
        price_step_units: i64,
        max_candidates_per_candle: usize,
        cluster_window_ms: u32,
        cluster_price_ticks: u32,
        from: UnixMs,
        to_exclusive: UnixMs,
    ) -> CacheSlice<BubbleVolumeSummary> {
        let key = bubble_dataset_key(
            ticker_info,
            timeframe_ms,
            price_step_units,
            max_candidates_per_candle,
            cluster_window_ms,
            cluster_price_ticks,
        );
        let record_from = from.saturating_sub(timeframe_ms.saturating_sub(1));
        self.read_records(
            CacheKind::BubbleSummary,
            &key,
            from,
            to_exclusive,
            record_from,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn store_bubble_summaries(
        &self,
        ticker_info: TickerInfo,
        timeframe_ms: u64,
        price_step_units: i64,
        max_candidates_per_candle: usize,
        cluster_window_ms: u32,
        cluster_price_ticks: u32,
        from: UnixMs,
        to_exclusive: UnixMs,
        records: &[BubbleVolumeSummary],
    ) {
        let key = bubble_dataset_key(
            ticker_info,
            timeframe_ms,
            price_step_units,
            max_candidates_per_candle,
            cluster_window_ms,
            cluster_price_ticks,
        );
        self.store_records(CacheKind::BubbleSummary, &key, from, to_exclusive, records);
    }

    /// Removes every persisted market-data record while keeping the database
    /// open and its schema metadata intact.
    pub fn clear_all(&self) -> Result<(), String> {
        let write = self.db.begin_write().map_err(|error| error.to_string())?;

        for definition in [
            COVERAGE_TABLE,
            KLINE_TABLE,
            TRADE_TABLE,
            OI_TABLE,
            BUBBLE_TABLE,
        ] {
            let mut table = write
                .open_table(definition)
                .map_err(|error| error.to_string())?;
            table
                .retain(|_, _| false)
                .map_err(|error| error.to_string())?;
        }

        write.commit().map_err(|error| error.to_string())?;
        log::warn!("CACHE Cleared | path={}", self.path.display());
        Ok(())
    }

    fn read_records<T: CacheRecord>(
        &self,
        kind: CacheKind,
        dataset_key: &str,
        from: UnixMs,
        to_exclusive: UnixMs,
        record_from: UnixMs,
    ) -> CacheSlice<T> {
        if from >= to_exclusive {
            return CacheSlice::miss(from, to_exclusive);
        }

        let coverage_blob = match self.read_value(COVERAGE_TABLE, dataset_key) {
            Ok(Some(blob)) => blob,
            Ok(None) => return CacheSlice::miss(from, to_exclusive),
            Err(error) => {
                log::error!(
                    "CACHE Read Error | dataset={} key={} error={error}",
                    kind.label(),
                    dataset_key
                );
                return CacheSlice::miss(from, to_exclusive);
            }
        };

        let coverage = match decode_checked::<StoredCoverage>(&coverage_blob) {
            Ok(coverage)
                if coverage.schema == CACHE_SCHEMA
                    && coverage.dataset_key == dataset_key
                    && valid_intervals(&coverage.intervals) =>
            {
                coverage
            }
            Ok(_) | Err(_) => {
                self.invalidate(kind, dataset_key, None, "invalid coverage record");
                return CacheSlice::miss(from, to_exclusive);
            }
        };

        let from_u64 = from.as_u64();
        let to_u64 = to_exclusive.as_u64();
        let intersections = coverage_intersections(&coverage.intervals, from_u64, to_u64);
        let gaps_u64 = coverage_gaps(&coverage.intervals, from_u64, to_u64);
        if intersections.is_empty() {
            return CacheSlice::miss(from, to_exclusive);
        }

        let mut required_buckets = BTreeSet::new();
        for (covered_from, covered_to) in &intersections {
            add_bucket_starts(kind, &mut required_buckets, *covered_from, *covered_to);
        }
        let mut optional_buckets = BTreeSet::new();
        if record_from < from {
            add_bucket_starts(
                kind,
                &mut optional_buckets,
                record_from.as_u64(),
                from.as_u64(),
            );
            optional_buckets.retain(|bucket| !required_buckets.contains(bucket));
        }

        let bucket_specs = required_buckets
            .iter()
            .map(|bucket| (*bucket, true))
            .chain(optional_buckets.iter().map(|bucket| (*bucket, false)))
            .collect::<Vec<_>>();
        let bucket_keys = bucket_specs
            .iter()
            .map(|(bucket, _)| bucket_key(dataset_key, *bucket))
            .collect::<Vec<_>>();
        let bucket_blobs = match self.read_values(kind.table(), &bucket_keys) {
            Ok(blobs) => blobs,
            Err(error) => {
                log::error!(
                    "CACHE Read Error | dataset={} key={} error={error}",
                    kind.label(),
                    dataset_key
                );
                return CacheSlice::miss(from, to_exclusive);
            }
        };

        let mut records = Vec::new();
        for (((bucket_start, required), key), blob) in
            bucket_specs.into_iter().zip(bucket_keys).zip(bucket_blobs)
        {
            let blob = match blob {
                Some(blob) => blob,
                None if !required => continue,
                None => {
                    self.invalidate(kind, dataset_key, Some(&key), "missing covered bucket");
                    return CacheSlice::miss(from, to_exclusive);
                }
            };

            let bucket = match decode_checked::<StoredBucket<T>>(&blob) {
                Ok(bucket)
                    if bucket.schema == CACHE_SCHEMA
                        && bucket.dataset_key == dataset_key
                        && bucket.bucket_start == bucket_start
                        && bucket.records.len() <= MAX_RECORDS_PER_BUCKET
                        && bucket.records.iter().all(|record| {
                            record.is_semantically_valid()
                                && bucket_for(kind, record.timestamp().as_u64()) == bucket_start
                        }) =>
                {
                    bucket
                }
                Ok(_) | Err(_) => {
                    self.invalidate(kind, dataset_key, Some(&key), "invalid data bucket");
                    return CacheSlice::miss(from, to_exclusive);
                }
            };
            records.extend(bucket.records);
        }

        records.retain(|record| {
            let time = record.timestamp();
            time >= record_from && time < to_exclusive
        });
        records = T::normalize(records);
        let gaps = gaps_u64
            .into_iter()
            .map(|(gap_from, gap_to)| (UnixMs::new(gap_from), UnixMs::new(gap_to)))
            .collect::<Vec<_>>();

        log::info!(
            "CACHE {} | dataset={} key={} records={} gaps={}",
            if gaps.is_empty() { "Hit" } else { "Partial" },
            kind.label(),
            dataset_key,
            records.len(),
            gaps.len()
        );

        CacheSlice { records, gaps }
    }

    fn store_records<T: CacheRecord>(
        &self,
        kind: CacheKind,
        dataset_key: &str,
        from: UnixMs,
        to_exclusive: UnixMs,
        records: &[T],
    ) {
        if from >= to_exclusive {
            return;
        }
        // An empty response is not durable evidence that a range is genuinely
        // empty: it can mean a future request, a reconnect race, exchange lag or
        // a partial API failure. Keep negative coverage short-lived in memory
        // instead of poisoning the persistent cache across restarts.
        if records.is_empty() {
            log::debug!(
                "CACHE Write Skipped | dataset={} key={} range={}..{} reason=empty_unverified",
                kind.label(),
                dataset_key,
                from.as_u64(),
                to_exclusive.as_u64()
            );
            return;
        }
        if records.iter().any(|record| !record.is_semantically_valid()) {
            log::warn!(
                "CACHE Write Rejected | dataset={} key={} reason=invalid_source_record",
                kind.label(),
                dataset_key
            );
            return;
        }

        if let Err(error) = self.store_records_inner(
            kind,
            dataset_key,
            from.as_u64(),
            to_exclusive.as_u64(),
            records,
        ) {
            log::error!(
                "CACHE Write Error | dataset={} key={} records={} error={error}",
                kind.label(),
                dataset_key,
                records.len()
            );
        }
    }

    fn store_records_inner<T: CacheRecord>(
        &self,
        kind: CacheKind,
        dataset_key: &str,
        from: u64,
        to_exclusive: u64,
        records: &[T],
    ) -> Result<(), String> {
        let mut records_by_bucket: BTreeMap<u64, Vec<T>> = BTreeMap::new();
        add_empty_buckets(kind, &mut records_by_bucket, from, to_exclusive);
        for record in records {
            records_by_bucket
                .entry(bucket_for(kind, record.timestamp().as_u64()))
                .or_default()
                .push(record.clone());
        }

        let write = self.db.begin_write().map_err(|error| error.to_string())?;
        {
            let mut table = write
                .open_table(kind.table())
                .map_err(|error| error.to_string())?;
            for (bucket_start, new_records) in records_by_bucket {
                let key = bucket_key(dataset_key, bucket_start);
                let existing_blob = {
                    table
                        .get(key.as_str())
                        .map_err(|error| error.to_string())?
                        .map(|value| value.value().to_vec())
                };
                let mut merged = existing_blob
                    .and_then(|blob| decode_checked::<StoredBucket<T>>(&blob).ok())
                    .filter(|bucket| {
                        bucket.schema == CACHE_SCHEMA
                            && bucket.dataset_key == dataset_key
                            && bucket.bucket_start == bucket_start
                            && bucket.records.len() <= MAX_RECORDS_PER_BUCKET
                            && bucket.records.iter().all(|record| {
                                record.is_semantically_valid()
                                    && bucket_for(kind, record.timestamp().as_u64()) == bucket_start
                            })
                    })
                    .map_or_else(Vec::new, |bucket| bucket.records);
                merged.extend(new_records);
                merged = T::normalize(merged);
                if merged.len() > MAX_RECORDS_PER_BUCKET {
                    return Err(format!(
                        "bucket {bucket_start} exceeds {MAX_RECORDS_PER_BUCKET} records"
                    ));
                }
                let encoded = encode_checked(&StoredBucket {
                    schema: CACHE_SCHEMA,
                    dataset_key: dataset_key.to_string(),
                    bucket_start,
                    records: merged,
                })?;
                table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(|error| error.to_string())?;
            }
        }

        {
            let mut coverage_table = write
                .open_table(COVERAGE_TABLE)
                .map_err(|error| error.to_string())?;
            let existing_blob = {
                coverage_table
                    .get(dataset_key)
                    .map_err(|error| error.to_string())?
                    .map(|value| value.value().to_vec())
            };
            let mut intervals = existing_blob
                .and_then(|blob| decode_checked::<StoredCoverage>(&blob).ok())
                .filter(|coverage| {
                    coverage.schema == CACHE_SCHEMA
                        && coverage.dataset_key == dataset_key
                        && valid_intervals(&coverage.intervals)
                })
                .map_or_else(Vec::new, |coverage| coverage.intervals);
            intervals.push((from, to_exclusive));
            intervals = merge_intervals(intervals);
            let encoded = encode_checked(&StoredCoverage {
                schema: CACHE_SCHEMA,
                dataset_key: dataset_key.to_string(),
                intervals,
            })?;
            coverage_table
                .insert(dataset_key, encoded.as_slice())
                .map_err(|error| error.to_string())?;
        }

        write.commit().map_err(|error| error.to_string())?;
        log::debug!(
            "CACHE Stored | dataset={} key={} range={}..{} records={}",
            kind.label(),
            dataset_key,
            from,
            to_exclusive,
            records.len()
        );
        Ok(())
    }

    fn read_value(
        &self,
        definition: TableDefinition<'static, &'static str, &'static [u8]>,
        key: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        let read = self.db.begin_read().map_err(|error| error.to_string())?;
        let table = read
            .open_table(definition)
            .map_err(|error| error.to_string())?;
        let value = table.get(key).map_err(|error| error.to_string())?;
        Ok(value.map(|guard| guard.value().to_vec()))
    }

    fn read_values(
        &self,
        definition: TableDefinition<'static, &'static str, &'static [u8]>,
        keys: &[String],
    ) -> Result<Vec<Option<Vec<u8>>>, String> {
        let read = self.db.begin_read().map_err(|error| error.to_string())?;
        let table = read
            .open_table(definition)
            .map_err(|error| error.to_string())?;
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            let value = table.get(key.as_str()).map_err(|error| error.to_string())?;
            values.push(value.map(|guard| guard.value().to_vec()));
        }
        Ok(values)
    }

    fn invalidate(
        &self,
        kind: CacheKind,
        dataset_key: &str,
        bucket_key: Option<&str>,
        reason: &str,
    ) {
        let result = (|| -> Result<(), String> {
            let write = self.db.begin_write().map_err(|error| error.to_string())?;
            if let Some(bucket_key) = bucket_key {
                let mut table = write
                    .open_table(kind.table())
                    .map_err(|error| error.to_string())?;
                let removed = table
                    .remove(bucket_key)
                    .map_err(|error| error.to_string())?;
                drop(removed);
            }
            {
                let mut coverage = write
                    .open_table(COVERAGE_TABLE)
                    .map_err(|error| error.to_string())?;
                let removed = coverage
                    .remove(dataset_key)
                    .map_err(|error| error.to_string())?;
                drop(removed);
            }
            write.commit().map_err(|error| error.to_string())
        })();

        match result {
            Ok(()) => log::warn!(
                "CACHE Invalidated | dataset={} key={} reason={reason}",
                kind.label(),
                dataset_key
            ),
            Err(error) => log::error!(
                "CACHE Invalidation Error | dataset={} key={} reason={reason} error={error}",
                kind.label(),
                dataset_key
            ),
        }
    }
}

fn open_initialized_database(path: &Path) -> Result<Database, String> {
    let db = Database::create(path).map_err(|error| error.to_string())?;
    let write = db.begin_write().map_err(|error| error.to_string())?;
    {
        let mut meta = write
            .open_table(META_TABLE)
            .map_err(|error| error.to_string())?;
        let existing_schema = {
            meta.get(SCHEMA_KEY)
                .map_err(|error| error.to_string())?
                .map(|value| value.value())
        };
        if let Some(existing_schema) = existing_schema {
            if existing_schema != CACHE_SCHEMA {
                return Err(format!(
                    "unsupported cache schema {existing_schema}, expected {CACHE_SCHEMA}"
                ));
            }
        } else {
            meta.insert(SCHEMA_KEY, &CACHE_SCHEMA)
                .map_err(|error| error.to_string())?;
        }
    }
    // Opening all tables here validates their persisted definitions before the
    // cache is considered usable.
    {
        let _ = write
            .open_table(COVERAGE_TABLE)
            .map_err(|error| error.to_string())?;
        let _ = write
            .open_table(KLINE_TABLE)
            .map_err(|error| error.to_string())?;
        let _ = write
            .open_table(TRADE_TABLE)
            .map_err(|error| error.to_string())?;
        let _ = write
            .open_table(OI_TABLE)
            .map_err(|error| error.to_string())?;
        let _ = write
            .open_table(BUBBLE_TABLE)
            .map_err(|error| error.to_string())?;
    }
    write.commit().map_err(|error| error.to_string())?;
    Ok(db)
}

fn quarantine_database(path: &Path, cause: &str) -> Result<(), String> {
    let timestamp = UnixMs::now().as_u64();
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("market.redb");
    let quarantine = path.with_file_name(format!("{file_name}.corrupt-{timestamp}"));
    match std::fs::rename(path, &quarantine) {
        Ok(()) => {
            log::warn!(
                "CACHE Database Quarantined | path={} quarantine={} cause={cause}",
                path.display(),
                quarantine.display()
            );
            Ok(())
        }
        Err(rename_error) => {
            std::fs::remove_file(path).map_err(|remove_error| {
                format!(
                    "cannot quarantine invalid cache ({rename_error}) or remove it ({remove_error})"
                )
            })?;
            log::warn!(
                "CACHE Database Removed | path={} cause={cause} rename_error={rename_error}",
                path.display()
            );
            Ok(())
        }
    }
}

fn dataset_key(ticker_info: TickerInfo, suffix: &str) -> String {
    format!(
        "v{CACHE_SCHEMA}|{}|{suffix}",
        ticker_info.ticker.symbol_and_exchange_string()
    )
}

fn open_interest_dataset_key(ticker_info: TickerInfo, timeframe: Timeframe) -> String {
    dataset_key(
        ticker_info,
        &format!("open_interest|timeframe_ms={}", timeframe.to_milliseconds()),
    )
}

fn bubble_dataset_key(
    ticker_info: TickerInfo,
    timeframe_ms: u64,
    price_step_units: i64,
    max_candidates_per_candle: usize,
    cluster_window_ms: u32,
    cluster_price_ticks: u32,
) -> String {
    dataset_key(
        ticker_info,
        &format!(
            "bubble_summary|algorithm=v2|timeframe_ms={timeframe_ms}|price_step_units={price_step_units}|max_candidates={max_candidates_per_candle}|cluster_ms={cluster_window_ms}|cluster_ticks={cluster_price_ticks}"
        ),
    )
}

fn bucket_for(kind: CacheKind, timestamp: u64) -> u64 {
    let bucket_ms = kind.bucket_ms();
    (timestamp / bucket_ms) * bucket_ms
}

fn bucket_key(dataset_key: &str, bucket_start: u64) -> String {
    format!("{dataset_key}|bucket={bucket_start:020}")
}

fn add_bucket_starts(kind: CacheKind, target: &mut BTreeSet<u64>, from: u64, to_exclusive: u64) {
    if from >= to_exclusive {
        return;
    }
    let bucket_ms = kind.bucket_ms();
    let mut bucket = bucket_for(kind, from);
    let last_bucket = bucket_for(kind, to_exclusive.saturating_sub(1));
    loop {
        target.insert(bucket);
        if bucket >= last_bucket {
            break;
        }
        let next = bucket.saturating_add(bucket_ms);
        if next <= bucket {
            break;
        }
        bucket = next;
    }
}

fn add_empty_buckets<T>(
    kind: CacheKind,
    target: &mut BTreeMap<u64, Vec<T>>,
    from: u64,
    to_exclusive: u64,
) {
    let mut starts = BTreeSet::new();
    add_bucket_starts(kind, &mut starts, from, to_exclusive);
    for start in starts {
        target.entry(start).or_default();
    }
}

fn encode_checked<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    let payload = bincode::serde::encode_to_vec(value, bincode::config::standard())
        .map_err(|error| error.to_string())?;
    if payload.len() > MAX_BLOB_BYTES {
        return Err(format!("cache blob is too large: {} bytes", payload.len()));
    }
    let payload_len = u64::try_from(payload.len()).map_err(|error| error.to_string())?;
    let mut encoded = Vec::with_capacity(HEADER_LEN + payload.len());
    encoded.extend_from_slice(CACHE_MAGIC);
    encoded.extend_from_slice(&CACHE_SCHEMA.to_le_bytes());
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encoded.extend_from_slice(&crc32(&payload).to_le_bytes());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn decode_checked<T: DeserializeOwned>(encoded: &[u8]) -> Result<T, String> {
    if encoded.len() < HEADER_LEN || &encoded[..8] != CACHE_MAGIC {
        return Err("invalid cache magic/header".to_string());
    }
    let schema = u64::from_le_bytes(
        encoded[8..16]
            .try_into()
            .map_err(|_| "invalid schema header")?,
    );
    if schema != CACHE_SCHEMA {
        return Err(format!("unsupported cache blob schema {schema}"));
    }
    let payload_len = u64::from_le_bytes(
        encoded[16..24]
            .try_into()
            .map_err(|_| "invalid payload length header")?,
    );
    let payload_len = usize::try_from(payload_len).map_err(|error| error.to_string())?;
    if payload_len > MAX_BLOB_BYTES || encoded.len() != HEADER_LEN + payload_len {
        return Err("invalid cache payload length".to_string());
    }
    let expected_crc = u32::from_le_bytes(
        encoded[24..28]
            .try_into()
            .map_err(|_| "invalid checksum header")?,
    );
    let payload = &encoded[HEADER_LEN..];
    if crc32(payload) != expected_crc {
        return Err("cache checksum mismatch".to_string());
    }
    let (value, consumed) =
        bincode::serde::decode_from_slice::<T, _>(payload, bincode::config::standard())
            .map_err(|error| error.to_string())?;
    if consumed != payload.len() {
        return Err("trailing bytes in cache payload".to_string());
    }
    Ok(value)
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

fn valid_intervals(intervals: &[(u64, u64)]) -> bool {
    intervals.iter().all(|(from, to)| from < to)
        && intervals.windows(2).all(|window| {
            let (_, previous_to) = window[0];
            let (next_from, _) = window[1];
            previous_to < next_from
        })
}

fn merge_intervals(mut intervals: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    intervals.retain(|(from, to)| from < to);
    intervals.sort_unstable_by_key(|interval| interval.0);
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(intervals.len());
    for (from, to) in intervals {
        if let Some((_, previous_to)) = merged.last_mut()
            && from <= *previous_to
        {
            *previous_to = (*previous_to).max(to);
        } else {
            merged.push((from, to));
        }
    }
    merged
}

fn coverage_intersections(
    intervals: &[(u64, u64)],
    from: u64,
    to_exclusive: u64,
) -> Vec<(u64, u64)> {
    intervals
        .iter()
        .filter_map(|(covered_from, covered_to)| {
            let intersection_from = from.max(*covered_from);
            let intersection_to = to_exclusive.min(*covered_to);
            (intersection_from < intersection_to).then_some((intersection_from, intersection_to))
        })
        .collect()
}

fn coverage_gaps(intervals: &[(u64, u64)], from: u64, to_exclusive: u64) -> Vec<(u64, u64)> {
    if from >= to_exclusive {
        return Vec::new();
    }
    let mut cursor = from;
    let mut gaps = Vec::new();
    for (covered_from, covered_to) in coverage_intersections(intervals, from, to_exclusive) {
        if cursor < covered_from {
            gaps.push((cursor, covered_from));
        }
        cursor = cursor.max(covered_to);
    }
    if cursor < to_exclusive {
        gaps.push((cursor, to_exclusive));
    }
    gaps
}

/// Merge V2 smart-cluster summaries by stable cluster id. Overlapping fetches keep the most
/// complete cluster and are never summed, preventing duplicate volume.
pub fn merge_bubble_summaries(
    summaries: impl IntoIterator<Item = BubbleVolumeSummary>,
    max_candidates_per_candle: usize,
) -> Vec<BubbleVolumeSummary> {
    let mut grouped: BTreeMap<UnixMs, BTreeMap<u64, BubbleCandidate>> = BTreeMap::new();
    for summary in summaries {
        if summary.algorithm_version != data::chart::kline::BUBBLE_SUMMARY_ALGORITHM_VERSION {
            continue;
        }
        for candidate in summary.candidates {
            grouped
                .entry(summary.candle_time)
                .or_default()
                .entry(candidate.id)
                .and_modify(|existing| {
                    if candidate.trade_count > existing.trade_count
                        || (candidate.trade_count == existing.trade_count
                            && candidate.last_time > existing.last_time)
                    {
                        *existing = candidate;
                    }
                })
                .or_insert(candidate);
        }
    }

    grouped
        .into_iter()
        .map(|(candle_time, candidates)| {
            let mut candidates = candidates.into_values().collect::<Vec<_>>();
            candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.total_qty));
            candidates.truncate(max_candidates_per_candle);
            BubbleVolumeSummary::new(candle_time, candidates)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bubble_candidate(
        qty: f64,
        first_time: u64,
        last_time: u64,
        trade_count: usize,
    ) -> BubbleCandidate {
        let qty = exchange::unit::Qty::from_f64(qty);
        BubbleCandidate {
            id: first_time,
            candle_time: UnixMs::new(60_000),
            first_time: UnixMs::new(first_time),
            last_time: UnixMs::new(last_time),
            weighted_time: UnixMs::new((first_time + last_time) / 2),
            vwap_price: exchange::unit::Price::from_f64(100.0),
            total_qty: qty,
            buy_qty: qty,
            sell_qty: exchange::unit::Qty::ZERO,
            delta_qty: qty,
            trade_count,
            largest_trade_qty: qty,
            percentile_rank: 0.0,
            importance_score: qty.to_f32_lossy(),
        }
    }

    fn ticker_info() -> TickerInfo {
        TickerInfo::new(
            exchange::Ticker::new("BTCUSDT", exchange::adapter::Exchange::BinanceLinear),
            0.1,
            0.001,
            None,
        )
    }

    #[test]
    fn open_interest_cache_is_separated_by_source_timeframe() {
        assert_ne!(
            open_interest_dataset_key(ticker_info(), Timeframe::M1),
            open_interest_dataset_key(ticker_info(), Timeframe::M5)
        );
    }

    #[test]
    fn merges_coverage_and_finds_only_real_gaps() {
        let coverage = merge_intervals(vec![(100, 200), (150, 250), (300, 400)]);
        assert_eq!(coverage, vec![(100, 250), (300, 400)]);
        assert_eq!(
            coverage_gaps(&coverage, 50, 450),
            vec![(50, 100), (250, 300), (400, 450)]
        );
    }

    #[test]
    fn checked_blob_detects_corruption() {
        let coverage = StoredCoverage {
            schema: CACHE_SCHEMA,
            dataset_key: "test".to_string(),
            intervals: vec![(1, 2)],
        };
        let mut encoded = encode_checked(&coverage).unwrap();
        let last = encoded.len() - 1;
        encoded[last] ^= 0xff;
        assert!(decode_checked::<StoredCoverage>(&encoded).is_err());
    }

    #[test]
    fn overlapping_bubble_summaries_are_not_added_twice() {
        let candle_time = UnixMs::new(60_000);
        let duplicate = bubble_candidate(10.0, 61_000, 62_000, 4);
        let merged = merge_bubble_summaries(
            [
                BubbleVolumeSummary::new(candle_time, vec![duplicate]),
                BubbleVolumeSummary::new(candle_time, vec![duplicate]),
            ],
            3,
        );

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].candidates.len(), 1);
        assert_eq!(merged[0].candidates[0].total_qty.to_f64(), 10.0);
        assert_eq!(merged[0].candidates[0].trade_count, 4);
    }

    #[test]
    fn different_cluster_ids_are_preserved_without_summing() {
        let candle_time = UnixMs::new(60_000);
        let merged = merge_bubble_summaries(
            [
                BubbleVolumeSummary::new(
                    candle_time,
                    vec![bubble_candidate(10.0, 61_000, 62_000, 4)],
                ),
                BubbleVolumeSummary::new(
                    candle_time,
                    vec![bubble_candidate(5.0, 62_001, 63_000, 2)],
                ),
            ],
            3,
        );

        assert_eq!(merged[0].candidates.len(), 2);
        assert_eq!(
            merged[0]
                .candidates
                .iter()
                .map(|c| c.total_qty.to_f64())
                .sum::<f64>(),
            15.0
        );
    }

    #[test]
    fn clear_all_removes_data_and_coverage_but_keeps_database_usable() {
        let path = std::env::temp_dir().join(format!(
            "flowsurface-cache-clear-{}-{}.redb",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let db = open_initialized_database(&path).unwrap();
        let cache = MarketDataCache {
            db,
            path: path.clone(),
        };

        let write = cache.db.begin_write().unwrap();
        {
            let mut data = write.open_table(KLINE_TABLE).unwrap();
            data.insert("test-bucket", b"cached".as_slice()).unwrap();
            let mut coverage = write.open_table(COVERAGE_TABLE).unwrap();
            coverage
                .insert("test-dataset", b"covered".as_slice())
                .unwrap();
        }
        write.commit().unwrap();

        cache.clear_all().unwrap();
        assert!(
            cache
                .read_value(KLINE_TABLE, "test-bucket")
                .unwrap()
                .is_none()
        );
        assert!(
            cache
                .read_value(COVERAGE_TABLE, "test-dataset")
                .unwrap()
                .is_none()
        );

        drop(cache);
        std::fs::remove_file(path).unwrap();
    }
}
