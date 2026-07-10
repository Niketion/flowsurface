# Market data layer

The market-data pipeline is split into five responsibilities:

1. **Consumers** (charts and indicators) declare a `DataRequirement`.
2. **MarketDataCoordinator** compares the requirement with covered and in-flight ranges.
3. **Persistent cache** in `connector::persistent_cache` resolves locally covered ranges.
4. **Fetch executors** in `connector::fetcher` call the exchange adapter only for remaining gaps.
5. **Consumers** derive presentation data from the shared raw dataset.

An indicator must never add a fetch variant named after itself. Session/daily/hourly volume
profiles and trade-accurate VWAP request `DataSet::Trades`; a candle-based VWAP requests
`DataSet::Klines`. This allows one REST response and one live stream to feed multiple studies.

## Range semantics

All intervals are half-open: `[from, to)`. A successful empty response is recorded as
`CoverageKind::Empty`, because retrying an illiquid tail forever is incorrect. Coverage is
merged per complete `RequestKey` (exchange, market, ticker, stream and dataset), never merely
per symbol.

The coordinator subtracts both persisted coverage and current in-flight work from a new
requirement. Only remaining gaps become `PlannedFetch` values. Failures receive bounded
exponential retry cooldown; reset increments the generation so late results cannot contaminate
a new chart configuration.

## Adding an indicator

- Express the raw data dependency as one or more `DataRequirement` values.
- Feed fetched and live records into a separate derivation engine.
- Store only derived state in the indicator; do not own transport tasks or retry timers.
- For anchored/session studies, align the requested range before submitting it. Session
  calendars belong in a calendar/range-policy component, not in an exchange adapter.

The existing REST adapter functions remain the transport boundary during migration. The next
step is to route the current `FetchRange` calls through this coordinator, then move bubble
summarisation out of the fetcher and into a derived-data engine consuming shared trades.

## Persistent redb cache

Every bounded historical fetch is cached under a complete dataset identity: exchange/market,
internal ticker, dataset, timeframe and any derivation settings that affect the result. Klines,
raw trades, open interest and Bubble summaries all use the same read-before-network policy.
Coverage is persisted independently from payloads, so a successful empty interval is reusable
and a later request downloads only uncovered gaps. Trade buckets use one-minute partitions to
limit write amplification; lower-density datasets use hourly partitions.

Each redb value contains a format magic, schema version, payload length and CRC32 checksum.
Records are also checked semantically (timestamps, OHLC relationships, finite/non-negative
values and Bubble quantity invariants) on read. A missing, corrupt or invalid bucket is deleted
and its dataset coverage is invalidated before the range falls back to REST. A database that
cannot be opened, has incompatible table definitions or uses an unsupported schema is renamed
with a `.corrupt-<timestamp>` suffix and recreated. Cache failures never prevent network fetches.

## Session volume profile consumer

The candlestick SVP is the first consumer following this contract. It uses the same raw `Trades`
stream and historical coverage as footprint/bubbles and derives price rows inside the chart.
Sessions are UTC-aligned (weekly sessions open Monday 00:00 UTC) and can be 30 minutes, hourly,
4-hour, daily, or weekly. Historical acquisition advances in one-hour chunks.

POC and the value-area expansion are calculated from total traded volume even when the visual
mode is Delta; this preserves the standard market-profile definition. Delta changes bar length
and buy/sell colour only. The overlay also exposes VAH, VAL, session VWAP, and session high/low.

## Unified indicator model

Kline indicators now declare one of two render placements:

- panel indicators (Volume, Bar Analysis, CVD, Open Interest) receive their own lower split;
- overlay indicators (Volume Bubbles, Session Volume Profile, VWAP) render on the main price canvas.

Both placements use the same `KlineIndicator` registry, market availability, toggle flow,
ordering and saved `indicators` list. Overlay indicators are excluded from lower-panel split
calculation. An enabled overlay that requires trades automatically reconciles the pane's Trades
stream at runtime, including when it is toggled after application startup.

Overlay configuration is exposed only inside the Indicators modal. General candlestick settings
no longer present independent SVP or Bubbles sections. The VWAP implementation is trade-weighted,
resets at its configured anchor, and optionally draws weighted standard-deviation bands.

### CVD rendering

The CVD panel supports `Candlesticks` (default) and `Line` rendering from its indicator settings.
Each CVD candle opens at the previous cumulative close and closes at `open + buy - sell`. High and
low use the bucket's directional buy/sell excursion, preserving a readable volume-delta envelope
even when the exchange feed does not retain the exact intrabar trade ordering. Candle width and
wicks are configurable; line width remains configurable when Line mode is selected.
