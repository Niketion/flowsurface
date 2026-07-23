use data::chart::gex::{
    Config, GexExpiryFilter, GexFreshness, GexSignModel, GexSnapshot, calculate_gex_at,
};
use exchange::{
    UnixMs,
    options::{
        OptionInstrument, OptionsProvider, OptionsUnderlying, RawOptionChainSnapshot,
        deribit::{DeribitError, DeribitOptionsClient},
    },
};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

pub const INSTRUMENT_TTL_MS: u64 = 10 * 60 * 1_000;
pub const MARKET_SNAPSHOT_TTL_MS: u64 = 15 * 1_000;
pub const FRESH_THRESHOLD_MS: u64 = 45 * 1_000;
pub const EXPIRED_THRESHOLD_MS: u64 = 5 * 60 * 1_000;
const FAILURE_BACKOFF_BASE_MS: u64 = 5_000;
const FAILURE_BACKOFF_MAX_MS: u64 = 2 * 60 * 1_000;
const CACHE_SCHEMA: u32 = 1;
const CACHE_FILENAME: &str = "gex_option_chain_v1.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct OptionsChainKey {
    pub provider: OptionsProvider,
    pub underlying: OptionsUnderlying,
}

impl OptionsChainKey {
    pub const fn deribit(underlying: OptionsUnderlying) -> Self {
        Self {
            provider: OptionsProvider::Deribit,
            underlying,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GexFetchKind {
    Instruments(OptionsChainKey),
    Snapshot(OptionsChainKey),
}

impl GexFetchKind {
    pub const fn key(self) -> OptionsChainKey {
        match self {
            Self::Instruments(key) | Self::Snapshot(key) => key,
        }
    }
}

#[derive(Debug, Clone)]
pub enum GexFetchResult {
    Instruments {
        key: OptionsChainKey,
        result: Result<Vec<OptionInstrument>, Arc<str>>,
    },
    Snapshot {
        key: OptionsChainKey,
        result: Result<RawOptionChainSnapshot, Arc<str>>,
    },
}

#[derive(Debug, Clone)]
struct CachedInstruments {
    values: Arc<[OptionInstrument]>,
    refreshed_at: UnixMs,
}

#[derive(Debug, Clone)]
struct CachedRawSnapshot {
    value: Arc<RawOptionChainSnapshot>,
    received_at: UnixMs,
    revision: u64,
    loaded_from_disk: bool,
}

#[derive(Debug, Clone)]
struct CachedGexSnapshot {
    value: Arc<GexSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DerivedGexKey {
    chain: OptionsChainKey,
    model: GexSignModel,
    expiry: GexExpiryFilter,
    min_oi_bits: u64,
    min_gex_bits: u64,
    revision: u64,
}

#[derive(Debug, Clone)]
struct FailureState {
    attempts: u32,
    retry_after: UnixMs,
    last_error: Arc<str>,
}

#[derive(Debug)]
pub struct GexDataCoordinator {
    instruments: FxHashMap<OptionsChainKey, CachedInstruments>,
    raw_snapshots: FxHashMap<OptionsChainKey, CachedRawSnapshot>,
    derived_snapshots: FxHashMap<DerivedGexKey, CachedGexSnapshot>,
    in_flight: FxHashSet<GexFetchKind>,
    failures: FxHashMap<OptionsChainKey, FailureState>,
    subscribers: FxHashMap<OptionsChainKey, usize>,
    force_refresh: FxHashSet<OptionsChainKey>,
    last_freshness: FxHashMap<OptionsChainKey, GexFreshness>,
    next_revision: u64,
    cache_path: PathBuf,
}

impl Default for GexDataCoordinator {
    fn default() -> Self {
        Self::new(data::data_path(Some(CACHE_FILENAME)))
    }
}

impl GexDataCoordinator {
    pub fn new(cache_path: PathBuf) -> Self {
        let mut coordinator = Self {
            instruments: FxHashMap::default(),
            raw_snapshots: FxHashMap::default(),
            derived_snapshots: FxHashMap::default(),
            in_flight: FxHashSet::default(),
            failures: FxHashMap::default(),
            subscribers: FxHashMap::default(),
            force_refresh: FxHashSet::default(),
            last_freshness: FxHashMap::default(),
            next_revision: 1,
            cache_path,
        };
        coordinator.load_persistent();
        coordinator
    }

    pub fn set_consumers<I>(&mut self, consumers: I)
    where
        I: IntoIterator<Item = OptionsUnderlying>,
    {
        let mut next = FxHashMap::default();
        for underlying in consumers {
            *next
                .entry(OptionsChainKey::deribit(underlying))
                .or_insert(0usize) += 1;
        }
        for (&key, &count) in &next {
            if count > 0 && self.subscribers.get(&key).copied().unwrap_or(0) == 0 {
                self.force_refresh.insert(key);
            }
        }
        self.subscribers = next;
    }

    pub fn subscriber_count(&self, underlying: OptionsUnderlying) -> usize {
        self.subscribers
            .get(&OptionsChainKey::deribit(underlying))
            .copied()
            .unwrap_or(0)
    }

    pub fn reconnect(&mut self) {
        self.force_refresh.extend(
            self.subscribers
                .iter()
                .filter_map(|(&key, &count)| (count > 0).then_some(key)),
        );
    }

    pub fn due_fetches(&mut self, now: UnixMs, online: bool) -> Vec<GexFetchKind> {
        if !online {
            return Vec::new();
        }
        let keys = self
            .subscribers
            .iter()
            .filter_map(|(&key, &count)| (count > 0).then_some(key))
            .collect::<Vec<_>>();
        let mut due = Vec::new();
        for key in keys {
            if self
                .failures
                .get(&key)
                .is_some_and(|failure| now < failure.retry_after)
            {
                continue;
            }
            let instruments_due = self
                .instruments
                .get(&key)
                .is_none_or(|cached| now.saturating_diff(cached.refreshed_at) >= INSTRUMENT_TTL_MS);
            let force = self.force_refresh.contains(&key);
            let raw_due = self.raw_snapshots.get(&key).is_none_or(|cached| {
                now.saturating_diff(cached.received_at) >= MARKET_SNAPSHOT_TTL_MS
            });

            let kind = if instruments_due {
                Some(GexFetchKind::Instruments(key))
            } else if force || raw_due {
                Some(GexFetchKind::Snapshot(key))
            } else {
                None
            };
            if let Some(kind) = kind
                && self.in_flight.insert(kind)
            {
                due.push(kind);
            }
        }
        due
    }

    pub fn instruments_for(&self, key: OptionsChainKey) -> Arc<[OptionInstrument]> {
        self.instruments
            .get(&key)
            .map(|cached| cached.values.clone())
            .unwrap_or_default()
    }

    pub fn complete(&mut self, completion: GexFetchResult, now: UnixMs) {
        match completion {
            GexFetchResult::Instruments { key, result } => {
                self.in_flight.remove(&GexFetchKind::Instruments(key));
                match result {
                    Ok(values) if !values.is_empty() => {
                        self.instruments.insert(
                            key,
                            CachedInstruments {
                                values: values.into(),
                                refreshed_at: now,
                            },
                        );
                        self.failures.remove(&key);
                        self.force_refresh.insert(key);
                    }
                    Ok(_) => self.record_failure(key, "empty instrument metadata".into(), now),
                    Err(error) => self.record_failure(key, error, now),
                }
            }
            GexFetchResult::Snapshot { key, result } => {
                self.in_flight.remove(&GexFetchKind::Snapshot(key));
                self.force_refresh.remove(&key);
                match result {
                    Ok(value) if !value.contracts.is_empty() => {
                        let revision = self.next_revision;
                        self.next_revision = self.next_revision.saturating_add(1);
                        self.raw_snapshots.insert(
                            key,
                            CachedRawSnapshot {
                                value: Arc::new(value),
                                received_at: now,
                                revision,
                                loaded_from_disk: false,
                            },
                        );
                        self.derived_snapshots
                            .retain(|derived, _| derived.chain != key);
                        self.failures.remove(&key);
                        if let Err(error) = self.save_persistent() {
                            log::warn!("GEX cache write failed: {error}");
                        }
                    }
                    Ok(_) => self.record_failure(key, "empty option chain".into(), now),
                    Err(error) => self.record_failure(key, error, now),
                }
            }
        }
    }

    pub fn derived(
        &mut self,
        underlying: OptionsUnderlying,
        config: &Config,
        now: UnixMs,
    ) -> Option<Arc<GexSnapshot>> {
        let chain = OptionsChainKey::deribit(underlying);
        let raw = self.raw_snapshots.get(&chain)?;
        let key = DerivedGexKey {
            chain,
            model: config.sign_model,
            expiry: config.expiry_filter,
            min_oi_bits: config.min_open_interest.to_bits(),
            min_gex_bits: config.min_absolute_gex.to_bits(),
            revision: raw.revision,
        };
        if let Some(cached) = self.derived_snapshots.get(&key) {
            return Some(cached.value.clone());
        }
        let value = Arc::new(calculate_gex_at(&raw.value, config, now));
        self.derived_snapshots.insert(
            key,
            CachedGexSnapshot {
                value: value.clone(),
            },
        );
        Some(value)
    }

    pub fn freshness(&mut self, underlying: OptionsUnderlying, now: UnixMs) -> GexFreshness {
        let key = OptionsChainKey::deribit(underlying);
        let freshness = if self.failures.contains_key(&key) {
            GexFreshness::Error
        } else if let Some(raw) = self.raw_snapshots.get(&key) {
            if raw.loaded_from_disk {
                GexFreshness::Stale
            } else {
                let age = now.saturating_diff(raw.received_at);
                if age <= FRESH_THRESHOLD_MS {
                    GexFreshness::Fresh
                } else if age <= EXPIRED_THRESHOLD_MS {
                    GexFreshness::Stale
                } else {
                    GexFreshness::Expired
                }
            }
        } else {
            GexFreshness::Loading
        };
        let previous = self.last_freshness.insert(key, freshness);
        if freshness == GexFreshness::Stale && previous != Some(GexFreshness::Stale) {
            log::warn!("GEX SnapshotStale underlying={underlying}");
        }
        freshness
    }

    pub fn last_error(&self, underlying: OptionsUnderlying) -> Option<&str> {
        self.failures
            .get(&OptionsChainKey::deribit(underlying))
            .map(|failure| failure.last_error.as_ref())
    }

    pub fn invalidate_persistent(&mut self) -> std::io::Result<()> {
        self.raw_snapshots.clear();
        self.derived_snapshots.clear();
        if self.cache_path.exists() {
            std::fs::remove_file(&self.cache_path)?;
        }
        Ok(())
    }

    fn record_failure(&mut self, key: OptionsChainKey, error: Arc<str>, now: UnixMs) {
        let attempts = self
            .failures
            .get(&key)
            .map_or(1, |failure| failure.attempts.saturating_add(1));
        let exponent = attempts.saturating_sub(1).min(8);
        let backoff = FAILURE_BACKOFF_BASE_MS
            .saturating_mul(1u64 << exponent)
            .min(FAILURE_BACKOFF_MAX_MS);
        self.failures.insert(
            key,
            FailureState {
                attempts,
                retry_after: now.saturating_add(backoff),
                last_error: error,
            },
        );
    }

    fn load_persistent(&mut self) {
        let Ok(bytes) = std::fs::read(&self.cache_path) else {
            return;
        };
        let Ok(stored) = serde_json::from_slice::<StoredCache>(&bytes) else {
            log::warn!("GEX persistent snapshot is corrupt; ignoring it");
            return;
        };
        if stored.schema != CACHE_SCHEMA {
            return;
        }
        for snapshot in stored.snapshots {
            if snapshot.contracts.is_empty() {
                continue;
            }
            let key = OptionsChainKey {
                provider: snapshot.provider,
                underlying: snapshot.underlying,
            };
            let revision = self.next_revision;
            self.next_revision = self.next_revision.saturating_add(1);
            self.raw_snapshots.insert(
                key,
                CachedRawSnapshot {
                    received_at: snapshot.observed_at,
                    value: Arc::new(snapshot),
                    revision,
                    loaded_from_disk: true,
                },
            );
        }
    }

    fn save_persistent(&self) -> std::io::Result<()> {
        let stored = StoredCache {
            schema: CACHE_SCHEMA,
            snapshots: self
                .raw_snapshots
                .values()
                .map(|cached| (*cached.value).clone())
                .collect(),
        };
        let bytes = serde_json::to_vec(&stored).map_err(std::io::Error::other)?;
        atomic_write(&self.cache_path, &bytes)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredCache {
    schema: u32,
    snapshots: Vec<RawOptionChainSnapshot>,
}

fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, bytes)?;
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    std::fs::rename(temporary, path)
}

pub async fn execute_fetch(
    client: DeribitOptionsClient,
    request: GexFetchKind,
    instruments: Arc<[OptionInstrument]>,
) -> GexFetchResult {
    let key = request.key();
    match request {
        GexFetchKind::Instruments(_) => GexFetchResult::Instruments {
            key,
            result: client
                .fetch_instruments(key.underlying)
                .await
                .map_err(error_text),
        },
        GexFetchKind::Snapshot(_) => GexFetchResult::Snapshot {
            key,
            result: client
                .fetch_chain(key.underlying, &instruments)
                .await
                .map_err(error_text),
        },
    }
}

fn error_text(error: DeribitError) -> Arc<str> {
    Arc::from(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange::options::{
        OptionInstrument, OptionMarketPoint, OptionRight, RawOptionContractSnapshot,
    };

    fn coordinator() -> GexDataCoordinator {
        GexDataCoordinator::new(std::env::temp_dir().join(format!(
            "flowsurface-gex-test-{}.json",
            uuid::Uuid::new_v4()
        )))
    }

    fn instrument() -> OptionInstrument {
        OptionInstrument {
            instrument_name: "BTC-TEST".into(),
            underlying: OptionsUnderlying::Btc,
            expiration_timestamp: UnixMs::new(2_000_000_000_000),
            strike: 100_000.0,
            right: OptionRight::Call,
            contract_size: 1.0,
        }
    }

    fn snapshot(observed_at: UnixMs) -> RawOptionChainSnapshot {
        let instrument = instrument();
        RawOptionChainSnapshot {
            provider: OptionsProvider::Deribit,
            underlying: OptionsUnderlying::Btc,
            source_spot: 100_000.0,
            contracts: vec![RawOptionContractSnapshot {
                market: OptionMarketPoint {
                    instrument_name: instrument.instrument_name.clone(),
                    open_interest_underlying: 10.0,
                    mark_iv_percent: 50.0,
                    underlying_price: 100_000.0,
                    interest_rate: 0.0,
                    observed_at,
                },
                instrument,
            }]
            .into(),
            observed_at,
        }
    }

    fn seed_instruments(coordinator: &mut GexDataCoordinator, now: UnixMs) {
        let key = OptionsChainKey::deribit(OptionsUnderlying::Btc);
        coordinator.complete(
            GexFetchResult::Instruments {
                key,
                result: Ok(vec![instrument()]),
            },
            now,
        );
    }

    #[test]
    fn consumers_and_inflight_deduplicate_fetches() {
        let now = UnixMs::new(1_800_000_000_000);
        let mut value = coordinator();
        assert!(value.due_fetches(now, true).is_empty());
        value.set_consumers([OptionsUnderlying::Btc, OptionsUnderlying::Btc]);
        assert_eq!(value.subscriber_count(OptionsUnderlying::Btc), 2);
        assert_eq!(value.due_fetches(now, true).len(), 1);
        assert!(value.due_fetches(now, true).is_empty());
        value.set_consumers([]);
        assert!(
            value
                .due_fetches(now.saturating_add(INSTRUMENT_TTL_MS), true)
                .is_empty()
        );
    }

    #[test]
    fn btc_and_eth_are_separate_and_offline_stops_polling() {
        let now = UnixMs::new(1_800_000_000_000);
        let mut value = coordinator();
        value.set_consumers([OptionsUnderlying::Btc, OptionsUnderlying::Eth]);
        assert!(value.due_fetches(now, false).is_empty());
        let due = value.due_fetches(now, true);
        assert_eq!(due.len(), 2);
        assert_ne!(due[0].key(), due[1].key());
    }

    #[test]
    fn failures_backoff_and_keep_last_valid_snapshot() {
        let now = UnixMs::new(1_800_000_000_000);
        let key = OptionsChainKey::deribit(OptionsUnderlying::Btc);
        let mut value = coordinator();
        value.set_consumers([OptionsUnderlying::Btc]);
        seed_instruments(&mut value, now);
        value.complete(
            GexFetchResult::Snapshot {
                key,
                result: Ok(snapshot(now)),
            },
            now,
        );
        assert!(
            value
                .derived(OptionsUnderlying::Btc, &Config::default(), now)
                .is_some()
        );
        value.complete(
            GexFetchResult::Snapshot {
                key,
                result: Err("network".into()),
            },
            now.saturating_add(MARKET_SNAPSHOT_TTL_MS),
        );
        assert!(
            value
                .derived(OptionsUnderlying::Btc, &Config::default(), now)
                .is_some()
        );
        assert!(
            value
                .due_fetches(now.saturating_add(MARKET_SNAPSHOT_TTL_MS + 1), true)
                .is_empty()
        );
    }

    #[test]
    fn config_only_changes_derived_cache_and_raw_revision_invalidates_it() {
        let now = UnixMs::new(1_800_000_000_000);
        let key = OptionsChainKey::deribit(OptionsUnderlying::Btc);
        let mut value = coordinator();
        value.set_consumers([OptionsUnderlying::Btc]);
        seed_instruments(&mut value, now);
        value.complete(
            GexFetchResult::Snapshot {
                key,
                result: Ok(snapshot(now)),
            },
            now,
        );
        let first = value
            .derived(OptionsUnderlying::Btc, &Config::default(), now)
            .expect("derived");
        let absolute = value
            .derived(
                OptionsUnderlying::Btc,
                &Config {
                    sign_model: GexSignModel::AbsoluteGamma,
                    ..Config::default()
                },
                now,
            )
            .expect("derived");
        assert_ne!(first.model, absolute.model);
        assert!(value.due_fetches(now, true).is_empty());
        value.complete(
            GexFetchResult::Snapshot {
                key,
                result: Ok(snapshot(now.saturating_add(1))),
            },
            now.saturating_add(1),
        );
        let second = value
            .derived(OptionsUnderlying::Btc, &Config::default(), now)
            .expect("derived");
        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn reconnect_forces_refresh_and_freshness_transitions() {
        let now = UnixMs::new(1_800_000_000_000);
        let key = OptionsChainKey::deribit(OptionsUnderlying::Btc);
        let mut value = coordinator();
        value.set_consumers([OptionsUnderlying::Btc]);
        seed_instruments(&mut value, now);
        value.complete(
            GexFetchResult::Snapshot {
                key,
                result: Ok(snapshot(now)),
            },
            now,
        );
        assert_eq!(
            value.freshness(OptionsUnderlying::Btc, now),
            GexFreshness::Fresh
        );
        assert_eq!(
            value.freshness(
                OptionsUnderlying::Btc,
                now.saturating_add(FRESH_THRESHOLD_MS + 1)
            ),
            GexFreshness::Stale
        );
        assert_eq!(
            value.freshness(
                OptionsUnderlying::Btc,
                now.saturating_add(EXPIRED_THRESHOLD_MS + 1)
            ),
            GexFreshness::Expired
        );
        value.reconnect();
        assert_eq!(value.due_fetches(now.saturating_add(1), true).len(), 1);
    }

    #[test]
    fn corrupt_persistent_snapshot_is_ignored() {
        let path = std::env::temp_dir().join(format!(
            "flowsurface-gex-corrupt-{}.json",
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&path, b"not-json").expect("fixture");
        let value = GexDataCoordinator::new(path.clone());
        assert!(value.raw_snapshots.is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn persistent_snapshot_loads_stale_and_refreshes_with_consumer() {
        let path = std::env::temp_dir().join(format!(
            "flowsurface-gex-persisted-{}.json",
            uuid::Uuid::new_v4()
        ));
        let now = UnixMs::new(1_800_000_000_000);
        let key = OptionsChainKey::deribit(OptionsUnderlying::Btc);
        let mut first = GexDataCoordinator::new(path.clone());
        first.complete(
            GexFetchResult::Snapshot {
                key,
                result: Ok(snapshot(now)),
            },
            now,
        );
        let mut restored = GexDataCoordinator::new(path.clone());
        assert_eq!(
            restored.freshness(OptionsUnderlying::Btc, now),
            GexFreshness::Stale
        );
        restored.set_consumers([OptionsUnderlying::Btc]);
        assert!(matches!(
            restored.due_fetches(now, true).as_slice(),
            [GexFetchKind::Instruments(_)]
        ));
        let _ = std::fs::remove_file(path);
    }
}
