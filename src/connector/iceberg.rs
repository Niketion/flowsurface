//! Shared lifecycle and cache for per-market iceberg detector engines.

use data::orderflow::iceberg::{BinanceIcebergDetector, IcebergDetectorConfig, IcebergEvent};
use exchange::{TickerInfo, UnixMs, orderflow::OrderFlowEvent, unit::PriceStep};
use rustc_hash::FxHashMap;

const RELEASE_GRACE_MS: u64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DetectorKey {
    pub ticker_info: TickerInfo,
    pub tick_size: PriceStep,
}

struct SharedDetector {
    engine: BinanceIcebergDetector,
    consumers: usize,
    released_at: Option<UnixMs>,
}

#[derive(Default)]
pub struct IcebergDetectorRegistry {
    detectors: FxHashMap<DetectorKey, SharedDetector>,
}

impl IcebergDetectorRegistry {
    pub fn sync_requirements(
        &mut self,
        requirements: Vec<(DetectorKey, IcebergDetectorConfig)>,
        now: UnixMs,
    ) {
        let mut desired: FxHashMap<DetectorKey, (usize, IcebergDetectorConfig)> =
            FxHashMap::default();
        for (key, config) in requirements
            .into_iter()
            .filter(|(_, config)| config.enabled)
        {
            let entry = desired.entry(key).or_insert((0, config));
            entry.0 = entry.0.saturating_add(1);
        }
        for (key, shared) in &mut self.detectors {
            if let Some((count, _)) = desired.remove(key) {
                shared.consumers = count;
                shared.released_at = None;
            } else if shared.consumers > 0 {
                shared.consumers = 0;
                shared.released_at = Some(now);
            }
        }
        for (key, (count, config)) in desired {
            match BinanceIcebergDetector::new(key.ticker_info, key.tick_size, config) {
                Ok(engine) => {
                    self.detectors.insert(
                        key,
                        SharedDetector {
                            engine,
                            consumers: count,
                            released_at: None,
                        },
                    );
                    log::info!(
                        "IcebergDetectorStarted | exchange={} ticker={} consumers={count}",
                        key.ticker_info.exchange(),
                        key.ticker_info.ticker
                    );
                }
                Err(reason) => log::debug!(
                    "IcebergDetectorUnsupported | ticker={} reason={reason}",
                    key.ticker_info.ticker
                ),
            }
        }
    }

    pub fn ingest(&mut self, event: OrderFlowEvent) -> Vec<IcebergEvent> {
        let ticker_info = event.ticker_info();
        self.detectors
            .iter_mut()
            .filter(|(key, shared)| key.ticker_info == ticker_info && shared.consumers > 0)
            .flat_map(|(_, shared)| shared.engine.ingest(event.clone()))
            .collect()
    }

    pub fn collect_garbage(&mut self, now: UnixMs) {
        self.detectors.retain(|key, shared| {
            let keep = shared.consumers > 0
                || shared
                    .released_at
                    .is_some_and(|released| now.saturating_diff(released) < RELEASE_GRACE_MS);
            if !keep {
                log::info!(
                    "IcebergDetectorStopped | exchange={} ticker={}",
                    key.ticker_info.exchange(),
                    key.ticker_info.ticker
                );
            }
            keep
        });
    }

    #[cfg(test)]
    fn detector_count(&self) -> usize {
        self.detectors.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange::{Ticker, adapter::Exchange};

    fn key() -> DetectorKey {
        let ticker_info = TickerInfo::new(
            Ticker::new("BTCUSDT", Exchange::BinanceLinear),
            0.1,
            0.001,
            None,
        );
        DetectorKey {
            ticker_info,
            tick_size: ticker_info.min_ticksize.into(),
        }
    }

    #[test]
    fn two_consumers_share_one_engine_and_last_release_expires_it() {
        let mut registry = IcebergDetectorRegistry::default();
        let config = IcebergDetectorConfig {
            enabled: true,
            ..Default::default()
        };
        registry.sync_requirements(vec![(key(), config), (key(), config)], UnixMs::new(1_000));
        assert_eq!(registry.detector_count(), 1);
        registry.sync_requirements(vec![(key(), config)], UnixMs::new(1_000));
        registry.collect_garbage(UnixMs::new(7_000));
        assert_eq!(registry.detector_count(), 1);
        registry.sync_requirements(Vec::new(), UnixMs::new(7_000));
        registry.collect_garbage(UnixMs::new(12_001));
        assert_eq!(registry.detector_count(), 0);
    }
}
