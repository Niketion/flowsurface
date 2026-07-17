use data::chart::gex::{Config, GexFreshness, GexSnapshot};
use exchange::options::OptionsUnderlying;
use std::{sync::Arc, time::Instant};

#[derive(Debug, Clone, Copy)]
pub enum Message {
    ZoomIn,
    ZoomOut,
    PanUp,
    PanDown,
    AutoFit,
    Scrolled(iced::mouse::ScrollDelta),
}

#[derive(Debug, Clone, Copy)]
pub enum Action {
    ViewChanged,
}

pub struct GexChart {
    underlying: OptionsUnderlying,
    snapshot: Option<Arc<GexSnapshot>>,
    freshness: GexFreshness,
    config: Config,
    visible_fraction: f64,
    center_offset: isize,
    last_tick: Instant,
    error: Option<Arc<str>>,
}

impl GexChart {
    pub fn new(underlying: OptionsUnderlying, config: Option<Config>) -> Self {
        Self {
            underlying,
            snapshot: None,
            freshness: GexFreshness::Loading,
            config: config.unwrap_or_default(),
            visible_fraction: 1.0,
            center_offset: 0,
            last_tick: Instant::now(),
            error: None,
        }
    }

    pub fn underlying(&self) -> OptionsUnderlying {
        self.underlying
    }

    pub fn set_underlying(&mut self, underlying: OptionsUnderlying) {
        if self.underlying != underlying {
            self.underlying = underlying;
            self.snapshot = None;
            self.freshness = GexFreshness::Loading;
            self.error = None;
            self.auto_fit();
        }
    }

    pub fn set_snapshot(
        &mut self,
        snapshot: Option<Arc<GexSnapshot>>,
        freshness: GexFreshness,
        error: Option<Arc<str>>,
    ) {
        if snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.underlying != self.underlying)
        {
            return;
        }
        self.snapshot = snapshot;
        self.freshness = freshness;
        self.error = error;
        self.last_tick = Instant::now();
    }

    pub fn snapshot(&self) -> Option<&Arc<GexSnapshot>> {
        self.snapshot.as_ref()
    }

    pub fn freshness(&self) -> GexFreshness {
        self.freshness
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn set_config(&mut self, config: Config) {
        self.config = config;
        self.last_tick = Instant::now();
    }

    pub fn last_tick(&self) -> Instant {
        self.last_tick
    }

    pub fn update(&mut self, message: Message) -> Action {
        match message {
            Message::ZoomIn => self.visible_fraction = (self.visible_fraction * 0.8).max(0.1),
            Message::ZoomOut => self.visible_fraction = (self.visible_fraction * 1.25).min(1.0),
            Message::PanUp => self.center_offset = self.center_offset.saturating_add(1),
            Message::PanDown => self.center_offset = self.center_offset.saturating_sub(1),
            Message::AutoFit => self.auto_fit(),
            Message::Scrolled(delta) => {
                let y = match delta {
                    iced::mouse::ScrollDelta::Lines { y, .. }
                    | iced::mouse::ScrollDelta::Pixels { y, .. } => y,
                };
                if y > 0.0 {
                    self.visible_fraction = (self.visible_fraction * 0.8).max(0.1);
                } else if y < 0.0 {
                    self.visible_fraction = (self.visible_fraction * 1.25).min(1.0);
                }
            }
        }
        self.last_tick = Instant::now();
        Action::ViewChanged
    }

    fn auto_fit(&mut self) {
        self.visible_fraction = 1.0;
        self.center_offset = 0;
    }

    pub fn visible_strikes(&self) -> &[data::chart::gex::GexStrike] {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return &[];
        };
        let strikes = snapshot.strikes.as_ref();
        if strikes.is_empty() {
            return strikes;
        }
        let range = (self.config.price_range_percent.max(0.0) / 100.0).min(1.0);
        let minimum = snapshot.source_spot * (1.0 - range);
        let maximum = snapshot.source_spot * (1.0 + range);
        let filtered_start = strikes.partition_point(|strike| strike.strike < minimum);
        let filtered_end = strikes.partition_point(|strike| strike.strike <= maximum);
        let filtered = &strikes[filtered_start..filtered_end];
        if filtered.is_empty() {
            return filtered;
        }
        let configured = self.config.max_visible_strikes.max(1).min(filtered.len());
        let count = ((configured as f64 * self.visible_fraction).round() as usize)
            .max(1)
            .min(filtered.len());
        let spot_index = filtered
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                (a.strike - snapshot.source_spot)
                    .abs()
                    .total_cmp(&(b.strike - snapshot.source_spot).abs())
            })
            .map_or(0, |(index, _)| index);
        let center = spot_index
            .saturating_add_signed(self.center_offset)
            .min(filtered.len() - 1);
        let start = center
            .saturating_sub(count / 2)
            .min(filtered.len().saturating_sub(count));
        &filtered[start..start + count]
    }

    pub fn view(&self) -> iced::Element<'_, Message> {
        crate::widget::chart::gex::view(self)
    }
}
