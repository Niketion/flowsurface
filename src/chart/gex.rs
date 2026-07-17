use data::chart::gex::{Config, GexFreshness, GexSnapshot};
use exchange::options::OptionsUnderlying;
use std::{sync::Arc, time::Instant};

#[derive(Debug, Clone, Copy)]
pub enum Message {
    ZoomIn,
    ZoomOut,
    AutoFit,
    Scrolled(iced::mouse::ScrollDelta),
    DragStarted,
    Dragged(iced::Point),
    DragEnded,
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
    dragging: bool,
    last_drag_y: Option<f32>,
    drag_remainder: f32,
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
            dragging: false,
            last_drag_y: None,
            drag_remainder: 0.0,
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
            Message::ZoomIn if self.can_zoom_in() => {
                self.visible_fraction = (self.visible_fraction * 0.8).max(0.1);
            }
            Message::ZoomOut if self.can_zoom_out() => {
                self.visible_fraction = (self.visible_fraction * 1.25).min(1.0);
            }
            Message::ZoomIn | Message::ZoomOut => {}
            Message::AutoFit => self.auto_fit(),
            Message::Scrolled(delta) => {
                let rows = match delta {
                    iced::mouse::ScrollDelta::Lines { y, .. } => y.round() as isize,
                    iced::mouse::ScrollDelta::Pixels { y, .. } => (y / 32.0).round() as isize,
                };
                if rows != 0 {
                    self.center_offset = self.center_offset.saturating_add(rows);
                }
            }
            Message::DragStarted => {
                self.dragging = true;
                self.last_drag_y = None;
                self.drag_remainder = 0.0;
            }
            Message::Dragged(point) if self.dragging => {
                if let Some(previous) = self.last_drag_y {
                    self.drag_remainder += point.y - previous;
                    let rows = (self.drag_remainder / 12.0).trunc() as isize;
                    if rows != 0 {
                        self.center_offset = self.center_offset.saturating_add(rows);
                        self.drag_remainder -= rows as f32 * 12.0;
                    }
                }
                self.last_drag_y = Some(point.y);
            }
            Message::Dragged(_) => {}
            Message::DragEnded => {
                self.dragging = false;
                self.last_drag_y = None;
                self.drag_remainder = 0.0;
            }
        }
        self.clamp_center_offset();
        self.last_tick = Instant::now();
        Action::ViewChanged
    }

    fn auto_fit(&mut self) {
        self.visible_fraction = 1.0;
        self.center_offset = 0;
        self.dragging = false;
        self.last_drag_y = None;
        self.drag_remainder = 0.0;
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
        let count = self.visible_count(filtered.len());
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

    pub fn can_zoom_in(&self) -> bool {
        let len = self.filtered_strikes().len();
        self.visible_count(len) > len.min(5)
    }

    pub fn can_zoom_out(&self) -> bool {
        let len = self.filtered_strikes().len();
        self.visible_count(len) < self.config.max_visible_strikes.max(1).min(len)
    }

    pub fn is_dragging(&self) -> bool {
        self.dragging
    }

    fn filtered_strikes(&self) -> &[data::chart::gex::GexStrike] {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return &[];
        };
        let strikes = snapshot.strikes.as_ref();
        let range = (self.config.price_range_percent.max(0.0) / 100.0).min(1.0);
        let minimum = snapshot.source_spot * (1.0 - range);
        let maximum = snapshot.source_spot * (1.0 + range);
        let start = strikes.partition_point(|strike| strike.strike < minimum);
        let end = strikes.partition_point(|strike| strike.strike <= maximum);
        &strikes[start..end]
    }

    fn visible_count(&self, available: usize) -> usize {
        let configured = self.config.max_visible_strikes.max(1).min(available);
        ((configured as f64 * self.visible_fraction).round() as usize)
            .max(available.min(5))
            .min(available)
    }

    fn clamp_center_offset(&mut self) {
        let Some(snapshot) = self.snapshot.as_ref() else {
            self.center_offset = 0;
            return;
        };
        let filtered = self.filtered_strikes();
        if filtered.is_empty() {
            self.center_offset = 0;
            return;
        }
        let spot = snapshot.source_spot;
        let spot_index = filtered
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| (a.strike - spot).abs().total_cmp(&(b.strike - spot).abs()))
            .map_or(0, |(index, _)| index);
        self.center_offset = self.center_offset.clamp(
            -(spot_index as isize),
            (filtered.len() - 1 - spot_index) as isize,
        );
    }

    pub fn view(&self) -> iced::Element<'_, Message> {
        crate::widget::chart::gex::view(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::chart::gex::{GexSignModel, GexStrike};
    use exchange::{UnixMs, options::OptionsProvider};

    fn chart() -> GexChart {
        let mut chart = GexChart::new(
            OptionsUnderlying::Btc,
            Some(Config {
                max_visible_strikes: 20,
                price_range_percent: 100.0,
                ..Config::default()
            }),
        );
        let strikes = (50..=150)
            .step_by(5)
            .map(|strike| GexStrike {
                strike: f64::from(strike),
                call_gex_1pct: 1.0,
                put_gex_1pct: -1.0,
                net_gex_1pct: 0.0,
                absolute_gamma_1pct: 2.0,
                call_open_interest: 1.0,
                put_open_interest: 1.0,
                expiration_count: 1,
            })
            .collect::<Vec<_>>();
        chart.set_snapshot(
            Some(Arc::new(GexSnapshot {
                provider: OptionsProvider::Deribit,
                underlying: OptionsUnderlying::Btc,
                model: GexSignModel::CallPutOiProxy,
                source_spot: 100.0,
                observed_at: UnixMs::new(1),
                calculated_at: UnixMs::new(1),
                net_gex_1pct: Some(0.0),
                absolute_gex_1pct: 1.0,
                call_wall: Some(120.0),
                put_wall: Some(80.0),
                gamma_flip: Some(95.0),
                strikes: strikes.into(),
            })),
            GexFreshness::Fresh,
            None,
        );
        chart
    }

    #[test]
    fn scroll_pans_higher_and_lower_without_zooming() {
        let mut chart = chart();
        let fraction = chart.visible_fraction;
        chart.update(Message::Scrolled(iced::mouse::ScrollDelta::Lines {
            x: 0.0,
            y: 2.0,
        }));
        assert_eq!(chart.center_offset, 2);
        chart.update(Message::Scrolled(iced::mouse::ScrollDelta::Pixels {
            x: 0.0,
            y: -32.0,
        }));
        assert_eq!(chart.center_offset, 1);
        assert_eq!(chart.visible_fraction, fraction);
        assert!(!chart.visible_strikes().is_empty());
    }

    #[test]
    fn zoom_buttons_disable_at_limits_and_windows_stay_nonempty() {
        let mut chart = chart();
        while chart.can_zoom_in() {
            chart.update(Message::ZoomIn);
            assert!(!chart.visible_strikes().is_empty());
        }
        assert!(!chart.can_zoom_in());
        while chart.can_zoom_out() {
            chart.update(Message::ZoomOut);
            assert!(!chart.visible_strikes().is_empty());
        }
        assert!(!chart.can_zoom_out());
        assert_eq!(chart.visible_strikes().len(), 20);
    }

    #[test]
    fn pan_is_clamped_to_available_strikes() {
        let mut chart = chart();
        for _ in 0..100 {
            chart.update(Message::Scrolled(iced::mouse::ScrollDelta::Lines {
                x: 0.0,
                y: 10.0,
            }));
        }
        assert!(!chart.visible_strikes().is_empty());
        assert!(chart.visible_strikes().last().unwrap().strike <= 150.0);
    }
}
