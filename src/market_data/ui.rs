//! Unified market data loading/progress UI.
//!
//! `MarketDataProgressWidget` provides a unified view of all market data
//! loading operations, replacing the per-pane loading status with a
//! single grouped view that shows all active fetches, cached data, and
//! consumer information.

use super::job::{FetchJob, FetchJobStatus};
use super::progress::MarketDataProgressSnapshot;
use iced::{
    Alignment, Element, Theme,
    widget::{column, container, row, text},
};

/// A unified progress widget for market data loading.
#[derive(Debug, Clone)]
pub struct MarketDataProgressWidget {
    /// The current progress snapshot
    snapshot: MarketDataProgressSnapshot,
    /// Whether the widget is expanded (showing details)
    expanded: bool,
    /// Maximum number of jobs to show before truncating
    max_visible_jobs: usize,
}

impl MarketDataProgressWidget {
    /// Create a new progress widget.
    pub fn new() -> Self {
        Self {
            snapshot: MarketDataProgressSnapshot::empty(),
            expanded: false,
            max_visible_jobs: 5,
        }
    }

    /// Create a widget with custom settings.
    #[allow(dead_code)] // Public API — widget lifecycle management
    pub fn with_settings(max_visible_jobs: usize) -> Self {
        Self {
            max_visible_jobs,
            ..Self::new()
        }
    }

    /// Update the widget with a new snapshot.
    #[allow(dead_code)] // Public API — widget lifecycle management
    pub fn update(&mut self, snapshot: MarketDataProgressSnapshot) {
        self.snapshot = snapshot;
    }

    /// Toggle expanded/collapsed state.
    #[allow(dead_code)] // Public API — widget lifecycle management
    pub fn toggle_expanded(&mut self) {
        self.expanded = !self.expanded;
    }

    /// Set expanded state.
    #[allow(dead_code)] // Public API — widget lifecycle management
    pub fn set_expanded(&mut self, expanded: bool) {
        self.expanded = expanded;
    }

    /// Check if the widget is expanded.
    #[allow(dead_code)] // Public API — widget lifecycle management
    pub fn is_expanded(&self) -> bool {
        self.expanded
    }

    /// Check if data is currently loading.
    #[allow(dead_code)] // Public API — widget lifecycle management
    pub fn is_loading(&self) -> bool {
        self.snapshot.is_loading()
    }

    /// Render the progress widget.
    pub fn view(&self) -> Element<'static, Message> {
        if !self.snapshot.is_loading() && self.snapshot.message == "Ready" {
            return row![].into();
        }

        let mut content = column![].spacing(4);

        // Header with status
        let header = self.view_header();
        content = content.push(header);

        // Active jobs (if expanded)
        if self.expanded && self.snapshot.is_loading() {
            let jobs = self.view_active_jobs();
            content = content.push(jobs);
        }

        // Summary (if collapsed and loading)
        if !self.expanded && self.snapshot.is_loading() {
            let summary = self.view_summary();
            content = content.push(summary);
        }

        container(content)
            .padding(8)
            .style(|theme: &Theme| {
                let palette = theme.extended_palette();
                container::Style {
                    text_color: Some(palette.background.base.text),
                    background: Some(palette.background.weak.color.into()),
                    border: iced::Border::default()
                        .color(palette.primary.strong.color.scale_alpha(0.3))
                        .width(1),
                    ..Default::default()
                }
            })
            .into()
    }

    /// Render the header with status and expand/collapse button.
    fn view_header(&self) -> Element<'static, Message> {
        let status_text = if self.snapshot.is_loading() {
            let count = self.snapshot.active_job_count();
            format!("Loading market data ({})", count)
        } else {
            self.snapshot.message.clone()
        };

        let status_indicator = if self.snapshot.is_loading() {
            // Pulsing indicator for loading
            text("●")
                .size(12)
                .color(iced::Color::from_rgb(1.0, 0.6, 0.0)) // Orange
        } else {
            text("●")
                .size(12)
                .color(iced::Color::from_rgb(0.0, 0.8, 0.0)) // Green
        };

        let expand_button = if self.snapshot.is_loading() {
            let label = if self.expanded { "▼" } else { "▶" };
            iced::widget::button(text(label).size(10))
                .on_press(Message::ToggleExpanded)
                .padding(2)
        } else {
            iced::widget::button(text(" ").size(10)).padding(2)
        };

        row![status_indicator, text(status_text).size(12), expand_button,]
            .spacing(6)
            .align_y(Alignment::Center)
            .into()
    }

    /// Render active jobs details.
    fn view_active_jobs(&self) -> Element<'static, Message> {
        let mut jobs_column = column![].spacing(2);

        let visible_jobs: Vec<&FetchJob> = self
            .snapshot
            .active_jobs
            .iter()
            .take(self.max_visible_jobs)
            .collect();

        for job in &visible_jobs {
            let job_view = self.view_job(job);
            jobs_column = jobs_column.push(job_view);
        }

        if self.snapshot.active_job_count() > self.max_visible_jobs {
            let remaining = self.snapshot.active_job_count() - self.max_visible_jobs;
            jobs_column = jobs_column.push(
                text(format!("  ... and {} more", remaining))
                    .size(10)
                    .color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
            );
        }

        // Cached segments info
        if self.snapshot.total_cached_records > 0 {
            let cached_text = format!(
                "Cached: {} records",
                format_number(self.snapshot.total_cached_records)
            );
            jobs_column = jobs_column.push(
                text(cached_text)
                    .size(10)
                    .color(iced::Color::from_rgb(0.0, 0.6, 0.0)),
            );
        }

        jobs_column.into()
    }

    /// Render a single job.
    fn view_job(&self, job: &FetchJob) -> Element<'static, Message> {
        let key_display = job.key.display_key();
        let range_display = job.range.format_display();
        let status_display = match &job.status {
            FetchJobStatus::Pending => "Pending".to_string(),
            FetchJobStatus::InProgress => {
                format!("{} fetched", format_number(job.progress.records_fetched))
            }
            FetchJobStatus::Completed { records } => {
                format!("Done ({})", format_number(*records))
            }
            FetchJobStatus::Failed { error, .. } => format!("Failed: {}", error),
            FetchJobStatus::Cancelled => "Cancelled".to_string(),
        };

        let consumers_display = job.consumer_names();

        row![
            text(format!("{}: {}", key_display, range_display))
                .size(10)
                .color(iced::Color::from_rgb(0.8, 0.8, 0.8)),
            text(status_display)
                .size(10)
                .color(iced::Color::from_rgb(0.6, 0.8, 1.0)),
            text(format!("[{}]", consumers_display))
                .size(9)
                .color(iced::Color::from_rgb(0.5, 0.5, 0.5)),
        ]
        .spacing(8)
        .into()
    }

    /// Render a collapsed summary.
    fn view_summary(&self) -> Element<'static, Message> {
        let fetching_count = self.snapshot.active_job_count();
        let fetched_total = self.snapshot.total_fetched_records;
        let cached_total = self.snapshot.total_cached_records;

        let summary = format!(
            "Fetching {} segments | Fetched: {} | Cached: {}",
            fetching_count,
            format_number(fetched_total),
            format_number(cached_total)
        );

        text(summary)
            .size(10)
            .color(iced::Color::from_rgb(0.7, 0.7, 0.7))
            .into()
    }
}

impl Default for MarketDataProgressWidget {
    fn default() -> Self {
        Self::new()
    }
}

/// Messages for the progress widget.
#[derive(Debug, Clone)]
pub enum Message {
    /// Toggle expanded/collapsed state
    ToggleExpanded,
    /// Refresh the progress
    #[allow(dead_code)] // Public API — message variant for widget refresh
    Refresh,
}

/// Format a number with commas for display.
fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Create a unified progress view from a snapshot.
///
/// This is a convenience function for creating a progress view
/// without managing a `MarketDataProgressWidget` instance.
pub fn unified_progress_view(snapshot: &MarketDataProgressSnapshot) -> Element<'static, Message> {
    let widget = MarketDataProgressWidget {
        snapshot: snapshot.clone(),
        expanded: false,
        max_visible_jobs: 5,
    };
    widget.view()
}

/// Create an expanded progress view from a snapshot.
#[allow(dead_code)] // Public API — alternative UI view for market data progress
pub fn expanded_progress_view(snapshot: &MarketDataProgressSnapshot) -> Element<'static, Message> {
    let widget = MarketDataProgressWidget {
        snapshot: snapshot.clone(),
        expanded: true,
        max_visible_jobs: 10,
    };
    widget.view()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::key::{MarketDataKey, MarketKind, Symbol, Venue};
    use crate::market_data::range::MarketDataRange;

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(123), "123");
        assert_eq!(format_number(1234), "1,234");
        assert_eq!(format_number(1234567), "1,234,567");
    }

    #[test]
    fn test_widget_empty() {
        let widget = MarketDataProgressWidget::new();
        assert!(!widget.is_loading());
        assert!(!widget.is_expanded());
    }

    #[test]
    fn test_widget_with_snapshot() {
        let key = MarketDataKey::trades(
            Venue::BinanceLinear,
            Symbol::new("BTCUSDT"),
            MarketKind::LinearPerps,
        );
        let range =
            MarketDataRange::new(exchange::UnixMs::new(100), exchange::UnixMs::new(200)).unwrap();

        let mut job = crate::market_data::job::FetchJob::new(
            key,
            range,
            vec![crate::market_data::requirement::ConsumerId::global(
                crate::market_data::requirement::ConsumerFeature::VolumeBubbles,
            )],
        );
        job.start();

        let mut snapshot = MarketDataProgressSnapshot::empty();
        snapshot.active_jobs.push(job);

        let mut widget = MarketDataProgressWidget::new();
        widget.update(snapshot);

        assert!(widget.is_loading());
        assert_eq!(widget.snapshot.active_job_count(), 1);
    }

    #[test]
    fn test_widget_toggle() {
        let mut widget = MarketDataProgressWidget::new();
        assert!(!widget.is_expanded());

        widget.toggle_expanded();
        assert!(widget.is_expanded());

        widget.toggle_expanded();
        assert!(!widget.is_expanded());
    }
}
