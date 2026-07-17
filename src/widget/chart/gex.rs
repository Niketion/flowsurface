use crate::{chart::gex, style};
use data::chart::gex::{GexFreshness, GexSignModel};
use iced::{
    Alignment, Border, Color, Element, Length, Point, Rectangle, Renderer, Theme, mouse,
    widget::{
        button, canvas, column, container, mouse_area, responsive, row, scrollable, space, stack,
        text, tooltip,
    },
};

pub fn view(chart: &gex::GexChart) -> Element<'_, gex::Message> {
    responsive(move |size| view_sized(chart, GexLayoutDensity::for_width(size.width))).into()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GexLayoutDensity {
    Full,
    Compact,
    Minimal,
}

impl GexLayoutDensity {
    fn for_width(width: f32) -> Self {
        if width >= 720.0 {
            Self::Full
        } else if width >= 520.0 {
            Self::Compact
        } else {
            Self::Minimal
        }
    }

    fn show_net_column(self) -> bool {
        self == Self::Full
    }
}

fn view_sized(chart: &gex::GexChart, density: GexLayoutDensity) -> Element<'_, gex::Message> {
    let Some(snapshot) = chart.snapshot() else {
        let message = match chart.freshness() {
            GexFreshness::Error => "GEX data unavailable.",
            _ => "Waiting for GEX data...",
        };
        return iced::widget::center(
            column![
                text(message).size(style::text_size::TITLE),
                text(chart.error().unwrap_or_default())
            ]
            .align_x(Alignment::Center),
        )
        .into();
    };

    let freshness = match chart.freshness() {
        GexFreshness::Loading => "Loading",
        GexFreshness::Fresh => "Fresh",
        GexFreshness::Stale => "Stale",
        GexFreshness::Expired => "Expired",
        GexFreshness::Error => "Error · cached",
    };
    let timestamp = snapshot
        .observed_at
        .format_utc("%Y-%m-%d %H:%M:%S UTC")
        .unwrap_or_else(|| "unknown".into());

    let title = row![
        text(if density == GexLayoutDensity::Minimal {
            format!("{} GEX", snapshot.underlying)
        } else {
            format!(
                "{} GEX · {} · {}",
                snapshot.underlying, snapshot.provider, snapshot.model
            )
        })
        .size(style::text_size::SECTION),
        space::horizontal(),
        tooltip(
            button(style::icon_text(style::Icon::ResizeSmall, 11))
                .padding(5)
                .on_press(gex::Message::AutoFit),
            container(text("Auto-fit · double-click").size(style::text_size::SMALL))
                .padding(5)
                .style(container::rounded_box),
            tooltip::Position::Bottom,
        ),
    ]
    .align_y(Alignment::Center);

    let mut header = column![title].spacing(5);
    if chart.config().show_summary {
        let exposure_row = row![
            summary_item(
                "Net GEX",
                snapshot
                    .net_gex_1pct
                    .map(format_exposure)
                    .unwrap_or_else(|| "N/A".into()),
            ),
            if density != GexLayoutDensity::Minimal {
                summary_item("Absolute GEX", format_exposure(snapshot.absolute_gex_1pct))
            } else {
                space::horizontal().width(0).into()
            },
        ]
        .spacing(4);
        let levels_row = row![
            summary_item(
                "Gamma Flip",
                format_level_distance(snapshot.gamma_flip, snapshot.source_spot),
            ),
            if density != GexLayoutDensity::Minimal {
                summary_item(
                    "Call Wall",
                    format_level_distance(snapshot.call_wall, snapshot.source_spot),
                )
            } else {
                space::horizontal().width(0).into()
            },
            if density != GexLayoutDensity::Minimal {
                summary_item(
                    "Put Wall",
                    format_level_distance(snapshot.put_wall, snapshot.source_spot),
                )
            } else {
                space::horizontal().width(0).into()
            },
        ]
        .spacing(4);
        let short_timestamp = snapshot
            .observed_at
            .format_utc("%H:%M:%S")
            .unwrap_or_else(|| "unknown".into());
        let metadata_row = row![
            text(format!("Expiry: {}", chart.config().expiry_filter)),
            text(format!("Freshness: {freshness}")),
            text(format!(
                "Snapshot: {}",
                if density == GexLayoutDensity::Minimal {
                    short_timestamp
                } else {
                    timestamp.clone()
                }
            )),
        ]
        .spacing(14);
        header = header
            .push(exposure_row)
            .push(levels_row)
            .push(metadata_row);
    }
    if snapshot.model == GexSignModel::CallPutOiProxy && snapshot.gamma_flip.is_none() {
        header = header.push(text("No gamma flip in the configured price range."));
    }
    if let Some(error) = chart.error() {
        header = header.push(text(format!("Last refresh error: {error}")));
    }

    if snapshot.strikes.is_empty() {
        return column![
            header,
            iced::widget::center(text("No valid options for the selected expiry filter."))
        ]
        .padding(8)
        .into();
    }

    let visible = chart.visible_strikes();
    if visible.is_empty() {
        return column![
            header,
            iced::widget::center(text("No strikes in the configured price range."))
        ]
        .padding(8)
        .into();
    }
    let max_visible = visible
        .iter()
        .map(|strike| {
            strike
                .call_gex_1pct
                .abs()
                .max(strike.put_gex_1pct.abs())
                .max(strike.net_gex_1pct.abs())
                .max(strike.absolute_gamma_1pct)
        })
        .fold(0.0_f64, f64::max)
        .max(f64::EPSILON);
    let step = visible
        .windows(2)
        .map(|pair| (pair[1].strike - pair[0].strike).abs())
        .filter(|step| *step > 0.0)
        .fold(f64::INFINITY, f64::min);
    let tolerance = if step.is_finite() {
        step * 0.5
    } else {
        snapshot.source_spot * 0.001
    };
    let proximity =
        |strike: f64, level: Option<f64>| level.is_some_and(|v| (strike - v).abs() <= tolerance);

    let mut strike_rows = column![].spacing(1);
    for strike in visible.iter().rev() {
        let absolute_mode = snapshot.model == GexSignModel::AbsoluteGamma;
        let put = if absolute_mode {
            1
        } else {
            portion(strike.put_gex_1pct.abs(), max_visible)
        };
        let call = portion(
            if absolute_mode {
                strike.absolute_gamma_1pct
            } else {
                strike.call_gex_1pct
            },
            max_visible,
        );
        let mut detail_label = String::new();
        if chart.config().show_net_gex {
            detail_label = if absolute_mode {
                format!("abs {}", format_exposure(strike.absolute_gamma_1pct))
            } else {
                format!("net {}", format_exposure(strike.net_gex_1pct))
            };
        }
        if chart.config().show_absolute_gamma && !absolute_mode {
            detail_label.push_str(&format!(
                "{}abs {}",
                if detail_label.is_empty() { "" } else { " · " },
                format_exposure(strike.absolute_gamma_1pct)
            ));
        }

        let left = if chart.config().show_put_gex && !absolute_mode {
            row![
                space::horizontal().width(Length::FillPortion(1000 - put)),
                container(space::horizontal())
                    .width(Length::FillPortion(put))
                    .height(8)
                    .style(container::danger)
            ]
        } else {
            row![space::horizontal()]
        };
        let right = if chart.config().show_call_gex {
            row![
                container(space::horizontal())
                    .width(Length::FillPortion(call))
                    .height(8)
                    .style(container::success),
                space::horizontal().width(Length::FillPortion(1000 - call))
            ]
        } else {
            row![space::horizontal()]
        };

        let is_call = chart.config().show_call_wall && proximity(strike.strike, snapshot.call_wall);
        let is_put = chart.config().show_put_wall && proximity(strike.strike, snapshot.put_wall);
        let mut badges = Vec::new();
        if is_call {
            badges.push(if density == GexLayoutDensity::Full {
                "Call Wall"
            } else {
                "CW"
            });
        }
        if is_put {
            badges.push(if density == GexLayoutDensity::Full {
                "Put Wall"
            } else {
                "PW"
            });
        }
        let badges = if badges.is_empty() {
            String::new()
        } else {
            format!("[{}]", badges.join(" · "))
        };
        let marker = if is_call {
            Some(MarkerColor::Success)
        } else if is_put {
            Some(MarkerColor::Danger)
        } else {
            None
        };
        let mut strike_row = row![
            left.width(Length::FillPortion(5)),
            container(space::horizontal())
                .width(2)
                .height(20)
                .style(|theme: &iced::Theme| {
                    let palette = theme.extended_palette();
                    container::Style {
                        background: Some(palette.background.base.text.scale_alpha(0.75).into()),
                        ..container::Style::default()
                    }
                }),
            right.width(Length::FillPortion(5)),
            text(format!("{:>10.2}", strike.strike)).width(88),
        ]
        .align_y(Alignment::Center)
        .spacing(3);
        if density.show_net_column() {
            strike_row = strike_row.push(
                text(detail_label)
                    .wrapping(iced::widget::text::Wrapping::None)
                    .width(Length::FillPortion(2)),
            );
        }
        if density != GexLayoutDensity::Compact || !badges.is_empty() {
            strike_row = strike_row.push(
                text(badges)
                    .wrapping(iced::widget::text::Wrapping::None)
                    .width(if density == GexLayoutDensity::Full {
                        Length::FillPortion(2)
                    } else {
                        Length::Fixed(34.0)
                    }),
            );
        }
        strike_row = strike_row.push(space::horizontal().width(10));
        let strike_row = container(strike_row)
            .padding([1, 3])
            .style(move |theme: &iced::Theme| marker_row_style(theme, marker));
        let detail = container(text(format!(
            "Strike: {:.2}\nDistance from spot: {:+.2} ({:+.2}%)\nCall GEX: {}\nPut GEX: {}\nNet GEX: {}\nAbsolute gamma: {}\nCall OI: {:.2}\nPut OI: {:.2}\nExpiries: {}",
            strike.strike,
            strike.strike - snapshot.source_spot,
            (strike.strike - snapshot.source_spot) / snapshot.source_spot * 100.0,
            format_exposure(strike.call_gex_1pct),
            format_exposure(strike.put_gex_1pct),
            format_exposure(strike.net_gex_1pct),
            format_exposure(strike.absolute_gamma_1pct),
            strike.call_open_interest,
            strike.put_open_interest,
            strike.expiration_count,
        )))
        .max_width(280)
        .padding(7)
        .style(container::rounded_box);
        strike_rows = strike_rows.push(tooltip(strike_row, detail, tooltip::Position::Left));
    }

    let axis_labels = row![
        text(format!(
            "−{}",
            format_exposure(max_visible).trim_start_matches('+')
        ))
        .width(Length::Fill),
        text("0").align_x(iced::alignment::Horizontal::Center),
        text(format_exposure(max_visible))
            .width(Length::Fill)
            .align_x(iced::alignment::Horizontal::Right),
        space::horizontal().width(Length::FillPortion(4)),
    ];
    let axis_line = row![
        container(space::horizontal())
            .height(1)
            .width(Length::FillPortion(5))
            .style(container::bordered_box),
        container(space::horizontal())
            .height(7)
            .width(2)
            .style(container::bordered_box),
        container(space::horizontal())
            .height(1)
            .width(Length::FillPortion(5))
            .style(container::bordered_box),
        space::horizontal().width(Length::FillPortion(4)),
    ]
    .align_y(Alignment::Center);
    let toolbar = row![
        text(if snapshot.model == GexSignModel::CallPutOiProxy {
            "Put GEX ←  GEX / 1%  → Call GEX"
        } else {
            "Absolute gamma concentration"
        }),
        space::horizontal(),
        if density == GexLayoutDensity::Full {
            text("scroll zoom · drag pan · double-click auto-fit").size(style::text_size::SMALL)
        } else {
            text("ⓘ").size(style::text_size::SMALL)
        },
    ];

    let interactive_profile = mouse_area(strike_rows)
        .on_double_click(gex::Message::AutoFit)
        .on_scroll(gex::Message::Scrolled)
        .on_press(gex::Message::DragStarted)
        .on_move(gex::Message::Dragged)
        .on_release(gex::Message::DragEnded)
        .interaction(iced::mouse::Interaction::Grab);
    let low = visible.first().map_or(0.0, |strike| strike.strike);
    let high = visible.last().map_or(1.0, |strike| strike.strike);
    let exact_levels = canvas(ProfileExactLevels {
        low,
        high,
        spot: chart
            .config()
            .show_current_price
            .then_some(snapshot.source_spot),
        flip: chart
            .config()
            .show_gamma_flip
            .then_some(snapshot.gamma_flip)
            .flatten(),
        density,
    })
    .width(Length::Fill)
    .height(Length::Fill);
    let profile_plot = stack![
        exact_levels,
        scrollable(interactive_profile).height(Length::Fill)
    ];
    let profile = column![toolbar, axis_labels, axis_line, profile_plot].spacing(2);
    let gap = if density == GexLayoutDensity::Full {
        6
    } else {
        3
    };
    let padding = if density == GexLayoutDensity::Full {
        8
    } else {
        4
    };
    let content = column![header, profile].spacing(gap).padding(padding);
    if matches!(
        chart.freshness(),
        GexFreshness::Stale | GexFreshness::Expired
    ) {
        container(content)
            .style(|theme: &iced::Theme| {
                let palette = theme.extended_palette();
                container::Style {
                    text_color: Some(palette.background.base.text.scale_alpha(0.62)),
                    ..container::Style::default()
                }
            })
            .into()
    } else {
        content.into()
    }
}

fn price_to_profile_y(price: f64, low: f64, high: f64, height: f32) -> Option<f32> {
    if !price.is_finite()
        || !low.is_finite()
        || !high.is_finite()
        || high <= low
        || price < low
        || price > high
        || height <= 0.0
    {
        return None;
    }
    Some(((high - price) / (high - low)) as f32 * height)
}

struct ProfileExactLevels {
    low: f64,
    high: f64,
    spot: Option<f64>,
    flip: Option<f64>,
    density: GexLayoutDensity,
}

impl canvas::Program<gex::Message> for ProfileExactLevels {
    type State = ();

    fn update(
        &self,
        _state: &mut Self::State,
        _event: &canvas::Event,
        _bounds: Rectangle,
        _cursor: mouse::Cursor,
    ) -> Option<canvas::Action<gex::Message>> {
        None
    }

    fn draw(
        &self,
        _state: &Self::State,
        renderer: &Renderer,
        theme: &Theme,
        bounds: Rectangle,
        _cursor: mouse::Cursor,
    ) -> Vec<canvas::Geometry> {
        let palette = theme.extended_palette();
        let mut frame = canvas::Frame::new(renderer, bounds.size());
        let spot_y = self
            .spot
            .and_then(|price| price_to_profile_y(price, self.low, self.high, bounds.height));
        let flip_y = self
            .flip
            .and_then(|price| price_to_profile_y(price, self.low, self.high, bounds.height));
        let draw_line = |frame: &mut canvas::Frame, y: f32, color: Color, width: f32| {
            frame.stroke(
                &canvas::Path::line(Point::new(0.0, y), Point::new(bounds.width, y)),
                canvas::Stroke::default()
                    .with_color(color)
                    .with_width(width),
            );
        };
        if let Some(y) = flip_y {
            draw_line(&mut frame, y, palette.warning.strong.color, 1.6);
        }
        if let Some(y) = spot_y {
            draw_line(&mut frame, y, palette.primary.strong.color, 1.2);
        }

        let compact = self.density != GexLayoutDensity::Full;
        match (spot_y, self.spot, flip_y, self.flip) {
            (Some(sy), Some(spot), Some(fy), Some(flip)) if (sy - fy).abs() < 13.0 => {
                draw_profile_badge(
                    &mut frame,
                    bounds.width,
                    (sy + fy) * 0.5,
                    &format!(
                        "{} {spot:.2} · {} {flip:.2}",
                        if compact { "S" } else { "Spot" },
                        if compact { "GF" } else { "Gamma Flip" }
                    ),
                    palette.warning.strong.color,
                );
            }
            _ => {
                if let (Some(y), Some(price)) = (spot_y, self.spot) {
                    draw_profile_badge(
                        &mut frame,
                        bounds.width,
                        y,
                        &format!("{} {price:.2}", if compact { "S" } else { "Spot" }),
                        palette.primary.strong.color,
                    );
                }
                if let (Some(y), Some(price)) = (flip_y, self.flip) {
                    draw_profile_badge(
                        &mut frame,
                        bounds.width,
                        y,
                        &format!("{} {price:.2}", if compact { "GF" } else { "Gamma Flip" }),
                        palette.warning.strong.color,
                    );
                }
            }
        }
        vec![frame.into_geometry()]
    }
}

fn draw_profile_badge(frame: &mut canvas::Frame, right: f32, y: f32, label: &str, color: Color) {
    let width = label.chars().count() as f32 * 5.4 + 8.0;
    let x = profile_badge_x(right, width);
    frame.fill(
        &canvas::Path::rectangle(Point::new(x, y - 7.0), iced::Size::new(width, 14.0)),
        color.scale_alpha(0.88),
    );
    frame.fill_text(canvas::Text {
        content: label.to_string(),
        position: Point::new(x + 4.0, y),
        size: iced::Pixels(8.0),
        color: Color::WHITE,
        align_y: iced::alignment::Vertical::Center,
        font: style::AZERET_MONO,
        ..canvas::Text::default()
    });
}

fn profile_badge_x(plot_width: f32, badge_width: f32) -> f32 {
    (plot_width - badge_width - 14.0).max(2.0)
}

#[derive(Clone, Copy)]
enum MarkerColor {
    Success,
    Danger,
}

fn marker_row_style(theme: &iced::Theme, marker: Option<MarkerColor>) -> container::Style {
    let Some(marker) = marker else {
        return container::Style::default();
    };
    let palette = theme.extended_palette();
    let color = match marker {
        MarkerColor::Success => palette.success.strong.color,
        MarkerColor::Danger => palette.danger.strong.color,
    };
    container::Style {
        background: Some(color.scale_alpha(0.07).into()),
        border: Border {
            color: color.scale_alpha(0.55),
            width: 1.0,
            radius: 2.0.into(),
        },
        ..container::Style::default()
    }
}

fn summary_item(label: &'static str, value: String) -> Element<'static, gex::Message> {
    container(
        column![
            text(label).size(style::text_size::SMALL),
            text(value)
                .size(style::text_size::BODY)
                .wrapping(iced::widget::text::Wrapping::None)
        ]
        .spacing(1),
    )
    .padding([3, 6])
    .style(container::rounded_box)
    .into()
}

fn portion(value: f64, max: f64) -> u16 {
    ((value / max).clamp(0.0, 1.0) * 999.0).round() as u16 + 1
}

fn format_level_distance(value: Option<f64>, spot: f64) -> String {
    value.map_or_else(
        || "N/A".into(),
        |value| {
            let distance = value - spot;
            format!(
                "{value:.2} · {distance:+.2} ({:+.2}%)",
                distance / spot * 100.0
            )
        },
    )
}

pub fn format_exposure(value: f64) -> String {
    let sign = if value >= 0.0 { "+" } else { "−" };
    let absolute = value.abs();
    if absolute >= 1_000_000_000.0 {
        format!("{sign}${:.2}B", absolute / 1_000_000_000.0)
    } else if absolute >= 1_000_000.0 {
        format!("{sign}${:.2}M", absolute / 1_000_000.0)
    } else if absolute >= 1_000.0 {
        format!("{sign}${:.2}K", absolute / 1_000.0)
    } else {
        format!("{sign}${absolute:.2}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responsive_density_uses_explicit_thresholds() {
        assert_eq!(
            GexLayoutDensity::for_width(719.0),
            GexLayoutDensity::Compact
        );
        assert_eq!(GexLayoutDensity::for_width(720.0), GexLayoutDensity::Full);
        assert_eq!(
            GexLayoutDensity::for_width(519.0),
            GexLayoutDensity::Minimal
        );
        assert_eq!(
            GexLayoutDensity::for_width(520.0),
            GexLayoutDensity::Compact
        );
        assert!(GexLayoutDensity::Full.show_net_column());
        assert!(!GexLayoutDensity::Compact.show_net_column());
        assert!(!GexLayoutDensity::Minimal.show_net_column());
    }

    #[test]
    fn exact_price_mapping_is_stable_and_drops_outside_levels() {
        assert_eq!(price_to_profile_y(150.0, 100.0, 200.0, 400.0), Some(200.0));
        assert_eq!(price_to_profile_y(150.0, 100.0, 200.0, 800.0), Some(400.0));
        assert_eq!(price_to_profile_y(99.0, 100.0, 200.0, 400.0), None);
        assert_eq!(price_to_profile_y(201.0, 100.0, 200.0, 400.0), None);
    }

    #[test]
    fn profile_badge_is_anchored_to_right_edge() {
        assert_eq!(profile_badge_x(600.0, 100.0), 486.0);
        assert_eq!(profile_badge_x(800.0, 100.0), 686.0);
    }
}
