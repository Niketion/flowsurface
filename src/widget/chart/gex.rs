use crate::{chart::gex, style};
use data::chart::gex::{GexFreshness, GexSignModel};
use iced::{
    Alignment, Border, Element, Length,
    widget::{button, column, container, mouse_area, row, scrollable, space, text, tooltip},
};

pub fn view(chart: &gex::GexChart) -> Element<'_, gex::Message> {
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
        text(format!(
            "{} GEX · {} · {}",
            snapshot.underlying, snapshot.provider, snapshot.model
        ))
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
            summary_item("Absolute GEX", format_exposure(snapshot.absolute_gex_1pct)),
        ]
        .spacing(4);
        let levels_row = row![
            summary_item(
                "Gamma Flip",
                format_level_distance(snapshot.gamma_flip, snapshot.source_spot),
            ),
            summary_item(
                "Call Wall",
                format_level_distance(snapshot.call_wall, snapshot.source_spot),
            ),
            summary_item(
                "Put Wall",
                format_level_distance(snapshot.put_wall, snapshot.source_spot),
            ),
        ]
        .spacing(4);
        let metadata_row = row![
            text(format!("Expiry: {}", chart.config().expiry_filter)),
            text(format!("Freshness: {freshness}")),
            text(format!("Snapshot: {timestamp}")),
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

        let is_spot = chart.config().show_current_price
            && (strike.strike - snapshot.source_spot).abs() <= tolerance;
        let is_flip =
            chart.config().show_gamma_flip && proximity(strike.strike, snapshot.gamma_flip);
        let is_call = chart.config().show_call_wall && proximity(strike.strike, snapshot.call_wall);
        let is_put = chart.config().show_put_wall && proximity(strike.strike, snapshot.put_wall);
        let mut badges = Vec::new();
        if is_spot {
            badges.push("Spot");
        }
        if is_flip {
            badges.push("Flip");
        }
        if is_call {
            badges.push("Call Wall");
        }
        if is_put {
            badges.push("Put Wall");
        }
        let badges = if badges.is_empty() {
            String::new()
        } else {
            format!("[{}]", badges.join(" · "))
        };
        let marker = if is_flip || is_spot {
            Some(MarkerColor::Warning)
        } else if is_call {
            Some(MarkerColor::Success)
        } else if is_put {
            Some(MarkerColor::Danger)
        } else {
            None
        };
        let strike_row = row![
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
            text(detail_label).width(Length::FillPortion(2)),
            text(badges).width(Length::FillPortion(2)),
        ]
        .align_y(Alignment::Center)
        .spacing(3);
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
        .padding(7)
        .style(container::rounded_box);
        strike_rows =
            strike_rows.push(tooltip(strike_row, detail, tooltip::Position::FollowCursor));
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
        text("scroll zoom · drag pan · double-click auto-fit").size(style::text_size::SMALL),
    ];

    let interactive_profile = mouse_area(strike_rows)
        .on_double_click(gex::Message::AutoFit)
        .on_scroll(gex::Message::Scrolled)
        .on_press(gex::Message::DragStarted)
        .on_move(gex::Message::Dragged)
        .on_release(gex::Message::DragEnded)
        .interaction(iced::mouse::Interaction::Grab);
    let profile = column![
        toolbar,
        axis_labels,
        axis_line,
        scrollable(interactive_profile).height(Length::Fill)
    ]
    .spacing(2);
    let content = column![header, profile].spacing(6).padding(8);
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

#[derive(Clone, Copy)]
enum MarkerColor {
    Warning,
    Success,
    Danger,
}

fn marker_row_style(theme: &iced::Theme, marker: Option<MarkerColor>) -> container::Style {
    let Some(marker) = marker else {
        return container::Style::default();
    };
    let palette = theme.extended_palette();
    let color = match marker {
        MarkerColor::Warning => palette.warning.strong.color,
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
            text(value).size(style::text_size::BODY)
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
