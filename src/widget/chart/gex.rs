use crate::{chart::gex, style};
use data::chart::gex::{GexFreshness, GexSignModel};
use iced::{
    Element, Length,
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
            .align_x(iced::Alignment::Center),
        )
        .into();
    };

    let status = match chart.freshness() {
        GexFreshness::Loading => "Loading",
        GexFreshness::Fresh => "Fresh",
        GexFreshness::Stale => "Stale · GEX data is stale.",
        GexFreshness::Expired => "Expired · GEX data unavailable.",
        GexFreshness::Error => "Error · previous valid snapshot retained",
    };
    let updated = snapshot
        .observed_at
        .format_utc("%H:%M:%S")
        .unwrap_or_else(|| "unknown".into());
    let mut summary = column![
        text(format!(
            "{} GEX · {}",
            snapshot.underlying, snapshot.provider
        ))
        .size(style::text_size::TITLE),
        text(format!("Model: {}", snapshot.model)),
        text(format!("Updated: {}    Status: {}", updated, status)),
    ]
    .spacing(2);
    if chart.config().show_summary {
        summary = summary.push(text(format!(
            "Net GEX: {} / 1%    Absolute GEX: {} / 1%",
            snapshot
                .net_gex_1pct
                .map(format_exposure)
                .unwrap_or_else(|| "N/A".into()),
            format_exposure(snapshot.absolute_gex_1pct)
        )));
        summary = summary.push(text(format!(
            "Call Wall: {}    Put Wall: {}    Gamma Flip: {}",
            format_level(snapshot.call_wall),
            format_level(snapshot.put_wall),
            format_level(snapshot.gamma_flip)
        )));
        summary = summary.push(text(format!("Expiries: {}", chart.config().expiry_filter)));
    }
    if snapshot.model == GexSignModel::CallPutOiProxy && snapshot.gamma_flip.is_none() {
        summary = summary.push(text("No gamma flip in the configured price range."));
    }
    if let Some(error) = chart.error() {
        summary = summary.push(text(format!("Last refresh error: {error}")));
    }

    if snapshot.strikes.is_empty() {
        return column![
            summary,
            iced::widget::center(text("No valid options for the selected expiry filter."))
        ]
        .padding(8)
        .into();
    }

    let visible = chart.visible_strikes();
    if visible.is_empty() {
        return column![
            summary,
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
        })
        .fold(0.0_f64, f64::max)
        .max(f64::EPSILON);
    let mut strike_rows = column![].spacing(2);
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
        let mut net = if chart.config().show_net_gex {
            if absolute_mode {
                format!("  abs {}", format_exposure(strike.absolute_gamma_1pct))
            } else {
                format!("  net {}", format_exposure(strike.net_gex_1pct))
            }
        } else {
            String::new()
        };
        if chart.config().show_absolute_gamma && !absolute_mode {
            net.push_str(&format!(
                "  abs {}",
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
        let proximity = |level: Option<f64>| {
            level.is_some_and(|level| {
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
                (strike.strike - level).abs() <= tolerance
            })
        };
        let mut markers = String::new();
        if chart.config().show_current_price
            && (strike.strike - snapshot.source_spot).abs() <= snapshot.source_spot * 0.005
        {
            markers.push_str(" · Spot");
        }
        if chart.config().show_call_wall && proximity(snapshot.call_wall) {
            markers.push_str(" · Call Wall");
        }
        if chart.config().show_put_wall && proximity(snapshot.put_wall) {
            markers.push_str(" · Put Wall");
        }
        if chart.config().show_gamma_flip && proximity(snapshot.gamma_flip) {
            markers.push_str(" · Gamma Flip");
        }
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
        let strike_row = row![
            left.width(Length::FillPortion(4)),
            container(space::horizontal())
                .width(1)
                .height(18)
                .style(container::bordered_box),
            right.width(Length::FillPortion(4)),
            text(format!("{:>10.2}{net}{markers}", strike.strike)).width(Length::FillPortion(3)),
        ]
        .align_y(iced::Alignment::Center)
        .spacing(2);
        let detail = container(text(format!(
            "Strike {:.2}\nCall: {}\nPut: {}\nNet: {}\nAbsolute: {}\nCall OI: {:.2}\nPut OI: {:.2}\nExpiries: {}",
            strike.strike,
            format_exposure(strike.call_gex_1pct),
            format_exposure(strike.put_gex_1pct),
            format_exposure(strike.net_gex_1pct),
            format_exposure(strike.absolute_gamma_1pct),
            strike.call_open_interest,
            strike.put_open_interest,
            strike.expiration_count,
        )))
        .padding(6)
        .style(container::rounded_box);
        strike_rows =
            strike_rows.push(tooltip(strike_row, detail, tooltip::Position::FollowCursor));
    }

    let controls = row![
        button("Zoom +").on_press(gex::Message::ZoomIn),
        button("Zoom -").on_press(gex::Message::ZoomOut),
        button("Pan ↑").on_press(gex::Message::PanUp),
        button("Pan ↓").on_press(gex::Message::PanDown),
        button("Auto-fit").on_press(gex::Message::AutoFit),
        text(if snapshot.model == GexSignModel::CallPutOiProxy {
            "Call + / Put − (OI proxy)"
        } else {
            "Absolute gamma concentration"
        }),
    ]
    .spacing(4);

    let content = column![
        summary,
        controls,
        scrollable(strike_rows).height(Length::Fill)
    ]
    .spacing(6)
    .padding(8);
    let content = mouse_area(content)
        .on_double_click(gex::Message::AutoFit)
        .on_scroll(gex::Message::Scrolled);
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

fn portion(value: f64, max: f64) -> u16 {
    ((value / max).clamp(0.0, 1.0) * 999.0).round() as u16 + 1
}

fn format_level(value: Option<f64>) -> String {
    value.map_or_else(|| "N/A".into(), |value| format!("{value:.2}"))
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
