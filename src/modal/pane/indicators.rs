use crate::screen::dashboard::pane::{self, Message};
use crate::style::{self, Icon, icon_text};
use crate::widget::{column_drag, dragger_row, labeled_slider};

use data::chart::indicator::{Indicator, KlineIndicator, UiIndicator};
use data::chart::kline::{
    BubbleColorMode, Config as KlineConfig, CvdRenderStyle, CvdReset, SessionProfileInterval,
    SessionProfileMode, SessionProfilePlacement, VolumeBubbleSession,
};
use data::layout::pane::VisualConfig;
use data::util::format_with_commas;
use iced::{
    Element, Length, padding,
    widget::{button, checkbox, column, container, pane_grid, pick_list, row, space, text},
};

pub fn view<'a, I>(
    pane: pane_grid::Pane,
    state: &'a pane::State,
    selected: &[I],
    market_type: Option<exchange::adapter::MarketKind>,
) -> Element<'a, Message>
where
    I: Indicator + Copy + Into<UiIndicator>,
{
    let content_allows_dragging = matches!(state.content, pane::Content::Kline { .. });
    let content_row = if let Some(market) = market_type {
        content_row(
            pane,
            &state.content,
            selected,
            market,
            content_allows_dragging,
        )
    } else {
        column![].spacing(4).into()
    };

    container(content_row)
        .max_width(200)
        .padding(16)
        .style(style::chart_modal)
        .into()
}

pub fn view_kline<'a>(
    pane: pane_grid::Pane,
    state: &'a pane::State,
    selected: &[KlineIndicator],
    market_type: Option<exchange::adapter::MarketKind>,
    cfg: KlineConfig,
    bubble_scale: crate::chart::kline::VolumeBubbleQtyScale,
) -> Element<'a, Message> {
    let list: Element<'a, Message> = if let Some(market) = market_type {
        content_row(pane, &state.content, selected, market, true)
    } else {
        column![].into()
    };
    let mut sections = column![list].spacing(12);

    if selected.contains(&KlineIndicator::CumulativeDelta) {
        let cvd = cfg.cvd;
        let render_style = pick_list(
            CvdRenderStyle::ALL,
            Some(cvd.render_style),
            move |render_style| {
                config_message(
                    pane,
                    KlineConfig {
                        cvd: data::chart::kline::CvdConfig {
                            render_style,
                            ..cvd
                        },
                        ..cfg
                    },
                )
            },
        );
        let candle_width = labeled_slider(
            "Candle width",
            10.0..=100.0,
            cvd.candle_width_percent,
            move |candle_width_percent| {
                config_message(
                    pane,
                    KlineConfig {
                        cvd: data::chart::kline::CvdConfig {
                            candle_width_percent,
                            ..cvd
                        },
                        ..cfg
                    },
                )
            },
            |value| format!("{value:.0}%"),
            Some(1.0),
        );
        let line_width = labeled_slider(
            "Line width",
            0.5..=5.0,
            cvd.line_width,
            move |line_width| {
                config_message(
                    pane,
                    KlineConfig {
                        cvd: data::chart::kline::CvdConfig { line_width, ..cvd },
                        ..cfg
                    },
                )
            },
            |value| format!("{value:.1}px"),
            Some(0.1),
        );
        let show_wicks =
            checkbox(cvd.show_wicks)
                .label("Show wicks")
                .on_toggle(move |show_wicks| {
                    config_message(
                        pane,
                        KlineConfig {
                            cvd: data::chart::kline::CvdConfig { show_wicks, ..cvd },
                            ..cfg
                        },
                    )
                });
        let reset = pick_list(CvdReset::ALL, Some(cvd.reset), move |reset| {
            config_message(
                pane,
                KlineConfig {
                    cvd: data::chart::kline::CvdConfig { reset, ..cvd },
                    ..cfg
                },
            )
        });
        let style_controls: Element<'a, Message> = match cvd.render_style {
            CvdRenderStyle::Candlesticks => column![candle_width, show_wicks].spacing(6).into(),
            CvdRenderStyle::Line => column![line_width].spacing(6).into(),
        };
        sections = sections.push(indicator_card(
            "Cumulative Volume Delta",
            column![render_style, reset, style_controls].spacing(6),
        ));
    }

    if selected.contains(&KlineIndicator::SessionVolumeProfile) {
        let svp = cfg.session_volume_profile;
        let interval =
            pick_list(
                SessionProfileInterval::ALL,
                Some(svp.interval),
                move |interval| {
                    config_message(
                        pane,
                        KlineConfig {
                            session_volume_profile:
                                data::chart::kline::SessionVolumeProfileConfig { interval, ..svp },
                            ..cfg
                        },
                    )
                },
            );
        let placement =
            pick_list(
                SessionProfilePlacement::ALL,
                Some(svp.placement),
                move |placement| {
                    config_message(
                        pane,
                        KlineConfig {
                            session_volume_profile:
                                data::chart::kline::SessionVolumeProfileConfig { placement, ..svp },
                            ..cfg
                        },
                    )
                },
            );
        let mode = pick_list(SessionProfileMode::ALL, Some(svp.mode), move |mode| {
            config_message(
                pane,
                KlineConfig {
                    session_volume_profile: data::chart::kline::SessionVolumeProfileConfig {
                        mode,
                        ..svp
                    },
                    ..cfg
                },
            )
        });
        let width = labeled_slider(
            "Width",
            10.0..=90.0,
            svp.width_percent,
            move |width_percent| {
                config_message(
                    pane,
                    KlineConfig {
                        session_volume_profile: data::chart::kline::SessionVolumeProfileConfig {
                            width_percent,
                            ..svp
                        },
                        ..cfg
                    },
                )
            },
            |v| format!("{v:.0}%"),
            Some(1.0),
        );
        let value_area = labeled_slider(
            "Value area",
            50.0..=95.0,
            svp.value_area_percent,
            move |value_area_percent| {
                config_message(
                    pane,
                    KlineConfig {
                        session_volume_profile: data::chart::kline::SessionVolumeProfileConfig {
                            value_area_percent,
                            ..svp
                        },
                        ..cfg
                    },
                )
            },
            |v| format!("{v:.0}%"),
            Some(1.0),
        );
        let rows = labeled_slider(
            "Ticks / row",
            1.0..=50.0,
            svp.row_size_ticks as f32,
            move |v| {
                config_message(
                    pane,
                    KlineConfig {
                        session_volume_profile: data::chart::kline::SessionVolumeProfileConfig {
                            row_size_ticks: v as u16,
                            ..svp
                        },
                        ..cfg
                    },
                )
            },
            |v| format!("{v:.0}"),
            Some(1.0),
        );
        let poc =
            checkbox(svp.show_poc)
                .label("POC")
                .on_toggle(move |show_poc| {
                    config_message(
                        pane,
                        KlineConfig {
                            session_volume_profile:
                                data::chart::kline::SessionVolumeProfileConfig { show_poc, ..svp },
                            ..cfg
                        },
                    )
                });
        let va =
            checkbox(svp.show_value_area)
                .label("VAH / VAL")
                .on_toggle(move |show_value_area| {
                    config_message(
                        pane,
                        KlineConfig {
                            session_volume_profile:
                                data::chart::kline::SessionVolumeProfileConfig {
                                    show_value_area,
                                    ..svp
                                },
                            ..cfg
                        },
                    )
                });
        let vwap =
            checkbox(svp.show_vwap)
                .label("Session VWAP level")
                .on_toggle(move |show_vwap| {
                    config_message(
                        pane,
                        KlineConfig {
                            session_volume_profile:
                                data::chart::kline::SessionVolumeProfileConfig { show_vwap, ..svp },
                            ..cfg
                        },
                    )
                });
        let hi_lo = checkbox(svp.show_session_high_low)
            .label("Session high / low")
            .on_toggle(move |show_session_high_low| {
                config_message(
                    pane,
                    KlineConfig {
                        session_volume_profile: data::chart::kline::SessionVolumeProfileConfig {
                            show_session_high_low,
                            ..svp
                        },
                        ..cfg
                    },
                )
            });
        sections = sections.push(indicator_card(
            "Session Volume Profile",
            column![
                interval,
                placement,
                mode,
                width,
                value_area,
                rows,
                row![poc, va].spacing(8),
                vwap,
                hi_lo
            ]
            .spacing(6),
        ));
    }

    if selected.contains(&KlineIndicator::VolumeBubbles) {
        let bubbles = cfg.volume_bubbles;
        let session = pick_list(
            VolumeBubbleSession::ALL,
            Some(bubbles.session),
            move |session| {
                config_message(
                    pane,
                    KlineConfig {
                        volume_bubbles: data::chart::kline::VolumeBubbleConfig {
                            session,
                            ..bubbles
                        },
                        ..cfg
                    },
                )
            },
        );
        let mode = pick_list(
            BubbleColorMode::ALL,
            Some(bubbles.color_mode),
            move |color_mode| {
                config_message(
                    pane,
                    KlineConfig {
                        volume_bubbles: data::chart::kline::VolumeBubbleConfig {
                            color_mode,
                            ..bubbles
                        },
                        ..cfg
                    },
                )
            },
        );
        let count = labeled_slider(
            "Max / candle",
            1.0..=10.0,
            bubbles.max_bubbles_per_bar as f32,
            move |v| {
                config_message(
                    pane,
                    KlineConfig {
                        volume_bubbles: data::chart::kline::VolumeBubbleConfig {
                            max_bubbles_per_bar: v as usize,
                            ..bubbles
                        },
                        ..cfg
                    },
                )
            },
            |v| format!("{v:.0}"),
            Some(1.0),
        );
        let candidates = labeled_slider(
            "Historical candidates",
            1.0..=20.0,
            bubbles.max_candidates_per_candle as f32,
            move |v| {
                config_message(
                    pane,
                    KlineConfig {
                        volume_bubbles: data::chart::kline::VolumeBubbleConfig {
                            max_candidates_per_candle: v as usize,
                            ..bubbles
                        },
                        ..cfg
                    },
                )
            },
            |v| format!("{v:.0}"),
            Some(1.0),
        );
        let history = labeled_slider(
            "History window",
            1.0..=120.0,
            bubbles.history_window_minutes as f32,
            move |v| {
                config_message(
                    pane,
                    KlineConfig {
                        volume_bubbles: data::chart::kline::VolumeBubbleConfig {
                            history_window_minutes: v as u64,
                            ..bubbles
                        },
                        ..cfg
                    },
                )
            },
            |v| format!("{v:.0}m"),
            Some(1.0),
        );
        let min_qty = labeled_slider(
            "Minimum volume",
            bubble_scale.min..=bubble_scale.max,
            bubbles.min_qty.clamp(bubble_scale.min, bubble_scale.max),
            move |min_qty| {
                config_message(
                    pane,
                    KlineConfig {
                        volume_bubbles: data::chart::kline::VolumeBubbleConfig {
                            min_qty,
                            ..bubbles
                        },
                        ..cfg
                    },
                )
            },
            |v| format_with_commas(*v),
            Some(bubble_scale.step),
        );
        let min_radius = labeled_slider(
            "Minimum radius",
            1.0..=20.0,
            bubbles.min_radius_px,
            move |min_radius_px| {
                config_message(
                    pane,
                    KlineConfig {
                        volume_bubbles: data::chart::kline::VolumeBubbleConfig {
                            min_radius_px,
                            ..bubbles
                        },
                        ..cfg
                    },
                )
            },
            |v| format!("{v:.0}px"),
            Some(1.0),
        );
        let max_radius = labeled_slider(
            "Maximum radius",
            4.0..=40.0,
            bubbles.max_radius_px,
            move |max_radius_px| {
                config_message(
                    pane,
                    KlineConfig {
                        volume_bubbles: data::chart::kline::VolumeBubbleConfig {
                            max_radius_px,
                            ..bubbles
                        },
                        ..cfg
                    },
                )
            },
            |v| format!("{v:.0}px"),
            Some(1.0),
        );
        let labels = checkbox(bubbles.show_labels)
            .label("Labels")
            .on_toggle(move |show_labels| {
                config_message(
                    pane,
                    KlineConfig {
                        volume_bubbles: data::chart::kline::VolumeBubbleConfig {
                            show_labels,
                            ..bubbles
                        },
                        ..cfg
                    },
                )
            });
        let reuse = checkbox(bubbles.use_raw_trades_when_available)
            .label("Reuse shared raw trades")
            .on_toggle(move |use_raw_trades_when_available| {
                config_message(
                    pane,
                    KlineConfig {
                        volume_bubbles: data::chart::kline::VolumeBubbleConfig {
                            use_raw_trades_when_available,
                            ..bubbles
                        },
                        ..cfg
                    },
                )
            });
        sections = sections.push(indicator_card(
            "Volume Bubbles",
            column![
                session, mode, count, candidates, history, min_qty, min_radius, max_radius, labels,
                reuse
            ]
            .spacing(6),
        ));
    }

    if selected.contains(&KlineIndicator::Vwap) {
        let vwap = cfg.vwap;
        let anchor = pick_list(
            SessionProfileInterval::ALL,
            Some(vwap.anchor),
            move |anchor| {
                config_message(
                    pane,
                    KlineConfig {
                        vwap: data::chart::kline::VwapConfig { anchor, ..vwap },
                        ..cfg
                    },
                )
            },
        );
        let width = labeled_slider(
            "Line width",
            0.5..=5.0,
            vwap.line_width,
            move |line_width| {
                config_message(
                    pane,
                    KlineConfig {
                        vwap: data::chart::kline::VwapConfig { line_width, ..vwap },
                        ..cfg
                    },
                )
            },
            |v| format!("{v:.1}px"),
            Some(0.1),
        );
        let multiplier = labeled_slider(
            "Band multiplier",
            0.25..=3.0,
            vwap.band_multiplier,
            move |band_multiplier| {
                config_message(
                    pane,
                    KlineConfig {
                        vwap: data::chart::kline::VwapConfig {
                            band_multiplier,
                            ..vwap
                        },
                        ..cfg
                    },
                )
            },
            |v| format!("{v:.2}σ"),
            Some(0.25),
        );
        let bands = checkbox(vwap.show_bands)
            .label("Standard-deviation bands")
            .on_toggle(move |show_bands| {
                config_message(
                    pane,
                    KlineConfig {
                        vwap: data::chart::kline::VwapConfig { show_bands, ..vwap },
                        ..cfg
                    },
                )
            });
        let labels = checkbox(vwap.show_labels)
            .label("Labels")
            .on_toggle(move |show_labels| {
                config_message(
                    pane,
                    KlineConfig {
                        vwap: data::chart::kline::VwapConfig {
                            show_labels,
                            ..vwap
                        },
                        ..cfg
                    },
                )
            });
        sections = sections.push(indicator_card(
            "VWAP",
            column![anchor, width, multiplier, bands, labels].spacing(6),
        ));
    }

    if selected.contains(&KlineIndicator::GexLevels) {
        use data::chart::gex::{
            GexBasisMode, GexExpiryFilter, GexLevelColor, GexLevelsConfig, GexSignModel,
        };

        let levels = cfg.gex_levels();
        let update = move |next: GexLevelsConfig| config_message(pane, cfg.with_gex_levels(next));
        let model = pick_list(
            GexSignModel::ALL,
            Some(levels.enabled_model),
            move |enabled_model| {
                update(GexLevelsConfig {
                    enabled_model,
                    ..levels
                })
            },
        );
        let expiry = pick_list(
            GexExpiryFilter::ALL,
            Some(levels.expiry_filter),
            move |expiry_filter| {
                update(GexLevelsConfig {
                    expiry_filter,
                    ..levels
                })
            },
        );
        let basis = pick_list(
            GexBasisMode::ALL,
            Some(levels.basis_mode),
            move |basis_mode| {
                update(GexLevelsConfig {
                    basis_mode,
                    ..levels
                })
            },
        );
        let max_clusters = labeled_slider(
            "Maximum clusters",
            0.0..=10.0,
            levels.max_clusters as f32,
            move |value| {
                update(GexLevelsConfig {
                    max_clusters: value as usize,
                    ..levels
                })
            },
            |value| format!("{value:.0}"),
            Some(1.0),
        );
        let band_width = labeled_slider(
            "Band half-width",
            0.1..=1.5,
            levels.cluster_band_width,
            move |cluster_band_width| {
                update(GexLevelsConfig {
                    cluster_band_width,
                    ..levels
                })
            },
            |value| format!("{value:.1} × strike gap"),
            Some(0.1),
        );
        let line_width = labeled_slider(
            "Level line width",
            0.5..=3.0,
            levels.line_width,
            move |line_width| {
                update(GexLevelsConfig {
                    line_width,
                    ..levels
                })
            },
            |value| format!("{value:.1}px"),
            Some(0.1),
        );
        let flip_width = labeled_slider(
            "Gamma Flip width",
            1.0..=4.0,
            levels.gamma_flip_width,
            move |gamma_flip_width| {
                update(GexLevelsConfig {
                    gamma_flip_width,
                    ..levels
                })
            },
            |value| format!("{value:.1}px"),
            Some(0.1),
        );
        let line_opacity = labeled_slider(
            "Line opacity",
            0.1..=1.0,
            levels.line_opacity,
            move |line_opacity| {
                update(GexLevelsConfig {
                    line_opacity,
                    ..levels
                })
            },
            |value| format!("{:.0}%", value * 100.0),
            Some(0.05),
        );
        let band_opacity = labeled_slider(
            "Band opacity",
            0.02..=0.4,
            levels.band_opacity,
            move |band_opacity| {
                update(GexLevelsConfig {
                    band_opacity,
                    ..levels
                })
            },
            |value| format!("{:.0}%", value * 100.0),
            Some(0.02),
        );
        let toggle =
            |label: &'static str, current: bool, change: fn(&mut GexLevelsConfig, bool)| {
                checkbox(current).label(label).on_toggle(move |value| {
                    let mut next = levels;
                    change(&mut next, value);
                    update(next)
                })
            };
        let colors = column![
            row![
                text("Gamma Flip"),
                pick_list(
                    GexLevelColor::ALL,
                    Some(levels.gamma_flip_color),
                    move |gamma_flip_color| update(GexLevelsConfig {
                        gamma_flip_color,
                        ..levels
                    })
                )
            ]
            .spacing(8),
            row![
                text("Call / Put Walls"),
                pick_list(
                    GexLevelColor::ALL,
                    Some(levels.call_wall_color),
                    move |call_wall_color| update(GexLevelsConfig {
                        call_wall_color,
                        ..levels
                    })
                ),
                pick_list(
                    GexLevelColor::ALL,
                    Some(levels.put_wall_color),
                    move |put_wall_color| update(GexLevelsConfig {
                        put_wall_color,
                        ..levels
                    })
                )
            ]
            .spacing(8),
            row![
                text("Clusters"),
                pick_list(
                    GexLevelColor::ALL,
                    Some(levels.cluster_color),
                    move |cluster_color| update(GexLevelsConfig {
                        cluster_color,
                        ..levels
                    })
                )
            ]
            .spacing(8),
        ]
        .spacing(4);
        sections = sections.push(indicator_card(
            "GEX Levels",
            column![
                text("Uses the shared options snapshot; settings never trigger a chain fetch.")
                    .size(crate::style::text_size::SMALL),
                text("Model"),
                model,
                text("Expiry filter"),
                expiry,
                text("Price basis"),
                basis,
                toggle("Gamma Flip", levels.show_gamma_flip, |c, v| c
                    .show_gamma_flip =
                    v),
                toggle("Call Wall", levels.show_call_wall, |c, v| c
                    .show_call_wall =
                    v),
                toggle("Put Wall", levels.show_put_wall, |c, v| c.show_put_wall = v),
                toggle("Gamma Clusters", levels.show_top_clusters, |c, v| c
                    .show_top_clusters =
                    v),
                max_clusters,
                toggle("Clusters as bands", levels.clusters_as_bands, |c, v| c
                    .clusters_as_bands =
                    v),
                band_width,
                toggle("Show value", levels.show_value, |c, v| c.show_value = v),
                toggle(
                    "Show distance from price",
                    levels.show_distance_percent,
                    |c, v| c.show_distance_percent = v
                ),
                line_width,
                flip_width,
                line_opacity,
                band_opacity,
                text("Palette roles"),
                colors,
            ]
            .spacing(6),
        ));
    }

    container(crate::widget::scrollable_content(sections))
        .max_width(340)
        .padding(16)
        .style(style::chart_modal)
        .into()
}

fn config_message(pane: pane_grid::Pane, cfg: KlineConfig) -> Message {
    Message::VisualConfigChanged(pane, VisualConfig::Kline(cfg), false)
}

fn indicator_card<'a>(
    title: &'a str,
    content: impl Into<Element<'a, Message>>,
) -> Element<'a, Message> {
    let content: Element<'a, Message> = content.into();
    container(column![text(title).size(crate::style::text_size::SECTION), content].spacing(8))
        .padding(10)
        .style(style::chart_modal)
        .into()
}

fn build_indicator_row<'a, I>(
    pane: pane_grid::Pane,
    indicator: &I,
    is_selected: bool,
) -> Element<'a, Message>
where
    I: Indicator + Copy + Into<UiIndicator>,
{
    let content = if is_selected {
        row![
            text(indicator.to_string()),
            space::horizontal(),
            container(icon_text(Icon::Checkmark, 12)),
        ]
        .width(Length::Fill)
    } else {
        row![text(indicator.to_string())].width(Length::Fill)
    };

    button(content)
        .on_press(Message::PaneEvent(
            pane,
            pane::Event::ToggleIndicator((*indicator).into()),
        ))
        .width(Length::Fill)
        .style(move |theme, status| style::button::modifier(theme, status, is_selected))
        .into()
}

fn selected_list<'a, I>(
    pane: pane_grid::Pane,
    selected: &[I],
    reorderable: bool,
) -> Element<'a, Message>
where
    I: Indicator + Copy + Into<UiIndicator>,
{
    let elements: Vec<Element<_>> = selected
        .iter()
        .map(|indicator| {
            let base = build_indicator_row(pane, indicator, true);
            dragger_row(base, reorderable)
        })
        .collect();

    if reorderable {
        let mut draggable_column = column_drag::Column::new()
            .on_drag(move |event| Message::PaneEvent(pane, pane::Event::ReorderIndicator(event)))
            .spacing(4);
        for element in elements {
            draggable_column = draggable_column.push(element);
        }
        draggable_column.into()
    } else {
        iced::widget::Column::with_children(elements)
            .spacing(4)
            .into()
    }
}

fn available_list<'a, I>(pane: pane_grid::Pane, available: &[I]) -> Element<'a, Message>
where
    I: Indicator + Copy + Into<UiIndicator>,
{
    let elements: Vec<Element<_>> = available
        .iter()
        .map(|indicator| {
            let base = build_indicator_row(pane, indicator, false);
            dragger_row(base, false)
        })
        .collect();

    iced::widget::Column::with_children(elements)
        .spacing(4)
        .into()
}

fn content_row<'a, I>(
    pane: pane_grid::Pane,
    content: &pane::Content,
    selected: &[I],
    market: exchange::adapter::MarketKind,
    allows_drag: bool,
) -> Element<'a, Message>
where
    I: Indicator + Copy + Into<UiIndicator>,
{
    let reorderable = allows_drag && selected.len() >= 2;

    let selected: Vec<I> = selected
        .iter()
        .copied()
        .filter(|indicator| content.allows_indicator((*indicator).into()))
        .collect();

    let selected_list = if !selected.is_empty() {
        Some(selected_list(pane, &selected, reorderable))
    } else {
        None
    };

    let available: Vec<I> = I::for_market(market)
        .iter()
        .filter(|indicator| {
            !selected.contains(indicator) && content.allows_indicator((**indicator).into())
        })
        .cloned()
        .collect();
    let available_list = if !available.is_empty() {
        Some(available_list(pane, &available))
    } else {
        None
    };

    let mut col = iced::widget::Column::new();
    if let Some(sel) = selected_list {
        col = col.push(sel);
    }
    if let Some(avail) = available_list {
        col = col.push(avail);
    }

    column![
        container(text("Indicators").size(crate::style::text_size::SECTION))
            .padding(padding::bottom(8)),
        col.spacing(4)
    ]
    .spacing(4)
    .into()
}
