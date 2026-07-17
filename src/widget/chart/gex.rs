use crate::{chart::gex, style};
use data::chart::gex::{GexFreshness, GexSignModel, GexStrike};
use iced::{
    Alignment, Color, Element, Length, Point, Rectangle, Renderer, Size, Theme, mouse,
    widget::{button, canvas, column, container, mouse_area, responsive, row, space, text},
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
}

fn view_sized(chart: &gex::GexChart, density: GexLayoutDensity) -> Element<'_, gex::Message> {
    let Some(snapshot) = chart.snapshot() else {
        return iced::widget::center(text(if chart.freshness() == GexFreshness::Error {
            "GEX data unavailable."
        } else {
            "Waiting for GEX data..."
        }))
        .into();
    };
    let visible = chart.visible_strikes();
    if visible.is_empty() {
        return iced::widget::center(text("No strikes in the configured range.")).into();
    }

    let mut zoom_in = button("Zoom +")
        .padding([3, 7])
        .style(iced::widget::button::secondary);
    if chart.can_zoom_in() {
        zoom_in = zoom_in.on_press(gex::Message::ZoomIn);
    }
    let mut zoom_out = button("Zoom −")
        .padding([3, 7])
        .style(iced::widget::button::secondary);
    if chart.can_zoom_out() {
        zoom_out = zoom_out.on_press(gex::Message::ZoomOut);
    }
    let controls = row![zoom_in, zoom_out].spacing(4);
    let header = header_view(chart, snapshot, density);
    let table = canvas(GexProfileTable {
        snapshot,
        strikes: visible,
        density,
        config: chart.config(),
    })
    .width(Length::Fill)
    .height(Length::Fill);
    let table = mouse_area(table)
        .on_double_click(gex::Message::AutoFit)
        .on_scroll(gex::Message::Scrolled)
        .on_press(gex::Message::DragStarted)
        .on_move(gex::Message::Dragged)
        .on_release(gex::Message::DragEnded)
        .interaction(if chart.is_dragging() {
            iced::mouse::Interaction::Grabbing
        } else {
            iced::mouse::Interaction::Grab
        });
    let padding = if density == GexLayoutDensity::Full {
        8
    } else {
        4
    };
    column![header, controls, table]
        .spacing(4)
        .padding(padding)
        .into()
}

fn header_view<'a>(
    chart: &gex::GexChart,
    snapshot: &data::chart::gex::GexSnapshot,
    _density: GexLayoutDensity,
) -> Element<'a, gex::Message> {
    let cfg = chart.config();
    if !cfg.show_summary {
        return space::vertical().height(0).into();
    }
    let status = match chart.freshness() {
        GexFreshness::Loading => "Loading",
        GexFreshness::Fresh => "Fresh",
        GexFreshness::Stale => "Stale",
        GexFreshness::Expired => "Expired",
        GexFreshness::Error => "Error",
    };
    let abnormal = chart.freshness() != GexFreshness::Fresh;
    let mut fields: Vec<Element<'a, gex::Message>> = Vec::new();
    let mut push = |label: &'static str, value: String| {
        fields.push(
            container(
                row![
                    text(label).size(style::text_size::SMALL),
                    text(value)
                        .size(style::text_size::SMALL)
                        .wrapping(iced::widget::text::Wrapping::None)
                ]
                .spacing(4)
                .align_y(Alignment::Center),
            )
            .padding([2, 5])
            .style(container::rounded_box)
            .into(),
        );
    };
    if cfg.show_header_net_gex {
        push(
            "Net",
            snapshot
                .net_gex_1pct
                .map(format_exposure)
                .unwrap_or_else(|| "N/A".into()),
        );
    }
    if cfg.show_header_absolute_gex {
        push("Abs", format_exposure(snapshot.absolute_gex_1pct));
    }
    if cfg.show_header_gamma_flip {
        push("GF", format_level(snapshot.gamma_flip));
    }
    if cfg.show_header_call_wall {
        push("CW", format_level(snapshot.call_wall));
    }
    if cfg.show_header_put_wall {
        push("PW", format_level(snapshot.put_wall));
    }
    if cfg.show_header_model {
        push(
            "Model",
            if snapshot.model == GexSignModel::CallPutOiProxy {
                "OI Proxy".into()
            } else {
                "Absolute".into()
            },
        );
    }
    if cfg.show_header_expiry {
        push("Expiry", cfg.expiry_filter.to_string());
    }
    if cfg.show_header_freshness {
        push("●", status.into());
    }
    if cfg.show_header_snapshot || abnormal {
        push(
            "At",
            snapshot
                .observed_at
                .format_utc("%H:%M:%S")
                .unwrap_or_else(|| "unknown".into()),
        );
    }
    if abnormal && let Some(error) = chart.error() {
        push("!", error.to_string());
    }
    row(fields).spacing(4).align_y(Alignment::Center).into()
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct GexTableColumns {
    put_bounds: Rectangle,
    zero_x: f32,
    call_bounds: Rectangle,
    strike_bounds: Rectangle,
    net_bounds: Option<Rectangle>,
    abs_bounds: Option<Rectangle>,
    level_bounds: Rectangle,
}

fn table_columns(width: f32, density: GexLayoutDensity) -> GexTableColumns {
    let usable = (width - 10.0).max(120.0);
    let (bars, strike, net, abs) = match density {
        GexLayoutDensity::Full => (0.42, 0.13, 0.13, 0.13),
        GexLayoutDensity::Compact => (0.50, 0.16, 0.15, 0.0),
        GexLayoutDensity::Minimal => (0.60, 0.20, 0.0, 0.0),
    };
    let bar_width = usable * bars;
    let half = bar_width * 0.5;
    let strike_x = bar_width;
    let strike_width = usable * strike;
    let net_width = usable * net;
    let abs_width = usable * abs;
    let level_x = strike_x + strike_width + net_width + abs_width;
    GexTableColumns {
        put_bounds: Rectangle::new(Point::ORIGIN, Size::new(half, 0.0)),
        zero_x: half,
        call_bounds: Rectangle::new(Point::new(half, 0.0), Size::new(half, 0.0)),
        strike_bounds: Rectangle::new(Point::new(strike_x, 0.0), Size::new(strike_width, 0.0)),
        net_bounds: (net > 0.0).then(|| {
            Rectangle::new(
                Point::new(strike_x + strike_width, 0.0),
                Size::new(net_width, 0.0),
            )
        }),
        abs_bounds: (abs > 0.0).then(|| {
            Rectangle::new(
                Point::new(strike_x + strike_width + net_width, 0.0),
                Size::new(abs_width, 0.0),
            )
        }),
        level_bounds: Rectangle::new(
            Point::new(level_x, 0.0),
            Size::new((usable - level_x).max(30.0), 0.0),
        ),
    }
}

struct GexProfileTable<'a> {
    snapshot: &'a data::chart::gex::GexSnapshot,
    strikes: &'a [GexStrike],
    density: GexLayoutDensity,
    config: &'a data::chart::gex::Config,
}

impl canvas::Program<gex::Message> for GexProfileTable<'_> {
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
        cursor: mouse::Cursor,
    ) -> Vec<canvas::Geometry> {
        let palette = theme.extended_palette();
        let mut frame = canvas::Frame::new(renderer, bounds.size());
        let columns = table_columns(bounds.width, self.density);
        let header_h = 22.0;
        let row_h = ((bounds.height - header_h) / self.strikes.len() as f32).clamp(10.0, 22.0);
        let table_bottom = (header_h + row_h * self.strikes.len() as f32).min(bounds.height);
        let max_gex = self
            .strikes
            .iter()
            .map(|s| {
                if self.snapshot.model == GexSignModel::AbsoluteGamma {
                    s.absolute_gamma_1pct
                } else {
                    s.call_gex_1pct.abs().max(s.put_gex_1pct.abs())
                }
            })
            .fold(f64::EPSILON, f64::max);
        let row_centers = (0..self.strikes.len())
            .map(|index| header_h + (self.strikes.len() - index) as f32 * row_h - row_h * 0.5)
            .collect::<Vec<_>>();
        let hovered = cursor.position_in(bounds).and_then(|point| {
            (point.y >= header_h && point.y < table_bottom)
                .then(|| ((point.y - header_h) / row_h) as usize)
                .filter(|index| *index < self.strikes.len())
                .map(|visual| self.strikes.len() - 1 - visual)
        });

        draw_table_header(&mut frame, &columns, max_gex, palette.background.base.text);
        for (index, strike) in self.strikes.iter().enumerate().rev() {
            let y = row_centers[index];
            let top = y - row_h * 0.5;
            let is_call = self.snapshot.call_wall == Some(strike.strike);
            let is_put = self.snapshot.put_wall == Some(strike.strike);
            if hovered == Some(index) || is_call || is_put {
                let color = if is_call {
                    palette.success.strong.color
                } else if is_put {
                    palette.danger.strong.color
                } else {
                    palette.background.base.text
                };
                frame.fill(
                    &canvas::Path::rectangle(
                        Point::new(0.0, top),
                        Size::new(columns.level_bounds.x + columns.level_bounds.width, row_h),
                    ),
                    color.scale_alpha(if hovered == Some(index) { 0.09 } else { 0.06 }),
                );
            }
            draw_strike_row(
                &mut frame,
                &columns,
                strike,
                y,
                row_h,
                max_gex,
                self.density,
                is_call,
                is_put,
                self.snapshot.model == GexSignModel::AbsoluteGamma,
                palette,
            );
        }
        draw_references(
            &mut frame,
            &columns,
            self.strikes,
            &row_centers,
            self.config
                .show_current_price
                .then_some(self.snapshot.source_spot),
            self.config
                .show_gamma_flip
                .then_some(self.snapshot.gamma_flip)
                .flatten(),
            self.density,
            palette,
        );
        let (zero_start, zero_end) = zero_line_points(&columns, table_bottom);
        frame.stroke(
            &canvas::Path::line(zero_start, zero_end),
            canvas::Stroke::default()
                .with_color(palette.background.base.text.scale_alpha(0.7))
                .with_width(1.2),
        );
        if let (Some(index), Some(point)) = (hovered, cursor.position_in(bounds)) {
            draw_hover(
                &mut frame,
                bounds.size(),
                point,
                &self.strikes[index],
                self.snapshot.source_spot,
                palette,
            );
        }
        vec![frame.into_geometry()]
    }
}

fn zero_line_points(columns: &GexTableColumns, table_bottom: f32) -> (Point, Point) {
    (
        Point::new(columns.zero_x, 0.0),
        Point::new(columns.zero_x, table_bottom),
    )
}

fn draw_table_header(
    frame: &mut canvas::Frame,
    columns: &GexTableColumns,
    max_gex: f64,
    color: Color,
) {
    let (negative_x, zero_x, positive_x) = scale_label_positions(columns);
    draw_text_at(
        frame,
        &format!("−{}", compact_abs(max_gex)),
        negative_x,
        11.0,
        9.0,
        color,
        iced::alignment::Horizontal::Left,
    );
    draw_text_at(
        frame,
        "0",
        zero_x,
        11.0,
        11.0,
        color,
        iced::alignment::Horizontal::Center,
    );
    draw_text_at(
        frame,
        &format!("+{}", compact_abs(max_gex)),
        positive_x,
        11.0,
        9.0,
        color,
        iced::alignment::Horizontal::Right,
    );
    draw_cell_text(frame, "Strike", columns.strike_bounds, 11.0, color);
    if let Some(bounds) = columns.net_bounds {
        draw_cell_text(frame, "Net", bounds, 11.0, color);
    }
    if let Some(bounds) = columns.abs_bounds {
        draw_cell_text(frame, "Abs", bounds, 11.0, color);
    }
    draw_cell_text(frame, "Level", columns.level_bounds, 11.0, color);
}

fn scale_label_positions(columns: &GexTableColumns) -> (f32, f32, f32) {
    (
        columns.put_bounds.x + 3.0,
        columns.zero_x,
        columns.call_bounds.x + columns.call_bounds.width - 3.0,
    )
}

#[allow(clippy::too_many_arguments)]
fn draw_strike_row(
    frame: &mut canvas::Frame,
    columns: &GexTableColumns,
    strike: &GexStrike,
    y: f32,
    row_h: f32,
    max_gex: f64,
    density: GexLayoutDensity,
    call_wall: bool,
    put_wall: bool,
    absolute_mode: bool,
    palette: &iced::theme::palette::Extended,
) {
    let put_width = if absolute_mode {
        0.0
    } else {
        (strike.put_gex_1pct.abs() / max_gex) as f32 * columns.put_bounds.width
    };
    let call_width = (if absolute_mode {
        strike.absolute_gamma_1pct
    } else {
        strike.call_gex_1pct.abs()
    } / max_gex) as f32
        * columns.call_bounds.width;
    let bar_h = (row_h * 0.38).clamp(3.0, 7.0);
    frame.fill(
        &canvas::Path::rectangle(
            Point::new(columns.zero_x - put_width, y - bar_h * 0.5),
            Size::new(put_width, bar_h),
        ),
        palette.danger.strong.color,
    );
    frame.fill(
        &canvas::Path::rectangle(
            Point::new(columns.zero_x, y - bar_h * 0.5),
            Size::new(call_width, bar_h),
        ),
        palette.success.strong.color,
    );
    draw_cell_text(
        frame,
        &format!("{:.2}", strike.strike),
        columns.strike_bounds,
        y,
        palette.background.base.text,
    );
    if let Some(bounds) = columns.net_bounds {
        draw_cell_text(
            frame,
            &format_exposure(strike.net_gex_1pct),
            bounds,
            y,
            palette.background.base.text,
        );
    }
    if let Some(bounds) = columns.abs_bounds {
        draw_cell_text(
            frame,
            &format_exposure(strike.absolute_gamma_1pct),
            bounds,
            y,
            palette.background.base.text,
        );
    }
    let level = match (call_wall, put_wall) {
        (true, true) => {
            if density == GexLayoutDensity::Minimal {
                "CW/PW"
            } else {
                "Call / Put Wall"
            }
        }
        (true, false) => {
            if density == GexLayoutDensity::Minimal {
                "CW"
            } else {
                "Call Wall"
            }
        }
        (false, true) => {
            if density == GexLayoutDensity::Minimal {
                "PW"
            } else {
                "Put Wall"
            }
        }
        _ => "",
    };
    draw_cell_text(
        frame,
        level,
        columns.level_bounds,
        y,
        palette.background.base.text,
    );
}

fn reference_y_for_price(
    price: f64,
    visible_strikes: &[GexStrike],
    row_centers: &[f32],
) -> Option<f32> {
    if visible_strikes.len() != row_centers.len() || visible_strikes.is_empty() {
        return None;
    }
    if price < visible_strikes.first()?.strike || price > visible_strikes.last()?.strike {
        return None;
    }
    match visible_strikes.binary_search_by(|strike| strike.strike.total_cmp(&price)) {
        Ok(index) => Some(row_centers[index]),
        Err(upper) if upper > 0 && upper < visible_strikes.len() => {
            let lower = upper - 1;
            let low = visible_strikes[lower].strike;
            let high = visible_strikes[upper].strike;
            let fraction = ((price - low) / (high - low)) as f32;
            Some(row_centers[lower] + (row_centers[upper] - row_centers[lower]) * fraction)
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn draw_references(
    frame: &mut canvas::Frame,
    columns: &GexTableColumns,
    strikes: &[GexStrike],
    centers: &[f32],
    spot: Option<f64>,
    flip: Option<f64>,
    density: GexLayoutDensity,
    palette: &iced::theme::palette::Extended,
) {
    let spot_y = spot.and_then(|price| reference_y_for_price(price, strikes, centers));
    let flip_y = flip.and_then(|price| reference_y_for_price(price, strikes, centers));
    if let (Some(sy), Some(fy), Some(spot), Some(flip)) = (spot_y, flip_y, spot, flip)
        && (sy - fy).abs() < 9.0
    {
        draw_reference_band(
            frame,
            columns,
            (sy + fy) * 0.5,
            &format!(
                "{} {:.2} · {} {:.2}",
                if density == GexLayoutDensity::Minimal {
                    "S"
                } else {
                    "Spot"
                },
                spot,
                if density == GexLayoutDensity::Minimal {
                    "GF"
                } else {
                    "Gamma Flip"
                },
                flip
            ),
            palette.warning.strong.color,
        );
        return;
    }
    if let (Some(y), Some(price)) = (spot_y, spot) {
        draw_reference_band(
            frame,
            columns,
            y,
            &format!(
                "{} {:.2}",
                if density == GexLayoutDensity::Minimal {
                    "S"
                } else {
                    "Spot"
                },
                price
            ),
            palette.primary.strong.color,
        );
    }
    if let (Some(y), Some(price)) = (flip_y, flip) {
        draw_reference_band(
            frame,
            columns,
            y,
            &format!(
                "{} {:.2}",
                if density == GexLayoutDensity::Minimal {
                    "GF"
                } else {
                    "Gamma Flip"
                },
                price
            ),
            palette.warning.strong.color,
        );
    }
}

fn draw_reference_band(
    frame: &mut canvas::Frame,
    columns: &GexTableColumns,
    y: f32,
    label: &str,
    color: Color,
) {
    frame.fill(
        &canvas::Path::rectangle(
            Point::new(0.0, y - 1.5),
            Size::new(columns.call_bounds.x + columns.call_bounds.width, 3.0),
        ),
        color.scale_alpha(0.22),
    );
    frame.stroke(
        &canvas::Path::line(
            Point::new(0.0, y),
            Point::new(columns.call_bounds.x + columns.call_bounds.width, y),
        ),
        canvas::Stroke::default()
            .with_color(color.scale_alpha(0.8))
            .with_width(0.8),
    );
    draw_cell_text(frame, label, columns.level_bounds, y, color);
}

fn hover_bounds(cursor: Point, tooltip: Size, chart: Size) -> Rectangle {
    let gap = 12.0;
    let mut x = cursor.x + gap;
    if x + tooltip.width > chart.width {
        x = cursor.x - gap - tooltip.width;
    }
    let mut y = cursor.y + gap;
    if y + tooltip.height > chart.height {
        y = cursor.y - gap - tooltip.height;
    }
    Rectangle::new(
        Point::new(
            x.clamp(0.0, (chart.width - tooltip.width).max(0.0)),
            y.clamp(0.0, (chart.height - tooltip.height).max(0.0)),
        ),
        tooltip,
    )
}

fn draw_hover(
    frame: &mut canvas::Frame,
    chart_size: Size,
    cursor: Point,
    strike: &GexStrike,
    spot: f64,
    palette: &iced::theme::palette::Extended,
) {
    let bounds = hover_bounds(cursor, Size::new(250.0, 148.0), chart_size);
    frame.fill(
        &canvas::Path::rectangle(bounds.position(), bounds.size()),
        palette.background.base.color.scale_alpha(0.96),
    );
    let distance = strike.strike - spot;
    let rows = [
        ("Strike", format!("{:.2}", strike.strike)),
        (
            "Distance",
            format!("{distance:+.2} ({:+.2}%)", distance / spot * 100.0),
        ),
        ("Call GEX", format_exposure(strike.call_gex_1pct)),
        ("Put GEX", format_exposure(strike.put_gex_1pct)),
        ("Net GEX", format_exposure(strike.net_gex_1pct)),
        (
            "Absolute Gamma",
            format_exposure(strike.absolute_gamma_1pct),
        ),
        ("Call OI", format!("{:.2}", strike.call_open_interest)),
        ("Put OI", format!("{:.2}", strike.put_open_interest)),
        ("Expiries", strike.expiration_count.to_string()),
    ];
    for (index, (label, value)) in rows.into_iter().enumerate() {
        let y = bounds.y + 10.0 + index as f32 * 15.0;
        draw_text_at(
            frame,
            label,
            bounds.x + 8.0,
            y,
            9.0,
            palette.background.base.text.scale_alpha(0.7),
            iced::alignment::Horizontal::Left,
        );
        draw_text_at(
            frame,
            &value,
            bounds.x + bounds.width - 8.0,
            y,
            9.0,
            palette.background.base.text,
            iced::alignment::Horizontal::Right,
        );
    }
}

fn draw_cell_text(frame: &mut canvas::Frame, value: &str, bounds: Rectangle, y: f32, color: Color) {
    draw_text_at(
        frame,
        value,
        bounds.x + bounds.width * 0.5,
        y,
        9.0,
        color,
        iced::alignment::Horizontal::Center,
    );
}

fn draw_text_at(
    frame: &mut canvas::Frame,
    value: &str,
    x: f32,
    y: f32,
    size: f32,
    color: Color,
    align: iced::alignment::Horizontal,
) {
    frame.fill_text(canvas::Text {
        content: value.to_string(),
        position: Point::new(x, y),
        size: iced::Pixels(size),
        color,
        align_x: align.into(),
        align_y: iced::alignment::Vertical::Center,
        font: style::AZERET_MONO,
        ..canvas::Text::default()
    });
}

fn format_level(value: Option<f64>) -> String {
    value.map_or_else(|| "N/A".into(), |value| format!("{value:.2}"))
}

fn compact_abs(value: f64) -> String {
    format_exposure(value.abs())
        .trim_start_matches('+')
        .to_string()
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

    fn strike(price: f64) -> GexStrike {
        GexStrike {
            strike: price,
            call_gex_1pct: 1.0,
            put_gex_1pct: -1.0,
            net_gex_1pct: 0.0,
            absolute_gamma_1pct: 2.0,
            call_open_interest: 1.0,
            put_open_interest: 1.0,
            expiration_count: 1,
        }
    }

    #[test]
    fn shared_columns_define_header_rows_and_zero() {
        for density in [
            GexLayoutDensity::Full,
            GexLayoutDensity::Compact,
            GexLayoutDensity::Minimal,
        ] {
            let columns = table_columns(800.0, density);
            assert_eq!(columns.zero_x, columns.put_bounds.width);
            assert_eq!(columns.zero_x, columns.call_bounds.x);
            let (_, label_zero, _) = scale_label_positions(&columns);
            assert_eq!(label_zero, columns.zero_x);
            let (start, end) = zero_line_points(&columns, 500.0);
            assert_eq!(start.x, columns.zero_x);
            assert_eq!(end.x, columns.zero_x);
            assert_eq!(start.y, 0.0);
            assert_eq!(end.y, 500.0);
        }
    }

    #[test]
    fn responsive_columns_are_explicit() {
        let full = table_columns(800.0, GexLayoutDensity::Full);
        assert!(full.net_bounds.is_some());
        assert!(full.abs_bounds.is_some());
        let compact = table_columns(600.0, GexLayoutDensity::Compact);
        assert!(compact.net_bounds.is_some());
        assert!(compact.abs_bounds.is_none());
        let minimal = table_columns(400.0, GexLayoutDensity::Minimal);
        assert!(minimal.net_bounds.is_none());
        assert!(minimal.abs_bounds.is_none());
    }

    #[test]
    fn hover_stays_inside_and_flips_left() {
        let chart = Size::new(500.0, 300.0);
        let right = hover_bounds(Point::new(490.0, 100.0), Size::new(250.0, 148.0), chart);
        assert!(right.x < 490.0);
        assert!(right.x >= 0.0 && right.x + right.width <= chart.width);
        let bottom = hover_bounds(Point::new(200.0, 295.0), Size::new(250.0, 148.0), chart);
        assert!(bottom.y < 295.0);
        assert!(bottom.y >= 0.0 && bottom.y + bottom.height <= chart.height);
    }

    #[test]
    fn references_interpolate_between_row_centers() {
        let strikes = vec![strike(100.0), strike(120.0), strike(160.0)];
        let centers = vec![90.0, 60.0, 30.0];
        assert_eq!(reference_y_for_price(110.0, &strikes, &centers), Some(75.0));
        assert_eq!(reference_y_for_price(140.0, &strikes, &centers), Some(45.0));
        assert_eq!(reference_y_for_price(99.0, &strikes, &centers), None);
        assert_eq!(reference_y_for_price(161.0, &strikes, &centers), None);
    }

    #[test]
    fn compact_numeric_format_has_no_line_breaks() {
        for value in [-7_190_000.0, 0.0, 31_000.0] {
            assert!(!format_exposure(value).contains('\n'));
        }
    }
}
