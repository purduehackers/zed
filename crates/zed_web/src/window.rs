//! `AppState::build_window_options` for the browser: the desktop options minus display,
//! decorations-from-env, tabbing and icon (one document-owned canvas, `gpui_web`).

use gpui::{App, TitlebarOptions, WindowKind, WindowOptions, point, px};
use theme::ActiveTheme as _;

/// The one workspace window's options.
pub fn build_window_options(_display: Option<uuid::Uuid>, cx: &mut App) -> WindowOptions {
    WindowOptions {
        titlebar: Some(TitlebarOptions {
            title: None,
            appears_transparent: true,
            traffic_light_position: Some(point(px(9.0), px(9.0))),
        }),
        window_bounds: None,
        focus: true,
        show: true,
        kind: WindowKind::Normal,
        is_movable: false,
        app_owns_titlebar_drag: false,
        window_background: cx.theme().window_background_appearance(),
        app_id: Some("zed-web".into()),
        window_decorations: Some(gpui::WindowDecorations::Server),
        window_min_size: Some(gpui::Size {
            width: px(360.0),
            height: px(240.0),
        }),
        ..Default::default()
    }
}
