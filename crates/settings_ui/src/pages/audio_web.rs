//! Browser stand-ins for the audio device pages. The browser build has no `cpal`/`rodio`
//! (BUILD-SPEC 3.2), so the device dropdowns render a disabled label and the audio test
//! window (a second top-level window, which `gpui_web` refuses) is a no-op.

use gpui::{AnyElement, App, Window};
use settings::{AudioInputDeviceName, AudioOutputDeviceName};
use ui::{Label, LabelSize, prelude::*};

use crate::{SettingField, SettingsFieldMetadata, SettingsUiFile};

const UNAVAILABLE: &str = "Audio devices are not available in the browser";

fn render_unavailable(
    id: &'static str,
    title: &'static str,
    description: &'static str,
) -> AnyElement {
    v_flex()
        .id(id)
        .gap_1()
        .child(Label::new(title))
        .child(
            Label::new(description)
                .size(LabelSize::Small)
                .color(Color::Muted),
        )
        .child(
            Label::new(UNAVAILABLE)
                .size(LabelSize::Small)
                .color(Color::Disabled),
        )
        .into_any_element()
}

/// Input-device dropdown: unavailable in the browser.
pub fn render_input_audio_device_dropdown(
    _field: SettingField<AudioInputDeviceName>,
    _file: SettingsUiFile,
    _metadata: Option<&SettingsFieldMetadata>,
    title: &'static str,
    description: &'static str,
    _window: &mut Window,
    _cx: &mut App,
) -> AnyElement {
    render_unavailable("audio-input-device-web", title, description)
}

/// Output-device dropdown: unavailable in the browser.
pub fn render_output_audio_device_dropdown(
    _field: SettingField<AudioOutputDeviceName>,
    _file: SettingsUiFile,
    _metadata: Option<&SettingsFieldMetadata>,
    title: &'static str,
    description: &'static str,
    _window: &mut Window,
    _cx: &mut App,
) -> AnyElement {
    render_unavailable("audio-output-device-web", title, description)
}

/// The audio test window is a second top-level window; the browser has one.
pub fn open_audio_test_window(_: &mut Window, _: &mut App) {}
