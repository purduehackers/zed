//! Browser stand-in for the update indicator. A browser tab's build is pinned by the shell
//! page (BUILD-SPEC 11.2), so there is never an update to show; the type keeps the title
//! bar's field and call sites identical on both targets.

use gpui::{Context, Empty, IntoElement, Render, Window};

/// The update-status indicator; always idle in the browser.
pub struct UpdateVersion;

impl UpdateVersion {
    /// Creates the (stateless) indicator.
    pub fn new(_: &mut Context<Self>) -> Self {
        Self
    }

    /// `zed::SimulateUpdateAvailable` is a no-op in the browser.
    pub fn update_simulation(&mut self, _: &mut Context<Self>) {}

    /// Never shows an update entry in the menu bar.
    pub fn show_update_in_menu_bar(&self) -> bool {
        false
    }
}

impl Render for UpdateVersion {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        Empty
    }
}
