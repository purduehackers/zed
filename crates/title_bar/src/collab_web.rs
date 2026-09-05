//! Browser stand-ins for the collaboration surface of the title bar. Calls, channels and
//! screen sharing are desktop-only (BUILD-SPEC 3.2), so the browser build renders nothing
//! where the desktop draws the collaborator list and call controls.

use gpui::{AnyElement, Context, Empty, IntoElement, Window};

use crate::TitleBar;

impl TitleBar {
    /// No collaborators in the browser: renders nothing.
    pub(crate) fn render_collaborator_list(
        &self,
        _: &mut Window,
        _: &mut Context<Self>,
    ) -> impl IntoElement {
        Empty
    }

    /// No call controls in the browser: renders nothing.
    pub(crate) fn render_call_controls(&self, _: &mut Window, _: &mut Context<Self>) -> AnyElement {
        Empty.into_any_element()
    }
}
