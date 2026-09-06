//! Anonymous sandbox participants. No account, call or channel service is involved.

use gpui::{AnyElement, Context, Empty, IntoElement, Window};
use ui::prelude::*;

use crate::TitleBar;

impl TitleBar {
    /// Keep presence inside Zed's title bar, without restoring the HTML status strip.
    pub(crate) fn render_collaborator_list(
        &self,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let project = self.project.read(cx);
        let mut participants = project.collaborators().values().collect::<Vec<_>>();
        participants.sort_by_key(|peer| peer.replica_id);
        let own = project.replica_id();
        let count = participants.len();
        h_flex()
            .gap_2()
            .children(participants.into_iter().take(5).map(|peer| {
                let index = peer.replica_id.as_u16().saturating_sub(8) as u32;
                Label::new(format!(
                    "Guest {}{}",
                    index + 1,
                    if peer.replica_id == own { " (you)" } else { "" }
                ))
                .size(LabelSize::Small)
                .color(Color::Custom(
                    cx.theme().players().color_for_participant(index).cursor,
                ))
            }))
            .when(count > 5, |row| {
                row.child(Label::new(format!("+{}", count - 5)).size(LabelSize::Small))
            })
    }

    /// No call controls in the browser: renders nothing.
    pub(crate) fn render_call_controls(&self, _: &mut Window, _: &mut Context<Self>) -> AnyElement {
        Empty.into_any_element()
    }
}
