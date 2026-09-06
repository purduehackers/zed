//! Anonymous sandbox participants. No account, call or channel service is involved.

use gpui::{AnyElement, Context, Empty, IntoElement, MouseButton, Window};
use project::anonymous_participant;
use ui::{Avatar, Tooltip, prelude::*};

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
        let own = project.replica_id();
        participants.sort_by_key(|peer| (peer.replica_id != own, peer.replica_id));
        let count = participants.len();
        let overflow = participants
            .iter()
            .skip(5)
            .map(|peer| anonymous_participant::identity(peer.replica_id).0)
            .collect::<Vec<_>>()
            .join("\n");
        h_flex()
            .id("web-collaborators")
            .flex_shrink_0()
            .gap_1()
            .children(participants.into_iter().take(5).map(|peer| {
                let index = peer.replica_id.as_u16().saturating_sub(8) as u32;
                let (name, image) = anonymous_participant::identity(peer.replica_id);
                let label = if peer.replica_id == own {
                    format!("{name} (you)")
                } else {
                    name.to_owned()
                };
                div()
                    .id(("anonymous-participant", peer.user_id))
                    .flex_shrink_0()
                    .child(
                        Avatar::new(image)
                            .size(px(24.))
                            .border_color(cx.theme().players().color_for_participant(index).cursor),
                    )
                    .tooltip(Tooltip::text(label))
                    .on_mouse_down(MouseButton::Left, |_, window, cx| {
                        window.prevent_default();
                        cx.stop_propagation();
                    })
            }))
            .when(count > 5, |row| {
                row.child(
                    h_flex()
                        .id("anonymous-participant-overflow")
                        .size(px(26.))
                        .justify_center()
                        .rounded_full()
                        .bg(cx.theme().colors().element_background)
                        .child(Label::new(format!("+{}", count - 5)).size(LabelSize::Small))
                        .tooltip(Tooltip::text(overflow)),
                )
            })
    }

    /// No call controls in the browser: renders nothing.
    pub(crate) fn render_call_controls(&self, _: &mut Window, _: &mut Context<Self>) -> AnyElement {
        Empty.into_any_element()
    }
}
