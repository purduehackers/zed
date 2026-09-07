//! Anonymous presence and host-backed browser call controls.

use gpui::{AnyElement, Context, Global, IntoElement, MouseButton, Window};
use project::anonymous_participant;
use serde::Deserialize;
use ui::{TintColor, Tooltip, prelude::*};

use crate::TitleBar;

#[derive(Clone, Default, Deserialize)]
pub struct WebCallStatus {
    pub phase: String,
    pub muted: bool,
    pub deafened: bool,
    pub sharing_screen: bool,
    pub screen_supported: bool,
    pub peers: usize,
    pub error: Option<String>,
}

pub struct WebCall {
    status: WebCallStatus,
    action: fn(&str, u16, &str),
}
impl Global for WebCall {}

pub fn init_web_calls(action: fn(&str, u16, &str), cx: &mut App) {
    cx.set_global(WebCall {
        status: WebCallStatus::default(),
        action,
    });
}

pub fn set_web_call_status(status: WebCallStatus, cx: &mut App) {
    cx.update_global::<WebCall, _>(|call, _| call.status = status);
}

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
            .map(|peer| anonymous_participant::name(peer.replica_id))
            .collect::<Vec<_>>()
            .join("\n");
        h_flex()
            .id("web-collaborators")
            .flex_shrink_0()
            .gap_1()
            .children(participants.into_iter().take(5).map(|peer| {
                let index = peer.replica_id.as_u16().saturating_sub(8) as u32;
                let name = anonymous_participant::name(peer.replica_id);
                let label = if peer.replica_id == own {
                    format!("{name} (you)")
                } else {
                    name.to_owned()
                };
                div()
                    .id(("anonymous-participant", peer.user_id))
                    .flex_shrink_0()
                    .child(
                        h_flex()
                            .size(px(26.))
                            .justify_center()
                            .rounded_full()
                            .border_1()
                            .border_color(cx.theme().players().color_for_participant(index).cursor)
                            .bg(cx.theme().colors().element_disabled)
                            .child(
                                Icon::new(IconName::Person)
                                    .color(Color::Muted)
                                    .size(IconSize::Small),
                            ),
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

    pub(crate) fn render_call_controls(
        &self,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let call = cx.global::<WebCall>();
        let status = &call.status;
        let action = call.action;
        let replica_id = self.project.read(cx).replica_id();
        let replica = replica_id.as_u16();
        let name = anonymous_participant::name(replica_id);
        let button = |id: &'static str, icon, label: &'static str, command: &'static str| {
            IconButton::new(id, icon)
                .icon_size(IconSize::Small)
                .aria_label(label)
                .tooltip(Tooltip::text(label))
                .on_click(move |_, _, _| action(command, replica, name))
        };
        if status.phase != "joined" {
            let joining = status.phase == "joining";
            let label = if joining {
                "Joining Call…"
            } else {
                "Join Call"
            };
            return button("join-call", IconName::OnCall, label, "join")
                .disabled(joining)
                .when_some(status.error.clone(), |this, message| {
                    this.tooltip(Tooltip::text(message))
                })
                .into_any_element();
        }
        h_flex()
            .gap_1()
            .child(
                button(
                    "call-participants",
                    IconName::OnCall,
                    "Call Participants",
                    "show",
                )
                .tooltip(Tooltip::text(
                    status
                        .error
                        .clone()
                        .unwrap_or_else(|| format!("{} in this call", status.peers)),
                )),
            )
            .child(
                button(
                    "mute-microphone",
                    if status.muted {
                        IconName::MicMute
                    } else {
                        IconName::Mic
                    },
                    if status.muted {
                        "Unmute Microphone"
                    } else {
                        "Mute Microphone"
                    },
                    "microphone",
                )
                .toggle_state(status.muted)
                .selected_style(ButtonStyle::Tinted(TintColor::Error)),
            )
            .child(
                button(
                    "mute-sound",
                    if status.deafened {
                        IconName::AudioOff
                    } else {
                        IconName::AudioOn
                    },
                    if status.deafened {
                        "Unmute Audio"
                    } else {
                        "Mute Audio"
                    },
                    "audio",
                )
                .toggle_state(status.deafened)
                .selected_style(ButtonStyle::Tinted(TintColor::Error)),
            )
            .when(status.screen_supported, |row| {
                row.child(
                    button(
                        "screen-share",
                        IconName::Screen,
                        if status.sharing_screen {
                            "Stop Sharing Screen"
                        } else {
                            "Share Screen"
                        },
                        "screen",
                    )
                    .toggle_state(status.sharing_screen)
                    .selected_style(ButtonStyle::Tinted(TintColor::Accent)),
                )
            })
            .child(button("leave-call", IconName::Exit, "Leave Call", "leave"))
            .into_any_element()
    }
}
