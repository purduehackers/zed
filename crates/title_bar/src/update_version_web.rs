//! Zed's update UI, driven by the browser host instead of the native installer.

use gpui::{Empty, Global, PromptLevel, Render, actions};
use serde::Deserialize;
use ui::{UpdateButton, prelude::*};

actions!(auto_update, [Check]);

#[derive(Clone, Default, PartialEq, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum WebUpdateStatus {
    #[default]
    Idle,
    Checking,
    Downloading {
        build: String,
        progress: Option<f32>,
    },
    Ready {
        build: String,
    },
    Installing {
        build: String,
    },
    Error {
        message: String,
    },
}

struct WebUpdater {
    status: WebUpdateStatus,
    action: fn(&str),
}

impl Global for WebUpdater {}

pub fn init_web_updates(action: fn(&str), cx: &mut App) {
    cx.set_global(WebUpdater {
        status: WebUpdateStatus::Idle,
        action,
    });
    cx.on_action(|_: &Check, cx| (cx.global::<WebUpdater>().action)("check"));
}

pub fn set_web_update_status(status: WebUpdateStatus, cx: &mut App) {
    cx.update_global::<WebUpdater, _>(|updater, _| updater.status = status);
}

pub struct UpdateVersion {
    status: WebUpdateStatus,
    dismissed: bool,
}

impl UpdateVersion {
    pub fn new(cx: &mut Context<Self>) -> Self {
        cx.observe_global::<WebUpdater>(|this, cx| {
            let status = &cx.global::<WebUpdater>().status;
            if this.status != *status {
                this.status = status.clone();
                this.dismissed = false;
                cx.notify();
            }
        })
        .detach();
        Self {
            status: cx.global::<WebUpdater>().status.clone(),
            dismissed: false,
        }
    }

    /// `zed::SimulateUpdateAvailable` is a no-op in the browser.
    pub fn update_simulation(&mut self, _: &mut Context<Self>) {}

    /// The browser has no user menu; `auto_update::Check` restores the indicator.
    pub fn show_update_in_menu_bar(&self) -> bool {
        false
    }
}

impl Render for UpdateVersion {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.dismissed {
            return Empty.into_any_element();
        }
        match &self.status {
            WebUpdateStatus::Idle => Empty.into_any_element(),
            WebUpdateStatus::Checking => UpdateButton::checking().into_any_element(),
            WebUpdateStatus::Downloading { build, progress } => UpdateButton::downloading(*progress)
                .tooltip(format!("Preparing editor {build}; you can keep working."))
                .into_any_element(),
            WebUpdateStatus::Installing { build } => UpdateButton::installing(build.clone()).into_any_element(),
            WebUpdateStatus::Ready { build } => UpdateButton::updated(format!("Update to {build}. Restarts the shared workspace."))
                .on_click(cx.listener(|_, _, window, cx| {
                    let answer = window.prompt(
                        PromptLevel::Warning,
                        "Restart this shared workspace to update?",
                        Some("Files and editor state will be preserved. Terminals restart and everyone connected will reconnect."),
                        &["Restart and Update", "Later"],
                        cx,
                    );
                    let action = cx.global::<WebUpdater>().action;
                    cx.spawn(async move |_, _| {
                        if answer.await == Ok(0) { action("install"); }
                    }).detach();
                }))
                .on_dismiss(cx.listener(|this, _, _, cx| { this.dismissed = true; cx.notify(); }))
                .into_any_element(),
            WebUpdateStatus::Error { message } => UpdateButton::errored(message.clone())
                .on_click(|_, _, cx| (cx.global::<WebUpdater>().action)("check"))
                .on_dismiss(cx.listener(|this, _, _, cx| { this.dismissed = true; cx.notify(); }))
                .into_any_element(),
        }
    }
}
