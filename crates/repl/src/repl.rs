pub mod components;
mod jupyter_settings;
pub mod kernels;
pub mod notebook;
mod outputs;
mod repl_editor;
mod repl_sessions_ui;
mod repl_settings;
mod repl_store;
mod session;

use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
use std::time::Duration;

#[cfg(not(target_family = "wasm"))]
use async_dispatcher::{Dispatcher, Runnable, set_dispatcher};
use gpui::App;
#[cfg(not(target_family = "wasm"))]
use gpui::{PlatformDispatcher, Priority, RunnableMeta};
pub use jupyter_protocol::ExecutionState;
use project::Fs;

pub use crate::jupyter_settings::JupyterSettings;
pub use crate::kernels::{Kernel, KernelSpecification, KernelStatus, PythonEnvKernelSpecification};
pub use crate::repl_editor::*;
pub use crate::repl_sessions_ui::{
    ClearCurrentOutput, ClearOutputs, Interrupt, ReplSessionsPage, Restart, Run, Sessions, Shutdown,
};
pub use crate::repl_settings::ReplSettings;
pub use crate::repl_store::ReplStore;
pub use crate::session::Session;

pub const KERNEL_DOCS_URL: &str = "https://zed.dev/docs/repl#changing-kernels";

pub fn init(fs: Arc<dyn Fs>, cx: &mut App) {
    #[cfg(not(target_family = "wasm"))]
    set_dispatcher(zed_dispatcher(cx));
    repl_sessions_ui::init(cx);
    ReplStore::init(fs, cx);
}

fn message(
    content: impl Into<jupyter_protocol::JupyterMessageContent>,
    parent: Option<&jupyter_protocol::JupyterMessage>,
) -> jupyter_protocol::JupyterMessage {
    #[cfg(not(target_family = "wasm"))]
    return jupyter_protocol::JupyterMessage::new(content, parent);
    #[cfg(target_family = "wasm")]
    {
        // jupyter-protocol's constructor unconditionally calls std::SystemTime,
        // which panics on wasm32-unknown-unknown. Use the browser-aware clock.
        let content = content.into();
        jupyter_protocol::JupyterMessage {
            header: jupyter_protocol::Header {
                msg_id: uuid::Uuid::new_v4().to_string(),
                session: parent
                    .map(|message| message.header.session.clone())
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                username: "runtimelib".to_string(),
                date: chrono::Utc::now(),
                msg_type: content.message_type().to_owned(),
                version: "5.3".to_string(),
            },
            parent_header: parent.map(|message| message.header.clone()),
            zmq_identities: parent
                .map(|message| message.zmq_identities.clone())
                .unwrap_or_default(),
            metadata: serde_json::json!({}),
            content,
            buffers: Vec::new(),
            channel: None,
        }
    }
}

#[cfg(not(target_family = "wasm"))]
fn zed_dispatcher(cx: &mut App) -> impl Dispatcher {
    struct ZedDispatcher {
        dispatcher: Arc<dyn PlatformDispatcher>,
    }

    // PlatformDispatcher is _super_ close to the same interface we put in
    // async-dispatcher, except for the task label in dispatch. Later we should
    // just make that consistent so we have this dispatcher ready to go for
    // other crates in Zed.
    impl Dispatcher for ZedDispatcher {
        #[track_caller]
        fn dispatch(&self, runnable: Runnable) {
            let (wrapper, task) = async_task::Builder::new()
                .metadata(RunnableMeta::new_with_callers_location())
                .spawn(|_| async move { runnable.run() }, {
                    let dispatcher = self.dispatcher.clone();
                    move |r| dispatcher.dispatch(r, Priority::default())
                });
            wrapper.schedule();
            task.detach();
        }

        #[track_caller]
        fn dispatch_after(&self, duration: Duration, runnable: Runnable) {
            let (wrapper, task) = async_task::Builder::new()
                .metadata(RunnableMeta::new_with_callers_location())
                .spawn(|_| async move { runnable.run() }, {
                    let dispatcher = self.dispatcher.clone();
                    move |r| dispatcher.dispatch_after(duration, r)
                });
            wrapper.schedule();
            task.detach();
        }
    }

    ZedDispatcher {
        dispatcher: cx.background_executor().dispatcher().clone(),
    }
}
