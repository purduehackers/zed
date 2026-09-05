//! The browser edit-prediction registry (b11 §3.19): `crates/zed`'s
//! `edit_prediction_registry` restricted to the providers a tab can reach. Codestral goes
//! through the proxy (`edit_predictions.codestral.api_url` is seeded to
//! `<origin>/api/ai/codestral`, its key is the placeholder the credentials provider answers);
//! an OpenAI-compatible FIM endpoint is honoured only when its `api_url` is a proxied
//! `<origin>/api/ai/compat/<name>` URL, because the editor CSP (`connect-src 'self' …`)
//! blocks every other host. Copilot (needs a local language server), Zed and Mercury (need
//! Zed sign-in) and Ollama (`localhost`) yield no provider, each with one notice per tab.

use std::{cell::RefCell, rc::Rc, sync::Arc};

use client::{Client, UserStore};
use codestral::{CodestralEditPredictionDelegate, load_codestral_api_key};
use collections::{HashMap, HashSet};
use edit_prediction::{EditPredictionModel, ZedEditPredictionDelegate, fim};
use editor::{EditPredictionRequestTrigger, Editor};
use gpui::{AnyWindowHandle, App, AppContext as _, Context, Entity, WeakEntity};
use language::language_settings::{
    EditPredictionPromptFormat, EditPredictionProvider, all_language_settings,
};
use settings::SettingsStore;
use ui::Window;
use workspace::notifications::{
    NotificationId, show_app_notification, simple_message_notification::MessageNotification,
};
use zed_web_core::ai_proxy;

/// A provider configuration the browser can serve.
#[derive(Copy, Clone, PartialEq, Eq)]
enum WebEditPredictionConfig {
    /// `CodestralEditPredictionDelegate` against the proxied Codestral URL.
    Codestral,
    /// `ZedEditPredictionDelegate` in FIM mode against a proxied OpenAI-compatible endpoint
    /// (the store needs no cloud credentials for `OpenAiCompatibleApi`).
    Fim(EditPredictionModel),
}

/// Why a selected provider yields no predictions in the browser. Each variant raises one
/// notice per tab (the same provider re-selected later is not repeated).
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
enum Unavailable {
    Copilot,
    Zed,
    Mercury,
    Ollama,
    /// `open_ai_compatible_api` is missing or its `api_url` is not a proxied URL.
    OpenAiCompatibleNotProxied,
    /// `prompt_format: "infer"` and the model name matches no known FIM format.
    PromptFormatUnknown,
}

impl Unavailable {
    fn notification_id(self) -> NotificationId {
        let name = match self {
            Self::Copilot => "copilot",
            Self::Zed => "zed",
            Self::Mercury => "mercury",
            Self::Ollama => "ollama",
            Self::OpenAiCompatibleNotProxied => "open-ai-compatible",
            Self::PromptFormatUnknown => "prompt-format",
        };
        NotificationId::Named(format!("web-edit-prediction-{name}").into())
    }

    fn message(self, origin: &str) -> String {
        match self {
            Self::Copilot => "Copilot edit predictions are not available in the browser yet; \
                choose Codestral or a proxied OpenAI-compatible endpoint in \
                `edit_predictions.provider`."
                .to_owned(),
            Self::Zed => "Zed's edit prediction requires Zed sign-in, which is not available in \
                the browser; choose Codestral or a proxied OpenAI-compatible endpoint in \
                `edit_predictions.provider`."
                .to_owned(),
            Self::Mercury => "Mercury edit predictions go through Zed's cloud and require Zed \
                sign-in, which is not available in the browser."
                .to_owned(),
            Self::Ollama => "Ollama edit predictions need a server on your machine, which a \
                browser tab cannot reach; choose Codestral or a proxied OpenAI-compatible \
                endpoint instead."
                .to_owned(),
            Self::OpenAiCompatibleNotProxied => format!(
                "OpenAI-compatible edit predictions reach only the AI proxy from the browser: \
                 set `edit_predictions.open_ai_compatible_api.api_url` to \
                 `{origin}/api/ai/compat/<name>` after adding that endpoint at {}.",
                ai_proxy::manage_keys_url(origin)
            ),
            Self::PromptFormatUnknown => "The prompt format of the configured edit-prediction \
                model could not be inferred from its name; set \
                `edit_predictions.open_ai_compatible_api.prompt_format`."
                .to_owned(),
        }
    }
}

/// The configuration for the current settings and, when it yields no provider because of
/// the browser, why.
fn config_for_settings(
    origin: &str,
    cx: &App,
) -> (Option<WebEditPredictionConfig>, Option<Unavailable>) {
    let settings = &all_language_settings(None, cx).edit_predictions;
    match settings.provider {
        EditPredictionProvider::None => (None, None),
        EditPredictionProvider::Copilot => (None, Some(Unavailable::Copilot)),
        EditPredictionProvider::Zed => (None, Some(Unavailable::Zed)),
        EditPredictionProvider::Mercury => (None, Some(Unavailable::Mercury)),
        EditPredictionProvider::Ollama => (None, Some(Unavailable::Ollama)),
        EditPredictionProvider::Codestral => (Some(WebEditPredictionConfig::Codestral), None),
        EditPredictionProvider::OpenAiCompatibleApi => {
            let Some(custom) = settings.open_ai_compatible_api.as_ref() else {
                return (None, Some(Unavailable::OpenAiCompatibleNotProxied));
            };
            // The setting defaults to "" and is not seeded (b11 §4.3): only a proxied
            // endpoint is reachable under the editor CSP.
            if ai_proxy::provider_for_url(origin, &custom.api_url).is_none() {
                return (None, Some(Unavailable::OpenAiCompatibleNotProxied));
            }
            // The prompt-format inference of `edit_prediction_registry.rs`, verbatim.
            let mut format = custom.prompt_format;
            if format == EditPredictionPromptFormat::Infer {
                match fim::infer_prompt_format(&custom.model) {
                    Some(inferred) => format = inferred,
                    None => return (None, Some(Unavailable::PromptFormatUnknown)),
                }
            }
            let model = if matches!(format, EditPredictionPromptFormat::Zeta(_)) {
                EditPredictionModel::Zeta
            } else if format == EditPredictionPromptFormat::Sweep {
                EditPredictionModel::SweepPrompt
            } else {
                EditPredictionModel::Fim { format }
            };
            (Some(WebEditPredictionConfig::Fim(model)), None)
        }
    }
}

type Editors = Rc<RefCell<HashMap<WeakEntity<Editor>, AnyWindowHandle>>>;

/// The notices raised so far in this tab.
type Notified = Rc<RefCell<HashSet<Unavailable>>>;

fn notify_unavailable(unavailable: Unavailable, origin: &str, notified: &Notified, cx: &mut App) {
    if !notified.borrow_mut().insert(unavailable) {
        return;
    }
    log::info!("edit prediction provider unavailable in the browser: {unavailable:?}");
    let message = unavailable.message(origin);
    show_app_notification(unavailable.notification_id(), cx, move |cx| {
        cx.new(|cx| MessageNotification::new(message.clone(), cx))
    });
}

/// Installs the registry: the store, the per-editor assignment, the settings and user-store
/// observers and the `ClearHistory` action. Runs after `edit_prediction::init` (b11 §3.18
/// step 30a). `origin` is the control plane the proxied endpoints hang off.
pub fn init(client: Arc<Client>, user_store: Entity<UserStore>, origin: &str, cx: &mut App) {
    edit_prediction::EditPredictionStore::global(&client, &user_store, cx);

    let origin = origin.to_owned();
    let editors: Editors = Rc::default();
    let notified: Notified = Rc::default();

    let (config, unavailable) = config_for_settings(&origin, cx);
    if config == Some(WebEditPredictionConfig::Codestral) {
        load_codestral_api_key(cx).detach();
    }
    // At boot only an explicit choice is worth a notice: `zed` is the shipped default, so
    // a user who never touched the setting is told nothing until they pick a provider.
    if let Some(unavailable) = unavailable
        && unavailable != Unavailable::Zed
    {
        notify_unavailable(unavailable, &origin, &notified, cx);
    }

    cx.observe_new({
        let editors = editors.clone();
        let client = client.clone();
        let user_store = user_store.clone();
        let origin = origin.clone();
        move |editor: &mut Editor, window, cx: &mut Context<Editor>| {
            if !editor.mode().is_full() {
                return;
            }
            let Some(window) = window else {
                return;
            };

            let editor_handle = cx.entity().downgrade();
            cx.on_release({
                let editor_handle = editor_handle.clone();
                let editors = editors.clone();
                move |_, _| {
                    editors.borrow_mut().remove(&editor_handle);
                }
            })
            .detach();

            editors
                .borrow_mut()
                .insert(editor_handle, window.window_handle());
            let (config, _) = config_for_settings(&origin, cx);
            assign(
                editor,
                config,
                EditPredictionRequestTrigger::EditorCreated,
                &client,
                user_store.clone(),
                window,
                cx,
            );
        }
    })
    .detach();

    cx.on_action(clear_edit_prediction_store_edit_history);

    cx.subscribe(&user_store, {
        let editors = editors.clone();
        let client = client.clone();
        let origin = origin.clone();
        move |user_store, event, cx| match event {
            client::user::Event::PrivateUserInfoUpdated
            | client::user::Event::OrganizationChanged => {
                let (config, _) = config_for_settings(&origin, cx);
                assign_all(
                    &editors,
                    config,
                    EditPredictionRequestTrigger::UserInfoChanged,
                    &client,
                    user_store,
                    cx,
                );
            }
            _ => {}
        }
    })
    .detach();

    cx.observe_global::<SettingsStore>({
        let mut previous = (config, unavailable);
        move |cx| {
            let current = config_for_settings(&origin, cx);
            if current == previous {
                return;
            }
            previous = current;
            let (config, unavailable) = current;
            // A change is the user's doing: every unavailable choice is worth a notice.
            if let Some(unavailable) = unavailable {
                notify_unavailable(unavailable, &origin, &notified, cx);
            }
            assign_all(
                &editors,
                config,
                EditPredictionRequestTrigger::ProviderChanged,
                &client,
                user_store.clone(),
                cx,
            );
        }
    })
    .detach();
}

fn clear_edit_prediction_store_edit_history(_: &edit_prediction::ClearHistory, cx: &mut App) {
    if let Some(ep_store) = edit_prediction::EditPredictionStore::try_global(cx) {
        ep_store.update(cx, |ep_store, _| ep_store.clear_history());
    }
}

fn assign_all(
    editors: &Editors,
    config: Option<WebEditPredictionConfig>,
    trigger: EditPredictionRequestTrigger,
    client: &Arc<Client>,
    user_store: Entity<UserStore>,
    cx: &mut App,
) {
    if config == Some(WebEditPredictionConfig::Codestral) {
        load_codestral_api_key(cx).detach();
    }
    for (editor, window) in editors.borrow().iter() {
        _ = window.update(cx, |_window, window, cx| {
            _ = editor.update(cx, |editor, cx| {
                assign(
                    editor,
                    config,
                    trigger,
                    client,
                    user_store.clone(),
                    window,
                    cx,
                );
            })
        });
    }
}

/// The Codestral and `Zed(model)` arms of `edit_prediction_registry::assign_edit_prediction_provider`,
/// verbatim; `None` clears the editor's provider.
fn assign(
    editor: &mut Editor,
    config: Option<WebEditPredictionConfig>,
    trigger: EditPredictionRequestTrigger,
    client: &Arc<Client>,
    user_store: Entity<UserStore>,
    window: &mut Window,
    cx: &mut Context<Editor>,
) {
    let singleton_buffer = editor.buffer().read(cx).as_singleton();

    match config {
        None => {
            editor.set_edit_prediction_provider::<ZedEditPredictionDelegate>(
                None, trigger, window, cx,
            );
        }
        Some(WebEditPredictionConfig::Codestral) => {
            let http_client = client.http_client();
            let provider = cx.new(|_| CodestralEditPredictionDelegate::new(http_client));
            editor.set_edit_prediction_provider(Some(provider), trigger, window, cx);
        }
        Some(WebEditPredictionConfig::Fim(model)) => {
            let ep_store = edit_prediction::EditPredictionStore::global(client, &user_store, cx);

            if let Some(organization_configuration) =
                user_store.read(cx).current_organization_configuration()
                && !organization_configuration.edit_prediction.is_enabled
            {
                editor.set_edit_prediction_provider::<ZedEditPredictionDelegate>(
                    None, trigger, window, cx,
                );
                return;
            }

            if let Some(project) = editor.project() {
                ep_store.update(cx, |ep_store, cx| {
                    ep_store.set_edit_prediction_model(model);
                    if let Some(buffer) = &singleton_buffer {
                        ep_store.register_buffer(buffer, project, cx);
                    }
                });

                let provider = cx.new(|cx| {
                    ZedEditPredictionDelegate::new(
                        project.clone(),
                        singleton_buffer,
                        client,
                        &user_store,
                        cx,
                    )
                });
                editor.set_edit_prediction_provider(Some(provider), trigger, window, cx);
            }
        }
    }
}
