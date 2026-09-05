use std::sync::Arc;

use ::settings::{Settings, SettingsStore};
use client::{Client, UserStore};
use collections::HashMap;
// Only the native extension-host wiring collects provider ids.
#[cfg(not(target_family = "wasm"))]
use collections::HashSet;
use credentials_provider::CredentialsProvider;
use gpui::{App, Context, Entity};
use language_model::{LanguageModelProviderId, LanguageModelRegistry};
use provider::deepseek::DeepSeekLanguageModelProvider;

pub mod extension;
pub mod provider;
mod settings;

pub use crate::extension::init_proxy as init_extension_proxy;

use crate::provider::anthropic::AnthropicLanguageModelProvider;
use crate::provider::anthropic_compatible::AnthropicCompatibleLanguageModelProvider;
#[cfg(not(target_family = "wasm"))]
use crate::provider::bedrock::BedrockLanguageModelProvider;
use crate::provider::cloud::CloudLanguageModelProvider;
use crate::provider::copilot_chat::CopilotChatLanguageModelProvider;
use crate::provider::google::GoogleLanguageModelProvider;
#[cfg(not(target_family = "wasm"))]
use crate::provider::llama_cpp::LlamaCppLanguageModelProvider;
#[cfg(not(target_family = "wasm"))]
use crate::provider::lmstudio::LmStudioLanguageModelProvider;
pub use crate::provider::mistral::MistralLanguageModelProvider;
#[cfg(not(target_family = "wasm"))]
use crate::provider::ollama::OllamaLanguageModelProvider;
use crate::provider::open_ai::OpenAiLanguageModelProvider;
use crate::provider::open_ai_compatible::OpenAiCompatibleLanguageModelProvider;
use crate::provider::open_router::OpenRouterLanguageModelProvider;
#[cfg(not(target_family = "wasm"))]
use crate::provider::openai_subscribed::OpenAiSubscribedProvider;
use crate::provider::opencode::OpenCodeLanguageModelProvider;
use crate::provider::vercel_ai_gateway::VercelAiGatewayLanguageModelProvider;
use crate::provider::x_ai::XAiLanguageModelProvider;
pub use crate::settings::*;

pub fn init(user_store: Entity<UserStore>, client: Arc<Client>, cx: &mut App) {
    let credentials_provider = client.credentials_provider();
    let registry = LanguageModelRegistry::global(cx);
    registry.update(cx, |registry, cx| {
        register_language_model_providers(
            registry,
            user_store,
            client.clone(),
            credentials_provider.clone(),
            cx,
        );
    });

    // Subscribe to extension store events to track LLM extension installations. The browser
    // build has no local extension host (BUILD-SPEC 3.2): extensions run in the sandbox and
    // reach the client through `project::RemoteExtensionStore`, so it registers no
    // extension-provided providers here.
    #[cfg(not(target_family = "wasm"))]
    if let Some(extension_store) = extension_host::ExtensionStore::try_global(cx) {
        cx.subscribe(&extension_store, {
            let registry = registry.downgrade();
            move |extension_store, event, cx| {
                let Some(registry) = registry.upgrade() else {
                    return;
                };
                match event {
                    extension_host::Event::ExtensionInstalled(extension_id) => {
                        if let Some(manifest) = extension_store
                            .read(cx)
                            .extension_manifest_for_id(extension_id)
                        {
                            if !manifest.language_model_providers.is_empty() {
                                registry.update(cx, |registry, cx| {
                                    registry.extension_installed(extension_id.clone(), cx);
                                });
                            }
                        }
                    }
                    extension_host::Event::ExtensionUninstalled(extension_id) => {
                        registry.update(cx, |registry, cx| {
                            registry.extension_uninstalled(extension_id, cx);
                        });
                    }
                    extension_host::Event::ExtensionsUpdated => {
                        let mut new_ids = HashSet::default();
                        for (extension_id, entry) in extension_store.read(cx).installed_extensions()
                        {
                            if !entry.manifest.language_model_providers.is_empty() {
                                new_ids.insert(extension_id.clone());
                            }
                        }
                        registry.update(cx, |registry, cx| {
                            registry.sync_installed_llm_extensions(new_ids, cx);
                        });
                    }
                    _ => {}
                }
            }
        })
        .detach();

        // Initialize with currently installed extensions
        registry.update(cx, |registry, cx| {
            let mut initial_ids = HashSet::default();
            for (extension_id, entry) in extension_store.read(cx).installed_extensions() {
                if !entry.manifest.language_model_providers.is_empty() {
                    initial_ids.insert(extension_id.clone());
                }
            }
            registry.sync_installed_llm_extensions(initial_ids, cx);
        });
    }

    let mut compatible_providers = CompatibleProviders::from_settings(cx);

    registry.update(cx, |registry, cx| {
        register_compatible_providers(
            registry,
            &CompatibleProviders::default(),
            &compatible_providers,
            &client,
            &credentials_provider,
            cx,
        );
    });

    let registry = registry.downgrade();
    cx.observe_global::<SettingsStore>(move |cx| {
        let Some(registry) = registry.upgrade() else {
            return;
        };
        let compatible_providers_new = CompatibleProviders::from_settings(cx);
        if compatible_providers_new != compatible_providers {
            registry.update(cx, |registry, cx| {
                register_compatible_providers(
                    registry,
                    &compatible_providers,
                    &compatible_providers_new,
                    &client,
                    &credentials_provider,
                    cx,
                );
            });
            compatible_providers = compatible_providers_new;
        }
    })
    .detach();
}

#[derive(Default, PartialEq, Eq)]
struct CompatibleProviders(HashMap<Arc<str>, CompatibleProviderKind>);

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum CompatibleProviderKind {
    OpenAi,
    Anthropic,
}

impl CompatibleProviders {
    fn from_settings(cx: &App) -> Self {
        let settings = AllLanguageModelSettings::get_global(cx);
        let mut providers: HashMap<Arc<str>, CompatibleProviderKind> = settings
            .openai_compatible
            .keys()
            .map(|id| (id.clone(), CompatibleProviderKind::OpenAi))
            .collect();
        for id in settings.anthropic_compatible.keys() {
            // The registry has a single provider ID namespace, so a name can
            // only refer to one provider. OpenAI-compatible entries win
            // collisions because they predate Anthropic-compatible ones, so
            // existing configurations keep working.
            if providers.contains_key(id) {
                log::warn!(
                    "ignoring `anthropic_compatible` provider `{id}`: \
                     an `openai_compatible` provider with the same name exists"
                );
            } else {
                providers.insert(id.clone(), CompatibleProviderKind::Anthropic);
            }
        }
        Self(providers)
    }
}

fn register_compatible_providers(
    registry: &mut LanguageModelRegistry,
    old: &CompatibleProviders,
    new: &CompatibleProviders,
    client: &Arc<Client>,
    credentials_provider: &Arc<dyn CredentialsProvider>,
    cx: &mut Context<LanguageModelRegistry>,
) {
    for (provider_id, old_kind) in &old.0 {
        if new.0.get(provider_id) != Some(old_kind) {
            registry.unregister_provider(LanguageModelProviderId::from(provider_id.clone()), cx);
        }
    }

    for (provider_id, kind) in &new.0 {
        if old.0.get(provider_id) != Some(kind) {
            match kind {
                CompatibleProviderKind::OpenAi => registry.register_provider(
                    Arc::new(OpenAiCompatibleLanguageModelProvider::new(
                        provider_id.clone(),
                        client.http_client(),
                        credentials_provider.clone(),
                        cx,
                    )),
                    cx,
                ),
                CompatibleProviderKind::Anthropic => registry.register_provider(
                    Arc::new(AnthropicCompatibleLanguageModelProvider::new(
                        provider_id.clone(),
                        client.http_client(),
                        credentials_provider.clone(),
                        cx,
                    )),
                    cx,
                ),
            }
        }
    }
}

fn register_language_model_providers(
    registry: &mut LanguageModelRegistry,
    user_store: Entity<UserStore>,
    client: Arc<Client>,
    credentials_provider: Arc<dyn CredentialsProvider>,
    cx: &mut Context<LanguageModelRegistry>,
) {
    registry.register_provider(
        Arc::new(CloudLanguageModelProvider::new(
            user_store,
            client.clone(),
            cx,
        )),
        cx,
    );
    registry.register_provider(
        Arc::new(AnthropicLanguageModelProvider::new(
            client.http_client(),
            credentials_provider.clone(),
            cx,
        )),
        cx,
    );
    registry.register_provider(
        Arc::new(OpenAiLanguageModelProvider::new(
            client.http_client(),
            credentials_provider.clone(),
            cx,
        )),
        cx,
    );
    // The `localhost` providers are desktop-only: a browser tab cannot reach the user's
    // machine (the editor CSP allows only its own origin and the sandbox hosts), so they would
    // sit in the model picker and always fail with an opaque network error (b11 §4.2).
    #[cfg(not(target_family = "wasm"))]
    registry.register_provider(
        Arc::new(OllamaLanguageModelProvider::new(
            client.http_client(),
            credentials_provider.clone(),
            cx,
        )),
        cx,
    );
    #[cfg(not(target_family = "wasm"))]
    registry.register_provider(
        Arc::new(LmStudioLanguageModelProvider::new(
            client.http_client(),
            credentials_provider.clone(),
            cx,
        )),
        cx,
    );
    #[cfg(not(target_family = "wasm"))]
    registry.register_provider(
        Arc::new(LlamaCppLanguageModelProvider::new(
            client.http_client(),
            credentials_provider.clone(),
            cx,
        )),
        cx,
    );
    registry.register_provider(
        Arc::new(DeepSeekLanguageModelProvider::new(
            client.http_client(),
            credentials_provider.clone(),
            cx,
        )),
        cx,
    );
    registry.register_provider(
        Arc::new(GoogleLanguageModelProvider::new(
            client.http_client(),
            credentials_provider.clone(),
            cx,
        )),
        cx,
    );
    registry.register_provider(
        MistralLanguageModelProvider::global(
            client.http_client(),
            credentials_provider.clone(),
            cx,
        ),
        cx,
    );
    #[cfg(not(target_family = "wasm"))]
    registry.register_provider(
        Arc::new(BedrockLanguageModelProvider::new(
            client.http_client(),
            credentials_provider.clone(),
            cx,
        )),
        cx,
    );
    registry.register_provider(
        Arc::new(OpenRouterLanguageModelProvider::new(
            client.http_client(),
            credentials_provider.clone(),
            cx,
        )),
        cx,
    );
    registry.register_provider(
        Arc::new(VercelAiGatewayLanguageModelProvider::new(
            client.http_client(),
            credentials_provider.clone(),
            cx,
        )),
        cx,
    );
    registry.register_provider(
        Arc::new(XAiLanguageModelProvider::new(
            client.http_client(),
            credentials_provider.clone(),
            cx,
        )),
        cx,
    );
    registry.register_provider(
        Arc::new(OpenCodeLanguageModelProvider::new(
            client.http_client(),
            credentials_provider.clone(),
            cx,
        )),
        cx,
    );
    registry.register_provider(Arc::new(CopilotChatLanguageModelProvider::new(cx)), cx);
    #[cfg(not(target_family = "wasm"))]
    registry.register_provider(
        Arc::new(OpenAiSubscribedProvider::new(
            client.http_client(),
            credentials_provider,
            cx,
        )),
        cx,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use clock::FakeSystemClock;
    use feature_flags::FeatureFlagAppExt as _;
    use gpui::{AppContext as _, AsyncApp, BorrowAppContext as _, TestAppContext};
    use http_client::FakeHttpClient;
    use language::language_settings::all_language_settings;
    use language_model::{
        AuthenticateError, IconOrSvg, LanguageModelProvider as _, LanguageModelProviderState as _,
        ProviderSettingsView,
    };
    use release_channel::AppVersion;
    use std::future::Future;
    use std::pin::Pin;
    use ui::IconName;
    use zed_web_core::ai_proxy;

    /// The placeholder the browser credentials provider answers with for a proxied URL; the
    /// control plane strips it. Taken from the browser half rather than copied: the copy had
    /// nothing asserting it still agreed, and `zed_web_core` pins its own constants against
    /// the shared contract fixture (`docs/contracts/ai-providers.v1.json`), so the whole
    /// chain now moves together or fails.
    const PROXY_PLACEHOLDER_KEY: &[u8] = ai_proxy::PLACEHOLDER_KEY.as_bytes();

    /// The built-in providers `zed_web` points at `<origin>/api/ai/<id>`.
    const PROXIED_PROVIDERS: &[&str] = ai_proxy::PROXIED_LANGUAGE_MODEL_PROVIDERS;

    struct FakeCredentialsProvider;

    impl CredentialsProvider for FakeCredentialsProvider {
        fn read_credentials<'a>(
            &'a self,
            _url: &'a str,
            _cx: &'a AsyncApp,
        ) -> Pin<Box<dyn Future<Output = Result<Option<(String, Vec<u8>)>>> + 'a>> {
            Box::pin(async { Ok(None) })
        }

        fn write_credentials<'a>(
            &'a self,
            _url: &'a str,
            _username: &'a str,
            _password: &'a [u8],
            _cx: &'a AsyncApp,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn delete_credentials<'a>(
            &'a self,
            _url: &'a str,
            _cx: &'a AsyncApp,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    /// The browser credentials provider's contract, as `language_models` sees it: the
    /// placeholder for `<origin>/api/ai/<id>` URLs whose provider the control plane holds a
    /// key for, `Ok(None)` for every other URL, and never `Err`.
    struct ProxyPlaceholderCredentials {
        origin: String,
        configured: Vec<&'static str>,
        asked: parking_lot::Mutex<Vec<String>>,
    }

    impl ProxyPlaceholderCredentials {
        fn new(origin: &str, configured: &[&'static str]) -> Self {
            Self {
                origin: origin.to_owned(),
                configured: configured.to_vec(),
                asked: parking_lot::Mutex::new(Vec::new()),
            }
        }

        fn asked(&self) -> Vec<String> {
            self.asked.lock().clone()
        }
    }

    impl CredentialsProvider for ProxyPlaceholderCredentials {
        fn read_credentials<'a>(
            &'a self,
            url: &'a str,
            _cx: &'a AsyncApp,
        ) -> Pin<Box<dyn Future<Output = Result<Option<(String, Vec<u8>)>>> + 'a>> {
            self.asked.lock().push(url.to_owned());
            let prefix = format!("{}/api/ai/", self.origin);
            let credential = url
                .strip_prefix(&prefix)
                .filter(|id| self.configured.contains(id))
                .map(|_| ("Bearer".to_owned(), PROXY_PLACEHOLDER_KEY.to_vec()));
            Box::pin(async move { Ok(credential) })
        }

        fn write_credentials<'a>(
            &'a self,
            _url: &'a str,
            _username: &'a str,
            _password: &'a [u8],
            _cx: &'a AsyncApp,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn delete_credentials<'a>(
            &'a self,
            _url: &'a str,
            _cx: &'a AsyncApp,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    /// A keychain that fails, the way the stock web platform's does; counts the reads.
    #[derive(Default)]
    struct FailingCredentialsProvider {
        reads: std::sync::atomic::AtomicUsize,
    }

    impl FailingCredentialsProvider {
        fn reads(&self) -> usize {
            self.reads.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl CredentialsProvider for FailingCredentialsProvider {
        fn read_credentials<'a>(
            &'a self,
            _url: &'a str,
            _cx: &'a AsyncApp,
        ) -> Pin<Box<dyn Future<Output = Result<Option<(String, Vec<u8>)>>> + 'a>> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async { Err(anyhow::anyhow!("credential storage is not available")) })
        }

        fn write_credentials<'a>(
            &'a self,
            _url: &'a str,
            _username: &'a str,
            _password: &'a [u8],
            _cx: &'a AsyncApp,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
            Box::pin(async { Err(anyhow::anyhow!("credential storage is not available")) })
        }

        fn delete_credentials<'a>(
            &'a self,
            _url: &'a str,
            _cx: &'a AsyncApp,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
            Box::pin(async { Err(anyhow::anyhow!("credential storage is not available")) })
        }
    }

    fn set_user_settings(content: serde_json::Value, cx: &mut App) {
        cx.update_global::<SettingsStore, _>(|store, cx| {
            store
                .set_user_settings(&content.to_string(), cx)
                .expect("failed to parse test settings");
        });
    }

    /// The settings document `zed_web` seeds (b11 §4.3), for `origin` — produced by the
    /// function that seeds it in the browser, not by a second copy of its output.
    fn proxy_settings(origin: &str) -> serde_json::Value {
        ai_proxy::ai_proxy_settings_overrides(origin)
    }

    /// The `openai_compatible` settings block for [`PROBE_ID`], pointed at `api_url`.
    fn compat_settings(api_url: &str) -> serde_json::Value {
        serde_json::json!({
            "language_models": {
                "openai_compatible": {
                    PROBE_ID: { "api_url": api_url, "available_models": [] }
                }
            }
        })
    }

    /// A compat-provider id chosen so that no developer machine exports its API-key variable
    /// (`ZS_PROXY_PROBE_API_KEY`). An OpenAI-compatible provider builds its [`EnvVar`] per
    /// instance from its id (`api_compatible.rs`: `format!("{id}_API_KEY")`, no `LazyLock`
    /// static), which is what makes the placeholder path assertable on every machine.
    const PROBE_ID: &str = "zs_proxy_probe";
    const PROBE_ENV_VAR: &str = "ZS_PROXY_PROBE_API_KEY";
    /// `provider_for_url` parses `compat/<name>`; this is the id the control plane holds a
    /// key under for [`PROBE_ID`].
    const PROBE_PROVIDER: &str = "compat/zs_proxy_probe";

    /// A non-empty provider key in the environment pre-empts the credentials provider:
    /// `ApiKeyState::load_if_needed` returns from the `static LazyLock<EnvVar>` the provider
    /// declares (which a test cannot replace) without ever calling `read_credentials`. A
    /// branded provider's placeholder path is therefore unobservable on a machine that
    /// exports one — but the test must not silently pass either, so instead of returning it
    /// asserts that documented pre-emption with [`assert_env_var_pre_empted`], and
    /// `test_proxy_placeholder_binds_a_compat_provider` carries the same contract
    /// hermetically. Loud, and a hard failure under CI, where nothing should export provider
    /// keys into the unit-test environment.
    fn env_key_pre_empts(names: &[&str]) -> bool {
        let set: Vec<&str> = names
            .iter()
            .copied()
            .filter(|name| std::env::var(name).is_ok_and(|value| !value.is_empty()))
            .collect();
        if set.is_empty() {
            return false;
        }
        let message = format!(
            "{} is set in the environment and pre-empts the credentials provider under test; asserting the env-var contract instead of the placeholder one",
            set.join(" and ")
        );
        eprintln!("{message}");
        assert!(std::env::var_os("CI").is_none(), "{message}");
        true
    }

    /// `(has_key, is_from_env_var, env_var_name)` of a compat provider's `ApiKeyState`. Its
    /// `settings_view` is a `SubPage`, not `ApiKey`, so the branded providers' route to those
    /// three facts does not exist here; `observable_entity` is the public one.
    fn compat_key_state(
        provider: &OpenAiCompatibleLanguageModelProvider,
        cx: &mut TestAppContext,
    ) -> (bool, bool, String) {
        let state = provider
            .observable_entity()
            .expect("a compat provider exposes its state entity");
        cx.update(|cx| {
            let key_state = &state.read(cx).api_key_state;
            (
                key_state.has_key(),
                key_state.is_from_env_var(),
                key_state.env_var_name().to_string(),
            )
        })
    }

    /// The half of the contract that still holds when [`env_key_pre_empts`] found a key: the
    /// environment wins, the settings page says so, and the credentials provider is never
    /// asked at all.
    async fn assert_env_var_pre_empted(
        provider: &dyn language_model::LanguageModelProvider,
        credentials: &ProxyPlaceholderCredentials,
        cx: &mut TestAppContext,
    ) {
        authenticate(provider, cx).await;
        assert!(cx.update(|cx| provider.is_authenticated(cx)));
        let Some(ProviderSettingsView::ApiKey(config)) = cx.update(|cx| provider.settings_view(cx))
        else {
            panic!("the provider has an API-key settings view");
        };
        assert!(config.has_key);
        assert!(config.is_from_env_var);
        assert!(
            credentials.asked().is_empty(),
            "an exported key pre-empts the credentials provider, which is then never asked"
        );
    }

    /// `authenticate` resolves `Ok(())` whatever the keychain answered: `ApiKeyState::
    /// load_if_needed` discards `into_authenticate_result()` and reports the outcome through
    /// `is_authenticated`/`has_key`, which is what the callers assert on.
    async fn authenticate(
        provider: &dyn language_model::LanguageModelProvider,
        cx: &mut TestAppContext,
    ) {
        cx.update(|cx| provider.authenticate(cx)).await.ok();
    }

    #[gpui::test]
    async fn test_proxy_placeholder_credentials_authenticate_a_proxied_provider(
        cx: &mut TestAppContext,
    ) {
        let (client, _) = cx.update(init_test);
        let origin = "https://zs.example.com";
        cx.update(|cx| set_user_settings(proxy_settings(origin), cx));

        // Each provider gets its own fake so `asked()` is an exact list either way.
        let credentials = Arc::new(ProxyPlaceholderCredentials::new(origin, &["anthropic"]));
        let anthropic = cx.update(|cx| {
            AnthropicLanguageModelProvider::new(client.http_client(), credentials.clone(), cx)
        });
        if env_key_pre_empts(&["ANTHROPIC_API_KEY"]) {
            assert_env_var_pre_empted(&anthropic, &credentials, cx).await;
        } else {
            assert!(!cx.update(|cx| anthropic.is_authenticated(cx)));
            authenticate(&anthropic, cx).await;
            assert!(cx.update(|cx| anthropic.is_authenticated(cx)));
            assert_eq!(
                credentials.asked(),
                vec![format!("{origin}/api/ai/anthropic")],
                "the key is looked up under the seeded api_url"
            );
            let Some(ProviderSettingsView::ApiKey(config)) =
                cx.update(|cx| anthropic.settings_view(cx))
            else {
                panic!("the Anthropic provider has an API-key settings view");
            };
            assert!(config.has_key);
            assert!(!config.is_from_env_var);
        }

        // The control plane holds no OpenAI key: `Ok(None)` is "not configured", which is
        // what the settings page turns into the key prompt.
        let openai_credentials = Arc::new(ProxyPlaceholderCredentials::new(origin, &["anthropic"]));
        let openai = cx.update(|cx| {
            OpenAiLanguageModelProvider::new(client.http_client(), openai_credentials.clone(), cx)
        });
        if env_key_pre_empts(&["OPENAI_API_KEY"]) {
            assert_env_var_pre_empted(&openai, &openai_credentials, cx).await;
            return;
        }
        authenticate(&openai, cx).await;
        assert!(!cx.update(|cx| openai.is_authenticated(cx)));
        assert_eq!(
            openai_credentials.asked(),
            vec![format!("{origin}/api/ai/openai")]
        );
        let Some(ProviderSettingsView::ApiKey(config)) = cx.update(|cx| openai.settings_view(cx))
        else {
            panic!("the OpenAI provider has an API-key settings view");
        };
        assert!(!config.has_key);
    }

    /// The same contract with nothing in the environment able to pre-empt it, so it asserts
    /// on every machine: an OpenAI-compatible provider under [`PROBE_ID`], which is also the
    /// `compat/<name>` URL shape `provider_for_url` parses and the branded providers never
    /// exercise. Covers the seeded-URL → credential binding in both directions.
    #[gpui::test]
    async fn test_proxy_placeholder_binds_a_compat_provider(cx: &mut TestAppContext) {
        assert!(
            std::env::var_os(PROBE_ENV_VAR).is_none(),
            "{PROBE_ENV_VAR} is exported; this test's provider id exists precisely so that \
             nothing in the environment pre-empts its credentials provider"
        );
        let (client, _) = cx.update(init_test);
        let origin = "https://zs.example.com";
        let proxied = ai_proxy::proxy_api_url(origin, PROBE_PROVIDER);
        assert_eq!(proxied, format!("{origin}/api/ai/compat/{PROBE_ID}"));
        let credentials = Arc::new(ProxyPlaceholderCredentials::new(origin, &[PROBE_PROVIDER]));

        cx.update(|cx| set_user_settings(compat_settings(&proxied), cx));
        let provider = cx.update(|cx| {
            OpenAiCompatibleLanguageModelProvider::new(
                PROBE_ID.into(),
                client.http_client(),
                credentials.clone(),
                cx,
            )
        });
        assert!(!cx.update(|cx| provider.is_authenticated(cx)));
        authenticate(&provider, cx).await;
        assert!(cx.update(|cx| provider.is_authenticated(cx)));
        assert_eq!(
            credentials.asked(),
            vec![proxied.clone()],
            "the key is looked up under the proxied api_url"
        );
        let (has_key, is_from_env_var, env_var_name) = compat_key_state(&provider, cx);
        assert!(has_key);
        assert!(!is_from_env_var);
        assert_eq!(env_var_name, PROBE_ENV_VAR);

        // Pointed away from the proxy the placeholder must not follow: the control plane
        // holds a key for `compat/zs_proxy_probe`, not for the provider's own host.
        let direct = "https://api.groq.com/openai/v1";
        cx.update(|cx| set_user_settings(compat_settings(direct), cx));
        authenticate(&provider, cx).await;
        assert!(
            !cx.update(|cx| provider.is_authenticated(cx)),
            "no placeholder for a non-proxy URL"
        );
        assert_eq!(credentials.asked().last().map(String::as_str), Some(direct));

        // And back: the key is re-read for the new URL rather than reused across it.
        cx.update(|cx| set_user_settings(compat_settings(&proxied), cx));
        authenticate(&provider, cx).await;
        assert!(cx.update(|cx| provider.is_authenticated(cx)));
        assert_eq!(
            credentials.asked().last().map(String::as_str),
            Some(proxied.as_str())
        );
    }

    #[gpui::test]
    async fn test_proxy_placeholder_is_bound_to_the_proxied_url(cx: &mut TestAppContext) {
        let (client, _) = cx.update(init_test);
        let origin = "https://zs.example.com";
        let credentials = Arc::new(ProxyPlaceholderCredentials::new(origin, &["anthropic"]));

        // Direct mode: the user's own api_url is a provider host, not the proxy, so the
        // placeholder must not be handed out for it.
        cx.update(|cx| {
            set_user_settings(
                serde_json::json!({
                    "language_models": { "anthropic": { "api_url": "https://api.anthropic.com" } }
                }),
                cx,
            )
        });
        let anthropic = cx.update(|cx| {
            AnthropicLanguageModelProvider::new(client.http_client(), credentials.clone(), cx)
        });
        if env_key_pre_empts(&["ANTHROPIC_API_KEY"]) {
            assert_env_var_pre_empted(&anthropic, &credentials, cx).await;
            return;
        }
        authenticate(&anthropic, cx).await;
        assert!(
            !cx.update(|cx| anthropic.is_authenticated(cx)),
            "no placeholder for a non-proxy URL"
        );
        assert_eq!(
            credentials.asked(),
            vec!["https://api.anthropic.com".to_owned()]
        );

        // Switching the api_url to the proxy re-reads the key for the new URL.
        cx.update(|cx| set_user_settings(proxy_settings(origin), cx));
        authenticate(&anthropic, cx).await;
        assert!(cx.update(|cx| anthropic.is_authenticated(cx)));
        assert_eq!(
            credentials.asked().last().map(String::as_str),
            Some("https://zs.example.com/api/ai/anthropic")
        );
    }

    /// A failing keychain (the stock web platform's, or a browser provider that turned a
    /// failed inventory request into `Err`) reads as "no key" too: `LoadStatus::Error`
    /// leaves the provider unauthenticated, and `ApiKeyState::load_if_needed` returns early
    /// only from `Loaded`, so every later `authenticate` reads the keychain again. The
    /// browser provider relies on exactly that when it answers `Ok(None)` for an unknown
    /// inventory without caching it: the next `authenticate` asks the control plane again.
    /// Driven through [`PROBE_ID`] so nothing in the environment can pre-empt the read and
    /// turn the retry assertion into a no-op.
    #[gpui::test]
    async fn test_proxy_credentials_failure_leaves_the_provider_unauthenticated(
        cx: &mut TestAppContext,
    ) {
        assert!(std::env::var_os(PROBE_ENV_VAR).is_none(), "{PROBE_ENV_VAR}");
        let (client, _) = cx.update(init_test);
        let origin = "https://zs.example.com";
        cx.update(|cx| {
            set_user_settings(
                compat_settings(&ai_proxy::proxy_api_url(origin, PROBE_PROVIDER)),
                cx,
            )
        });
        let credentials = Arc::new(FailingCredentialsProvider::default());
        let provider = cx.update(|cx| {
            OpenAiCompatibleLanguageModelProvider::new(
                PROBE_ID.into(),
                client.http_client(),
                credentials.clone(),
                cx,
            )
        });
        authenticate(&provider, cx).await;
        assert!(!cx.update(|cx| provider.is_authenticated(cx)));
        assert_eq!(credentials.reads(), 1);
        let (has_key, is_from_env_var, _) = compat_key_state(&provider, cx);
        assert!(!has_key, "a failed read leaves the provider with no key");
        assert!(!is_from_env_var);

        // The failure is not sticky: the next `authenticate` reads again.
        authenticate(&provider, cx).await;
        assert_eq!(credentials.reads(), 2, "a failed read is retried");
        assert!(!cx.update(|cx| provider.is_authenticated(cx)));
    }

    /// `ApiKey::load_from_system_keychain` is the direct form (`codestral`,
    /// `edit_prediction` use `ApiKeyState` the same way): `Ok(None)` is
    /// `CredentialsNotFound`, `Err` is `Other`, and the placeholder loads as a key.
    #[gpui::test]
    async fn test_proxy_placeholder_loads_as_a_keychain_key(cx: &mut TestAppContext) {
        cx.update(init_test);
        let origin = "https://zs.example.com";
        let credentials = ProxyPlaceholderCredentials::new(origin, &["codestral"]);
        let async_cx = cx.to_async();

        let key = language_model::ApiKey::load_from_system_keychain(
            &format!("{origin}/api/ai/codestral"),
            &credentials,
            &async_cx,
        )
        .await
        .expect("the placeholder loads");
        assert_eq!(key.key().as_bytes(), PROXY_PLACEHOLDER_KEY);

        let missing = language_model::ApiKey::load_from_system_keychain(
            &format!("{origin}/api/ai/openai"),
            &credentials,
            &async_cx,
        )
        .await
        .expect_err("an unconfigured provider has no key");
        assert!(
            matches!(missing, AuthenticateError::CredentialsNotFound),
            "{missing:?}"
        );

        let failed = language_model::ApiKey::load_from_system_keychain(
            "https://codestral.mistral.ai",
            &FailingCredentialsProvider::default(),
            &async_cx,
        )
        .await
        .expect_err("a failing keychain is an error");
        assert!(matches!(failed, AuthenticateError::Other(_)), "{failed:?}");
    }

    #[gpui::test]
    fn test_proxy_seeded_api_urls_reach_every_proxied_provider(cx: &mut App) {
        init_test(cx);
        let origin = "https://zs.example.com";
        set_user_settings(proxy_settings(origin), cx);
        let settings = AllLanguageModelSettings::get_global(cx);
        let expected = |id: &str| format!("{origin}/api/ai/{id}");
        assert_eq!(settings.anthropic.api_url, expected("anthropic"));
        assert_eq!(settings.openai.api_url, expected("openai"));
        assert_eq!(settings.google.api_url, expected("google"));
        assert_eq!(settings.mistral.api_url, expected("mistral"));
        assert_eq!(settings.deepseek.api_url, expected("deepseek"));
        assert_eq!(settings.open_router.api_url, expected("open_router"));
        assert_eq!(settings.x_ai.api_url, expected("x_ai"));
        assert_eq!(settings.opencode.api_url, expected("opencode"));
        assert_eq!(
            settings.vercel_ai_gateway.api_url,
            expected("vercel_ai_gateway")
        );
        // The `localhost` providers are not seeded (and not registered on wasm).
        assert_eq!(settings.ollama.api_url, "http://localhost:11434");
        assert_eq!(settings.lmstudio.api_url, "http://localhost:1234/api/v0");
        assert_eq!(settings.llama_cpp.api_url, "http://localhost:8080");
        assert_eq!(
            all_language_settings(None, cx)
                .edit_predictions
                .codestral
                .api_url
                .as_deref(),
            Some("https://zs.example.com/api/ai/codestral")
        );
    }

    /// The two halves of b11 agree on the ids, and the ids the browser gates out on wasm
    /// name providers this crate really registers. `PROXIED_PROVIDERS` above is the browser
    /// half's own constant, so a tenth provider added there without a settings row here
    /// fails the first loop; `LOCAL_ONLY_LANGUAGE_MODEL_PROVIDERS` is only ever read by
    /// tests, so a rename upstream would otherwise leave it naming nothing while the three
    /// `#[cfg(not(target_family = "wasm"))]` gates above quietly stopped matching it — and
    /// the wasm bundle would register a picker entry the editor CSP always fails. That the
    /// gates hold on wasm is asserted in the browser, through `test_hooks::ai_keys`.
    #[gpui::test]
    fn test_proxy_provider_ids_match_the_browser_half(cx: &mut App) {
        let (client, credentials) = init_test(cx);
        let origin = "https://zs.example.com";
        set_user_settings(proxy_settings(origin), cx);
        let settings = AllLanguageModelSettings::get_global(cx);
        let seeded: Vec<&str> = PROXIED_PROVIDERS
            .iter()
            .copied()
            .filter(|id| {
                let api_url = match *id {
                    "anthropic" => &settings.anthropic.api_url,
                    "openai" => &settings.openai.api_url,
                    "google" => &settings.google.api_url,
                    "mistral" => &settings.mistral.api_url,
                    "deepseek" => &settings.deepseek.api_url,
                    "open_router" => &settings.open_router.api_url,
                    "x_ai" => &settings.x_ai.api_url,
                    "opencode" => &settings.opencode.api_url,
                    "vercel_ai_gateway" => &settings.vercel_ai_gateway.api_url,
                    unknown => panic!(
                        "{unknown} is proxied by the browser but this test has no settings \
                         row for it; add one so its seeded api_url is exercised"
                    ),
                };
                *api_url == ai_proxy::proxy_api_url(origin, id)
            })
            .collect();
        assert_eq!(seeded, PROXIED_PROVIDERS.to_vec());
        assert_eq!(PROXY_PLACEHOLDER_KEY, b"zs-proxy-v1");

        let local_only: Vec<String> = vec![
            OllamaLanguageModelProvider::new(client.http_client(), credentials.clone(), cx)
                .id()
                .0
                .to_string(),
            LmStudioLanguageModelProvider::new(client.http_client(), credentials.clone(), cx)
                .id()
                .0
                .to_string(),
            LlamaCppLanguageModelProvider::new(client.http_client(), credentials.clone(), cx)
                .id()
                .0
                .to_string(),
        ];
        assert_eq!(
            local_only.iter().map(String::as_str).collect::<Vec<_>>(),
            ai_proxy::LOCAL_ONLY_LANGUAGE_MODEL_PROVIDERS.to_vec()
        );
    }

    fn init_test(cx: &mut App) -> (Arc<Client>, Arc<dyn CredentialsProvider>) {
        let settings_store = SettingsStore::test(cx);
        cx.set_global(settings_store);
        cx.set_global(db::AppDatabase::test_new());
        let app_version = AppVersion::global(cx);
        release_channel::init_test(app_version, release_channel::ReleaseChannel::Dev, cx);
        gpui_tokio::init(cx);
        cx.update_flags(false, Vec::new());

        let client = Client::new(
            Arc::new(FakeSystemClock::new()),
            FakeHttpClient::with_404_response(),
            cx,
        );
        (client, Arc::new(FakeCredentialsProvider))
    }

    fn update_compatible_provider_settings(
        openai: &[&str],
        anthropic: &[&str],
        cx: &mut App,
    ) -> CompatibleProviders {
        fn section(ids: &[&str]) -> serde_json::Value {
            ids.iter()
                .map(|id| {
                    (
                        id.to_string(),
                        serde_json::json!({
                            "api_url": "https://example.com",
                            "available_models": [],
                        }),
                    )
                })
                .collect::<serde_json::Map<String, serde_json::Value>>()
                .into()
        }

        let content = serde_json::json!({
            "language_models": {
                "openai_compatible": section(openai),
                "anthropic_compatible": section(anthropic),
            }
        })
        .to_string();
        cx.update_global::<SettingsStore, _>(|store, cx| {
            store
                .set_user_settings(&content, cx)
                .expect("failed to parse test settings");
        });
        CompatibleProviders::from_settings(cx)
    }

    fn provider_icons(registry: &LanguageModelRegistry, id: &str) -> Vec<IconOrSvg> {
        registry
            .providers()
            .into_iter()
            .filter(|provider| provider.id().0.as_ref() == id)
            .map(|provider| provider.icon())
            .collect()
    }

    #[gpui::test]
    fn test_compatible_provider_id_collision_resolves_when_one_entry_is_removed(cx: &mut App) {
        let (client, credentials_provider) = init_test(cx);
        let registry = cx.new(|_| LanguageModelRegistry::default());

        // The same provider name is configured in both `openai_compatible`
        // and `anthropic_compatible` settings sections; the OpenAI-compatible
        // entry wins the collision.
        let both = update_compatible_provider_settings(&["acme"], &["acme"], cx);
        registry.update(cx, |registry, cx| {
            register_compatible_providers(
                registry,
                &CompatibleProviders::default(),
                &both,
                &client,
                &credentials_provider,
                cx,
            );
        });
        assert_eq!(
            registry.read_with(cx, |registry, _| provider_icons(registry, "acme")),
            vec![IconOrSvg::Icon(IconName::AiOpenAiCompat)],
            "the OpenAI-compatible provider should win the name collision"
        );

        // The user removes the `anthropic_compatible` entry; the remaining
        // `openai_compatible` entry must stay registered.
        let openai_only = update_compatible_provider_settings(&["acme"], &[], cx);
        registry.update(cx, |registry, cx| {
            register_compatible_providers(
                registry,
                &both,
                &openai_only,
                &client,
                &credentials_provider,
                cx,
            );
        });
        assert_eq!(
            registry.read_with(cx, |registry, _| provider_icons(registry, "acme")),
            vec![IconOrSvg::Icon(IconName::AiOpenAiCompat)],
            "the provider registered for `acme` should be the OpenAI-compatible one"
        );
    }

    #[gpui::test]
    fn test_compatible_provider_changes_kind_and_unregisters(cx: &mut App) {
        let (client, credentials_provider) = init_test(cx);
        let registry = cx.new(|_| LanguageModelRegistry::default());

        let both = update_compatible_provider_settings(&["acme"], &["acme"], cx);
        registry.update(cx, |registry, cx| {
            register_compatible_providers(
                registry,
                &CompatibleProviders::default(),
                &both,
                &client,
                &credentials_provider,
                cx,
            );
        });

        // Removing the `openai_compatible` entry hands the name over to the
        // remaining `anthropic_compatible` entry.
        let anthropic_only = update_compatible_provider_settings(&[], &["acme"], cx);
        registry.update(cx, |registry, cx| {
            register_compatible_providers(
                registry,
                &both,
                &anthropic_only,
                &client,
                &credentials_provider,
                cx,
            );
        });
        assert_eq!(
            registry.read_with(cx, |registry, _| provider_icons(registry, "acme")),
            vec![IconOrSvg::Icon(IconName::AiAnthropicCompat)],
            "after removing the openai_compatible entry, the anthropic_compatible provider should be registered"
        );

        // Removing the last entry unregisters the provider entirely.
        let none = update_compatible_provider_settings(&[], &[], cx);
        registry.update(cx, |registry, cx| {
            register_compatible_providers(
                registry,
                &anthropic_only,
                &none,
                &client,
                &credentials_provider,
                cx,
            );
        });
        assert_eq!(
            registry.read_with(cx, |registry, _| provider_icons(registry, "acme")),
            Vec::new(),
            "removing all entries should unregister the provider"
        );
    }
}
