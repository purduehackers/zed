//! The browser half of the AI provider proxy (b11 §3.17): a credentials provider that
//! answers Zed's key lookups for `<origin>/api/ai/<provider>` URLs from the control plane's
//! key inventory (a placeholder the proxy strips; the real key never reaches the tab),
//! stores a key typed in the settings page through `PUT /api/ai/keys/<provider>`, keeps keys
//! for every other URL in memory for the tab (there is no keychain on the web), and an HTTP
//! client wrapper that turns the proxy's own refusals (`x-zs-ai-error`) into an in-editor
//! notice pointing at the keys page. Installed before `Client::production`, which captures
//! the credentials provider global and the HTTP client.
//!
//! One documented deviation from b11 §3.17: a key write is confirmed by re-reading the
//! inventory, and a re-read that fails outright (a 429 or 5xx on the confirming `GET`, both
//! counted against the same 30/min route limit) is logged and treated as saved, because the
//! `PUT` itself answered 2xx. Only an inventory that was read and still lacks the provider is
//! an error. A 401/403 on the inventory read — the `zs_ai` cookie lapsed — is reported once
//! per tab as "reload the page" instead of silently reading as "no keys".
//!
//! Two rules exist because `ApiKeyState::store` reports success whatever this provider
//! returns (`language_model/src/api_key.rs`: it `log_err()`s the result and then sets the
//! load status): every failing write *and* every failing delete raises a notification, and a
//! `/api/ai/…` URL belonging to another origin (a settings document synced from `www`, a
//! preview host or a different scheme — [`ai_proxy::foreign_proxy_provider`]) is refused
//! loudly instead of being kept in tab-local memory that a reload silently loses. The
//! inventory request itself carries [`KEYS_REQUEST_TIMEOUT`], because the browser `fetch`
//! client has no deadline and one request that never settles would wedge every credential
//! lookup in the tab.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context as _, Result, anyhow};
use credentials_provider::CredentialsProvider;
use futures::{
    AsyncReadExt as _, FutureExt as _, StreamExt as _,
    channel::mpsc,
    future::{BoxFuture, Either, Shared, select},
};
use gpui::{App, AppContext as _, AsyncApp, BackgroundExecutor, Entity, actions};
use http_client::{AsyncBody, HttpClient, Method, Request, Response, Url, http::HeaderValue};
use language_model::{Event as RegistryEvent, LanguageModelRegistry};
use language_models::AllLanguageModelSettings;
use parking_lot::Mutex;
use settings::Settings as _;
use web_time::Instant;
use workspace::notifications::{
    NotificationId, show_app_notification, simple_message_notification::MessageNotification,
};
use zed_web_core::ai_proxy::{
    self, AI_ERROR_HEADER, KEYS_CACHE_TTL_SECS, KeysCache, KeysOutcome, Lookup, ProxyError,
    ProxyNotice, ProxyProvider,
};

use crate::bridge;

actions!(
    zed_web,
    [
        /// Opens the AI keys page of the control plane in a new tab.
        ManageAiKeys
    ]
);

/// How long one `GET /api/ai/keys` answer is reused before it is asked again.
const KEYS_CACHE_TTL: Duration = Duration::from_secs(KEYS_CACHE_TTL_SECS);
/// How long one `GET /api/ai/keys` may run before it is abandoned. The browser `fetch`
/// client applies no deadline of its own and [`KeysCache`] keeps every later lookup joined
/// to the request in flight, so a socket that never settles (a half-open connection after a
/// network change, a proxy holding it open) would otherwise leave every provider
/// unauthenticated — and every key write hanging on its confirming read — for the life of
/// the tab, with no retry and no error. Timing out resolves to `Unknown`, which is not
/// cached, so the next lookup asks again.
const KEYS_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
/// Responses larger than this are not the inventory or an error envelope.
const MAX_RESPONSE_BYTES: usize = 256 * 1024;

/// One HTTP exchange with the keys routes, reduced to what the callers decide on.
struct KeysExchange {
    status: u16,
    error_code: Option<String>,
    body: String,
}

/// One `GET /api/ai/keys` in flight, shared by every provider that asks while it runs.
type KeysRequest = Shared<BoxFuture<'static, KeysOutcome>>;

/// The credentials provider for the browser. Both fields are `parking_lot::Mutex` (the trait
/// is `Send + Sync`) and no guard is held across an `await`: every method takes the lock,
/// reads or installs state, drops it, and only then awaits. The single-flight and TTL rules
/// live in the host-tested [`KeysCache`].
pub struct ProxyCredentialsProvider {
    origin: String,
    http: Arc<dyn HttpClient>,
    /// Only used for [`KEYS_REQUEST_TIMEOUT`]'s timer.
    executor: BackgroundExecutor,
    configured: Mutex<KeysCache<KeysRequest>>,
    /// Keys for non-proxy URLs (direct mode), for the tab's lifetime only.
    session_keys: Mutex<HashMap<String, (String, Vec<u8>)>>,
    /// Whether the "session expired; reload" notice was raised in this tab: every provider
    /// asks at boot and after each TTL, and only a reload can fix it.
    session_expired_notified: AtomicBool,
}

impl ProxyCredentialsProvider {
    pub fn new(origin: String, http: Arc<dyn HttpClient>, executor: BackgroundExecutor) -> Self {
        Self {
            origin,
            http,
            executor,
            configured: Mutex::new(KeysCache::new(KEYS_CACHE_TTL)),
            session_keys: Mutex::new(HashMap::new()),
            session_expired_notified: AtomicBool::new(false),
        }
    }

    /// Forgets the cached inventory; the next lookup asks the control plane again, and the
    /// answer of a request still in flight is discarded (it predates whatever changed).
    pub fn invalidate(&self) {
        self.configured.lock().invalidate();
    }

    async fn configured_ids(&self) -> KeysOutcome {
        let (request, ticket) = {
            let mut cache = self.configured.lock();
            match cache.lookup(Instant::now()) {
                Lookup::Fresh(ids) => return KeysOutcome::Known(ids),
                Lookup::Join(request, ticket) => (request, ticket),
                Lookup::Start => {
                    let request: KeysRequest = fetch_keys(
                        self.http.clone(),
                        self.executor.clone(),
                        ai_proxy::keys_url(&self.origin, None),
                    )
                    .boxed()
                    .shared();
                    let ticket = cache.start(request.clone());
                    (request, ticket)
                }
            }
        };
        let outcome = request.await;
        self.configured
            .lock()
            .settle(ticket, &outcome, Instant::now());
        outcome
    }

    async fn put_key(&self, provider: &ProxyProvider, password: &[u8]) -> Result<()> {
        let payload = ai_proxy::write_key_payload(password)?;
        let url = ai_proxy::keys_url(&self.origin, Some(&provider.id));
        let exchange = exchange(self.http.as_ref(), Method::PUT, &url, Some(payload)).await?;
        self.invalidate();
        if !(200..300).contains(&exchange.status) {
            return Err(anyhow!(describe(&exchange, &self.origin, provider)));
        }
        // `ApiKeyState::store` reports "API Key Configured" whatever this returns, so the
        // inventory is re-read and a key the control plane does not hold is an error. A
        // re-read that fails outright is not (the module doc records the deviation from
        // b11 §3.17): the 2xx `PUT` is the authoritative answer, and turning a 429 on the
        // confirming `GET` into "not saved" would tell the user the opposite of the truth.
        match self.configured_ids().await {
            KeysOutcome::Known(ids) if ids.contains(&provider.id) => Ok(()),
            KeysOutcome::Known(_) => Err(anyhow!(
                "The {} key was not saved: the control plane still reports it unconfigured. Manage keys at {}.",
                provider.label(),
                ai_proxy::manage_keys_url(&self.origin)
            )),
            KeysOutcome::Disabled => Err(anyhow!(
                "The AI proxy is disabled on this deployment, so the {} key cannot be stored.",
                provider.label()
            )),
            KeysOutcome::Unknown { status, detail } => {
                log::warn!(
                    "{} key stored, but the inventory could not be re-read (HTTP {status}: {detail})",
                    provider.id
                );
                Ok(())
            }
        }
    }

    /// Raises the notice `ApiKeyState::store` does not: it only logs what the credentials
    /// provider returned and then reports the key as saved (or removed) either way, so a
    /// failure the user is not told about looks exactly like a success until the next
    /// `authenticate` contradicts it.
    fn report_failure(&self, id: &'static str, error: &anyhow::Error, cx: &AsyncApp) {
        let message = format!("{error:#}");
        let manage_keys_url = ai_proxy::manage_keys_url(&self.origin);
        cx.update(|cx| {
            notify(
                NotificationId::Named(id.into()),
                message,
                Some(manage_keys_url),
                cx,
            )
        });
    }

    async fn delete_key(&self, provider: &ProxyProvider) -> Result<()> {
        let url = ai_proxy::keys_url(&self.origin, Some(&provider.id));
        let exchange = exchange(self.http.as_ref(), Method::DELETE, &url, None).await?;
        self.invalidate();
        match exchange.status {
            // Already gone (deleted in the dashboard while this tab was open) is success.
            200..=299 | 404 => Ok(()),
            _ => Err(anyhow!(describe(&exchange, &self.origin, provider))),
        }
    }
}

fn describe(exchange: &KeysExchange, origin: &str, provider: &ProxyProvider) -> String {
    let error: ProxyError = ai_proxy::parse_proxy_error(
        exchange.status,
        exchange.error_code.as_deref(),
        &exchange.body,
    );
    ai_proxy::describe_proxy_error(&error, origin, provider)
}

async fn fetch_keys(
    http: Arc<dyn HttpClient>,
    executor: BackgroundExecutor,
    url: String,
) -> KeysOutcome {
    let request = Box::pin(exchange(http.as_ref(), Method::GET, &url, None));
    let deadline = Box::pin(executor.timer(KEYS_REQUEST_TIMEOUT));
    match select(request, deadline).await {
        Either::Left((Ok(exchange), _)) => {
            ai_proxy::parse_keys_outcome(exchange.status, &exchange.body)
        }
        Either::Left((Err(error), _)) => KeysOutcome::Unknown {
            status: 0,
            detail: format!("{error:#}"),
        },
        // Dropping `request` here abandons the fetch; `Unknown` is not cached, so the next
        // lookup starts a fresh one instead of joining a request that never settles.
        Either::Right(((), _)) => KeysOutcome::Unknown {
            status: 0,
            detail: format!(
                "the key inventory request timed out after {} seconds",
                KEYS_REQUEST_TIMEOUT.as_secs()
            ),
        },
    }
}

/// One request to the keys routes. The `zs_ai` cookie rides along because the browser
/// `fetch` client sends same-origin credentials by default.
async fn exchange(
    http: &dyn HttpClient,
    method: Method,
    url: &str,
    json_body: Option<String>,
) -> Result<KeysExchange> {
    let mut builder = Request::builder()
        .method(method)
        .uri(url)
        .header("Accept", "application/json");
    let body = match json_body {
        Some(json) => {
            builder = builder.header("Content-Type", "application/json");
            AsyncBody::from(json)
        }
        None => AsyncBody::empty(),
    };
    let request = builder.body(body).context("building the keys request")?;
    let mut response = http
        .send(request)
        .await
        .with_context(|| format!("requesting {url}"))?;
    let status = response.status().as_u16();
    let error_code = response
        .headers()
        .get(AI_ERROR_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let mut body = String::new();
    response
        .body_mut()
        .take(MAX_RESPONSE_BYTES as u64)
        .read_to_string(&mut body)
        .await
        .with_context(|| format!("reading the response of {url}"))?;
    Ok(KeysExchange {
        status,
        error_code,
        body,
    })
}

impl CredentialsProvider for ProxyCredentialsProvider {
    fn read_credentials<'a>(
        &'a self,
        url: &'a str,
        cx: &'a AsyncApp,
    ) -> Pin<Box<dyn Future<Output = Result<Option<(String, Vec<u8>)>>> + 'a>> {
        Box::pin(async move {
            let Some(provider) = ai_proxy::provider_for_url(&self.origin, url) else {
                if let Some(foreign) = ai_proxy::foreign_proxy_provider(&self.origin, url) {
                    log::warn!(
                        "{url} is the AI proxy of another origin than {}; the placeholder is \
                         only valid for this page's proxy, so {} reads as unconfigured",
                        self.origin,
                        foreign.id
                    );
                    return Ok(None);
                }
                return Ok(self.session_keys.lock().get(url).cloned());
            };
            let outcome = self.configured_ids().await;
            match &outcome {
                KeysOutcome::Unknown { status, detail } => {
                    log::warn!(
                        "AI keys inventory unavailable (HTTP {status}: {detail}); {} reads as unconfigured for now",
                        provider.id
                    );
                    // The cookie lapsed (or the request was refused as cross-site): every
                    // provider would show the key prompt for a key the user already has.
                    if outcome.is_session_expired()
                        && !self.session_expired_notified.swap(true, Ordering::AcqRel)
                    {
                        cx.update(|cx| {
                            notify(
                                NotificationId::Named("ai-keys-unauthenticated".into()),
                                ai_proxy::SESSION_EXPIRED_MESSAGE.to_owned(),
                                None,
                                cx,
                            )
                        });
                    }
                }
                KeysOutcome::Disabled => {
                    log::debug!(
                        "the AI proxy is disabled; {} reads as unconfigured",
                        provider.id
                    )
                }
                KeysOutcome::Known(_) => {}
            }
            // Never `Err`: `ApiKeyState` turns a credentials error into an authentication
            // error on the provider, and every provider asks at boot.
            Ok(ai_proxy::credential_for(&provider, &outcome))
        })
    }

    fn write_credentials<'a>(
        &'a self,
        url: &'a str,
        username: &'a str,
        password: &'a [u8],
        cx: &'a AsyncApp,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(async move {
            let Some(provider) = ai_proxy::provider_for_url(&self.origin, url) else {
                if let Some(foreign) = ai_proxy::foreign_proxy_provider(&self.origin, url) {
                    // Tab-local memory would look saved (`ApiKeyState::store` shows "API Key
                    // Configured" whatever this returns) and be gone on reload, and the URL
                    // is unreachable under the editor CSP anyway.
                    let error = anyhow!(
                        "The {} key was not saved: this page is served from {}, but the \
                         provider's api_url points at the AI proxy of another origin ({url}), \
                         which the browser cannot reach. Set api_url to {} and try again.",
                        foreign.label(),
                        self.origin,
                        ai_proxy::proxy_api_url(&self.origin, &foreign.id)
                    );
                    self.report_failure("ai-key-foreign-origin", &error, cx);
                    return Err(error);
                }
                ai_proxy::ensure_not_placeholder(password)?;
                self.session_keys
                    .lock()
                    .insert(url.to_owned(), (username.to_owned(), password.to_vec()));
                return Ok(());
            };
            let result = self.put_key(&provider, password).await;
            if let Err(error) = &result {
                // `ApiKeyState::store` only logs this error and then shows the key as
                // configured; the user must hear that nothing was saved.
                self.report_failure("ai-key-save-failed", error, cx);
            }
            result
        })
    }

    fn delete_credentials<'a>(
        &'a self,
        url: &'a str,
        cx: &'a AsyncApp,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(async move {
            let Some(provider) = ai_proxy::provider_for_url(&self.origin, url) else {
                self.session_keys.lock().remove(url);
                return Ok(());
            };
            let result = self.delete_key(&provider).await;
            if let Err(error) = &result {
                // Same asymmetry as the write path: `ApiKeyState::store` logs this and then
                // sets `LoadStatus::NotPresent`, so the settings page shows the key gone
                // while the control plane still holds it — and the next `authenticate` flips
                // it back to "API Key Configured" with no explanation.
                self.report_failure("ai-key-delete-failed", error, cx);
            }
            result
        })
    }
}

/// Wraps the browser HTTP client so a proxied request the control plane refuses (a
/// `x-zs-ai-error` such as `ai_key_missing`, `ai_spend_cap`, `ai_disabled`) raises an
/// in-editor notice with a "Manage AI keys" button, in addition to the dialect-shaped error
/// the provider crate shows in the thread. Bodies are passed through untouched.
struct ProxyAwareHttpClient {
    inner: Arc<dyn HttpClient>,
    origin: String,
    notices: mpsc::UnboundedSender<(ProxyProvider, String)>,
}

impl HttpClient for ProxyAwareHttpClient {
    fn user_agent(&self) -> Option<&HeaderValue> {
        self.inner.user_agent()
    }

    fn proxy(&self) -> Option<&Url> {
        self.inner.proxy()
    }

    fn send(&self, request: Request<AsyncBody>) -> BoxFuture<'static, Result<Response<AsyncBody>>> {
        let provider = ai_proxy::provider_for_url(&self.origin, &request.uri().to_string());
        let response = self.inner.send(request);
        let Some(provider) = provider else {
            return response;
        };
        let notices = self.notices.clone();
        async move {
            let response = response.await?;
            if let Some(code) = response
                .headers()
                .get(AI_ERROR_HEADER)
                .and_then(|value| value.to_str().ok())
            {
                // A dropped receiver only means the app is gone.
                notices.unbounded_send((provider, code.to_owned())).ok();
            }
            Ok(response)
        }
        .boxed()
    }
}

fn notify(id: NotificationId, message: String, manage_keys_url: Option<String>, cx: &mut App) {
    show_app_notification(id, cx, move |cx| {
        cx.new(|cx| {
            let notification = MessageNotification::new(message.clone(), cx);
            match &manage_keys_url {
                Some(url) => {
                    let url = url.clone();
                    notification
                        .primary_message("Manage AI keys")
                        .primary_on_click(move |_, cx| cx.open_url(&url))
                }
                None => notification,
            }
        })
    });
}

fn spawn_notice_loop(
    mut notices: mpsc::UnboundedReceiver<(ProxyProvider, String)>,
    origin: String,
    cx: &mut App,
) {
    cx.spawn(async move |cx| {
        while let Some((provider, code)) = notices.next().await {
            let Some(ProxyNotice {
                message,
                offer_manage_keys,
            }) = ai_proxy::notice_for(&code, &provider, &origin)
            else {
                log::debug!("proxy refused a {} request: {code}", provider.id);
                continue;
            };
            log::warn!("proxy refused a {} request: {code}", provider.id);
            let manage_keys_url = offer_manage_keys.then(|| ai_proxy::manage_keys_url(&origin));
            cx.update(|cx| {
                notify(
                    NotificationId::Named(format!("ai-proxy-{code}").into()),
                    message,
                    manage_keys_url,
                    cx,
                )
            });
        }
    })
    .detach();
}

/// An OpenCode model whose `custom_model_api_url` is not a proxied URL leaves the proxy for a
/// host the editor CSP blocks (`provider/opencode.rs` substitutes that URL wholesale), so the
/// request fails with an opaque network error in the thread. Raises one notice per tab when
/// such a model becomes the default (b11 §7 item 4b). Runs after `language_models::init`.
pub fn watch_default_model(origin: &str, cx: &mut App) {
    let registry = LanguageModelRegistry::global(cx);
    let origin = origin.to_owned();
    let notified = Arc::new(AtomicBool::new(false));
    let check = move |registry: &Entity<LanguageModelRegistry>, cx: &mut App| {
        let Some(configured) = registry.read(cx).default_model() else {
            return;
        };
        if configured.provider.id().0.as_ref() != "opencode" {
            return;
        }
        let model_id = configured.model.id().0;
        // Model ids are `<subscription prefix>/<model name>`.
        let Some((_, model_name)) = model_id.split_once('/') else {
            return;
        };
        let custom_url = AllLanguageModelSettings::get_global(cx)
            .opencode
            .available_models
            .iter()
            .filter(|model| model.name == model_name)
            .find_map(|model| model.custom_model_api_url.clone())
            .filter(|url| !url.is_empty());
        let Some(custom_url) = custom_url else {
            return;
        };
        if ai_proxy::provider_for_url(&origin, &custom_url).is_some() {
            return;
        }
        if notified.swap(true, Ordering::AcqRel) {
            return;
        }
        log::warn!("opencode model {model_id} uses {custom_url}, which bypasses the AI proxy");
        notify(
            NotificationId::Named("ai-opencode-custom-url".into()),
            format!(
                "The selected OpenCode model \"{model_name}\" sends requests to {custom_url}, which the browser cannot reach: only the AI proxy at {} is allowed. Point custom_model_api_url at a proxied endpoint or pick another model.",
                ai_proxy::proxy_api_url(&origin, "opencode")
            ),
            None,
            cx,
        );
    };
    check(&registry, cx);
    cx.subscribe(&registry, move |registry, event, cx| {
        if matches!(event, RegistryEvent::DefaultModelChanged) {
            check(&registry, cx);
        }
    })
    .detach();
}

/// Installs the proxy pieces: the credentials provider as the `zed_credentials_provider`
/// global, the notice-raising HTTP client as the app's, and the [`ManageAiKeys`] action.
/// Must run before `Client::production`, which captures both.
pub fn install(origin: &str, cx: &mut App) -> Arc<ProxyCredentialsProvider> {
    let base = cx.http_client();
    let provider = Arc::new(ProxyCredentialsProvider::new(
        origin.to_owned(),
        base.clone(),
        cx.background_executor().clone(),
    ));
    let credentials: Arc<dyn CredentialsProvider> = provider.clone();
    cx.set_global(zed_credentials_provider::ZedCredentialsProvider(
        credentials,
    ));

    let (notices_tx, notices_rx) = mpsc::unbounded();
    cx.set_http_client(Arc::new(ProxyAwareHttpClient {
        inner: base,
        origin: origin.to_owned(),
        notices: notices_tx,
    }));
    spawn_notice_loop(notices_rx, origin.to_owned(), cx);

    let manage_keys_url = ai_proxy::manage_keys_url(origin);
    cx.on_action(move |_: &ManageAiKeys, cx| cx.open_url(&manage_keys_url));
    provider
}

/// `window.location.origin`, canonicalised.
pub fn page_origin() -> Result<String> {
    let window = web_sys::window().context("no window: not on the main thread")?;
    let origin = window
        .location()
        .origin()
        .map_err(|error| anyhow!("location.origin: {}", bridge::describe_js_value(&error)))?;
    ai_proxy::normalize_origin(&origin)
}
