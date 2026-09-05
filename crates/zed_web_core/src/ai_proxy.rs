//! The browser side of the AI provider proxy (b11 §3.16): the control-plane paths, the
//! settings overrides that point every bring-your-own-key provider at
//! `<origin>/api/ai/<provider>`, the placeholder credential the proxy expects, and the
//! parsers for the `/api/ai/keys` responses and the proxy's dialect-shaped errors. Pure Rust
//! so `zed_web`'s wasm-only credentials provider (`zed_web/src/ai.rs`) is a thin shell over
//! host-tested rules.

use std::collections::HashSet;

use anyhow::{Context as _, Result, anyhow, ensure};
use serde::Deserialize;

pub mod keys_cache;

pub use keys_cache::{KeysCache, Lookup, Ticket};

/// What the browser credentials provider answers for a proxied provider. The proxy strips it
/// and injects the user's real key; it is never a valid key on its own.
pub const PLACEHOLDER_KEY: &str = "zs-proxy-v1";
/// Path prefix of the proxy on the control plane.
pub const PROXY_PREFIX: &str = "/api/ai/";
/// The key inventory route (`GET`) and the per-provider key routes (`PUT`/`DELETE`).
pub const KEYS_PATH: &str = "/api/ai/keys";
/// The dashboard page where keys are managed.
pub const MANAGE_KEYS_PATH: &str = "/ai";
/// The username stored beside the placeholder; Zed's providers ignore it.
pub const PLACEHOLDER_USERNAME: &str = "Bearer";
/// Header carrying the proxy's machine-readable error code on `/api/ai/<provider>/…`.
pub const AI_ERROR_HEADER: &str = "x-zs-ai-error";
/// How long one `GET /api/ai/keys` answer stays fresh in the browser.
pub const KEYS_CACHE_TTL_SECS: u64 = 60;

/// Built-in `language_models` providers whose `api_url` is redirected to the proxy
/// (`assets/settings/default.json` ids).
pub const PROXIED_LANGUAGE_MODEL_PROVIDERS: &[&str] = &[
    "anthropic",
    "openai",
    "google",
    "mistral",
    "deepseek",
    "open_router",
    "x_ai",
    "opencode",
    "vercel_ai_gateway",
];
/// Edit-prediction providers whose `api_url` is redirected to the proxy.
pub const PROXIED_EDIT_PREDICTION_PROVIDERS: &[&str] = &["codestral"];
/// Providers whose default `api_url` is `localhost`, which the editor CSP (`connect-src
/// 'self' …`) makes unreachable from a tab; `language_models` skips them on wasm.
pub const LOCAL_ONLY_LANGUAGE_MODEL_PROVIDERS: &[&str] = &["ollama", "lmstudio", "llama.cpp"];

/// A provider the proxy serves, as parsed from a request or credentials URL.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProxyProvider {
    /// `anthropic`, `codestral`, `compat/<name>`, …
    pub id: String,
}

impl ProxyProvider {
    /// The display name the control plane uses for the built-ins.
    pub fn label(&self) -> String {
        match self.id.as_str() {
            "anthropic" => "Anthropic".into(),
            "openai" => "OpenAI".into(),
            "google" => "Google AI".into(),
            "mistral" => "Mistral".into(),
            "deepseek" => "DeepSeek".into(),
            "open_router" => "OpenRouter".into(),
            "x_ai" => "xAI".into(),
            "opencode" => "OpenCode".into(),
            "vercel_ai_gateway" => "Vercel AI Gateway".into(),
            "codestral" => "Codestral".into(),
            other => other
                .strip_prefix("compat/")
                .map(|name| format!("{name} (compatible)"))
                .unwrap_or_else(|| other.to_owned()),
        }
    }
}

/// Validates and canonicalises a page origin: `scheme://host[:port]`, lowercase, no path,
/// query, fragment or userinfo, no trailing slash. Default ports are dropped so
/// `https://zs.example.com:443` and `https://zs.example.com` compare equal.
pub fn normalize_origin(origin: &str) -> Result<String> {
    let trimmed = origin.trim().trim_end_matches('/');
    let (scheme, authority) = trimmed
        .split_once("://")
        .with_context(|| format!("origin {origin:?} must be scheme://host[:port]"))?;
    let scheme = scheme.to_ascii_lowercase();
    ensure!(
        scheme == "http" || scheme == "https",
        "origin {origin:?} must be http or https"
    );
    ensure!(!authority.is_empty(), "origin {origin:?} has no host");
    ensure!(
        !authority.contains(['/', '?', '#', '@', ' ', '\\']),
        "origin {origin:?} must not carry a path, query, fragment or userinfo"
    );
    Ok(format!(
        "{scheme}://{}",
        strip_default_port(&scheme, &authority.to_ascii_lowercase())
    ))
}

fn strip_default_port<'a>(scheme: &str, authority: &'a str) -> &'a str {
    let default_port = match scheme {
        "http" => ":80",
        "https" => ":443",
        _ => return authority,
    };
    authority.strip_suffix(default_port).unwrap_or(authority)
}

/// `{origin}/api/ai/{provider}`, no trailing slash: the value seeded into `api_url`.
pub fn proxy_api_url(origin: &str, provider: &str) -> String {
    format!("{}{PROXY_PREFIX}{provider}", origin.trim_end_matches('/'))
}

/// `{origin}/api/ai/keys` or `{origin}/api/ai/keys/{provider}`.
pub fn keys_url(origin: &str, provider: Option<&str>) -> String {
    let origin = origin.trim_end_matches('/');
    match provider {
        Some(provider) => format!("{origin}{KEYS_PATH}/{provider}"),
        None => format!("{origin}{KEYS_PATH}"),
    }
}

/// `{origin}/ai`: the dashboard page that manages keys.
pub fn manage_keys_url(origin: &str) -> String {
    format!("{}{MANAGE_KEYS_PATH}", origin.trim_end_matches('/'))
}

/// `url`'s path, and whether the URL addresses `origin`. `None` when `url` is neither an
/// origin-relative path nor an absolute `scheme://host[:port]/…` URL. A relative path is
/// always same-origin: the browser resolves it against the page.
fn split_origin<'a>(origin: &str, url: &'a str) -> Option<(&'a str, bool)> {
    let url = url.split(['?', '#']).next().unwrap_or_default();
    if url.starts_with('/') {
        return Some((url, true));
    }
    let (scheme, rest) = url.split_once("://")?;
    let (authority, path) = match rest.find('/') {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, "/"),
    };
    let scheme = scheme.to_ascii_lowercase();
    let authority = authority.to_ascii_lowercase();
    let candidate = format!("{scheme}://{}", strip_default_port(&scheme, &authority));
    Some((path, normalize_origin(origin).ok() == Some(candidate)))
}

/// The proxied provider a `/api/ai/…` path names, ignoring the origin: `None` for the
/// `keys`/`usage` routes, for `/api/ai/compat` with no name, and for every other path.
fn provider_for_path(path: &str) -> Option<ProxyProvider> {
    let rest = path.strip_prefix(PROXY_PREFIX)?;
    let mut segments = rest.split('/').filter(|segment| !segment.is_empty());
    let first = segments.next()?;
    let id = match first {
        "compat" => format!("compat/{}", segments.next()?),
        "keys" | "usage" => return None,
        provider => provider.to_owned(),
    };
    Some(ProxyProvider { id })
}

/// Which proxied provider `url` addresses, or `None` for every other URL (a provider host in
/// direct mode, a different deployment, the `keys`/`usage` routes). Matches an origin-relative
/// `/api/ai/…` path and an absolute URL whose scheme, host and port equal `origin`'s
/// (case-insensitively, default ports dropped); the query and fragment are ignored.
///
/// The comparison is the whole origin and not the host alone (b11 §3.16's prose says "host",
/// its own signature line says scheme and port too): the placeholder is only meaningful to
/// *this* page's proxy, the editor CSP is `connect-src 'self'`, so a `/api/ai/…` URL on any
/// other origin — a scheme downgrade, a port, `www` versus the apex — is unreachable from
/// the tab and must not read as configured. The case §3.16 worries about, a compat provider's
/// `api_url` persisted in `settings.json` and synced to a page served from another form of
/// the origin, is covered instead by the origin-relative form (which the §3.14 snippet
/// generator emits and this function accepts) and, when an absolute foreign URL does arrive,
/// by [`foreign_proxy_provider`], which makes it loud rather than silent.
pub fn provider_for_url(origin: &str, url: &str) -> Option<ProxyProvider> {
    let (path, same_origin) = split_origin(origin, url)?;
    if !same_origin {
        return None;
    }
    provider_for_path(path)
}

/// The proxied provider a `/api/ai/…` URL on a *different* origin names — the settings
/// document was written against another form of this deployment's origin (apex versus `www`,
/// a preview host, a scheme or port change) and synced here, or `origin` itself is malformed.
/// `None` for every URL [`provider_for_url`] accepts and for every non-proxy URL, so the two
/// are mutually exclusive. The credentials provider reports such a URL instead of silently
/// keeping the key in tab-local memory, where `ApiKeyState::store` would show it as saved and
/// the next reload would lose it.
pub fn foreign_proxy_provider(origin: &str, url: &str) -> Option<ProxyProvider> {
    let (path, same_origin) = split_origin(origin, url)?;
    if same_origin {
        return None;
    }
    provider_for_path(path)
}

/// The JSON object merged over the web defaults (b11 §4.3): every proxied provider's
/// `api_url` points at the proxy. `edit_predictions.open_ai_compatible_api.api_url` is
/// deliberately left alone (it defaults to `""` and has no single upstream), as are the
/// `localhost` providers, which are not registered on wasm at all.
pub fn ai_proxy_settings_overrides(origin: &str) -> serde_json::Value {
    let language_models: serde_json::Map<String, serde_json::Value> =
        PROXIED_LANGUAGE_MODEL_PROVIDERS
            .iter()
            .map(|provider| {
                (
                    (*provider).to_owned(),
                    serde_json::json!({ "api_url": proxy_api_url(origin, provider) }),
                )
            })
            .collect();
    let edit_predictions: serde_json::Map<String, serde_json::Value> =
        PROXIED_EDIT_PREDICTION_PROVIDERS
            .iter()
            .map(|provider| {
                (
                    (*provider).to_owned(),
                    serde_json::json!({ "api_url": proxy_api_url(origin, provider) }),
                )
            })
            .collect();
    serde_json::json!({
        "language_models": language_models,
        "edit_predictions": edit_predictions,
    })
}

/// Deep-merges [`ai_proxy_settings_overrides`] over `settings_json` (JSONC) and returns
/// plain JSON; the same merge rule as `merge_web_defaults`.
pub fn merge_ai_proxy_defaults(settings_json: &str, origin: &str) -> Result<String> {
    let mut base: serde_json::Value = serde_json_lenient::from_str(settings_json)
        .map_err(|error| anyhow!("settings are not valid JSONC: {error}"))?;
    crate::web_settings::deep_merge(&mut base, ai_proxy_settings_overrides(origin));
    Ok(serde_json::to_string_pretty(&base)?)
}

/// `GET /api/ai/keys` → `{ providers: [...] }`; unknown fields are ignored so an older bundle
/// reads a newer control plane.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeysResponse {
    pub providers: Vec<ProviderStatus>,
}

/// One row of the inventory; never a key value.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderStatus {
    pub id: String,
    pub configured: bool,
    #[serde(default)]
    pub env_name: Option<String>,
}

/// The ids the control plane holds a key for.
pub fn configured_ids(response: &KeysResponse) -> HashSet<String> {
    response
        .providers
        .iter()
        .filter(|provider| provider.configured)
        .map(|provider| provider.id.clone())
        .collect()
}

/// The result of one `GET /api/ai/keys`. Only `Known` is cached; the other two answer "no
/// key" for now and are retried on the next lookup, never reported as an error, because
/// `ApiKeyState` turns a credentials error into an authentication error on every provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeysOutcome {
    /// The inventory was read; these providers are configured.
    Known(HashSet<String>),
    /// `404 ai_disabled`: the deployment runs with `ZS_AI_PROXY=off`.
    Disabled,
    /// Anything else (a 401 after the cookie lapsed, a 429, a 5xx, offline).
    Unknown { status: u16, detail: String },
}

impl KeysOutcome {
    /// A 401 or 403 on the inventory read: the `zs_ai` cookie lapsed (past `oat + 24 h`, or
    /// its epoch was bumped) or the request was refused as cross-site. Nothing the tab can
    /// retry fixes it; only a page reload re-mints the cookie.
    pub fn is_session_expired(&self) -> bool {
        matches!(
            self,
            KeysOutcome::Unknown {
                status: 401 | 403,
                ..
            }
        )
    }
}

/// The one-time notice raised when [`KeysOutcome::is_session_expired`] holds.
pub const SESSION_EXPIRED_MESSAGE: &str = "Your editor session has expired, so AI provider keys cannot be read; reload the page to keep using AI features.";

/// The CONTRACTS §8.1 envelope of the `keys`/`usage` routes.
#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(rename = "type", default)]
    kind: Option<String>,
}

fn parse_error_body(body: &str) -> Option<ErrorBody> {
    serde_json::from_str::<ErrorEnvelope>(body)
        .ok()
        .map(|envelope| envelope.error)
}

/// Classifies one `GET /api/ai/keys` answer.
pub fn parse_keys_outcome(status: u16, body: &str) -> KeysOutcome {
    if (200..300).contains(&status) {
        return match serde_json::from_str::<KeysResponse>(body) {
            Ok(response) => KeysOutcome::Known(configured_ids(&response)),
            Err(error) => KeysOutcome::Unknown {
                status,
                detail: format!("the keys response could not be parsed: {error}"),
            },
        };
    }
    let error = parse_error_body(body);
    if status == 404
        && error.as_ref().and_then(|error| error.code.as_deref()) == Some("ai_disabled")
    {
        return KeysOutcome::Disabled;
    }
    let detail = error
        .and_then(|error| match (error.code, error.message) {
            (Some(code), Some(message)) => Some(format!("{code}: {message}")),
            (Some(code), None) => Some(code),
            (None, Some(message)) => Some(message),
            (None, None) => None,
        })
        .unwrap_or_else(|| format!("HTTP {status}"));
    KeysOutcome::Unknown { status, detail }
}

/// The credential handed to Zed for `provider`: the placeholder when the control plane holds
/// a key, `None` otherwise. Never a real key, and never the placeholder for an unconfigured
/// provider (that would make every provider look authenticated and fail at request time).
pub fn credential_for(
    provider: &ProxyProvider,
    outcome: &KeysOutcome,
) -> Option<(String, Vec<u8>)> {
    match outcome {
        KeysOutcome::Known(ids) if ids.contains(&provider.id) => Some((
            PLACEHOLDER_USERNAME.to_owned(),
            PLACEHOLDER_KEY.as_bytes().to_vec(),
        )),
        KeysOutcome::Known(_) | KeysOutcome::Disabled | KeysOutcome::Unknown { .. } => None,
    }
}

/// Refuses the placeholder as a key to store: any round-trip of a loaded credential through
/// `ApiKeyState::store` would otherwise overwrite the user's real key with it.
pub fn ensure_not_placeholder(password: &[u8]) -> Result<()> {
    ensure!(
        password != PLACEHOLDER_KEY.as_bytes(),
        "the proxy placeholder is not an API key; enter the provider's own key"
    );
    Ok(())
}

/// The `PUT /api/ai/keys/{provider}` body for a key typed in the editor. Applies the control
/// plane's own rule (1..8192 bytes of printable ASCII, no whitespace) so a bad key is
/// refused with a readable message here instead of a `400 invalid_body` there.
pub fn write_key_payload(password: &[u8]) -> Result<String> {
    ensure_not_placeholder(password)?;
    let key = std::str::from_utf8(password).context("the API key is not valid UTF-8")?;
    let key = key.trim();
    ensure!(!key.is_empty(), "the API key is empty");
    ensure!(key.len() <= 8192, "the API key is longer than 8192 bytes");
    ensure!(
        key.bytes().all(|byte| (0x21..=0x7e).contains(&byte)),
        "the API key must be printable ASCII without spaces"
    );
    Ok(serde_json::json!({ "key": key }).to_string())
}

/// A proxy-originated failure: the `keys`/`usage` envelope or a dialect-shaped body from
/// `/api/ai/<provider>/…` (b11 §4.5), reduced to what the editor shows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyError {
    pub status: u16,
    /// `ai_key_missing`, `ai_disabled`, `account_flagged`, `ai_spend_cap`, … when known.
    pub code: Option<String>,
    pub message: String,
}

/// Reads the machine-readable code from the `x-zs-ai-error` header first, then from the
/// envelope's `error.code` (an OpenAI-dialect body carries it as `zs_<code>`), and the
/// message from `error.message` in either dialect.
pub fn parse_proxy_error(status: u16, error_header: Option<&str>, body: &str) -> ProxyError {
    let error = parse_error_body(body);
    let code = error_header
        .map(str::trim)
        .filter(|header| !header.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            error.as_ref().and_then(|error| {
                error
                    .code
                    .as_deref()
                    .map(|code| code.strip_prefix("zs_").unwrap_or(code).to_owned())
            })
        });
    let message = error
        .as_ref()
        .and_then(|error| error.message.clone())
        .filter(|message| !message.trim().is_empty())
        .or_else(|| error.as_ref().and_then(|error| error.kind.clone()))
        .unwrap_or_else(|| {
            let body = body.trim();
            if body.is_empty() {
                format!("HTTP {status}")
            } else {
                format!("HTTP {status}: {body}")
            }
        });
    ProxyError {
        status,
        code,
        message,
    }
}

/// What the editor tells the user about a failed key save or delete: the proxy's own
/// message where it is meant for the user, a pointer at the dashboard otherwise.
pub fn describe_proxy_error(error: &ProxyError, origin: &str, provider: &ProxyProvider) -> String {
    let label = provider.label();
    let manage = manage_keys_url(origin);
    match error.code.as_deref() {
        Some("ai_disabled") => {
            format!(
                "The AI proxy is disabled on this deployment, so {label} keys cannot be stored."
            )
        }
        Some("unauthenticated") | Some("cross_site") => {
            "Your editor session has expired; reload the page and try again.".to_owned()
        }
        Some("account_flagged") => "AI features are disabled for this account.".to_owned(),
        Some("invalid_key") | Some("invalid_body") => {
            format!("The {label} key was not accepted: {}", error.message)
        }
        Some("rate_limited") => {
            format!("Too many key changes in a minute; wait and try again ({label}).")
        }
        Some("plan_required")
        | Some("compat_limit")
        | Some("export_not_supported")
        | Some("invalid_upstream")
        | Some("invalid_provider") => {
            format!("{} Manage keys at {manage}.", error.message)
        }
        _ => format!(
            "The {label} key could not be saved (HTTP {}): {}. Manage keys at {manage}.",
            error.status, error.message
        ),
    }
}

/// An in-editor notice raised when a proxied request comes back with an `x-zs-ai-error`
/// the user can act on; `None` for codes that Zed's own error display already covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyNotice {
    /// Names the failing provider and what to do.
    pub message: String,
    /// Whether a "Manage AI keys" button (→ `{origin}/ai`) belongs on the notice.
    pub offer_manage_keys: bool,
}

/// Maps a proxy error code on `/api/ai/<provider>/…` to the notice shown beside the thread's
/// own error message: the request was refused before it reached the provider, and the
/// dialect body may be swallowed by a provider crate that only reports the status.
pub fn notice_for(code: &str, provider: &ProxyProvider, origin: &str) -> Option<ProxyNotice> {
    let label = provider.label();
    let manage = manage_keys_url(origin);
    let (message, offer_manage_keys) = match code {
        "ai_key_missing" => (
            format!(
                "No {label} API key is configured for your account. Add one in Settings > AI or at {manage}."
            ),
            true,
        ),
        "ai_disabled" => (
            format!(
                "The AI proxy is disabled on this deployment, so {label} is unavailable in the browser."
            ),
            false,
        ),
        "account_flagged" => (
            "AI requests are disabled for this account.".to_owned(),
            false,
        ),
        "ai_spend_cap" => (
            format!(
                "Your monthly AI token cap is reached; {label} requests are refused until the next period. Usage is at {manage}."
            ),
            true,
        ),
        "ai_daily_limit" => (
            format!(
                "Your daily AI request cap is reached; {label} requests resume later. Usage is at {manage}."
            ),
            true,
        ),
        "too_many_streams" => (
            format!(
                "Too many concurrent AI requests; wait for one to finish before asking {label} again."
            ),
            false,
        ),
        "unauthenticated" | "cross_site" => (
            format!("Your editor session has expired; reload the page to keep using {label}."),
            false,
        ),
        _ => return None,
    };
    Some(ProxyNotice {
        message,
        offer_manage_keys,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = include_str!("../tests/fixtures/ai-providers.v1.json");

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Fixture {
        version: u32,
        placeholder_key: String,
        proxy_prefix: String,
        providers: Vec<FixtureProvider>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct FixtureProvider {
        id: String,
        settings_path: String,
    }

    fn fixture() -> Fixture {
        serde_json::from_str(FIXTURE).expect("the fixture is valid JSON")
    }

    #[test]
    fn normalize_origin_canonicalises_and_rejects_paths() {
        assert_eq!(
            normalize_origin("HTTPS://ZS.Example.com:443/").unwrap(),
            "https://zs.example.com"
        );
        assert_eq!(
            normalize_origin("http://localhost:3000").unwrap(),
            "http://localhost:3000"
        );
        assert_eq!(
            normalize_origin("http://localhost:80").unwrap(),
            "http://localhost"
        );
        for bad in [
            "zs.example.com",
            "ftp://zs.example.com",
            "https://zs.example.com/w/ws_1",
            "https://zs.example.com?x=1",
            "https://user:pw@zs.example.com",
            "https://",
            "",
        ] {
            assert!(normalize_origin(bad).is_err(), "{bad:?} should be refused");
        }
    }

    #[test]
    fn proxy_urls_have_no_trailing_slash() {
        assert_eq!(
            proxy_api_url("https://zs.example.com/", "anthropic"),
            "https://zs.example.com/api/ai/anthropic"
        );
        assert_eq!(
            keys_url("https://zs.example.com", None),
            "https://zs.example.com/api/ai/keys"
        );
        assert_eq!(
            keys_url("https://zs.example.com", Some("compat/groq")),
            "https://zs.example.com/api/ai/keys/compat/groq"
        );
        assert_eq!(
            manage_keys_url("https://zs.example.com"),
            "https://zs.example.com/ai"
        );
    }

    #[test]
    fn provider_for_url_matches_the_origin_and_relative_paths() {
        let origin = "https://zs.example.com";
        let provider = |id: &str| Some(ProxyProvider { id: id.to_owned() });
        assert_eq!(
            provider_for_url(origin, "https://zs.example.com/api/ai/anthropic"),
            provider("anthropic")
        );
        assert_eq!(
            provider_for_url(
                origin,
                "https://zs.example.com/api/ai/anthropic/v1/messages"
            ),
            provider("anthropic")
        );
        // Host and scheme compare case-insensitively; a default port is the same origin.
        assert_eq!(
            provider_for_url(origin, "HTTPS://ZS.EXAMPLE.COM:443/api/ai/openai?x=1#frag"),
            provider("openai")
        );
        assert_eq!(
            provider_for_url(
                "https://zs.example.com:443",
                "https://zs.example.com/api/ai/x_ai"
            ),
            provider("x_ai")
        );
        assert_eq!(
            provider_for_url(
                origin,
                "https://zs.example.com/api/ai/google/v1beta/models/x:streamGenerateContent?alt=sse&key=zs-proxy-v1"
            ),
            provider("google")
        );
        assert_eq!(
            provider_for_url(
                origin,
                "https://zs.example.com/api/ai/compat/groq/v1/chat/completions"
            ),
            provider("compat/groq")
        );
        assert_eq!(
            provider_for_url(origin, "/api/ai/codestral"),
            provider("codestral")
        );
        assert_eq!(
            provider_for_url(origin, "/api/ai/compat/groq"),
            provider("compat/groq")
        );

        assert_eq!(provider_for_url(origin, "https://api.anthropic.com"), None);
        assert_eq!(
            provider_for_url(origin, "https://www.zs.example.com/api/ai/anthropic"),
            None
        );
        assert_eq!(
            provider_for_url(origin, "http://zs.example.com/api/ai/anthropic"),
            None
        );
        assert_eq!(
            provider_for_url(origin, "https://zs.example.com:8443/api/ai/anthropic"),
            None
        );
        assert_eq!(
            provider_for_url(origin, "https://zs.example.com/api/workspaces"),
            None
        );
        assert_eq!(
            provider_for_url(origin, "https://zs.example.com/api/ai/"),
            None
        );
        assert_eq!(
            provider_for_url(origin, "https://zs.example.com/api/ai/keys"),
            None
        );
        assert_eq!(
            provider_for_url(origin, "https://zs.example.com/api/ai/usage"),
            None
        );
        assert_eq!(
            provider_for_url(origin, "https://zs.example.com/api/ai/compat"),
            None
        );
        assert_eq!(provider_for_url(origin, ""), None);
        assert_eq!(
            provider_for_url("not an origin", "https://zs.example.com/api/ai/x"),
            None
        );
    }

    /// Every absolute `/api/ai/…` URL `provider_for_url` refuses because its origin differs
    /// is reported by `foreign_proxy_provider` instead, so the credentials provider can say
    /// so rather than silently treating the key as a direct-mode one.
    #[test]
    fn a_proxy_path_on_another_origin_is_reported_not_ignored() {
        let origin = "https://zs.example.com";
        let provider = |id: &str| Some(ProxyProvider { id: id.to_owned() });
        for url in [
            "https://www.zs.example.com/api/ai/anthropic",
            "http://zs.example.com/api/ai/anthropic",
            "https://zs.example.com:8443/api/ai/anthropic",
            "https://zs-git-preview.vercel.app/api/ai/anthropic?x=1",
        ] {
            assert_eq!(provider_for_url(origin, url), None, "{url}");
            assert_eq!(
                foreign_proxy_provider(origin, url),
                provider("anthropic"),
                "{url}"
            );
        }
        assert_eq!(
            foreign_proxy_provider(origin, "https://www.zs.example.com/api/ai/compat/groq/v1"),
            provider("compat/groq")
        );
        // A malformed page origin cannot match anything, so every absolute proxy URL is
        // foreign — loud, never silently stored in tab memory.
        assert_eq!(
            foreign_proxy_provider("not an origin", "https://zs.example.com/api/ai/x_ai"),
            provider("x_ai")
        );

        // Never both: anything `provider_for_url` accepts, and anything that is not a proxy
        // path at all, is not foreign.
        for url in [
            "https://zs.example.com/api/ai/anthropic",
            "HTTPS://ZS.EXAMPLE.COM:443/api/ai/openai",
            "/api/ai/codestral",
            "/api/ai/keys",
            "https://api.anthropic.com/v1/messages",
            "https://www.zs.example.com/api/ai/keys",
            "https://www.zs.example.com/api/ai/usage",
            "https://www.zs.example.com/api/workspaces",
            "https://www.zs.example.com/api/ai/compat",
            "",
        ] {
            assert_eq!(foreign_proxy_provider(origin, url), None, "{url}");
        }
    }

    #[test]
    fn overrides_seed_every_proxied_provider_and_nothing_else() {
        let origin = "https://zs.example.com";
        let overrides = ai_proxy_settings_overrides(origin);
        let language_models = overrides["language_models"].as_object().unwrap();
        assert_eq!(
            language_models.len(),
            PROXIED_LANGUAGE_MODEL_PROVIDERS.len()
        );
        for provider in PROXIED_LANGUAGE_MODEL_PROVIDERS {
            assert_eq!(
                language_models[*provider],
                serde_json::json!({ "api_url": format!("{origin}/api/ai/{provider}") })
            );
        }
        assert_eq!(
            overrides["edit_predictions"],
            serde_json::json!({ "codestral": { "api_url": format!("{origin}/api/ai/codestral") } })
        );
        for local in LOCAL_ONLY_LANGUAGE_MODEL_PROVIDERS {
            assert!(
                language_models.get(*local).is_none(),
                "{local} must not be seeded"
            );
        }
    }

    #[test]
    fn merge_seeds_the_shipped_defaults() {
        let origin = "https://zs.example.com";
        let defaults = settings::default_settings();
        let merged = merge_ai_proxy_defaults(&defaults, origin).unwrap();
        let merged: serde_json::Value = serde_json::from_str(&merged).unwrap();
        let original: serde_json::Value = serde_json_lenient::from_str(&defaults).unwrap();

        for row in fixture().providers {
            let mut path = row.settings_path.split('.');
            let (section, provider, field) = (
                path.next().unwrap(),
                path.next().unwrap(),
                path.next().unwrap(),
            );
            assert_eq!(field, "api_url");
            assert_eq!(
                merged[section][provider]["api_url"],
                serde_json::json!(format!("{origin}/api/ai/{}", row.id)),
                "{}",
                row.settings_path
            );
            // Sibling keys of the section survive the merge, and `api_url` is the only key
            // the overrides add. Compared as sets, not counts: a count is satisfied by
            // construction when the shipped block is `{}` (as `anthropic_compatible` and
            // `bedrock` are), which would hide a second key the overrides started seeding.
            let mut expected: Vec<&String> = original[section][provider]
                .as_object()
                .map(|object| object.keys().collect())
                .unwrap_or_default();
            let api_url = "api_url".to_owned();
            if !expected.contains(&&api_url) {
                expected.push(&api_url);
            }
            expected.sort();
            let mut actual: Vec<&String> = merged[section][provider]
                .as_object()
                .unwrap()
                .keys()
                .collect();
            actual.sort();
            assert_eq!(
                actual, expected,
                "{} gained or lost keys",
                row.settings_path
            );
        }
        assert_eq!(
            merged["edit_predictions"]["open_ai_compatible_api"],
            original["edit_predictions"]["open_ai_compatible_api"],
            "open_ai_compatible_api must not be seeded"
        );
        for local in LOCAL_ONLY_LANGUAGE_MODEL_PROVIDERS {
            assert_eq!(
                merged["language_models"][*local], original["language_models"][*local],
                "{local} must keep its default"
            );
        }
        assert_eq!(
            merged["edit_predictions"]["provider"],
            original["edit_predictions"]["provider"]
        );
        for key in original.as_object().unwrap().keys() {
            assert!(
                merged.get(key).is_some(),
                "top-level key {key} lost in merge"
            );
        }
    }

    /// The merged document is what `zed_web` hands to `SettingsStore::new` as the defaults;
    /// a key the store rejects would fail the browser boot at `settings::init` time.
    #[gpui::test]
    fn merged_defaults_parse_in_store(cx: &mut gpui::App) {
        let origin = "https://zs.example.com";
        let merged =
            crate::web_settings::merge_web_defaults(&settings::default_settings()).unwrap();
        let merged = merge_ai_proxy_defaults(&merged, origin).unwrap();
        let store = settings::SettingsStore::new(cx, &merged);
        let content = store.raw_default_settings();
        let language_models = content.language_models.as_ref().unwrap();
        assert_eq!(
            language_models
                .anthropic
                .as_ref()
                .and_then(|anthropic| anthropic.api_url.as_deref()),
            Some("https://zs.example.com/api/ai/anthropic")
        );
        assert_eq!(
            language_models
                .vercel_ai_gateway
                .as_ref()
                .and_then(|gateway| gateway.api_url.as_deref()),
            Some("https://zs.example.com/api/ai/vercel_ai_gateway")
        );
        assert_eq!(
            content
                .project
                .all_languages
                .edit_predictions
                .as_ref()
                .and_then(|edit_predictions| edit_predictions.codestral.as_ref())
                .and_then(|codestral| codestral.api_url.as_deref()),
            Some("https://zs.example.com/api/ai/codestral")
        );
    }

    #[test]
    fn two_origins_in_one_process_get_two_documents() {
        let a = merge_ai_proxy_defaults(&settings::default_settings(), "https://a.example.com")
            .unwrap();
        let b = merge_ai_proxy_defaults(&settings::default_settings(), "https://b.example.com")
            .unwrap();
        assert_ne!(a, b);
        assert!(a.contains("https://a.example.com/api/ai/anthropic"));
        assert!(b.contains("https://b.example.com/api/ai/anthropic"));
        assert!(!a.contains("b.example.com"));
    }

    #[test]
    fn proxied_providers_are_a_subset_of_the_fixture() {
        let fixture = fixture();
        assert_eq!(fixture.version, 1);
        assert_eq!(fixture.placeholder_key, PLACEHOLDER_KEY);
        assert_eq!(fixture.proxy_prefix, PROXY_PREFIX);
        let ids: HashSet<&str> = fixture
            .providers
            .iter()
            .map(|provider| provider.id.as_str())
            .collect();
        for provider in PROXIED_LANGUAGE_MODEL_PROVIDERS
            .iter()
            .chain(PROXIED_EDIT_PREDICTION_PROVIDERS)
        {
            assert!(ids.contains(provider), "{provider} is not in the fixture");
        }
        for local in LOCAL_ONLY_LANGUAGE_MODEL_PROVIDERS {
            assert!(!ids.contains(local), "{local} must not be proxied");
        }
        // Append-only rule: a row this bundle does not know is ignored, not an error.
        let newer = r#"{ "providers": [
            { "id": "anthropic", "configured": true, "envName": "ANTHROPIC_API_KEY", "label": "Anthropic", "extra": 1 },
            { "id": "brand_new_provider", "configured": true, "envName": null }
        ] }"#;
        let outcome = parse_keys_outcome(200, newer);
        let KeysOutcome::Known(configured) = &outcome else {
            panic!("{outcome:?}");
        };
        assert!(configured.contains("anthropic"));
        assert!(configured.contains("brand_new_provider"));
        let unknown = ProxyProvider {
            id: "brand_new_provider".into(),
        };
        assert!(credential_for(&unknown, &outcome).is_some());
    }

    #[test]
    fn keys_outcomes_never_become_errors() {
        let known = parse_keys_outcome(
            200,
            r#"{ "providers": [
                { "id": "anthropic", "configured": true, "envName": "ANTHROPIC_API_KEY" },
                { "id": "openai", "configured": false, "envName": "OPENAI_API_KEY" },
                { "id": "compat/groq", "configured": true, "envName": null }
            ] }"#,
        );
        assert_eq!(
            known,
            KeysOutcome::Known(
                ["anthropic", "compat/groq"]
                    .into_iter()
                    .map(String::from)
                    .collect()
            )
        );
        assert_eq!(
            parse_keys_outcome(
                404,
                r#"{ "error": { "code": "ai_disabled", "message": "The AI proxy is disabled" } }"#
            ),
            KeysOutcome::Disabled
        );
        assert_eq!(
            parse_keys_outcome(
                404,
                r#"{ "error": { "code": "not_found", "message": "nope" } }"#
            ),
            KeysOutcome::Unknown {
                status: 404,
                detail: "not_found: nope".into()
            }
        );
        assert_eq!(
            parse_keys_outcome(429, ""),
            KeysOutcome::Unknown {
                status: 429,
                detail: "HTTP 429".into()
            }
        );
        assert!(matches!(
            parse_keys_outcome(200, "<html>"),
            KeysOutcome::Unknown { status: 200, .. }
        ));
        assert!(matches!(
            parse_keys_outcome(401, r#"{ "error": { "code": "unauthenticated", "message": "Sign in required" } }"#),
            KeysOutcome::Unknown { status: 401, ref detail } if detail == "unauthenticated: Sign in required"
        ));
    }

    #[test]
    fn only_401_and_403_read_as_an_expired_session() {
        let expired =
            |status: u16, body: &str| parse_keys_outcome(status, body).is_session_expired();
        assert!(expired(
            401,
            r#"{ "error": { "code": "unauthenticated", "message": "Sign in required" } }"#
        ));
        assert!(expired(403, r#"{ "error": { "code": "cross_site" } }"#));
        assert!(expired(403, ""));
        assert!(!expired(429, ""));
        assert!(!expired(503, ""));
        assert!(!expired(200, "<html>"));
        assert!(!expired(404, r#"{ "error": { "code": "ai_disabled" } }"#));
        assert!(!expired(200, r#"{ "providers": [] }"#));
        assert!(
            !KeysOutcome::Unknown {
                status: 0,
                detail: "offline".into()
            }
            .is_session_expired()
        );
        assert!(SESSION_EXPIRED_MESSAGE.contains("reload"));
    }

    #[test]
    fn credentials_are_the_placeholder_only_for_configured_providers() {
        let anthropic = ProxyProvider {
            id: "anthropic".into(),
        };
        let openai = ProxyProvider {
            id: "openai".into(),
        };
        let known = KeysOutcome::Known(["anthropic".to_owned()].into_iter().collect());
        assert_eq!(
            credential_for(&anthropic, &known),
            Some(("Bearer".to_owned(), b"zs-proxy-v1".to_vec()))
        );
        assert_eq!(credential_for(&openai, &known), None);
        assert_eq!(credential_for(&anthropic, &KeysOutcome::Disabled), None);
        assert_eq!(
            credential_for(
                &anthropic,
                &KeysOutcome::Unknown {
                    status: 503,
                    detail: "down".into()
                }
            ),
            None
        );
    }

    #[test]
    fn write_payload_refuses_the_placeholder_and_bad_keys() {
        assert!(write_key_payload(b"zs-proxy-v1").is_err());
        assert!(ensure_not_placeholder(b"zs-proxy-v1").is_err());
        assert!(ensure_not_placeholder(b"sk-ant-123").is_ok());
        assert!(write_key_payload(b"").is_err());
        assert!(write_key_payload(b"   ").is_err());
        assert!(write_key_payload(b"has space").is_err());
        assert!(write_key_payload("kéy".as_bytes()).is_err());
        assert!(write_key_payload(&vec![b'a'; 8193]).is_err());
        assert_eq!(
            write_key_payload(b"  sk-ant-abc123\n").unwrap(),
            r#"{"key":"sk-ant-abc123"}"#
        );
        let payload = write_key_payload(b"vck_x").unwrap();
        assert!(!payload.contains(PLACEHOLDER_KEY));
    }

    #[test]
    fn proxy_errors_are_read_from_both_dialects_and_the_header() {
        let anthropic = parse_proxy_error(
            401,
            Some("ai_key_missing"),
            r#"{ "type": "error", "error": { "type": "authentication_error", "message": "No Anthropic API key is configured" } }"#,
        );
        assert_eq!(anthropic.code.as_deref(), Some("ai_key_missing"));
        assert_eq!(anthropic.message, "No Anthropic API key is configured");

        let openai = parse_proxy_error(
            402,
            None,
            r#"{ "error": { "message": "monthly token cap reached", "type": "invalid_request_error", "code": "zs_ai_spend_cap" } }"#,
        );
        assert_eq!(openai.code.as_deref(), Some("ai_spend_cap"));
        assert_eq!(openai.message, "monthly token cap reached");

        let envelope = parse_proxy_error(
            400,
            None,
            r#"{ "error": { "code": "invalid_key", "message": "the placeholder is not a key" } }"#,
        );
        assert_eq!(envelope.code.as_deref(), Some("invalid_key"));

        let bare = parse_proxy_error(502, None, "Bad Gateway");
        assert_eq!(bare.code, None);
        assert_eq!(bare.message, "HTTP 502: Bad Gateway");
        assert_eq!(parse_proxy_error(500, Some(""), "").message, "HTTP 500");
    }

    #[test]
    fn descriptions_and_notices_point_at_the_dashboard() {
        let origin = "https://zs.example.com";
        let anthropic = ProxyProvider {
            id: "anthropic".into(),
        };
        let described = describe_proxy_error(
            &ProxyError {
                status: 500,
                code: None,
                message: "boom".into(),
            },
            origin,
            &anthropic,
        );
        assert!(described.contains("Anthropic"), "{described}");
        assert!(
            described.contains("https://zs.example.com/ai"),
            "{described}"
        );
        assert!(described.contains("boom"), "{described}");
        let disabled = describe_proxy_error(
            &ProxyError {
                status: 404,
                code: Some("ai_disabled".into()),
                message: "x".into(),
            },
            origin,
            &anthropic,
        );
        assert!(disabled.contains("disabled"), "{disabled}");

        let missing = notice_for("ai_key_missing", &anthropic, origin).unwrap();
        assert!(missing.offer_manage_keys);
        assert!(
            missing.message.contains("No Anthropic API key"),
            "{}",
            missing.message
        );
        assert!(missing.message.contains("https://zs.example.com/ai"));
        let compat = ProxyProvider {
            id: "compat/groq".into(),
        };
        let cap = notice_for("ai_spend_cap", &compat, origin).unwrap();
        assert!(cap.message.contains("groq (compatible)"), "{}", cap.message);
        assert!(
            !notice_for("ai_disabled", &anthropic, origin)
                .unwrap()
                .offer_manage_keys
        );
        assert_eq!(notice_for("path_not_allowed", &anthropic, origin), None);
        assert_eq!(notice_for("upstream_timeout", &anthropic, origin), None);
        assert_eq!(notice_for("", &anthropic, origin), None);
    }
}
