//! Session-token verification for `zed-remote-server serve`.
//!
//! Pure functions with no gpui dependency: the control plane mints ES256 JWTs
//! (BUILD-SPEC §4.3), the server verifies them before the WebSocket upgrade and on
//! every `/files`, `/extensions/*` and full `/health` request.

use std::path::PathBuf;

use anyhow::{Context as _, Result};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, errors::ErrorKind};
use subtle::ConstantTimeEq as _;

pub use remote::websocket_wire::{SUBPROTOCOL, split_subprotocol_header};

/// Query parameter carrying the token on `/files` downloads (`<a download>` cannot set
/// headers). Accepted on `/rpc` too as a documented, unused superset: both client targets send
/// the subprotocol list only.
pub const TOKEN_QUERY_PARAM: &str = "zs_token";
/// Query parameter that counts as "offered `zs.v1`" for clients that cannot send the
/// `Sec-WebSocket-Protocol` header.
pub const PROTOCOL_QUERY_PARAM: &str = "zs_proto";

/// The only signing algorithm the verifier accepts. HS256 confusion with the public key is
/// closed by this allowlist before any cryptography runs.
pub const EXPECTED_ALG: Algorithm = Algorithm::ES256;
/// Clock skew tolerated on `exp` (by jsonwebtoken) and on `iat` (checked here).
pub const LEEWAY_SECS: u64 = 30;
/// Longest lifetime (`exp - iat`) a token may claim (CONTRACTS §6.1: TTL max 3600). A token
/// minted with a longer lifetime is refused even with a valid signature, so a minting bug or a
/// leaked signing key cannot produce a credential the server honours indefinitely.
pub const MAX_TTL_SECS: u64 = 3600;
/// Longest token accepted anywhere; a compact ES256 JWT with these claims is under 1 KiB.
pub const MAX_TOKEN_BYTES: usize = 4096;

/// Claims of a session token (CONTRACTS.md §6.1). Every field is required by
/// deserialization; only `ws`, `aud`, `iss`, `exp` and `iat` are validated.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Claims {
    /// Stable, signed anonymous participant identity. Older app releases omit it.
    #[serde(default)]
    pub pid: Option<String>,
    /// Issuer; must equal `--issuer`.
    pub iss: String,
    /// User id.
    pub sub: String,
    /// Workspace id; compared with `--workspace-id` in constant time.
    pub ws: String,
    /// Per-connect session id minted by the control plane (D1): informational, carried into
    /// `SessionMeta`, `HelloAck.session_id` and the logs, never a resume key.
    pub sid: String,
    /// Audience; must equal `--audience`.
    pub aud: String,
    /// Issued-at, unix seconds. A value more than [`LEEWAY_SECS`] in the future is rejected.
    pub iat: u64,
    /// Expiry, unix seconds.
    pub exp: u64,
    /// Token id; used as the temp-file suffix for uploads.
    pub jti: String,
}

/// Why a token was rejected, with its HTTP status.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum AuthError {
    /// No token in any of the accepted placements.
    #[error("missing token")]
    Missing,
    /// The token could not be parsed, lacks a required claim, or is dated in the future.
    #[error("malformed token")]
    Malformed,
    /// No loaded key verifies the signature.
    #[error("bad signature")]
    BadSignature,
    /// `exp` is in the past (beyond the leeway).
    #[error("token expired")]
    Expired,
    /// `aud` does not match `--audience`.
    #[error("wrong audience")]
    WrongAudience,
    /// `iss` does not match `--issuer`.
    #[error("wrong issuer")]
    WrongIssuer,
    /// `ws` does not match `--workspace-id`.
    #[error("wrong workspace")]
    WrongWorkspace,
    /// The header names an algorithm other than ES256.
    #[error("unsupported algorithm")]
    WrongAlgorithm,
    /// `zs.v1` was not offered on an `/rpc` upgrade.
    #[error("subprotocol not offered")]
    MissingSubprotocol,
}

impl AuthError {
    /// The HTTP status a pre-upgrade or per-request rejection carries.
    pub fn status(&self) -> hyper::StatusCode {
        use hyper::StatusCode;
        match self {
            AuthError::Missing
            | AuthError::Malformed
            | AuthError::BadSignature
            | AuthError::Expired
            | AuthError::WrongAlgorithm => StatusCode::UNAUTHORIZED,
            AuthError::WrongAudience | AuthError::WrongIssuer | AuthError::WrongWorkspace => {
                StatusCode::FORBIDDEN
            }
            AuthError::MissingSubprotocol => StatusCode::UPGRADE_REQUIRED,
        }
    }

    /// The `{"error": ...}` body value.
    pub fn code(&self) -> &'static str {
        match self {
            AuthError::Missing => "missing_token",
            AuthError::Malformed => "malformed_token",
            AuthError::BadSignature => "bad_signature",
            AuthError::Expired => "token_expired",
            AuthError::WrongAudience => "wrong_audience",
            AuthError::WrongIssuer => "wrong_issuer",
            AuthError::WrongWorkspace => "wrong_workspace",
            AuthError::WrongAlgorithm => "unsupported_algorithm",
            AuthError::MissingSubprotocol => "subprotocol_not_offered",
        }
    }
}

/// The loaded public keys and the validation rules applied to every token.
pub struct AuthConfig {
    keys: Vec<DecodingKey>,
    validation: Validation,
    workspace_id: String,
}

impl AuthConfig {
    /// Loads every `-----BEGIN PUBLIC KEY-----` block of every file in `pem_files` (two keys
    /// during rotation).
    pub fn load(
        pem_files: &[PathBuf],
        issuer: &str,
        audience: &str,
        workspace_id: &str,
    ) -> Result<Self> {
        let mut contents = Vec::with_capacity(pem_files.len());
        for path in pem_files {
            contents.push(
                std::fs::read_to_string(path)
                    .with_context(|| format!("reading JWT public key {path:?}"))?,
            );
        }
        let pems: Vec<&str> = contents.iter().map(String::as_str).collect();
        Self::from_pems(&pems, issuer, audience, workspace_id)
    }

    /// Builds the config from in-memory PEM contents; each entry may hold several blocks.
    pub fn from_pems(
        pems: &[&str],
        issuer: &str,
        audience: &str,
        workspace_id: &str,
    ) -> Result<Self> {
        let mut keys = Vec::new();
        for pem in pems {
            for block in split_pem_blocks(pem) {
                keys.push(
                    DecodingKey::from_ec_pem(block.as_bytes())
                        .context("parsing an ES256 public key PEM block")?,
                );
            }
        }
        anyhow::ensure!(!keys.is_empty(), "no public key found in the JWT PEM input");

        let mut validation = Validation::new(EXPECTED_ALG);
        validation.set_audience(&[audience]);
        validation.set_issuer(&[issuer]);
        validation.set_required_spec_claims(&["exp", "aud", "iss", "sub"]);
        validation.leeway = LEEWAY_SECS;
        validation.validate_nbf = false;

        Ok(Self {
            keys,
            validation,
            workspace_id: workspace_id.to_owned(),
        })
    }

    /// Verifies `token` against every loaded key. The first `Ok` wins; if every key fails,
    /// the error reported is the first one that is not `BadSignature` (a token signed by a
    /// loaded key but expired reports `Expired`, not `BadSignature`). jsonwebtoken never looks
    /// at `iat`, so a token dated more than [`LEEWAY_SECS`] in the future is rejected here as
    /// `Malformed`; the `ws` claim is then compared in constant time.
    pub fn verify(&self, token: &str) -> Result<Claims, AuthError> {
        let mut first_error: Option<AuthError> = None;
        for key in &self.keys {
            match decode::<Claims>(token, key, &self.validation) {
                Ok(data) => return self.check_claims(data.claims),
                Err(error) => {
                    let mapped = map_error(error.kind());
                    match &first_error {
                        None => first_error = Some(mapped),
                        Some(AuthError::BadSignature) if mapped != AuthError::BadSignature => {
                            first_error = Some(mapped)
                        }
                        Some(_) => {}
                    }
                }
            }
        }
        Err(first_error.unwrap_or(AuthError::Malformed))
    }

    fn check_claims(&self, claims: Claims) -> Result<Claims, AuthError> {
        let now = jsonwebtoken::get_current_timestamp();
        if claims.iat > now + LEEWAY_SECS {
            return Err(AuthError::Malformed);
        }
        if claims.exp > claims.iat.saturating_add(MAX_TTL_SECS + LEEWAY_SECS) {
            return Err(AuthError::Malformed);
        }
        if claims
            .ws
            .as_bytes()
            .ct_eq(self.workspace_id.as_bytes())
            .unwrap_u8()
            != 1
        {
            return Err(AuthError::WrongWorkspace);
        }
        Ok(claims)
    }
}

/// Splits a PEM file into its `-----BEGIN ...-----` … `-----END ...-----` blocks.
fn split_pem_blocks(pem: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current: Option<Vec<&str>> = None;
    for line in pem.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("-----BEGIN ") {
            current = Some(vec![trimmed]);
        } else if trimmed.starts_with("-----END ") {
            if let Some(mut lines) = current.take() {
                lines.push(trimmed);
                blocks.push(lines.join("\n"));
            }
        } else if let Some(lines) = current.as_mut()
            && !trimmed.is_empty()
        {
            lines.push(trimmed);
        }
    }
    blocks
}

/// `jsonwebtoken::errors::ErrorKind` → [`AuthError`].
fn map_error(kind: &ErrorKind) -> AuthError {
    match kind {
        ErrorKind::ExpiredSignature => AuthError::Expired,
        ErrorKind::InvalidSignature => AuthError::BadSignature,
        ErrorKind::InvalidAudience => AuthError::WrongAudience,
        ErrorKind::MissingRequiredClaim(claim) if claim == "aud" => AuthError::WrongAudience,
        ErrorKind::InvalidIssuer => AuthError::WrongIssuer,
        ErrorKind::InvalidAlgorithm
        | ErrorKind::InvalidAlgorithmName
        | ErrorKind::MissingAlgorithm => AuthError::WrongAlgorithm,
        _ => AuthError::Malformed,
    }
}

/// Parses `Sec-WebSocket-Protocol: zs.v1, <jwt>` (any order, any whitespace).
/// `Err(MissingSubprotocol)` if `zs.v1` is absent, `Err(Missing)` if no second item.
pub fn token_from_subprotocol(header: &str) -> Result<String, AuthError> {
    if let Some((_, token)) = split_subprotocol_header(header) {
        return Ok(token.to_owned());
    }
    let items: Vec<&str> = header
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .collect();
    if !items.contains(&SUBPROTOCOL) {
        return Err(AuthError::MissingSubprotocol);
    }
    items
        .into_iter()
        .find(|item| *item != SUBPROTOCOL)
        .map(str::to_owned)
        .ok_or(AuthError::Missing)
}

/// Where a request may carry the token, in order of precedence:
/// 1. a `Sec-WebSocket-Protocol` item (native and browser clients),
/// 2. `Authorization: Bearer` (`/files`, `/health`),
/// 3. `?zs_token=` (`<a download>` cannot set headers).
///
/// Returns the token and whether `zs.v1` was offered (header item or `?zs_proto=zs.v1`).
pub fn extract_token<B>(req: &hyper::Request<B>) -> Result<(String, bool), AuthError> {
    let mut offered = false;
    let mut token: Option<String> = None;
    for value in req.headers().get_all(hyper::header::SEC_WEBSOCKET_PROTOCOL) {
        let Ok(value) = value.to_str() else {
            continue;
        };
        for item in value
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
        {
            if item == SUBPROTOCOL {
                offered = true;
            } else if token.is_none() {
                token = Some(item.to_owned());
            }
        }
    }
    if token.is_none() {
        token = bearer_token(req);
    }
    let query = req.uri().query().unwrap_or("");
    for (key, value) in query_pairs(query) {
        if key == PROTOCOL_QUERY_PARAM && value == SUBPROTOCOL {
            offered = true;
        } else if key == TOKEN_QUERY_PARAM && token.is_none() && !value.is_empty() {
            token = Some(value.into_owned());
        }
    }
    match token {
        Some(token) => Ok((token, offered)),
        None => Err(AuthError::Missing),
    }
}

/// The `Authorization: Bearer <token>` value of a request, if present.
pub fn bearer_token<B>(req: &hyper::Request<B>) -> Option<String> {
    let value = req.headers().get(hyper::header::AUTHORIZATION)?;
    let value = value.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_owned())
    }
}

/// Percent-decoded `key=value` pairs of a query string.
pub fn query_pairs(query: &str) -> impl Iterator<Item = (String, std::borrow::Cow<'_, str>)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = percent_encoding::percent_decode_str(key)
                .decode_utf8_lossy()
                .into_owned();
            let value = percent_encoding::percent_decode_str(value).decode_utf8_lossy();
            (key, value)
        })
}

/// Rewrites `zs_token=<..>` to `zs_token=***` for logging. The key is compared after
/// percent-decoding, exactly as [`query_pairs`] matches it, so an encoded spelling of the
/// parameter (`zs%5Ftoken`) is redacted the same way it is accepted.
pub fn redact_query(path_and_query: &str) -> String {
    let Some((path, query)) = path_and_query.split_once('?') else {
        return path_and_query.to_owned();
    };
    let redacted: Vec<String> = query
        .split('&')
        .map(|pair| {
            let key = pair.split_once('=').map_or(pair, |(key, _)| key);
            let decoded_key = percent_encoding::percent_decode_str(key).decode_utf8_lossy();
            if decoded_key == TOKEN_QUERY_PARAM {
                format!("{TOKEN_QUERY_PARAM}=***")
            } else {
                pair.to_owned()
            }
        })
        .collect();
    format!("{path}?{}", redacted.join("&"))
}

/// Whether `token` could be a compact JWT at all: at most [`MAX_TOKEN_BYTES`] and exactly
/// three non-empty base64url segments. Anything else is refused before a verification permit
/// is taken, so syntactic garbage costs the server nothing but a per-peer delay.
pub fn token_shape_is_plausible(token: &str) -> bool {
    if token.is_empty() || token.len() > MAX_TOKEN_BYTES {
        return false;
    }
    let mut segments = 0usize;
    for segment in token.split('.') {
        segments += 1;
        if segment.is_empty()
            || !segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return false;
        }
    }
    segments == 3
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};

    pub const PRIVATE_PEM: &str = include_str!("../../tests/fixtures/es256_private.pem");
    pub const PUBLIC_PEM: &str = include_str!("../../tests/fixtures/es256_public.pem");
    pub const OTHER_PRIVATE_PEM: &str =
        include_str!("../../tests/fixtures/es256_other_private.pem");
    pub const OTHER_PUBLIC_PEM: &str = include_str!("../../tests/fixtures/es256_other_public.pem");
    pub const ISSUER: &str = "zs";
    pub const AUDIENCE: &str = "sb_test";
    pub const WORKSPACE: &str = "ws_test";

    #[derive(serde::Serialize)]
    pub struct TestClaims {
        pub iss: String,
        pub sub: String,
        pub ws: String,
        pub sid: String,
        pub aud: String,
        pub iat: u64,
        pub exp: u64,
        pub jti: String,
    }

    pub fn now() -> u64 {
        jsonwebtoken::get_current_timestamp()
    }

    pub fn claims(sid: &str) -> TestClaims {
        TestClaims {
            iss: ISSUER.into(),
            sub: "user_1".into(),
            ws: WORKSPACE.into(),
            sid: sid.into(),
            aud: AUDIENCE.into(),
            iat: now(),
            exp: now() + 3600,
            jti: format!("jti_{sid}"),
        }
    }

    pub fn sign(claims: &TestClaims) -> String {
        sign_with(claims, PRIVATE_PEM)
    }

    pub fn sign_with(claims: &TestClaims, private_pem: &str) -> String {
        encode(
            &Header::new(Algorithm::ES256),
            claims,
            &EncodingKey::from_ec_pem(private_pem.as_bytes()).expect("test private key"),
        )
        .expect("signing a test token")
    }

    pub fn config() -> AuthConfig {
        AuthConfig::from_pems(&[PUBLIC_PEM], ISSUER, AUDIENCE, WORKSPACE).expect("test config")
    }

    pub fn token(sid: &str) -> String {
        sign(&claims(sid))
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};

    #[test]
    fn provider_is_available() {
        let token = token("sid_1");
        let key = DecodingKey::from_ec_pem(PUBLIC_PEM.as_bytes()).unwrap();
        let mut validation = Validation::new(Algorithm::ES256);
        validation.set_audience(&[AUDIENCE]);
        decode::<Claims>(&token, &key, &validation).expect("decode must not panic");
    }

    #[test]
    fn valid_token_verifies() {
        let claims = config().verify(&token("sid_1")).unwrap();
        assert_eq!(claims.sid, "sid_1");
        assert_eq!(claims.sub, "user_1");
        assert_eq!(claims.jti, "jti_sid_1");
    }

    #[test]
    fn expired_token_rejected() {
        let mut expired = claims("sid_1");
        expired.exp = now() - 120;
        assert_eq!(
            config().verify(&sign(&expired)).unwrap_err(),
            AuthError::Expired
        );

        let mut within_leeway = claims("sid_1");
        within_leeway.exp = now() - 10;
        assert!(config().verify(&sign(&within_leeway)).is_ok());
    }

    #[test]
    fn future_iat_rejected() {
        let mut future = claims("sid_1");
        future.iat = now() + 120;
        assert_eq!(
            config().verify(&sign(&future)).unwrap_err(),
            AuthError::Malformed
        );

        let mut near_future = claims("sid_1");
        near_future.iat = now() + 10;
        assert!(config().verify(&sign(&near_future)).is_ok());
    }

    #[test]
    fn wrong_audience_rejected() {
        let mut claims = claims("sid_1");
        claims.aud = "sb_other".into();
        assert_eq!(
            config().verify(&sign(&claims)).unwrap_err(),
            AuthError::WrongAudience
        );
    }

    #[test]
    fn wrong_issuer_rejected() {
        let mut claims = claims("sid_1");
        claims.iss = "evil".into();
        assert_eq!(
            config().verify(&sign(&claims)).unwrap_err(),
            AuthError::WrongIssuer
        );
    }

    #[test]
    fn wrong_workspace_rejected() {
        let mut claims = claims("sid_1");
        claims.ws = "ws_other".into();
        assert_eq!(
            config().verify(&sign(&claims)).unwrap_err(),
            AuthError::WrongWorkspace
        );
    }

    #[test]
    fn wrong_key_rejected() {
        let token = sign_with(&claims("sid_1"), OTHER_PRIVATE_PEM);
        assert_eq!(
            config().verify(&token).unwrap_err(),
            AuthError::BadSignature
        );
    }

    #[test]
    fn rotation_accepts_either_key() {
        let config =
            AuthConfig::from_pems(&[PUBLIC_PEM, OTHER_PUBLIC_PEM], ISSUER, AUDIENCE, WORKSPACE)
                .unwrap();
        let token = sign_with(&claims("sid_2"), OTHER_PRIVATE_PEM);
        assert_eq!(config.verify(&token).unwrap().sid, "sid_2");

        let joined = format!("{PUBLIC_PEM}\n{OTHER_PUBLIC_PEM}");
        let config = AuthConfig::from_pems(&[&joined], ISSUER, AUDIENCE, WORKSPACE).unwrap();
        assert!(config.verify(&token).is_ok());
        assert!(config.verify(&super::test_support::token("sid_1")).is_ok());
    }

    #[test]
    fn rotation_reports_specific_error() {
        let config =
            AuthConfig::from_pems(&[PUBLIC_PEM, OTHER_PUBLIC_PEM], ISSUER, AUDIENCE, WORKSPACE)
                .unwrap();
        let mut expired = claims("sid_2");
        expired.exp = now() - 120;
        let token = sign_with(&expired, OTHER_PRIVATE_PEM);
        assert_eq!(config.verify(&token).unwrap_err(), AuthError::Expired);
    }

    #[test]
    fn hs256_with_public_key_rejected() {
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims("sid_1"),
            &EncodingKey::from_secret(PUBLIC_PEM.as_bytes()),
        )
        .unwrap();
        assert_eq!(
            config().verify(&token).unwrap_err(),
            AuthError::WrongAlgorithm
        );
    }

    #[test]
    fn missing_claim_rejected() {
        #[derive(serde::Serialize)]
        struct WithoutSid {
            iss: String,
            sub: String,
            ws: String,
            aud: String,
            iat: u64,
            exp: u64,
            jti: String,
        }
        let claims = WithoutSid {
            iss: ISSUER.into(),
            sub: "user_1".into(),
            ws: WORKSPACE.into(),
            aud: AUDIENCE.into(),
            iat: now(),
            exp: now() + 3600,
            jti: "jti".into(),
        };
        let token = encode(
            &Header::new(Algorithm::ES256),
            &claims,
            &EncodingKey::from_ec_pem(PRIVATE_PEM.as_bytes()).unwrap(),
        )
        .unwrap();
        assert_eq!(config().verify(&token).unwrap_err(), AuthError::Malformed);
    }

    #[test]
    fn subprotocol_parsing() {
        assert_eq!(token_from_subprotocol("zs.v1, T").unwrap(), "T");
        assert_eq!(token_from_subprotocol("T, zs.v1").unwrap(), "T");
        assert_eq!(token_from_subprotocol(" zs.v1 ,T ").unwrap(), "T");
        assert_eq!(token_from_subprotocol("zs.v1"), Err(AuthError::Missing));
        assert_eq!(
            token_from_subprotocol("T"),
            Err(AuthError::MissingSubprotocol)
        );
    }

    #[test]
    fn extract_token_precedence() {
        let req = hyper::Request::builder()
            .uri("/rpc?zs_token=Q")
            .header("sec-websocket-protocol", "zs.v1, H")
            .header("authorization", "Bearer A")
            .body(())
            .unwrap();
        assert_eq!(extract_token(&req).unwrap(), ("H".to_owned(), true));

        let req = hyper::Request::builder()
            .uri("/files?zs_token=Q")
            .header("authorization", "Bearer A")
            .body(())
            .unwrap();
        assert_eq!(extract_token(&req).unwrap(), ("A".to_owned(), false));

        let req = hyper::Request::builder()
            .uri("/rpc?zs_proto=zs.v1&zs_token=Q")
            .body(())
            .unwrap();
        assert_eq!(extract_token(&req).unwrap(), ("Q".to_owned(), true));

        let req = hyper::Request::builder().uri("/rpc").body(()).unwrap();
        assert_eq!(extract_token(&req), Err(AuthError::Missing));
    }

    #[test]
    fn redact_query_hides_token() {
        assert_eq!(
            redact_query("/files?path=a&zs_token=abc"),
            "/files?path=a&zs_token=***"
        );
        assert_eq!(redact_query("/health"), "/health");
        assert_eq!(redact_query("/files?path=a"), "/files?path=a");
        let encoded_key = redact_query("/files?zs%5Ftoken=abc&path=a");
        assert!(!encoded_key.contains("abc"), "{encoded_key}");
        assert_eq!(encoded_key, "/files?zs_token=***&path=a");
        let req = hyper::Request::builder()
            .uri("/files?zs%5Ftoken=abc")
            .body(())
            .unwrap();
        assert_eq!(extract_token(&req).unwrap().0, "abc");
    }

    #[test]
    fn lifetime_over_max_ttl_rejected() {
        let mut long_lived = claims("sid_1");
        long_lived.exp = now() + MAX_TTL_SECS + LEEWAY_SECS + 60;
        assert_eq!(
            config().verify(&sign(&long_lived)).unwrap_err(),
            AuthError::Malformed
        );

        let mut at_max = claims("sid_1");
        at_max.exp = at_max.iat + MAX_TTL_SECS;
        assert!(config().verify(&sign(&at_max)).is_ok());
    }

    #[test]
    fn token_shape_check() {
        assert!(token_shape_is_plausible(&token("sid_1")));
        assert!(token_shape_is_plausible("aa.bb.cc"));
        for bad in ["", "garbage", "a.b", "a.b.c.d", "a..c", "a.b/c.d", "a.b=.c"] {
            assert!(!token_shape_is_plausible(bad), "{bad:?}");
        }
        let huge = format!("{}.b.c", "a".repeat(MAX_TOKEN_BYTES));
        assert!(!token_shape_is_plausible(&huge));
    }

    #[test]
    fn status_mapping() {
        assert_eq!(AuthError::Missing.status(), hyper::StatusCode::UNAUTHORIZED);
        assert_eq!(
            AuthError::WrongWorkspace.status(),
            hyper::StatusCode::FORBIDDEN
        );
        assert_eq!(
            AuthError::MissingSubprotocol.status(),
            hyper::StatusCode::UPGRADE_REQUIRED
        );
    }
}
