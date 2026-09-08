use cloud_api_types::ExtensionMetadata;
use collections::FxHashSet;
use extension::SchemaVersion;
use release_channel::ReleaseChannel;
use semver::Version;
use std::{ops::RangeInclusive, str::FromStr, sync::LazyLock};

/// The current extension [`SchemaVersion`] supported by Zed.
pub(crate) const CURRENT_SCHEMA_VERSION: SchemaVersion = SchemaVersion(1);

/// Extensions that should no longer be loaded or downloaded.
///
/// These snippets should no longer be downloaded or loaded, because their
/// functionality has been integrated into the core editor.
pub(crate) static SUPPRESSED_EXTENSIONS: LazyLock<FxHashSet<&str>> = LazyLock::new(|| {
    FxHashSet::from_iter([
        "snippets",
        "ruff",
        "ty",
        "basedpyright",
        "basher",
        // ACP
        "opencode",
        "mistral-vibe",
        "auggie",
        "stakpak",
        "codebuddy",
        "autohand-acp",
        "corust-agent",
        "factory-droid",
        "qqcode",
    ])
});

/// Whether `id` names an extension whose functionality moved into the core editor and
/// must no longer be installed or loaded (see [`SUPPRESSED_EXTENSIONS`]).
pub fn is_suppressed_extension(id: &str) -> bool {
    SUPPRESSED_EXTENSIONS.contains(id)
}

/// Returns the [`SchemaVersion`] range that is compatible with this version of Zed.
pub fn schema_version_range() -> RangeInclusive<SchemaVersion> {
    SchemaVersion::ZERO..=CURRENT_SCHEMA_VERSION
}

/// Returns whether the given extension version is compatible with this version of Zed.
pub fn is_version_compatible(
    release_channel: ReleaseChannel,
    extension_version: &ExtensionMetadata,
) -> bool {
    let schema_version = extension_version.manifest.schema_version.unwrap_or(0);
    if CURRENT_SCHEMA_VERSION.0 < schema_version {
        return false;
    }

    if let Some(wasm_api_version) = extension_version
        .manifest
        .wasm_api_version
        .as_ref()
        .and_then(|wasm_api_version| Version::from_str(wasm_api_version).ok())
        && !wasm_api_version_range(release_channel).contains(&wasm_api_version)
    {
        return false;
    }

    true
}

pub(crate) const MIN_API_VERSION: Version = Version::new(0, 0, 1);
pub(crate) const STABLE_MAX_API_VERSION: Version = Version::new(0, 7, 0);
pub(crate) const DEV_MAX_API_VERSION: Version = Version::new(0, 8, 0);

pub(crate) fn wasm_api_version_range(channel: ReleaseChannel) -> RangeInclusive<Version> {
    MIN_API_VERSION..=match channel {
        ReleaseChannel::Dev | ReleaseChannel::Nightly => DEV_MAX_API_VERSION,
        ReleaseChannel::Stable | ReleaseChannel::Preview => STABLE_MAX_API_VERSION,
    }
}
