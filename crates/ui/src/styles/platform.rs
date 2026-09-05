/// The platform style to use when rendering UI.
///
/// This can be used to abstract over platform differences.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum PlatformStyle {
    /// Display in macOS style.
    Mac,
    /// Display in Linux style.
    Linux,
    /// Display in Windows style.
    Windows,
}

/// The host style installed by [`PlatformStyle::set_platform_style`] (browser builds only).
#[cfg(target_family = "wasm")]
static PLATFORM_STYLE_OVERRIDE: std::sync::OnceLock<PlatformStyle> = std::sync::OnceLock::new();

impl PlatformStyle {
    /// Returns the [`PlatformStyle`] for the current platform.
    #[cfg(not(target_family = "wasm"))]
    pub const fn platform() -> Self {
        if cfg!(any(target_os = "linux", target_os = "freebsd")) {
            Self::Linux
        } else if cfg!(target_os = "windows") {
            Self::Windows
        } else {
            Self::Mac
        }
    }

    /// Returns the [`PlatformStyle`] of the browser's host once
    /// [`PlatformStyle::set_platform_style`] has run (the entry crate calls it from the boot
    /// config before the first frame); `Mac` until then.
    #[cfg(target_family = "wasm")]
    pub fn platform() -> Self {
        PLATFORM_STYLE_OVERRIDE.get().copied().unwrap_or(Self::Mac)
    }

    /// Browser builds only: installs the host platform style. Idempotent; the first call wins.
    #[cfg(target_family = "wasm")]
    pub fn set_platform_style(style: PlatformStyle) {
        PLATFORM_STYLE_OVERRIDE.set(style).ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_style_native_unchanged() {
        let expected = if cfg!(any(target_os = "linux", target_os = "freebsd")) {
            PlatformStyle::Linux
        } else if cfg!(target_os = "windows") {
            PlatformStyle::Windows
        } else {
            PlatformStyle::Mac
        };
        assert_eq!(PlatformStyle::platform(), expected);
    }
}
