//! Which desktop OS the browser runs on. It decides the keymap file set (D12) and the
//! `PlatformStyle` (⌘/⌥ versus Ctrl/Alt glyphs, title-bar layout).

/// The browser's host operating system.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum HostOs {
    /// macOS (and iPadOS, which reports `MacIntel`).
    Mac,
    /// Windows.
    Windows,
    /// Linux, ChromeOS, BSDs and anything else.
    Linux,
}

impl HostOs {
    /// Parses the `hostOs` override of the boot config: `"mac" | "windows" | "linux"`.
    pub fn from_override(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "mac" | "macos" | "darwin" => Some(Self::Mac),
            "windows" | "win" => Some(Self::Windows),
            "linux" => Some(Self::Linux),
            _ => None,
        }
    }

    /// Detects the host from `navigator.platform` and `navigator.userAgent` (the heuristic
    /// `gpui_web` uses for its modifier mapping). Safari on iPad reports `MacIntel`.
    pub fn from_platform(platform: &str, user_agent: &str) -> Self {
        let platform = platform.to_ascii_lowercase();
        let user_agent = user_agent.to_ascii_lowercase();
        if platform.contains("mac")
            || platform.contains("iphone")
            || platform.contains("ipad")
            || platform.contains("ipod")
        {
            Self::Mac
        } else if platform.contains("win") {
            Self::Windows
        } else if platform.is_empty() {
            if user_agent.contains("mac")
                || user_agent.contains("iphone")
                || user_agent.contains("ipad")
            {
                Self::Mac
            } else if user_agent.contains("windows") {
                Self::Windows
            } else {
                Self::Linux
            }
        } else {
            Self::Linux
        }
    }

    /// The name the shell uses for this host.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mac => "mac",
            Self::Windows => "windows",
            Self::Linux => "linux",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_platform_strings() {
        assert_eq!(HostOs::from_platform("MacIntel", ""), HostOs::Mac);
        assert_eq!(HostOs::from_platform("Win32", ""), HostOs::Windows);
        assert_eq!(HostOs::from_platform("Linux x86_64", ""), HostOs::Linux);
        assert_eq!(HostOs::from_platform("iPhone", ""), HostOs::Mac);
        assert_eq!(
            HostOs::from_platform("", "Mozilla/5.0 (Windows NT 10.0)"),
            HostOs::Windows
        );
        assert_eq!(
            HostOs::from_platform("", "Mozilla/5.0 (Macintosh)"),
            HostOs::Mac
        );
        assert_eq!(
            HostOs::from_platform("", "Mozilla/5.0 (X11; Linux)"),
            HostOs::Linux
        );
    }

    #[test]
    fn override_wins_over_detection() {
        assert_eq!(HostOs::from_override("windows"), Some(HostOs::Windows));
        assert_eq!(HostOs::from_override("Mac"), Some(HostOs::Mac));
        assert_eq!(HostOs::from_override("linux"), Some(HostOs::Linux));
        assert_eq!(HostOs::from_override("amiga"), None);
        let detected = HostOs::from_platform("MacIntel", "");
        let chosen = HostOs::from_override("windows").unwrap_or(detected);
        assert_eq!(chosen, HostOs::Windows);
    }
}
