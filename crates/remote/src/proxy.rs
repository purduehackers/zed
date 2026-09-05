use thiserror::Error;

#[derive(Copy, Clone, Error, Debug)]
#[repr(i32)]
pub enum ProxyLaunchError {
    // We're using 90 as the exit code, because 0-78 are often taken
    // by shells and other conventions and >128 also has certain meanings
    // in certain contexts.
    #[error("Attempted reconnect, but server not running.")]
    ServerNotRunning = 90,
    /// Another client holds the WebSocket session (close code 4001 superseded, or 4005
    /// session active).
    #[error("Another client is attached to this workspace session.")]
    SessionTakenOver = 91,
    /// The server build or protocol does not match this client (close code 4002 or 4006, or
    /// an incompatible `HelloAck.build`).
    #[error("Remote server build is incompatible with this client.")]
    IncompatibleServer = 92,
}

impl ProxyLaunchError {
    pub fn to_exit_code(self) -> i32 {
        self as i32
    }

    pub fn from_exit_code(exit_code: i32) -> Option<Self> {
        match exit_code {
            90 => Some(Self::ServerNotRunning),
            91 => Some(Self::SessionTakenOver),
            92 => Some(Self::IncompatibleServer),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_round_trip() {
        for error in [
            ProxyLaunchError::ServerNotRunning,
            ProxyLaunchError::SessionTakenOver,
            ProxyLaunchError::IncompatibleServer,
        ] {
            let code = error.to_exit_code();
            let parsed = ProxyLaunchError::from_exit_code(code).expect("known exit code");
            assert_eq!(parsed.to_exit_code(), code);
            assert!(!error.to_string().is_empty());
        }
        assert_eq!(
            ProxyLaunchError::from_exit_code(91).map(|e| e.to_exit_code()),
            Some(91)
        );
        assert_eq!(
            ProxyLaunchError::from_exit_code(92).map(|e| e.to_exit_code()),
            Some(92)
        );
        assert!(ProxyLaunchError::from_exit_code(93).is_none());
    }
}
