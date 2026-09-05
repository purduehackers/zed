//! Lifecycle notices from the sandbox (BUILD-SPEC 5.5): the kinds the server sends. The
//! notice surfaces as `project::Event::LifecycleNotice`; the toast and the keep-alive live
//! in the shell page (BUILD-SPEC 7.6), which receives `LifecycleKind::as_str()`.

use rpc::proto;

/// What a `LifecycleNotice` announces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleKind {
    /// The workspace stops in `seconds` unless activity resumes.
    IdleStopIn,
    /// The session cap is reached in `seconds`; the workspace then restarts.
    SessionCapIn,
    /// The workspace is stopping now; flush state inside the stopping window.
    Stopping,
    /// The workspace resumed.
    Resumed,
}

impl LifecycleKind {
    /// Decodes a wire value; `None` for kinds this build does not know.
    pub fn from_proto(kind: i32) -> Option<Self> {
        match proto::LifecycleKind::try_from(kind).ok()? {
            proto::LifecycleKind::IdleStopIn => Some(Self::IdleStopIn),
            proto::LifecycleKind::SessionCapIn => Some(Self::SessionCapIn),
            proto::LifecycleKind::Stopping => Some(Self::Stopping),
            proto::LifecycleKind::Resumed => Some(Self::Resumed),
        }
    }

    /// The wire value.
    pub fn to_proto(self) -> proto::LifecycleKind {
        match self {
            Self::IdleStopIn => proto::LifecycleKind::IdleStopIn,
            Self::SessionCapIn => proto::LifecycleKind::SessionCapIn,
            Self::Stopping => proto::LifecycleKind::Stopping,
            Self::Resumed => proto::LifecycleKind::Resumed,
        }
    }

    /// The snake_case spelling shared with the supervisor JSON and the shell's
    /// `onLifecycle` (D29): `idle_stop_in`, `session_cap_in`, `stopping`, `resumed`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IdleStopIn => "idle_stop_in",
            Self::SessionCapIn => "session_cap_in",
            Self::Stopping => "stopping",
            Self::Resumed => "resumed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_kind_as_str_is_snake_case() {
        assert_eq!(LifecycleKind::IdleStopIn.as_str(), "idle_stop_in");
        assert_eq!(LifecycleKind::SessionCapIn.as_str(), "session_cap_in");
        assert_eq!(LifecycleKind::Stopping.as_str(), "stopping");
        assert_eq!(LifecycleKind::Resumed.as_str(), "resumed");
    }

    #[test]
    fn lifecycle_kind_round_trips_through_proto() {
        for kind in [
            LifecycleKind::IdleStopIn,
            LifecycleKind::SessionCapIn,
            LifecycleKind::Stopping,
            LifecycleKind::Resumed,
        ] {
            assert_eq!(
                LifecycleKind::from_proto(kind.to_proto() as i32),
                Some(kind)
            );
        }
        assert_eq!(LifecycleKind::from_proto(42), None);
    }
}
