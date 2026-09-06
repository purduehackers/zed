pub mod json_log;
pub mod protocol;
pub mod proxy;
pub mod remote_client;
pub mod remote_identity;
mod transport;

#[cfg(target_os = "windows")]
pub use remote_client::OpenWslPath;
pub use remote_client::{
    ChannelEnds, CommandTemplate, ConnectionIdentifier, ConnectionState, Interactive, RemoteArch,
    RemoteClient, RemoteClientDelegate, RemoteClientEvent, RemoteConnection,
    RemoteConnectionOptions, RemoteOs, RemotePlatform, ServerChannel, ServerHub, connect,
    has_active_connection,
};
pub use remote_identity::{
    RemoteConnectionIdentity, remote_connection_identity, same_remote_connection_identity,
};
pub use transport::docker::DockerConnectionOptions;
pub use transport::ssh::{SshConnectionOptions, SshPortForwardOption};
pub use transport::websocket::wire as websocket_wire;
pub use transport::websocket::{
    CloseInfo, RefreshError, RefreshReason, WebSocketClientDelegate, WebSocketConnectionOptions,
    WebSocketRemoteConnection, WebSocketServerInfo, WebSocketSession, WebSocketSessionRefresh,
    client_build_id, session_refresh_provider, set_session_refresh_provider,
};
pub use transport::wsl::WslConnectionOptions;
#[cfg(target_os = "windows")]
pub use transport::wsl::wsl_path_to_windows_path;

#[cfg(any(test, feature = "test-support"))]
pub use transport::mock::{
    MockConnection, MockConnectionOptions, MockConnectionRegistry, MockDelegate,
};
#[cfg(any(test, feature = "test-support"))]
pub use transport::websocket::set_client_build_id_for_tests;
