//! Native end-to-end test against a running local control plane (round 2, lane local-backend).
//!
//! Driven by `apps/web/tests/e2e-native/flow.test.ts` (through `apps/web/scripts/dev-local.sh
//! e2e`): the TypeScript side creates the workspace through the API, calls `/connect` and hands
//! the D26 result over as `ZS_E2E_WS_URL`, `ZS_E2E_TOKEN`, `ZS_E2E_WORKSPACE_ID` and
//! `ZS_E2E_SESSION_ID`, plus `ZS_E2E_REPO_PATH` (the checkout on this machine), `ZS_E2E_FILE`
//! (a file in it to edit), `ZS_E2E_EXPECT_FILES` (comma-separated paths the worktree snapshot
//! must list) and `ZS_E2E_APPEND` (the text the edit appends). Without `ZS_E2E_WS_URL` the test
//! is a no-op, so a plain `cargo test -p remote` stays green.
//!
//! What it proves with the round-1 WebSocket transport and `RemoteClient` — no fake anywhere:
//! the dial, `Hello`/`HelloAck` and `RemoteStarted` (inside `RemoteClient::new`);
//! `AddWorktree` for the checkout and the `UpdateWorktree` snapshot listing the fixture files;
//! `OpenBufferByPath`, an `UpdateBuffer` edit and `SaveBuffer`; and the file on disk carrying
//! the edit afterwards.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{FutureExt as _, channel::oneshot, pin_mut, select_biased};
use gpui::{AppContext as _, AsyncApp, Entity, TestAppContext};
use remote::{
    ConnectionIdentifier, ConnectionState, RemoteClient, RemoteClientDelegate,
    WebSocketClientDelegate, WebSocketConnectionOptions, connect,
};
use rpc::{
    AnyProtoClient,
    proto::{self, REMOTE_SERVER_PROJECT_ID, RequestMessage, TypedEnvelope},
};

/// Our replica in the buffer's CRDT: what a `Project::remote` client uses (`clock::ReplicaId::REMOTE_SERVER`).
const CLIENT_REPLICA_ID: u32 = 1;
/// Budget for one request round trip and for each wait below.
const STEP_TIMEOUT: Duration = Duration::from_secs(90);

struct E2eEnv {
    ws_url: String,
    token: String,
    workspace_id: String,
    session_id: String,
    repo_path: PathBuf,
    file: String,
    expect_files: Vec<String>,
    append: String,
}

impl E2eEnv {
    /// `None` when the test was not asked for; a partial environment is a failure, never a skip.
    fn from_env() -> Option<Self> {
        let ws_url = std::env::var("ZS_E2E_WS_URL").ok()?;
        let required = |name: &str| {
            std::env::var(name).unwrap_or_else(|_| panic!("ZS_E2E_WS_URL is set but {name} is not"))
        };
        Some(Self {
            ws_url,
            token: required("ZS_E2E_TOKEN"),
            workspace_id: required("ZS_E2E_WORKSPACE_ID"),
            session_id: required("ZS_E2E_SESSION_ID"),
            repo_path: PathBuf::from(required("ZS_E2E_REPO_PATH")),
            file: std::env::var("ZS_E2E_FILE").unwrap_or_else(|_| "README.md".to_string()),
            expect_files: std::env::var("ZS_E2E_EXPECT_FILES")
                .unwrap_or_else(|_| "README.md".to_string())
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(str::to_owned)
                .collect(),
            append: std::env::var("ZS_E2E_APPEND")
                .unwrap_or_else(|_| "\ne2e: edited by the native client\n".to_string()),
        })
    }
}

/// What the server streams to us after `AddWorktree` and `OpenBufferByPath`, plus the two
/// server-to-client *requests* the flow triggers: `AllocateWorktreeId` (the headless server's
/// `WorktreeStore` lets the client assign worktree ids; the real client answers from
/// `WorktreeIdCounter`, we answer from `next_worktree_id`) and `UpdateBuffer` (operations the
/// server originates on an open buffer, acknowledged and folded into our version).
#[derive(Default)]
struct Collector {
    worktree_updates: Vec<proto::UpdateWorktree>,
    worktree_complete: bool,
    buffer_state: Option<proto::BufferState>,
    buffer_ops: Vec<proto::Operation>,
    buffer_complete: bool,
    /// Ids handed out through `AllocateWorktreeId`, starting at 1.
    next_worktree_id: u64,
    /// `UpdateBuffer` requests received from the server, keyed by buffer id.
    server_ops: Vec<(u64, Vec<proto::Operation>)>,
}

impl Collector {
    fn entry_paths(&self) -> Vec<String> {
        self.worktree_updates
            .iter()
            .flat_map(|update| {
                update
                    .updated_entries
                    .iter()
                    .map(|entry| entry.path.clone())
            })
            .collect()
    }
}

/// Minimal vector clock over the proto representation (replica → highest lamport value seen).
#[derive(Default)]
struct Version(BTreeMap<u32, u32>);

impl Version {
    fn observe(&mut self, replica_id: u32, value: u32) {
        let slot = self.0.entry(replica_id).or_default();
        *slot = (*slot).max(value);
    }

    fn observe_entries(&mut self, entries: &[proto::VectorClockEntry]) {
        for entry in entries {
            self.observe(entry.replica_id, entry.timestamp);
        }
    }

    fn observe_operation(&mut self, operation: &proto::Operation) {
        match &operation.variant {
            Some(proto::operation::Variant::Edit(edit)) => {
                self.observe(edit.replica_id, edit.lamport_timestamp);
            }
            Some(proto::operation::Variant::Undo(undo)) => {
                self.observe(undo.replica_id, undo.lamport_timestamp);
            }
            _ => {}
        }
    }

    fn next_lamport(&self) -> u32 {
        self.0.values().copied().max().unwrap_or(0) + 1
    }

    fn to_proto(&self) -> Vec<proto::VectorClockEntry> {
        self.0
            .iter()
            .map(|(replica_id, timestamp)| proto::VectorClockEntry {
                replica_id: *replica_id,
                timestamp: *timestamp,
            })
            .collect()
    }
}

fn init_test(cx: &mut TestAppContext) {
    // Real sockets under the deterministic dispatcher: the fake clock advances with real time
    // while the test parks (same arrangement as the transport's loopback tests).
    cx.executor().allow_parking();
    cx.update(|cx| {
        release_channel::init(semver::Version::new(0, 0, 0), cx);
        gpui_tokio::init(cx);
    });
}

async fn wait_until(
    cx: &TestAppContext,
    what: &str,
    timeout: Duration,
    mut condition: impl FnMut() -> bool,
) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        cx.executor().timer(Duration::from_millis(50)).await;
    }
}

async fn request<T: RequestMessage>(
    cx: &TestAppContext,
    client: &AnyProtoClient,
    what: &str,
    message: T,
) -> T::Response {
    let response = client.request(message).fuse();
    let deadline = cx.executor().timer(STEP_TIMEOUT).fuse();
    pin_mut!(response, deadline);
    select_biased! {
        response = response => response.unwrap_or_else(|error| panic!("{what} failed: {error:#}")),
        _ = deadline => panic!("{what} timed out after {STEP_TIMEOUT:?}"),
    }
}

#[gpui::test]
async fn native_client_opens_a_worktree_and_saves_an_edit(cx: &mut TestAppContext) {
    let Some(env) = E2eEnv::from_env() else {
        eprintln!("native_e2e: ZS_E2E_WS_URL is not set; nothing to do");
        return;
    };
    init_test(cx);

    // 1. Dial with the D26 connect result, then complete Hello/HelloAck and RemoteStarted
    //    (`RemoteClient::new` waits for the server's `RemoteStarted`).
    let options = WebSocketConnectionOptions::new(
        env.ws_url.clone(),
        env.workspace_id.clone(),
        env.session_id.clone(),
        env.token.clone(),
    );
    let delegate: Arc<dyn RemoteClientDelegate> =
        Arc::new(WebSocketClientDelegate::new(|status, _| {
            eprintln!("native_e2e: status {status:?}")
        }));
    let mut async_cx = cx.to_async();
    let connection = connect(options.into(), delegate.clone(), &mut async_cx)
        .await
        .expect("dialing the workspace");
    let (_cancel_tx, cancel_rx) = oneshot::channel();
    let client = cx
        .update(|cx| {
            RemoteClient::new(
                ConnectionIdentifier::setup(),
                connection,
                cancel_rx,
                delegate,
                cx,
            )
        })
        .await
        .expect("RemoteClient::new failed")
        .expect("RemoteClient::new was cancelled");
    wait_until(cx, "the client to report Connected", STEP_TIMEOUT, || {
        cx.update(|cx| client.read(cx).connection_state()) == ConnectionState::Connected
    })
    .await;
    let options = cx.update(|cx| client.read(cx).connection_options());
    let server_info = match options {
        remote::RemoteConnectionOptions::WebSocket(options) => options.server_info(),
        other => panic!("unexpected connection options {other:?}"),
    };
    let server_info = server_info.expect("HelloAck was seen");
    eprintln!(
        "native_e2e: connected; server build {} platform {:?} shell {} epoch {} resumed {}",
        server_info.build,
        server_info.platform,
        server_info.shell,
        server_info.epoch,
        server_info.resumed
    );
    assert!(!server_info.resumed, "a first connect is a fresh session");
    assert!(
        server_info.epoch > 0,
        "HelloAck carried the server's session epoch"
    );

    // 2. Subscribe to the server's project-scoped messages (project id 0 on the remote server).
    let proto_client = cx.update(|cx| client.read(cx).proto_client());
    let collector: Entity<Collector> = cx.new(|_| Collector::default());
    proto_client.subscribe_to_entity(REMOTE_SERVER_PROJECT_ID, &collector);
    proto_client.add_entity_message_handler(
        |this: Entity<Collector>,
         envelope: TypedEnvelope<proto::UpdateWorktree>,
         mut cx: AsyncApp| async move {
            this.update(&mut cx, |this, _| {
                if envelope.payload.is_last_update {
                    this.worktree_complete = true;
                }
                this.worktree_updates.push(envelope.payload);
            });
            Ok(())
        },
    );
    proto_client.add_entity_message_handler(
        |this: Entity<Collector>,
         envelope: TypedEnvelope<proto::CreateBufferForPeer>,
         mut cx: AsyncApp| async move {
            this.update(&mut cx, |this, _| match envelope.payload.variant {
                Some(proto::create_buffer_for_peer::Variant::State(state)) => {
                    this.buffer_state = Some(state);
                }
                Some(proto::create_buffer_for_peer::Variant::Chunk(chunk)) => {
                    this.buffer_ops.extend(chunk.operations);
                    if chunk.is_last {
                        this.buffer_complete = true;
                    }
                }
                None => {}
            });
            Ok(())
        },
    );

    // The server asks *us* for worktree ids (`WorktreeStore::next_worktree_id` with a downstream
    // client at REMOTE_SERVER_PROJECT_ID); without this handler AddWorktree fails with
    // "no handler registered for AllocateWorktreeId".
    proto_client.add_entity_request_handler(
        |this: Entity<Collector>,
         _envelope: TypedEnvelope<proto::AllocateWorktreeId>,
         mut cx: AsyncApp| async move {
            let worktree_id = this.update(&mut cx, |this, _| {
                this.next_worktree_id += 1;
                this.next_worktree_id
            });
            Ok(proto::AllocateWorktreeIdResponse { worktree_id })
        },
    );
    // Operations the server originates on a buffer arrive as requests and want an Ack.
    proto_client.add_entity_request_handler(
        |this: Entity<Collector>,
         envelope: TypedEnvelope<proto::UpdateBuffer>,
         mut cx: AsyncApp| async move {
            this.update(&mut cx, |this, _| {
                this.server_ops
                    .push((envelope.payload.buffer_id, envelope.payload.operations));
            });
            Ok(proto::Ack {})
        },
    );

    // 3. AddWorktree for the checkout; the snapshot must list the fixture files.
    let repo_path = env.repo_path.to_string_lossy().into_owned();
    let added = request(
        cx,
        &proto_client,
        "AddWorktree",
        proto::AddWorktree {
            project_id: REMOTE_SERVER_PROJECT_ID,
            path: repo_path.clone(),
            visible: true,
        },
    )
    .await;
    assert!(
        added.worktree_id >= 1,
        "the worktree id came from our AllocateWorktreeId handler"
    );
    let expected_root =
        std::fs::canonicalize(&env.repo_path).expect("the checkout exists on this machine");
    let reported_root = std::fs::canonicalize(&added.canonicalized_path)
        .unwrap_or_else(|_| PathBuf::from(&added.canonicalized_path));
    assert_eq!(
        reported_root, expected_root,
        "the worktree root is the checkout"
    );
    wait_until(cx, "the initial worktree scan", STEP_TIMEOUT, || {
        cx.update(|cx| collector.read(cx).worktree_complete)
    })
    .await;
    let entries = cx.update(|cx| collector.read(cx).entry_paths());
    eprintln!(
        "native_e2e: worktree {} has {} entries",
        added.worktree_id,
        entries.len()
    );
    for expected in &env.expect_files {
        assert!(
            entries.iter().any(|path| path == expected),
            "worktree snapshot lacks {expected}; entries: {entries:?}"
        );
    }

    // 4. Open the file, wait for its state and operations.
    let opened = request(
        cx,
        &proto_client,
        "OpenBufferByPath",
        proto::OpenBufferByPath {
            project_id: REMOTE_SERVER_PROJECT_ID,
            worktree_id: added.worktree_id,
            path: env.file.clone(),
        },
    )
    .await;
    wait_until(cx, "the buffer state and operations", STEP_TIMEOUT, || {
        cx.update(|cx| {
            let collector = collector.read(cx);
            collector.buffer_complete
                && collector
                    .buffer_state
                    .as_ref()
                    .is_some_and(|state| state.id == opened.buffer_id)
        })
    })
    .await;
    let (state, ops) = cx.update(|cx| {
        let collector = collector.read(cx);
        let mut ops = collector.buffer_ops.clone();
        for (buffer_id, server_ops) in &collector.server_ops {
            if *buffer_id == opened.buffer_id {
                ops.extend(server_ops.iter().cloned());
            }
        }
        (collector.buffer_state.clone().expect("buffer state"), ops)
    });
    let on_disk_before =
        std::fs::read_to_string(env.repo_path.join(&env.file)).expect("reading the file");
    assert_eq!(
        state.base_text, on_disk_before,
        "the buffer opened the file on disk"
    );
    let mut version = Version::default();
    version.observe_entries(&state.saved_version);
    for op in &ops {
        version.observe_operation(op);
    }

    // 5. Append text with one remote edit at the version we observed, then save at the version
    //    that includes it.
    let lamport = version.next_lamport();
    let edit = proto::Operation {
        variant: Some(proto::operation::Variant::Edit(proto::operation::Edit {
            replica_id: CLIENT_REPLICA_ID,
            lamport_timestamp: lamport,
            version: version.to_proto(),
            ranges: vec![proto::Range {
                start: state.base_text.len() as u64,
                end: state.base_text.len() as u64,
            }],
            new_text: vec![env.append.clone()],
        })),
    };
    request(
        cx,
        &proto_client,
        "UpdateBuffer",
        proto::UpdateBuffer {
            project_id: REMOTE_SERVER_PROJECT_ID,
            buffer_id: opened.buffer_id,
            operations: vec![edit],
        },
    )
    .await;
    version.observe(CLIENT_REPLICA_ID, lamport);
    let saved = request(
        cx,
        &proto_client,
        "SaveBuffer",
        proto::SaveBuffer {
            project_id: REMOTE_SERVER_PROJECT_ID,
            buffer_id: opened.buffer_id,
            version: version.to_proto(),
            new_path: None,
        },
    )
    .await;
    assert_eq!(saved.buffer_id, opened.buffer_id);
    let saved_version = {
        let mut v = Version::default();
        v.observe_entries(&saved.version);
        v
    };
    assert_eq!(
        saved_version.0.get(&CLIENT_REPLICA_ID).copied(),
        Some(lamport),
        "BufferSaved.version includes our edit"
    );

    // 6. The file on disk changed.
    let on_disk_after =
        std::fs::read_to_string(env.repo_path.join(&env.file)).expect("reading the file");
    assert_eq!(
        on_disk_after,
        format!("{on_disk_before}{}", env.append),
        "the saved file carries the appended text"
    );
    eprintln!(
        "native_e2e: ok worktree={} buffer={} entries={} saved_len={}",
        added.worktree_id,
        opened.buffer_id,
        entries.len(),
        on_disk_after.len()
    );
    println!(
        "ZS_E2E_RESULT={{\"worktreeId\":{},\"bufferId\":{},\"entries\":{},\"serverBuild\":{:?}}}",
        added.worktree_id,
        opened.buffer_id,
        entries.len(),
        server_info.build
    );
    drop(client);
}
