//! One existing replay broker per signed participant. The dispatcher never shares
//! sequence numbers, epochs or backpressure between browser connections.

use super::*;
use rpc::proto::PeerId;
use session::{BrokerCommand, ServeHooks, SessionBroker, SessionMeta};
use std::collections::HashMap;

const MAX_PARTICIPANTS: usize = 32;

struct ParticipantHooks {
    tx: mpsc::UnboundedSender<GpuiCommand>,
    channel: ServerChannel,
    peer: PeerId,
}

impl ServeHooks for ParticipantHooks {
    fn replica_id(&self) -> u16 {
        self.peer.id as u16
    }
    fn replay_healthy(&self) -> bool {
        self.channel.replay_healthy()
    }
    fn begin_fresh_session(&self) -> BoxFuture<'static, Result<remote::ChannelEnds>> {
        let (done, reply) = futures::channel::oneshot::channel();
        let sent = self
            .tx
            .unbounded_send(GpuiCommand::ResetParticipant {
                peer: self.peer,
                channel: self.channel.clone(),
                done,
            })
            .is_ok();
        async move {
            anyhow::ensure!(sent, "project hub is unavailable");
            Ok(reply.await?)
        }
        .boxed()
    }
    fn session_attached(&self, _: &SessionMeta) {
        self.tx
            .unbounded_send(GpuiCommand::ParticipantAttached(self.peer))
            .ok();
    }
    fn session_detached(&self, _: &SessionMeta) {
        self.tx
            .unbounded_send(GpuiCommand::ParticipantDetached(self.peer))
            .ok();
    }
    fn replace_buffered(&self, id: u32, replacement: Option<rpc::proto::Envelope>) {
        self.channel.replace_buffered(id, replacement);
    }
    fn request_quit(&self) {} // Only the dispatcher may quit the shared VM process.
}

pub async fn run(
    mut commands: tokio::sync::mpsc::UnboundedReceiver<BrokerCommand>,
    state: Arc<ServeState>,
    gpui: mpsc::UnboundedSender<GpuiCommand>,
) {
    let mut participants =
        HashMap::<String, tokio::sync::mpsc::UnboundedSender<BrokerCommand>>::new();
    let mut tasks = Vec::new();
    while let Some(command) = commands.recv().await {
        match command {
            BrokerCommand::Attach { ws, hello, claims } => {
                let Some(participant) = claims.pid.as_ref() else {
                    session::refuse_socket(ws, 1008, "participant identity required");
                    continue;
                };
                if !participant.starts_with("p_")
                    || participant.len() != 34
                    || !participant[2..].bytes().all(|c| c.is_ascii_hexdigit())
                {
                    session::refuse_socket(ws, 1008, "invalid participant identity");
                    continue;
                }
                if !participants.contains_key(participant) {
                    if participants.len() >= MAX_PARTICIPANTS {
                        session::refuse_socket(ws, 1008, "workspace participant limit reached");
                        continue;
                    }
                    let peer = PeerId {
                        owner_id: 0,
                        id: 8 + participants.len() as u32,
                    };
                    let (done, reply) = futures::channel::oneshot::channel();
                    if gpui
                        .unbounded_send(GpuiCommand::JoinParticipant {
                            peer,
                            participant: participant.clone(),
                            done,
                        })
                        .is_err()
                    {
                        session::refuse_socket(ws, 1011, "project hub unavailable");
                        continue;
                    }
                    let Ok((channel, ends)) = reply.await else {
                        session::refuse_socket(ws, 1011, "project hub unavailable");
                        continue;
                    };
                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                    tasks.push(tokio::spawn(
                        SessionBroker::new(
                            ends.incoming_tx,
                            ends.outgoing_rx,
                            rx,
                            state.clone(),
                            Arc::new(ParticipantHooks {
                                tx: gpui.clone(),
                                channel,
                                peer,
                            }),
                        )
                        .run(),
                    ));
                    participants.insert(participant.clone(), tx);
                }
                participants[participant]
                    .send(BrokerCommand::Attach { ws, hello, claims })
                    .ok();
            }
            // A browser closes its WebSocket; it cannot shut down the shared host.
            BrokerCommand::CloseSession { .. } => {}
            BrokerCommand::Shutdown { done } => {
                let mut closed = Vec::new();
                for tx in participants.values() {
                    let (done, reply) = tokio::sync::oneshot::channel();
                    if tx.send(BrokerCommand::Shutdown { done }).is_ok() {
                        closed.push(reply);
                    }
                }
                futures::future::join_all(closed).await;
                gpui.unbounded_send(GpuiCommand::Quit).ok();
                done.send(()).ok();
                break;
            }
        }
    }
    for task in tasks {
        task.abort();
    }
}
