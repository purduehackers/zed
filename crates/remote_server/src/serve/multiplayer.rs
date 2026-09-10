//! One existing replay broker per signed participant. The dispatcher never shares
//! sequence numbers, epochs or backpressure between browser connections.

use super::*;
use rpc::proto::PeerId;
use session::{BrokerCommand, ServeHooks, SessionBroker, SessionMeta};
use std::collections::{HashMap, VecDeque};

// Bound expensive replay channels, not the number of people who can ever visit a VM.
const MAX_REPLAY_BROKERS: usize = 32;

struct Participant {
    identity: String,
    peer: PeerId,
    tx: tokio::sync::mpsc::UnboundedSender<BrokerCommand>,
    task: tokio::task::JoinHandle<()>,
}

/// Oldest last-attach first. Only the broker can arbitrate retirement against socket
/// exit/reconnect, so a queued attach or half-open live connection is never evicted.
async fn reclaim_detached(
    participants: &mut VecDeque<Participant>,
    gpui: &mpsc::UnboundedSender<GpuiCommand>,
) -> bool {
    for index in 0..participants.len() {
        let (done, reply) = tokio::sync::oneshot::channel();
        let sent = participants[index]
            .tx
            .send(BrokerCommand::RetireIfDetached { done })
            .is_ok();
        if sent && !reply.await.unwrap_or(true) {
            continue;
        }
        let Some(participant) = participants.remove(index) else {
            continue;
        };
        if let Err(error) = participant.task.await {
            log::warn!("participant broker exited unexpectedly: {error}");
        }
        let (done, reply) = futures::channel::oneshot::channel();
        if gpui
            .unbounded_send(GpuiCommand::ReleaseParticipant {
                peer: participant.peer,
                done,
            })
            .is_err()
        {
            return false;
        }
        return reply.await.is_ok();
    }
    false
}

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
    let mut participants = VecDeque::<Participant>::new();
    // Keep only lightweight identities after eviction. Replica IDs cannot be reused
    // for another person while VM buffers retain their operations. Stable IDs also
    // preserve terminal ownership. The wire's u16 replica space bounds this registry.
    let mut identities = HashMap::<String, PeerId>::new();
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
                let existing = participants
                    .iter()
                    .position(|entry| entry.identity == *participant);
                if let Some(index) = existing {
                    if let Some(entry) = participants.remove(index) {
                        participants.push_back(entry);
                    }
                } else {
                    if participants.len() >= MAX_REPLAY_BROKERS
                        && !reclaim_detached(&mut participants, &gpui).await
                    {
                        session::refuse_socket(
                            ws,
                            1008,
                            "workspace has 32 connected participants; try again when someone leaves",
                        );
                        continue;
                    }
                    let next_peer = PeerId {
                        owner_id: 0,
                        id: 8 + identities.len() as u32,
                    };
                    let peer = identities.get(participant).copied().unwrap_or(next_peer);
                    if peer.id > u16::MAX as u32 {
                        session::refuse_socket(
                            ws,
                            1008,
                            "workspace replica identities exhausted; restart the workspace",
                        );
                        continue;
                    }
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
                    let task = tokio::spawn(
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
                    );
                    identities.insert(participant.clone(), peer);
                    participants.push_back(Participant {
                        identity: participant.clone(),
                        peer,
                        tx,
                        task,
                    });
                }
                if let Some(participant) = participants.back()
                    && let Err(error) =
                        participant
                            .tx
                            .send(BrokerCommand::Attach { ws, hello, claims })
                {
                    log::warn!("participant broker is unavailable: {error}");
                }
            }
            // A browser closes its WebSocket; it cannot shut down the shared host.
            BrokerCommand::CloseSession { .. } => {}
            BrokerCommand::RetireIfDetached { done } => {
                done.send(false).ok();
            }
            BrokerCommand::Shutdown { done } => {
                let mut closed = Vec::new();
                for participant in &participants {
                    let (done, reply) = tokio::sync::oneshot::channel();
                    if participant
                        .tx
                        .send(BrokerCommand::Shutdown { done })
                        .is_ok()
                    {
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
    for participant in participants {
        participant.task.abort();
    }
}
