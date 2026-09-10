//! One project, independent RPC sequence spaces. Shared handlers operate on the VM's
//! entities; replies use the requesting channel, while project events fan out.

use super::*;

pub struct ServerHub {
    handlers: Arc<Mutex<ProtoMessageHandlerSet>>,
    peers: Mutex<HashMap<PeerId, ServerChannel>>,
    active: Mutex<collections::HashSet<PeerId>>,
    terminal_owners: Mutex<HashMap<u64, PeerId>>,
    next_worktree_id: AtomicU64,
    executor: BackgroundExecutor,
}

impl ServerHub {
    pub fn new(cx: &App) -> Arc<Self> {
        Arc::new(Self {
            handlers: Arc::default(),
            peers: Mutex::default(),
            active: Mutex::default(),
            terminal_owners: Mutex::default(),
            next_worktree_id: AtomicU64::new(1),
            executor: cx.background_executor().clone(),
        })
    }

    pub fn add_peer(&self, peer: PeerId, cx: &App) -> (ServerChannel, ChannelEnds) {
        let (incoming_tx, incoming_rx) = mpsc::unbounded();
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded();
        let channel = ServerChannel {
            client: ChannelClient::new_for_peer(
                incoming_rx,
                outgoing_tx,
                cx,
                "participant",
                false,
                Some(peer),
                self.handlers.clone(),
            ),
        };
        self.peers.lock().insert(peer, channel.clone());
        (
            channel,
            ChannelEnds {
                incoming_tx,
                outgoing_rx,
            },
        )
    }

    pub fn remove_peer(&self, peer: PeerId) {
        self.active.lock().remove(&peer);
        self.peers.lock().remove(&peer);
    }

    pub fn set_active(&self, peer: PeerId, active: bool) {
        if active {
            self.active.lock().insert(peer);
        } else {
            self.active.lock().remove(&peer);
        }
    }

    pub fn own_terminal(&self, terminal: u64, peer: PeerId) {
        self.terminal_owners.lock().insert(terminal, peer);
    }

    pub fn owns_terminal(&self, terminal: u64, peer: PeerId) -> bool {
        self.terminal_owners.lock().get(&terminal) == Some(&peer)
    }

    fn recipients(&self, envelope: &Envelope) -> Vec<Arc<ChannelClient>> {
        use proto::envelope::Payload;
        let target = match &envelope.payload {
            Some(Payload::CreateBufferForPeer(message)) => message.peer_id,
            Some(Payload::CreateImageForPeer(message)) => message.peer_id,
            Some(Payload::CreateFileForPeer(message)) => message.peer_id,
            Some(Payload::TerminalOutput(message)) => self
                .terminal_owners
                .lock()
                .get(&message.terminal_id)
                .copied(),
            Some(Payload::TerminalExited(message)) => self
                .terminal_owners
                .lock()
                .get(&message.terminal_id)
                .copied(),
            _ => None,
        };
        // Unknown terminals must never be broadcast (including a fast process exit before
        // ownership registration). Their exit and scrollback remain available on attach.
        if target.is_none()
            && matches!(
                envelope.payload,
                Some(
                    Payload::TerminalOutput(_)
                        | Payload::TerminalExited(_)
                        | Payload::CreateBufferForPeer(_)
                        | Payload::CreateImageForPeer(_)
                        | Payload::CreateFileForPeer(_)
                )
            )
        {
            return Vec::new();
        }
        self.peers
            .lock()
            .iter()
            .filter(|(peer, channel)| {
                channel.replay_healthy() && target.is_none_or(|target| target == **peer)
            })
            .map(|(_, channel)| channel.client.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::TestAppContext;

    const A: PeerId = PeerId { owner_id: 0, id: 8 };
    const B: PeerId = PeerId { owner_id: 0, id: 9 };

    #[gpui::test]
    async fn peer_routing_and_independent_response_ids(cx: &mut TestAppContext) {
        let hub = cx.update(|cx| ServerHub::new(cx));
        let (_, mut a) = cx.update(|cx| hub.add_peer(A, cx));
        let (_, mut b) = cx.update(|cx| hub.add_peer(B, cx));
        let client: AnyProtoClient = hub.clone().into();
        let responder = cx.new(|_| ());
        client.add_request_handler(
            responder.downgrade(),
            |_, message: proto::TypedEnvelope<proto::Ping>, _| async move {
                // The supplied original_sender_id is ignored, even when request ids collide.
                assert!(matches!(message.original_sender_id, Some(A | B)));
                assert_eq!(message.original_sender_id, Some(message.sender_id));
                Ok(proto::Ack {})
            },
        );
        a.incoming_tx
            .unbounded_send(proto::Ping {}.into_envelope(100, None, Some(B)))
            .unwrap();
        b.incoming_tx
            .unbounded_send(proto::Ping {}.into_envelope(100, None, Some(A)))
            .unwrap();
        cx.run_until_parked();
        for ends in [&mut a, &mut b] {
            let mut replies = 0;
            while let Ok(envelope) = ends.outgoing_rx.try_recv() {
                if envelope.responding_to == Some(100) {
                    replies += 1;
                }
            }
            assert_eq!(replies, 1);
        }
        client
            .send(proto::CreateBufferForPeer {
                peer_id: Some(A),
                ..Default::default()
            })
            .unwrap();
        assert!(matches!(
            a.outgoing_rx.try_recv().unwrap().payload,
            Some(proto::envelope::Payload::CreateBufferForPeer(_))
        ));
        assert!(b.outgoing_rx.try_recv().is_err());
        hub.own_terminal(42, B);
        assert!(!hub.owns_terminal(42, A));
        client
            .send(proto::TerminalOutput {
                terminal_id: 42,
                ..Default::default()
            })
            .unwrap();
        assert!(a.outgoing_rx.try_recv().is_err());
        assert!(matches!(
            b.outgoing_rx.try_recv().unwrap().payload,
            Some(proto::envelope::Payload::TerminalOutput(_))
        ));
        let first = client
            .request(proto::AllocateWorktreeId {
                project_id: proto::REMOTE_SERVER_PROJECT_ID,
            })
            .await
            .unwrap();
        let second = client
            .request(proto::AllocateWorktreeId {
                project_id: proto::REMOTE_SERVER_PROJECT_ID,
            })
            .await
            .unwrap();
        assert_eq!(
            (first.worktree_id, second.worktree_id),
            (1, 2),
            "root IDs belong to the VM, not a participant"
        );
    }

    #[gpui::test]
    async fn slow_peer_is_bounded_without_blocking_others(cx: &mut TestAppContext) {
        let hub = cx.update(|cx| ServerHub::new(cx));
        let (slow, _a) = cx.update(|cx| hub.add_peer(A, cx));
        let (_, mut b) = cx.update(|cx| hub.add_peer(B, cx));
        for _ in 0..4096 {
            slow.client.send(proto::Ping {}).unwrap();
        }
        assert!(slow.client.send(proto::Ping {}).is_err());
        assert!(!slow.replay_healthy());
        assert_eq!(slow.client.buffer.lock().len(), 4096);
        let client: AnyProtoClient = hub.clone().into();
        client.send(proto::Ping {}).unwrap();
        cx.run_until_parked();
        let mut delivered = false;
        while let Ok(envelope) = b.outgoing_rx.try_recv() {
            delivered |= matches!(envelope.payload, Some(proto::envelope::Payload::Ping(_)));
        }
        assert!(delivered);
        let _fresh = cx.update(|cx| slow.begin_fresh_session(&cx.to_async()));
        assert!(slow.replay_healthy());
        assert_eq!(slow.client.replay_bytes.load(SeqCst), 0);
        assert!(slow.client.send(proto::Ping {}).is_ok());
    }
}

impl ProtoClient for ServerHub {
    fn peer_client(&self, peer: PeerId) -> Result<Option<AnyProtoClient>> {
        Ok(Some(
            self.peers
                .lock()
                .get(&peer)
                .context("participant is no longer connected")?
                .proto_client(),
        ))
    }

    fn request(
        &self,
        envelope: Envelope,
        name: &'static str,
    ) -> BoxFuture<'static, Result<Envelope>> {
        // The VM, not whichever tab arrived first, allocates shared roots.
        if matches!(
            envelope.payload,
            Some(proto::envelope::Payload::AllocateWorktreeId(_))
        ) {
            let worktree_id = self.next_worktree_id.fetch_add(1, SeqCst);
            return async move {
                Ok(proto::AllocateWorktreeIdResponse { worktree_id }.into_envelope(0, None, None))
            }
            .boxed();
        }
        if matches!(
            envelope.payload,
            Some(proto::envelope::Payload::LanguageServerPromptRequest(_))
        ) {
            let peer = self
                .active
                .lock()
                .iter()
                .min_by_key(|peer| peer.id)
                .copied();
            if let Some(channel) = peer.and_then(|peer| self.peers.lock().get(&peer).cloned()) {
                return ProtoClient::request(channel.client.as_ref(), envelope, name);
            }
            return async { anyhow::bail!("no participant can answer the language server prompt") }
                .boxed();
        }
        // Only shared-state notifications may fan out as requests. A prompt or search
        // callback must explicitly choose its initiating participant with `for_peer`.
        if !matches!(
            envelope.payload,
            Some(
                proto::envelope::Payload::UpdateBuffer(_)
                    | proto::envelope::Payload::TrustWorktrees(_)
                    | proto::envelope::Payload::RestrictWorktrees(_)
            )
        ) {
            return async move { anyhow::bail!("{name} needs a participant destination") }.boxed();
        }
        for peer in self.recipients(&envelope) {
            let response = ProtoClient::request(peer.as_ref(), envelope.clone(), name);
            let timeout = self.executor.timer(Duration::from_secs(30));
            self.executor
                .spawn(async move {
                    let _ = futures::future::select(response, timeout.boxed()).await;
                })
                .detach();
        }
        async { Ok(proto::Ack {}.into_envelope(0, None, None)) }.boxed()
    }

    fn send(&self, envelope: Envelope, _name: &'static str) -> Result<()> {
        anyhow::ensure!(
            envelope.responding_to.is_none(),
            "hub responses require a participant channel"
        );
        for peer in self.recipients(&envelope) {
            // One slow peer must not suppress delivery to the remaining participants.
            peer.send_dynamic(envelope.clone()).log_err();
        }
        Ok(())
    }

    fn send_response(&self, _: Envelope, _: &'static str) -> Result<()> {
        anyhow::bail!("hub responses require a participant channel")
    }

    fn message_handler_set(&self) -> &Mutex<ProtoMessageHandlerSet> {
        &self.handlers
    }
    fn is_via_collab(&self) -> bool {
        false
    }
    fn has_wsl_interop(&self) -> bool {
        false
    }
}
