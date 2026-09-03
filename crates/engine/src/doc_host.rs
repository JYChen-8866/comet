//! DocHost — per-chat `SessionDoc` handles: local snapshot persistence
//! (debounced), and the HOST-ONLY durable command executor.
//!
//! Pragmatic port of comet's `session-docs.ts` + the `main.ts` executor (spec:
//! feature-inventory §3.3, ARCHITECTURE §2 "command plane"):
//! - the doc IS the outbox: commands and user entries commit locally and persist
//!   via the SQLite snapshot store;
//! - on every doc change (local commit or remote import) the handle re-emits the joined
//!   transcript to watchers, drains pending commands, and schedules a snapshot save;
//! - command drain: evaluate via `evaluate_command` (with the DocsStore processed
//!   ledger), mark processed BEFORE execute, execute through the sessions engine, then
//!   write the outcome status back into the doc as the sole outcome writer.
//!
//! Chat ownership is gated on the workspace doc (`chats[chat_id].deviceId`), with
//! claim-on-first-command for unknown chats.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};

use tokio::sync::watch;

use comet_doc::{
    COMMAND_DEFAULT_TTL_MS, CommandBasedOn, CommandDisposition, DocError, EvaluationContext,
    MessagePart, MessageRole, MessageStatus, SessionCommandEntry, SessionCommandPayload,
    SessionCommandStatus, SessionDoc, SessionMessageEntry, evaluate_command,
    join_continuation_entries,
};
use comet_proto::{ContextTurn, HarnessId, UserInputAnswer, UserInputQuestion};
use comet_sync::DocsStore;

use crate::sessions::{SessionsEngine, SteerOutcome};
use crate::workspace_host::WorkspaceHost;
use crate::{EngineError, new_id, now_ms};

/// Immutable transcript snapshots are shared by every watcher.  A streaming
/// commit still builds one fresh snapshot from the Loro document, but handing
/// it through an `Arc` prevents each `watch` receiver (and embedded UI panel)
/// from cloning the complete transcript on every token batch.
pub type TranscriptSnapshot = Arc<Vec<SessionMessageEntry>>;

/// Debounce window for local snapshot saves after a doc change.
const SNAPSHOT_DEBOUNCE_MS: u64 = 1_000;
/// Transcript projection is a UI-facing snapshot, not the command/persistence
/// path. Coalesce a burst of CRDT token commits into at most one full joined
/// snapshot per frame so a long answer does not allocate the whole history for
/// every token.
const TRANSCRIPT_PUBLISH_DEBOUNCE_MS: u64 = 16;

fn schedule_transcript_publish(
    deadline: &mut Option<tokio::time::Instant>,
    now: tokio::time::Instant,
) {
    if deadline.is_none() {
        *deadline = Some(now + std::time::Duration::from_millis(TRANSCRIPT_PUBLISH_DEBOUNCE_MS));
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct DocRevisions {
    any: u64,
    messages: u64,
    commands: u64,
}

fn container_is_root(container: &loro::ContainerID, root: &str) -> bool {
    matches!(
        container,
        loro::ContainerID::Root { name, .. } if name.as_str() == root
    )
}

fn diff_touches_root(event: &loro::event::DiffEvent<'_>, root: &str) -> bool {
    event.events.iter().any(|change| {
        // Unknown diffs are conservatively treated as touching every root. A
        // redundant refresh is preferable to dropping a future-schema update.
        change.is_unknown
            || container_is_root(change.target, root)
            || change
                .path
                .iter()
                .any(|(container, _)| container_is_root(container, root))
    })
}

#[derive(Debug, Clone)]
pub struct DocHostConfig {
    pub device_id: String,
    /// Harness for doc-command runs on chats without a workspace `config` row.
    pub default_harness: HarnessId,
}

/// Allocation-free counters for the resident document layer. Sampling uses the
/// weak registry and the watch senders' current immutable snapshots; it never
/// re-reads a Loro document or clones transcript entries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DocHostMemoryDiagnostics {
    pub registry_slots: usize,
    pub open_doc_handles: usize,
    pub dirty_doc_handles: usize,
    pub transcript_watchers: usize,
    pub transcript_entries: usize,
}

struct DocHostInner {
    store: Arc<DocsStore>,
    config: DocHostConfig,
    /// DocHost methods are also called by synchronous UI code. Keep the
    /// engine runtime captured at assembly time so opening a chat from a GPUI
    /// callback never depends on that callback having a Tokio reactor.
    runtime: tokio::runtime::Handle,
    /// Runtime default harness (Aurin swaps AI backends from settings).
    default_harness: Mutex<HarnessId>,
    sessions: OnceLock<SessionsEngine>,
    workspace: OnceLock<WorkspaceHost>,
    /// Weak registry of open handles. Callers own the strong reference while a
    /// chat is in use; retaining an `Arc` here would keep every chat's Loro
    /// document, subscription, and transcript watch alive for the process
    /// lifetime after the user navigated away.
    handles: Mutex<HashMap<String, Weak<ChatDocHandle>>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone)]
pub struct DocHost {
    inner: Arc<DocHostInner>,
}

/// One open chat doc: the `SessionDoc`, its change plumbing, and the transcript watch.
pub struct ChatDocHandle {
    chat_id: String,
    device_id: String,
    doc: Arc<SessionDoc>,
    store: Arc<DocsStore>,
    dirty: Arc<AtomicBool>,
    /// `set_sessions`, the change pump and a local queue call can all request a
    /// drain concurrently. Serialize the evaluate/mark/execute transaction so
    /// two tasks can never pass the processed-ledger check for one command.
    drain_lock: tokio::sync::Mutex<()>,
    messages_tx: watch::Sender<TranscriptSnapshot>,
    /// Doc subscription (drop = unsubscribe) — bumps the change watch on every commit.
    _sub: loro::Subscription,
}

impl Drop for ChatDocHandle {
    fn drop(&mut self) {
        // Usually the chat task owns a dirty handle through the debounce and
        // clears this bit after saving. If the last UI/RPC owner disappears
        // before that task can wake, persist here so weak-handle reclamation
        // cannot turn into data loss.
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return;
        }
        match self.doc.export_snapshot() {
            Ok(bytes) => {
                if let Err(err) = self.store.save_snapshot(&self.chat_id, &bytes) {
                    tracing::warn!(
                        chat = %self.chat_id,
                        error = %err,
                        "final snapshot save on handle drop failed"
                    );
                }
            }
            Err(err) => {
                tracing::warn!(
                    chat = %self.chat_id,
                    error = %err,
                    "final snapshot export on handle drop failed"
                );
            }
        }
    }
}

impl ChatDocHandle {
    pub fn chat_id(&self) -> &str {
        &self.chat_id
    }

    pub fn doc(&self) -> &SessionDoc {
        &self.doc
    }

    pub fn doc_arc(&self) -> Arc<SessionDoc> {
        self.doc.clone()
    }

    /// Joined transcript watch — re-sent on every doc change (WatchDocMessages).
    pub fn watch_messages(&self) -> watch::Receiver<TranscriptSnapshot> {
        self.messages_tx.subscribe()
    }

    pub fn connected(&self) -> bool {
        true
    }

    /// Write a complete user message entry, idempotent by id (the client-minted message
    /// id — a re-executed command or optimistic echo never duplicates the entry).
    pub fn write_user_message(
        &self,
        message_id: &str,
        text: &str,
        created_at: i64,
    ) -> Result<(), DocError> {
        if self.doc.contains_message_id(message_id) {
            return Ok(());
        }
        self.doc.push_message(&SessionMessageEntry {
            id: message_id.to_string(),
            role: MessageRole::User,
            parts: vec![MessagePart::Text {
                id: "t0".into(),
                text: text.to_string(),
            }],
            created_at,
            device_id: self.device_id.clone(),
            status: Some(MessageStatus::Complete),
            continuation_of: None,
        })
    }

    /// Write a complete host-generated assistant message, idempotent by id.
    /// Used by Aurin to confirm AI document edits without starting a new run.
    pub fn write_assistant_message(
        &self,
        message_id: &str,
        text: &str,
        created_at: i64,
    ) -> Result<(), DocError> {
        if self.doc.contains_message_id(message_id) {
            return Ok(());
        }
        self.doc.push_message(&SessionMessageEntry {
            id: message_id.to_string(),
            role: MessageRole::Assistant,
            parts: vec![MessagePart::Text {
                id: "t0".into(),
                text: text.to_string(),
            }],
            created_at,
            device_id: self.device_id.clone(),
            status: Some(MessageStatus::Complete),
            continuation_of: None,
        })
    }

    /// Recovery sweep: stamp this device's abandoned `streaming` entries `aborted`, appending
    /// `note` as a visible error part so the transcript says WHY the turn
    /// ended (comet folded "Run interrupted by backend restart" the same
    /// way). Returns the stamped entries' `(id, created_at)` — recovery uses
    /// them for the resume-freshness check.
    pub fn mark_abandoned_streams(&self, note: &str) -> Result<Vec<(String, i64)>, DocError> {
        let mut stamped = Vec::new();
        for entry in self.doc.read_entries()? {
            if entry.role == MessageRole::Assistant
                && entry.status == Some(MessageStatus::Streaming)
                && entry.device_id == self.device_id
                && self
                    .doc
                    .set_message_status(&entry.id, MessageStatus::Aborted)?
            {
                let part_id = format!("{}-recovery", entry.id);
                if let Err(err) = self.doc.append_error_part(&entry.id, &part_id, note) {
                    tracing::warn!(chat = %self.chat_id, error = %err, "recovery note append failed");
                }
                stamped.push((entry.id.clone(), entry.created_at));
            }
        }
        if !stamped.is_empty() {
            self.publish_messages();
        }
        Ok(stamped)
    }

    fn publish_messages(&self) {
        match self.doc.read_entries() {
            Ok(entries) => {
                let joined = Arc::new(join_continuation_entries(entries));
                // send_replace: update the watch even with no subscribers yet, so a
                // late subscriber's first borrow sees the current transcript.
                self.messages_tx.send_replace(joined);
            }
            Err(err) => {
                tracing::warn!(chat = %self.chat_id, error = %err, "transcript read failed");
            }
        }
    }
}

impl DocHost {
    pub fn new(store: Arc<DocsStore>, config: DocHostConfig) -> Self {
        let default_harness = config.default_harness;
        let runtime =
            tokio::runtime::Handle::try_current().expect("DocHost::new requires a Tokio runtime");
        Self {
            inner: Arc::new(DocHostInner {
                store,
                config,
                runtime,
                default_harness: Mutex::new(default_harness),
                sessions: OnceLock::new(),
                workspace: OnceLock::new(),
                handles: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Wire the sessions engine (engine assembly; see `SessionsEngine::set_doc_host`).
    pub fn set_sessions(&self, sessions: SessionsEngine) {
        let _ = self.inner.sessions.set(sessions);
        // Commands may already be pending in warm-opened docs.
        let handles = self.live_handles();
        for handle in handles {
            let host = self.clone();
            self.inner
                .runtime
                .spawn(async move { host.drain_commands(&handle).await });
        }
    }

    /// Wire the workspace host (engine assembly) — the source of chat-ownership rows.
    pub fn set_workspace(&self, workspace: WorkspaceHost) {
        let _ = self.inner.workspace.set(workspace);
    }

    /// The workspace host, once wired (tests may assemble a DocHost without one).
    pub fn workspace(&self) -> Option<&WorkspaceHost> {
        self.inner.workspace.get()
    }

    pub fn device_id(&self) -> &str {
        &self.inner.config.device_id
    }

    pub fn memory_diagnostics(&self) -> DocHostMemoryDiagnostics {
        let mut handles = lock(&self.inner.handles);
        handles.retain(|_, handle| handle.strong_count() != 0);
        let live = handles.values().filter_map(Weak::upgrade);
        let mut diagnostics = DocHostMemoryDiagnostics {
            registry_slots: handles.len(),
            ..DocHostMemoryDiagnostics::default()
        };
        for handle in live {
            diagnostics.open_doc_handles += 1;
            diagnostics.dirty_doc_handles += usize::from(handle.dirty.load(Ordering::Acquire));
            diagnostics.transcript_watchers += handle.messages_tx.receiver_count();
            diagnostics.transcript_entries += handle.messages_tx.borrow().len();
        }
        diagnostics
    }

    /// Hot-swap the default harness (Aurin settings change the AI backend at
    /// runtime; chat runs queued through the doc use this default when the
    /// chat row carries no config).
    pub fn set_default_harness(&self, harness: HarnessId) {
        *lock(&self.inner.default_harness) = harness;
    }

    /// Open (or return) the chat's doc handle: load the local snapshot (or init
    /// fresh) and start the change-driven task.
    pub fn open(&self, chat_id: &str) -> Result<Arc<ChatDocHandle>, EngineError> {
        {
            let mut handles = lock(&self.inner.handles);
            if let Some(handle) = handles.get(chat_id).and_then(Weak::upgrade) {
                return Ok(handle);
            }
            // Opportunistically remove stale registry entries while the lock is
            // already held. This keeps the registry itself bounded by live
            // callers rather than by the number of chats ever opened.
            handles.retain(|_, handle| handle.strong_count() != 0);
        }
        let doc = match self.inner.store.load_snapshot(chat_id)? {
            Some(bytes) => {
                let raw = loro::LoroDoc::new();
                raw.import(&bytes)
                    .map_err(|e| EngineError::Other(format!("snapshot import failed: {e}")))?;
                SessionDoc::from_doc(raw)
            }
            None => SessionDoc::init(chat_id)?,
        };
        let doc = Arc::new(doc);

        let dirty = Arc::new(AtomicBool::new(false));
        let dirty_from_sub = dirty.clone();
        let (changed_tx, changed_rx) = watch::channel(DocRevisions::default());
        let sub = doc.doc().subscribe_root(Arc::new(move |diff| {
            dirty_from_sub.store(true, Ordering::Release);
            let messages_changed = diff_touches_root(&diff, "messages");
            let commands_changed = diff_touches_root(&diff, "commands");
            changed_tx.send_modify(|revision| {
                revision.any = revision.any.wrapping_add(1);
                if messages_changed {
                    revision.messages = revision.messages.wrapping_add(1);
                }
                if commands_changed {
                    revision.commands = revision.commands.wrapping_add(1);
                }
            });
        }));
        let joined = join_continuation_entries(doc.read_entries()?);
        let (messages_tx, _) = watch::channel(Arc::new(joined));

        let handle = Arc::new(ChatDocHandle {
            chat_id: chat_id.to_string(),
            device_id: self.inner.config.device_id.clone(),
            doc,
            store: self.inner.store.clone(),
            dirty,
            drain_lock: tokio::sync::Mutex::new(()),
            messages_tx,
            _sub: sub,
        });
        {
            let mut handles = lock(&self.inner.handles);
            if let Some(existing) = handles.get(chat_id).and_then(Weak::upgrade) {
                return Ok(existing); // racing open — keep the first
            }
            handles.insert(chat_id.to_string(), Arc::downgrade(&handle));
        }

        self.inner
            .runtime
            .spawn(chat_task(self.clone(), Arc::downgrade(&handle), changed_rx));
        Ok(handle)
    }

    /// Composer path: append an immutable pending command entry (rule 1). Durable by
    /// construction — the change subscription kicks the drain, so a local host executes
    /// immediately and an offline doc simply holds the entry until it syncs.
    pub fn queue_command(
        &self,
        chat_id: &str,
        payload: SessionCommandPayload,
    ) -> Result<String, EngineError> {
        let handle = self.open(chat_id)?;
        let id = new_id();
        let now = now_ms();
        let based_on = handle
            .doc
            .last_message_id()
            .map(|message_id| CommandBasedOn {
                turn_id: Some(message_id),
                frontier: None,
            });
        handle.doc.queue_command(&SessionCommandEntry {
            id: id.clone(),
            payload,
            issued_by: self.inner.config.device_id.clone(),
            issued_at: now,
            based_on,
            expires_at: Some(now + COMMAND_DEFAULT_TTL_MS),
            status: SessionCommandStatus::Pending,
            resolution: None,
        })?;
        // A unary RPC caller does not own a transcript stream. Keep this handle
        // alive until the durable command has been offered to the executor;
        // otherwise the weak registry and weak chat task can both disappear in
        // the gap immediately after `queue_command` returns.
        let host = self.clone();
        self.inner
            .runtime
            .spawn(async move { host.drain_commands(&handle).await });
        Ok(id)
    }

    /// §2.2 writer discipline: we host a chat iff its workspace row's `deviceId` is
    /// ours; a chat with no row is claimable (claim-on-first-command). Without a
    /// wired workspace host (bare-DocHost tests) every open chat is ours — M2's
    /// behavior, now the degenerate case.
    fn is_host(&self, chat_id: &str) -> bool {
        self.workspace().is_none_or(|ws| ws.is_host(chat_id))
    }

    /// Chat-config harness when the workspace row carries one, else the default.
    pub(crate) fn harness_for(&self, chat_id: &str) -> HarnessId {
        let fallback = *lock(&self.inner.default_harness);
        match self
            .workspace()
            .and_then(|ws| ws.chat_config(chat_id))
            .map(|config| config.harness)
        {
            // A chat created while the mock was active carries `Mock` in its
            // config row; once Aurin settings configure a real provider, the
            // runtime default wins so doc-queued runs actually use the AI.
            Some(HarnessId::Mock) if fallback != HarnessId::Mock => fallback,
            Some(harness) => harness,
            None => fallback,
        }
    }

    /// Drain pending commands (host-only): evaluate → mark processed BEFORE execute →
    /// execute → write the outcome as the sole outcome writer.
    pub async fn drain_commands(&self, handle: &Arc<ChatDocHandle>) {
        let _drain = handle.drain_lock.lock().await;
        let Some(sessions) = self.inner.sessions.get() else {
            return; // executor not wired yet; the set_sessions kick re-drains
        };
        if !self.is_host(&handle.chat_id) {
            return;
        }
        // Entries this pass decided to leave alone (processed dedupe hits).
        let mut skipped: HashSet<String> = HashSet::new();
        loop {
            let commands = match handle.doc.read_commands() {
                Ok(commands) => commands,
                Err(err) => {
                    tracing::warn!(chat = %handle.chat_id, error = %err, "command read failed");
                    return;
                }
            };
            let is_processed = |id: &str| self.inner.store.is_processed(id).unwrap_or(false);
            let Some(entry) = commands
                .iter()
                .find(|c| {
                    c.status == SessionCommandStatus::Pending
                        && !skipped.contains(&c.id)
                        && !is_processed(&c.id)
                })
                .cloned()
            else {
                return;
            };
            let message_ids = handle.doc.message_ids();
            let current_turn_id = message_ids.last().cloned();
            let turn_is_past = |turn_id: &str| message_ids.iter().any(|id| id == turn_id);
            let disposition = evaluate_command(
                &entry,
                &EvaluationContext {
                    is_processed: &is_processed,
                    now_ms: now_ms(),
                    entries: &commands,
                    current_turn_id: current_turn_id.as_deref(),
                    turn_is_past: &turn_is_past,
                },
            );
            // Mark BEFORE executing: a crash mid-execution must never double-run a
            // command whose side effect may already have happened.
            if let Err(err) = self.inner.store.mark_processed(&entry.id) {
                tracing::error!(chat = %handle.chat_id, error = %err, "processed-ledger write failed; halting drain");
                return;
            }
            match disposition {
                CommandDisposition::Skip => {
                    skipped.insert(entry.id.clone());
                }
                CommandDisposition::Expired => {
                    self.resolve_command(handle, &entry.id, SessionCommandStatus::Expired, None);
                }
                CommandDisposition::Superseded => {
                    self.resolve_command(handle, &entry.id, SessionCommandStatus::Superseded, None);
                }
                CommandDisposition::Execute => {
                    let (status, resolution) = match self.execute(sessions, handle, &entry).await {
                        Ok(outcome) => outcome,
                        Err(err) => (SessionCommandStatus::Rejected, Some(err.to_string())),
                    };
                    self.resolve_command(handle, &entry.id, status, resolution.as_deref());
                }
            }
        }
    }

    /// Host-only outcome write (ledger rule 2).
    fn resolve_command(
        &self,
        handle: &ChatDocHandle,
        command_id: &str,
        status: SessionCommandStatus,
        resolution: Option<&str>,
    ) {
        if let Err(err) = handle
            .doc
            .set_command_status(command_id, status, resolution)
        {
            tracing::warn!(
                chat = %handle.chat_id,
                command = %command_id,
                error = %err,
                "command outcome write failed"
            );
        }
    }

    async fn execute(
        &self,
        sessions: &SessionsEngine,
        handle: &Arc<ChatDocHandle>,
        entry: &SessionCommandEntry,
    ) -> Result<(SessionCommandStatus, Option<String>), EngineError> {
        let chat_id = &handle.chat_id;
        match &entry.payload {
            SessionCommandPayload::Run {
                request,
                message_id,
            } => {
                // Never start a second run under a live one: the harness owns
                // one runtime turn per session, and a racing dispatch wedges
                // the chat with "agent session has an uncommitted turn".
                if sessions.is_active(chat_id) {
                    return Ok((
                        SessionCommandStatus::Rejected,
                        Some(
                            "a run is already in progress for this chat; wait for it or interrupt it"
                                .into(),
                        ),
                    ));
                }
                // Claim-on-first-command: a run for a chat with no workspace row
                // creates the row under our device id (we are about to host it).
                if let Some(ws) = self.workspace() {
                    ws.claim_chat(chat_id, Some(&request.cwd))?;
                }
                let harness = self.harness_for(chat_id);
                if !request.document_refs.is_empty() {
                    eprintln!(
                        "Aurin Comet dispatch: harness={harness:?}, document refs={}",
                        request.document_refs.len()
                    );
                }
                let mut request = request.clone();
                request.context = context_turns(
                    &handle
                        .doc
                        .read_joined_entries_tail(MAX_CONTEXT_TURNS)
                        .unwrap_or_default(),
                );
                sessions
                    .dispatch(chat_id, harness, request, Some(message_id.clone()))
                    .await?;
                Ok((SessionCommandStatus::Applied, None))
            }
            SessionCommandPayload::Steer { prompt, message_id } => {
                match sessions.steer(chat_id, prompt, message_id.clone()).await? {
                    SteerOutcome::Accepted => Ok((SessionCommandStatus::Applied, None)),
                    SteerOutcome::NotSteerable => {
                        if sessions.is_active(chat_id) {
                            return Ok((
                                SessionCommandStatus::Rejected,
                                Some(
                                    "a run is already in progress and this harness cannot steer it"
                                        .into(),
                                ),
                            ));
                        }
                        // No live steerable run: the durable command still delivers —
                        // run it as the next turn (comet's fallback, executor-side).
                        // After an engine restart `last_request` is empty too, so
                        // rebuild the run config from the chat's workspace row
                        // (comet derived dispatch config from the chat row the
                        // same way — sessions.ts:601-620); dispatch's engine-owned
                        // resume then reattaches the prior harness conversation.
                        let request = sessions
                            .last_request(chat_id)
                            .or_else(|| self.request_from_chat_row(chat_id, prompt));
                        let Some(mut request) = request else {
                            return Ok((
                                SessionCommandStatus::Rejected,
                                Some("no live run and no prior run config".into()),
                            ));
                        };
                        request.prompt = prompt.clone();
                        request.resume = None; // dispatch re-derives the harness session
                        request.context = context_turns(
                            &handle
                                .doc
                                .read_joined_entries_tail(MAX_CONTEXT_TURNS)
                                .unwrap_or_default(),
                        );
                        // A reused config must not re-inline the PREVIOUS
                        // turn's images; this steer's own refs (if any) already
                        // ride the prompt text.
                        request.attachments = Vec::new();
                        sessions
                            .dispatch(
                                chat_id,
                                self.harness_for(chat_id),
                                request,
                                message_id.clone(),
                            )
                            .await?;
                        Ok((
                            SessionCommandStatus::Applied,
                            Some("queued as new turn".into()),
                        ))
                    }
                }
            }
            SessionCommandPayload::Interrupt {} => {
                sessions.interrupt(chat_id).await?;
                Ok((SessionCommandStatus::Applied, None))
            }
            SessionCommandPayload::RespondInput {
                request_id,
                answers,
            } => {
                eprintln!(
                    "[DocHost] RespondInput: chat_id={}, request_id={}",
                    chat_id, request_id
                );
                if sessions.respond_input(chat_id, request_id, answers.clone())? {
                    eprintln!("[DocHost] RespondInput: handled by live session");
                    return Ok((SessionCommandStatus::Applied, None));
                }
                eprintln!("[DocHost] RespondInput: no live session, checking for orphan fallback");
                // No live resolver. Only a request id the doc shows as an
                // OPEN question on a SETTLED entry gets the orphan fallback:
                // a mismatched or already-resolved id is a stale/buggy answer
                // and must still reject, and a still-streaming entry's
                // question belongs to the live run (a just-consumed resolver
                // racing a second answer must not spawn a duplicate turn).
                let questions = handle.doc.read_entries().ok().and_then(|entries| {
                    entries
                        .iter()
                        .rev()
                        .filter(|e| e.status != Some(MessageStatus::Streaming))
                        .find_map(|e| {
                            e.parts.iter().find_map(|p| match p {
                                MessagePart::Input {
                                    request_id: rid,
                                    questions,
                                    resolved: false,
                                    ..
                                } if rid == request_id => Some(questions.clone()),
                                _ => None,
                            })
                        })
                });
                let Some(questions) = questions else {
                    return Ok((
                        SessionCommandStatus::Rejected,
                        Some("no pending input request".into()),
                    ));
                };
                // The request id's resolver is gone, but a run is still live:
                // the answer already landed (or was consumed by another device)
                // and a fallback turn would collide with the live harness turn.
                if sessions.is_active(chat_id) {
                    return Ok((
                        SessionCommandStatus::Rejected,
                        Some("no pending input request for the live run".into()),
                    ));
                }
                // The run died under the question (engine restart, crash).
                // The question is still open in the doc and the command is
                // durable, so honor it anyway — stamp the part resolved and
                // deliver the answers as the next (resumed) turn, the same
                // fallback a dead-run steer takes. The question UI stays up
                // until the user answers (user requirement); this is what
                // makes that answer still WORK.
                let request = sessions
                    .last_request(chat_id)
                    .or_else(|| self.request_from_chat_row(chat_id, ""));
                let Some(mut request) = request else {
                    return Ok((
                        SessionCommandStatus::Rejected,
                        Some("no pending input request and no prior run config".into()),
                    ));
                };
                request.prompt = respond_input_prompt(&questions, answers);
                request.resume = None; // dispatch re-derives the harness session
                request.attachments = Vec::new();
                request.context = context_turns(
                    &handle
                        .doc
                        .read_joined_entries_tail(MAX_CONTEXT_TURNS)
                        .unwrap_or_default(),
                );
                if let Err(err) = handle.doc.resolve_input(request_id) {
                    tracing::warn!(chat = %chat_id, request = %request_id, error = %err,
                        "orphaned input resolve failed");
                }
                sessions
                    .dispatch(chat_id, self.harness_for(chat_id), request, None)
                    .await?;
                Ok((
                    SessionCommandStatus::Applied,
                    Some("answered as new turn".into()),
                ))
            }
        }
    }

    /// A steer-turned-run with no in-process `last_request` (engine restarted
    /// since the last turn): rebuild the run config from the chat's workspace
    /// row — cwd from the row, model/reasoning/options/sandbox from its config
    /// (composer defaults otherwise). `None` without a workspace host or row.
    // (Also the RespondInput dead-run fallback's config source.)
    pub(crate) fn request_from_chat_row(
        &self,
        chat_id: &str,
        prompt: &str,
    ) -> Option<comet_proto::RunRequest> {
        let workspace = self.workspace()?;
        let chat = match workspace.doc().chat(chat_id) {
            Ok(chat) => chat?,
            Err(err) => {
                tracing::warn!(chat = %chat_id, error = %err, "workspace chat read failed");
                return None;
            }
        };
        let config = chat.config;
        Some(comet_proto::RunRequest {
            prompt: prompt.to_string(),
            model: config.as_ref().and_then(|c| c.model.clone()),
            reasoning: config.as_ref().and_then(|c| c.reasoning),
            model_options: config
                .as_ref()
                .map(|c| c.model_options.clone())
                .unwrap_or_default(),
            context: Vec::new(),
            cwd: chat.cwd.unwrap_or_default(),
            sandbox: config
                .as_ref()
                .map(|c| c.sandbox)
                .unwrap_or(comet_proto::SandboxLevel::WorkspaceWrite),
            auto_approve: false,
            attachments: Vec::new(),
            document_refs: Vec::new(),
            resume: None,
        })
    }

    fn save_snapshot(&self, handle: &ChatDocHandle) {
        // Clear before exporting. A concurrent commit sets the bit again and
        // remains dirty for the next debounce instead of being lost behind a
        // post-save clear.
        handle.dirty.store(false, Ordering::Release);
        match handle.doc.export_snapshot() {
            Ok(bytes) => {
                if let Err(err) = self.inner.store.save_snapshot(&handle.chat_id, &bytes) {
                    handle.dirty.store(true, Ordering::Release);
                    tracing::warn!(chat = %handle.chat_id, error = %err, "snapshot save failed");
                }
            }
            Err(err) => {
                handle.dirty.store(true, Ordering::Release);
                tracing::warn!(chat = %handle.chat_id, error = %err, "snapshot export failed");
            }
        }
    }

    /// Persist every open doc now (shutdown path; bypasses the debounce).
    pub fn flush_all(&self) {
        let handles = self.live_handles();
        for handle in handles {
            self.save_snapshot(&handle);
        }
    }

    /// Upgrade currently live handles and prune dead weak entries. The engine
    /// must not keep a strong reference merely to service a later flush: a
    /// dropped chat view is the ownership boundary for its in-memory document.
    fn live_handles(&self) -> Vec<Arc<ChatDocHandle>> {
        let mut handles = lock(&self.inner.handles);
        let live = handles
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        handles.retain(|_, handle| handle.strong_count() != 0);
        live
    }
}

/// The resumed-turn prompt for answers to a question whose run died: each
/// answer paired with its question text so the reattached conversation reads
/// naturally. Pure.
pub fn respond_input_prompt(
    questions: &[UserInputQuestion],
    answers: &[UserInputAnswer],
) -> String {
    let mut lines = vec!["Answering your earlier question:".to_string()];
    for answer in answers {
        let picked = answer.labels.join(", ");
        let question = questions
            .iter()
            .find(|q| q.id == answer.question_id)
            .map(|q| q.question.trim())
            .filter(|q| !q.is_empty());
        match question {
            Some(question) => lines.push(format!("{question} — {picked}")),
            None => lines.push(picked),
        }
    }
    lines.join("\n")
}

// The harness request is rebuilt from the durable transcript at dispatch time.
// Keep this boundary bounded even when a caller supplied a smaller context:
// otherwise every retry would materialize the whole chat and then clone it
// again into the provider-specific request body.
const MAX_CONTEXT_TURNS: usize = 128;
const MAX_CONTEXT_TURN_BYTES: usize = 1024 * 1024;
const MAX_CONTEXT_BYTES: usize = 16 * 1024 * 1024;

fn bounded_prefix(text: &str, limit: usize) -> &str {
    if text.len() <= limit {
        return text;
    }
    let mut end = limit;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Build conversation history for a real AI backend from the chat doc: each
/// completed message becomes a `(role, text)` turn. This is what lets the
/// OpenAI-compatible harness see prior turns when a run is queued through the
/// durable command path (Aurin settings supply the provider/model).
fn context_turns(entries: &[SessionMessageEntry]) -> Vec<ContextTurn> {
    let mut selected = Vec::new();
    let mut total_bytes = 0usize;
    for entry in entries.iter().rev() {
        if selected.len() >= MAX_CONTEXT_TURNS {
            break;
        }
        let role = match entry.role {
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::System => "system",
        };
        let mut text = String::new();
        for part in &entry.parts {
            let MessagePart::Text {
                text: part_text, ..
            } = part
            else {
                continue;
            };
            if !text.is_empty() {
                if text.len() >= MAX_CONTEXT_TURN_BYTES {
                    break;
                }
                text.push('\n');
            }
            let remaining = MAX_CONTEXT_TURN_BYTES.saturating_sub(text.len());
            text.push_str(bounded_prefix(part_text, remaining));
        }
        if text.trim().is_empty() {
            continue;
        }
        // Account for the role/content framing as well as the text. This is
        // intentionally conservative; the OpenAI harness applies the same
        // budget after serialization.
        let value_bytes = text.len().saturating_add(role.len()).saturating_add(32);
        if total_bytes.saturating_add(value_bytes) > MAX_CONTEXT_BYTES {
            break;
        }
        total_bytes = total_bytes.saturating_add(value_bytes);
        selected.push(ContextTurn {
            role: role.to_owned(),
            content: text,
        });
    }
    selected.reverse();
    selected
}

#[cfg(test)]
mod context_tests {
    use super::*;
    use comet_doc::{MessagePart, MessageStatus};

    fn entry(index: usize, text: String) -> SessionMessageEntry {
        SessionMessageEntry {
            id: format!("m-{index}"),
            role: MessageRole::User,
            parts: vec![MessagePart::Text {
                id: "text".into(),
                text,
            }],
            created_at: index as i64,
            device_id: "test".into(),
            status: Some(MessageStatus::Complete),
            continuation_of: None,
        }
    }

    #[test]
    fn context_turns_keep_recent_history_within_budget() {
        let entries = (0..(MAX_CONTEXT_TURNS + 20))
            .map(|index| entry(index, format!("turn-{index}")))
            .collect::<Vec<_>>();
        let turns = context_turns(&entries);
        assert_eq!(turns.len(), MAX_CONTEXT_TURNS);
        assert_eq!(turns.first().unwrap().content, "turn-20");
        assert_eq!(turns.last().unwrap().content, "turn-147");
    }

    #[test]
    fn context_turns_truncate_one_large_entry() {
        let entries = vec![entry(0, "字".repeat(MAX_CONTEXT_TURN_BYTES + 8))];
        let turns = context_turns(&entries);
        assert_eq!(turns.len(), 1);
        assert!(turns[0].content.len() <= MAX_CONTEXT_TURN_BYTES);
        assert!(turns[0].content.is_char_boundary(turns[0].content.len()));
    }
}

/// Per-chat background task: reacts to doc changes (local commits and remote imports)
/// by re-publishing the transcript watch, draining commands, and debouncing snapshots.
/// It normally holds only a weak handle; a dirty doc gets one bounded strong
/// lease through its debounce so closing the view cannot discard unsaved data.
async fn chat_task(
    host: DocHost,
    weak: Weak<ChatDocHandle>,
    mut changed_rx: watch::Receiver<DocRevisions>,
) {
    // Initial pass: the snapshot may already carry pending commands.
    // The initial messages snapshot was read during open, after the
    // subscription was installed. Start at revision zero so a mutation racing
    // task startup is still published and persisted; adopting the receiver's
    // latest value here would silently mark that change as already handled.
    let mut observed = DocRevisions::default();
    {
        let Some(handle) = weak.upgrade() else { return };
        host.drain_commands(&handle).await;
    }
    let mut save_deadline: Option<tokio::time::Instant> = None;
    let mut transcript_deadline: Option<tokio::time::Instant> = None;
    // A dirty document must survive through its debounce window even when the
    // UI closes the RPC stream immediately after editing. This is the only
    // background strong owner; it is released as soon as the snapshot lands.
    let mut pending_save: Option<Arc<ChatDocHandle>> = None;
    loop {
        let sleep_until = match (transcript_deadline, save_deadline) {
            (Some(transcript), Some(save)) => transcript.min(save),
            (Some(transcript), None) => transcript,
            (None, Some(save)) => save,
            (None, None) => tokio::time::Instant::now(),
        };
        tokio::select! {
            changed = changed_rx.changed() => {
                if changed.is_err() {
                    break; // doc handle (and its change sender) is gone
                }
                let Some(handle) = weak.upgrade() else { break };
                let revisions = *changed_rx.borrow_and_update();
                if revisions.messages != observed.messages {
                    schedule_transcript_publish(&mut transcript_deadline, tokio::time::Instant::now());
                }
                if revisions.commands != observed.commands {
                    host.drain_commands(&handle).await;
                }
                observed = revisions;
                pending_save = Some(handle.clone());
                if save_deadline.is_none() {
                    save_deadline = Some(
                        tokio::time::Instant::now()
                            + std::time::Duration::from_millis(SNAPSHOT_DEBOUNCE_MS),
                    );
                }
            }
            _ = tokio::time::sleep_until(sleep_until), if transcript_deadline.is_some() || save_deadline.is_some() => {
                let now = tokio::time::Instant::now();
                if transcript_deadline.is_some_and(|deadline| deadline <= now) {
                    transcript_deadline = None;
                    if let Some(handle) = weak.upgrade() {
                        handle.publish_messages();
                    }
                }
                if save_deadline.is_some_and(|deadline| deadline <= now) {
                    save_deadline = None;
                    if let Some(handle) = pending_save.take() {
                        host.save_snapshot(&handle);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use comet_doc::{MessageRole, SessionCommandPayload};
    use comet_sync::DocsStore;

    #[test]
    fn transcript_publish_deadline_coalesces_a_burst_without_starvation() {
        let now = tokio::time::Instant::now();
        let mut deadline = None;

        schedule_transcript_publish(&mut deadline, now);
        let first = deadline.unwrap();
        schedule_transcript_publish(&mut deadline, now + std::time::Duration::from_millis(5));

        assert_eq!(deadline, Some(first));
        assert_eq!(
            first.duration_since(now),
            std::time::Duration::from_millis(TRANSCRIPT_PUBLISH_DEBOUNCE_MS)
        );
    }

    #[tokio::test]
    async fn transcript_publish_burst_keeps_every_committed_message() {
        let dir = tempfile::tempdir().unwrap();
        let host = DocHost::new(
            Arc::new(DocsStore::open(dir.path()).unwrap()),
            DocHostConfig {
                device_id: "device".into(),
                default_harness: HarnessId::Mock,
            },
        );
        let handle = host.open("chat").unwrap();
        let mut messages = handle.watch_messages();

        for index in 0..8 {
            handle
                .write_user_message(&format!("m{index}"), &format!("body {index}"), index)
                .unwrap();
        }

        tokio::time::timeout(std::time::Duration::from_secs(1), messages.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(messages.borrow_and_update().len(), 8);
    }

    #[tokio::test]
    async fn handle_registry_does_not_retain_dropped_chat_documents() {
        let dir = tempfile::tempdir().unwrap();
        let host = DocHost::new(
            Arc::new(DocsStore::open(dir.path()).unwrap()),
            DocHostConfig {
                device_id: "device".into(),
                default_harness: HarnessId::Mock,
            },
        );

        let first = host.open("chat").unwrap();
        assert_eq!(host.live_handles().len(), 1);
        drop(first);

        // The registry contains only a Weak and the per-chat task also holds a
        // Weak, so after the caller releases its owner the document can be
        // collected without waiting for a global engine shutdown.
        for _ in 0..16 {
            tokio::task::yield_now().await;
            if host.live_handles().is_empty() {
                break;
            }
        }
        assert!(host.live_handles().is_empty());

        let second = host.open("chat").unwrap();
        assert_eq!(host.live_handles().len(), 1);
        drop(second);
    }

    #[tokio::test]
    async fn memory_diagnostics_follow_handle_watch_and_snapshot_lifetimes() {
        let dir = tempfile::tempdir().unwrap();
        let host = DocHost::new(
            Arc::new(DocsStore::open(dir.path()).unwrap()),
            DocHostConfig {
                device_id: "device".into(),
                default_harness: HarnessId::Mock,
            },
        );
        let handle = host.open("chat").unwrap();
        let messages = handle.watch_messages();
        let initial = host.memory_diagnostics();
        assert_eq!(initial.registry_slots, 1);
        assert_eq!(initial.open_doc_handles, 1);
        assert_eq!(initial.transcript_watchers, 1);
        assert_eq!(initial.transcript_entries, 0);

        handle
            .write_user_message("m1", "diagnose me", now_ms())
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if host.memory_diagnostics().transcript_entries == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(host.memory_diagnostics().dirty_doc_handles, 1);

        drop(messages);
        assert_eq!(host.memory_diagnostics().transcript_watchers, 0);
        drop(handle);
        // A dirty handle is leased only until its debounced snapshot lands.
        tokio::time::sleep(std::time::Duration::from_millis(SNAPSHOT_DEBOUNCE_MS + 100)).await;
        assert_eq!(
            host.memory_diagnostics(),
            DocHostMemoryDiagnostics::default()
        );
    }

    #[tokio::test]
    async fn dirty_handle_survives_only_until_debounced_snapshot_is_saved() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(DocsStore::open(dir.path()).unwrap());
        let host = DocHost::new(
            store.clone(),
            DocHostConfig {
                device_id: "device".into(),
                default_harness: HarnessId::Mock,
            },
        );
        let handle = host.open("chat").unwrap();
        let mut messages = handle.watch_messages();
        handle
            .write_user_message("m1", "persist me", now_ms())
            .unwrap();
        // Also covers a mutation racing task startup: it must reach the
        // transcript watch rather than being adopted as an already-seen rev.
        tokio::time::timeout(std::time::Duration::from_secs(1), messages.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(messages.borrow().len(), 1);
        drop(handle);

        // The chat task temporarily owns the dirty handle, then releases it
        // immediately after the debounce save.
        assert_eq!(host.live_handles().len(), 1);
        tokio::time::sleep(std::time::Duration::from_millis(SNAPSHOT_DEBOUNCE_MS + 100)).await;
        assert!(store.load_snapshot("chat").unwrap().is_some());
        assert!(host.live_handles().is_empty());

        // Dropping even earlier, before the task can claim that lease, uses
        // the handle's final dirty-save fallback instead of losing the edit.
        let immediate = host.open("immediate").unwrap();
        immediate
            .write_user_message("m2", "persist immediately", now_ms())
            .unwrap();
        drop(immediate);
        assert!(store.load_snapshot("immediate").unwrap().is_some());
        assert!(host.live_handles().is_empty());
    }

    #[test]
    fn loro_diff_classification_separates_messages_from_commands() {
        let doc = SessionDoc::init("chat").unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_from_sub = seen.clone();
        let _sub = doc.doc().subscribe_root(Arc::new(move |event| {
            lock(&seen_from_sub).push((
                diff_touches_root(&event, "messages"),
                diff_touches_root(&event, "commands"),
            ));
        }));

        doc.push_message(&SessionMessageEntry {
            id: "m1".into(),
            role: MessageRole::User,
            parts: vec![MessagePart::Text {
                id: "t0".into(),
                text: "hello".into(),
            }],
            created_at: 1,
            device_id: "device".into(),
            status: Some(MessageStatus::Complete),
            continuation_of: None,
        })
        .unwrap();
        assert!(
            lock(&seen)
                .iter()
                .any(|&(messages, commands)| messages && !commands)
        );

        lock(&seen).clear();
        doc.queue_command(&SessionCommandEntry {
            id: "c1".into(),
            payload: SessionCommandPayload::Interrupt {},
            issued_by: "device".into(),
            issued_at: 2,
            based_on: None,
            expires_at: None,
            status: SessionCommandStatus::Pending,
            resolution: None,
        })
        .unwrap();
        assert!(
            lock(&seen)
                .iter()
                .any(|&(messages, commands)| !messages && commands)
        );
    }
}
