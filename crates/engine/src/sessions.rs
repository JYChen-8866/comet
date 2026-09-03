//! SessionsEngine — per-chat agent runs: dispatch, steering, interrupts, input bridging,
//! journal + broadcast fan-out, and 120ms coalesced doc streaming.
//!
//! Pragmatic port of comet's `sessions.ts` (spec: feature-inventory §3.2):
//! - every `AgentEvent` is (a) appended to the on-disk run journal, (b) broadcast to
//!   in-process subscribers, (c) folded via `fold_event_into_parts` and diffed into the
//!   chat's `SessionDoc` through `SegmentWriter` on a coalesced `STREAM_COMMIT_MS` timer;
//! - the user message entry is pushed to the doc immediately on dispatch (id = the
//!   command's client-minted message id, so optimistic echoes never flicker);
//! - a `Steered` event splits the assistant entry at the exact boundary;
//! - recovery (interrupt or a stale journal at boot) stamps the streaming entry `aborted`.
//!
//! Scope notes: sessions are keyed by chat id (one live run per chat). Comet's pulse
//! loop is ported as the 15s liveness heartbeat in `drive_run`; its stall watchdog is
//! deliberately NOT ported (rejected in review — agents may legitimately wait on
//! something for far longer than any timeout, and a live child IS the working signal).
//! Every dying path must instead carry its own visible error (child crash with stderr,
//! spawn failure, stream error, engine-restart recovery).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use chrono::Utc;
use futures::StreamExt;
use tokio::sync::{broadcast, mpsc, oneshot, watch};

use comet_doc::{
    DocError, MessagePart, MessageRole, MessageStatus, STREAM_COMMIT_MS, SegmentWriter, SessionDoc,
    fold_event_into_parts_in_place, sanitize_tool_call,
};
use comet_harness::{CancellationToken, Harness, RunControls, SteerMessage};
use comet_proto::{
    AgentEvent, DoneStatus, HarnessId, RunRequest, Session, SessionStatus, UserInputAnswer,
    UserInputQuestion,
};

use crate::doc_host::{ChatDocHandle, DocHost};
use crate::registry::HarnessRegistry;
use crate::run_journal::RunJournal;
use crate::{EngineError, new_id, now_ms};

/// One journaled event: the durable seq plus the event, as broadcast to subscribers.
#[derive(Debug, Clone)]
pub struct JournaledEvent {
    pub seq: u64,
    pub event: AgentEvent,
}

/// Outcome of a steer attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SteerOutcome {
    /// Delivered into the live run's steering mailbox.
    Accepted,
    /// No live steerable run — the caller should dispatch the prompt as a new turn.
    NotSteerable,
}

type PendingInputs = Arc<Mutex<HashMap<String, oneshot::Sender<Vec<UserInputAnswer>>>>>;

/// The input bridge is fed by harness protocol tasks and drained by the single
/// run loop.  It must apply backpressure in memory even if journal/doc writes
/// temporarily stall that loop.  Real harnesses ask one question set at a
/// time; these limits leave ample headroom for concurrent tool requests while
/// turning a malformed or hostile protocol stream into a bounded failure.
const INPUT_EVENT_BUFFER: usize = 64;
const MAX_PENDING_INPUTS_PER_RUN: usize = 32;
const MAX_INPUT_QUESTIONS: usize = 32;
const MAX_INPUT_OPTIONS_PER_QUESTION: usize = 128;
const MAX_INPUT_FIELD_BYTES: usize = 64 * 1024;
const MAX_INPUT_REQUEST_BYTES: usize = 256 * 1024;

fn input_questions_within_budget(questions: &[UserInputQuestion]) -> bool {
    if questions.len() > MAX_INPUT_QUESTIONS {
        return false;
    }
    let mut total = 0usize;
    for question in questions {
        if question.options.len() > MAX_INPUT_OPTIONS_PER_QUESTION {
            return false;
        }
        for value in std::iter::once(question.id.as_str())
            .chain(std::iter::once(question.header.as_str()))
            .chain(std::iter::once(question.question.as_str()))
            .chain(question.options.iter().map(String::as_str))
        {
            if value.len() > MAX_INPUT_FIELD_BYTES {
                return false;
            }
            total = total.saturating_add(value.len());
            if total > MAX_INPUT_REQUEST_BYTES {
                return false;
            }
        }
    }
    true
}

/// Register one harness question and enqueue its visible event atomically from
/// the caller's perspective.  If either the resolver table or event queue is
/// full, the sender is removed/dropped before returning; awaiting the returned
/// receiver then fails immediately instead of parking a harness task forever.
fn enqueue_input_request(
    pending: &PendingInputs,
    engine_tx: &mpsc::Sender<AgentEvent>,
    questions: Vec<UserInputQuestion>,
) -> oneshot::Receiver<Vec<UserInputAnswer>> {
    let (resolver, receiver) = oneshot::channel();
    if !input_questions_within_budget(&questions) {
        tracing::warn!(
            questions = questions.len(),
            "rejecting harness input request outside memory budget"
        );
        return receiver;
    }

    let request_id = new_id();
    {
        let mut pending = lock(pending);
        if pending.len() >= MAX_PENDING_INPUTS_PER_RUN {
            tracing::warn!(
                pending = pending.len(),
                limit = MAX_PENDING_INPUTS_PER_RUN,
                "rejecting harness input request: pending resolver limit reached"
            );
            return receiver;
        }
        pending.insert(request_id.clone(), resolver);
    }

    if let Err(error) = engine_tx.try_send(AgentEvent::InputRequested {
        request_id: request_id.clone(),
        questions,
    }) {
        lock(pending).remove(&request_id);
        tracing::warn!(
            request = %request_id,
            error = %error,
            "rejecting harness input request: run event bridge is saturated"
        );
    }
    receiver
}

/// A harness-native session id plus the cwd it was created under. Harness
/// session stores are cwd-scoped (claude keys conversations by project
/// directory — comet sessions.ts:563 "harness session stores are keyed by
/// cwd"), so resume is only injected for runs launched from the same cwd.
#[derive(Debug, Clone)]
struct HarnessSessionRef {
    session_id: String,
    cwd: String,
}

#[derive(Debug, Clone)]
struct HarnessSessionEntry {
    session: HarnessSessionRef,
    last_used: u64,
}

#[derive(Debug, Default)]
struct HarnessSessionCache {
    entries: HashMap<String, HarnessSessionEntry>,
    clock: u64,
}

#[derive(Debug, Clone)]
struct StatusEntry {
    session: Session,
    last_used: u64,
}

#[derive(Debug, Default)]
struct StatusCache {
    entries: HashMap<String, StatusEntry>,
    clock: u64,
}

// Session rows are a live UI cache; the durable workspace rows remain the
// source of truth for remote/history views. Keep a generous hot set, but do
// not let a long-lived process retain one `Session` forever for every chat.
const MAX_STATUS_ENTRIES: usize = 256;

impl StatusCache {
    fn get(&self, chat_id: &str) -> Option<Session> {
        self.entries.get(chat_id).map(|entry| entry.session.clone())
    }

    fn any_active(&self) -> bool {
        self.entries.values().any(|entry| {
            matches!(
                entry.session.status,
                comet_proto::SessionStatus::Working | comet_proto::SessionStatus::AwaitingInput
            )
        })
    }

    fn is_active(&self, chat_id: &str) -> bool {
        self.entries.get(chat_id).is_some_and(|entry| {
            matches!(
                entry.session.status,
                comet_proto::SessionStatus::Working | comet_proto::SessionStatus::AwaitingInput
            )
        })
    }

    fn snapshot(&self) -> Vec<Session> {
        self.entries
            .values()
            .map(|entry| entry.session.clone())
            .collect()
    }

    fn set_status(
        &mut self,
        chat_id: &str,
        device_id: &str,
        status: SessionStatus,
        fresh_start: bool,
    ) -> Session {
        self.clock = self.clock.saturating_add(1);
        let now = Utc::now();
        let entry = self
            .entries
            .entry(chat_id.to_owned())
            .or_insert_with(|| StatusEntry {
                session: Session {
                    chat_id: chat_id.to_owned(),
                    device_id: device_id.to_owned(),
                    status,
                    started_at: None,
                    updated_at: now,
                },
                last_used: self.clock,
            });
        entry.session.status = status;
        entry.session.updated_at = now;
        if fresh_start {
            entry.session.started_at = Some(now);
        }
        entry.last_used = self.clock;
        let session = entry.session.clone();
        self.trim();
        session
    }

    fn touch(&mut self, chat_id: &str) -> Option<Session> {
        self.clock = self.clock.saturating_add(1);
        let entry = self.entries.get_mut(chat_id)?;
        entry.last_used = self.clock;
        Some(entry.session.clone())
    }

    fn trim(&mut self) {
        while self.entries.len() > MAX_STATUS_ENTRIES {
            let Some(oldest) = self
                .entries
                .iter()
                .filter(|(_, entry)| {
                    matches!(
                        entry.session.status,
                        comet_proto::SessionStatus::Idle | comet_proto::SessionStatus::Errored
                    )
                })
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(chat_id, _)| chat_id.clone())
            else {
                // All remaining rows are active. Never evict a live/awaiting
                // session merely to satisfy the soft cache bound.
                break;
            };
            self.entries.remove(&oldest);
        }
    }
}

const MAX_HARNESS_SESSIONS: usize = 256;

impl HarnessSessionCache {
    fn remember(&mut self, chat_id: &str, session: HarnessSessionRef) {
        self.clock = self.clock.saturating_add(1);
        self.entries.insert(
            chat_id.to_owned(),
            HarnessSessionEntry {
                session,
                last_used: self.clock,
            },
        );
        self.trim();
    }

    fn forget(&mut self, chat_id: &str) {
        self.remember(
            chat_id,
            HarnessSessionRef {
                session_id: String::new(),
                cwd: String::new(),
            },
        );
    }

    fn get(&mut self, chat_id: &str) -> Option<HarnessSessionRef> {
        let entry = self.entries.get_mut(chat_id)?;
        self.clock = self.clock.saturating_add(1);
        entry.last_used = self.clock;
        Some(entry.session.clone())
    }

    fn trim(&mut self) {
        while self.entries.len() > MAX_HARNESS_SESSIONS {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(chat_id, _)| chat_id.clone())
            else {
                break;
            };
            self.entries.remove(&oldest);
        }
    }
}

struct RunHandle {
    run_id: String,
    steerable: bool,
    steer_tx: mpsc::Sender<SteerMessage>,
    /// Harness-level cancellation (protocol interrupt + child teardown).
    interrupt_token: CancellationToken,
    /// Engine-level cancel: arms the run task's grace deadline so a harness that
    /// ignores its token can never strand the run.
    cancel: watch::Sender<bool>,
    engine_tx: mpsc::Sender<AgentEvent>,
    pending_inputs: PendingInputs,
}

/// The fallback path only needs the stable run configuration. Keeping the
/// complete `RunRequest` here retained the prompt, context transcript,
/// attachment paths, and document references for every chat ever used. Those
/// fields are rebuilt from the durable document at dispatch time, so retaining
/// them is both redundant and an avoidable memory multiplier.
#[derive(Debug, Clone)]
struct LastRequestConfig {
    model: Option<String>,
    reasoning: Option<comet_proto::ReasoningLevel>,
    model_options: serde_json::Map<String, serde_json::Value>,
    cwd: String,
    sandbox: comet_proto::SandboxLevel,
    auto_approve: bool,
}

#[derive(Debug, Clone)]
struct LastRequestEntry {
    config: LastRequestConfig,
    last_used: u64,
}

#[derive(Debug, Default)]
struct LastRequestCache {
    entries: HashMap<String, LastRequestEntry>,
    clock: u64,
}

const MAX_LAST_REQUESTS: usize = 128;
const MAX_LAST_REQUEST_STRING_BYTES: usize = 8 * 1024;
const MAX_LAST_REQUEST_OPTIONS_BYTES: usize = 64 * 1024;

impl LastRequestCache {
    fn insert(&mut self, chat_id: &str, request: &RunRequest) {
        self.clock = self.clock.saturating_add(1);
        let config = LastRequestConfig {
            model: bounded_optional_string(request.model.as_deref()),
            reasoning: request.reasoning,
            model_options: bounded_model_options(&request.model_options),
            cwd: bounded_string(&request.cwd),
            sandbox: request.sandbox,
            auto_approve: request.auto_approve,
        };
        self.entries.insert(
            chat_id.to_owned(),
            LastRequestEntry {
                config,
                last_used: self.clock,
            },
        );
        while self.entries.len() > MAX_LAST_REQUESTS {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(chat_id, _)| chat_id.clone())
            else {
                break;
            };
            self.entries.remove(&oldest);
        }
    }

    fn get(&mut self, chat_id: &str) -> Option<RunRequest> {
        let entry = self.entries.get_mut(chat_id)?;
        self.clock = self.clock.saturating_add(1);
        entry.last_used = self.clock;
        Some(RunRequest {
            prompt: String::new(),
            model: entry.config.model.clone(),
            reasoning: entry.config.reasoning,
            model_options: entry.config.model_options.clone(),
            cwd: entry.config.cwd.clone(),
            sandbox: entry.config.sandbox,
            auto_approve: entry.config.auto_approve,
            resume: None,
            attachments: Vec::new(),
            document_refs: Vec::new(),
            context: Vec::new(),
        })
    }
}

fn bounded_string(value: &str) -> String {
    if value.len() <= MAX_LAST_REQUEST_STRING_BYTES {
        return value.to_owned();
    }
    let mut end = MAX_LAST_REQUEST_STRING_BYTES;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

fn bounded_optional_string(value: Option<&str>) -> Option<String> {
    value.map(bounded_string)
}

fn bounded_model_options(
    options: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    // Model options are normally a handful of short scalar values. If a
    // provider supplies an unexpectedly large object, dropping the optional
    // cache copy is safer than retaining an unbounded tree; the active request
    // still receives the original options.
    let Ok(encoded) = serde_json::to_vec(options) else {
        return serde_json::Map::new();
    };
    if encoded.len() > MAX_LAST_REQUEST_OPTIONS_BYTES {
        return serde_json::Map::new();
    }
    options.clone()
}

struct Inner {
    device_id: String,
    journal: Arc<RunJournal>,
    registry: Arc<HarnessRegistry>,
    doc_host: OnceLock<DocHost>,
    /// chat_id → live run.
    runs: Mutex<HashMap<String, RunHandle>>,
    /// chat_id → broadcast hub (retained across runs so subscribers survive turns).
    hubs: Mutex<HashMap<String, broadcast::Sender<JournaledEvent>>>,
    statuses: Mutex<StatusCache>,
    sessions_tx: watch::Sender<Vec<Session>>,
    /// Compact, bounded fallback config per chat. Prompt/context/attachments are
    /// intentionally excluded; they are rebuilt from the durable doc.
    last_requests: Mutex<LastRequestCache>,
    /// Harness-native session ids per chat (resume continuity across turns).
    /// This is a bounded live-process cache over the durable copy on the
    /// workspace chat row (comet kept the same pair on
    /// `chats.harness_session_id`). An empty session id is the "do not resume"
    /// tombstone after a rejected resume; the durable row remains authoritative
    /// when an old cache entry is evicted.
    harness_sessions: Mutex<HarnessSessionCache>,
    /// Auto-titler for untitled chats (wired at engine assembly; absent in bare tests).
    titles: OnceLock<crate::titles::TitleGenerator>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone)]
pub struct SessionsEngine {
    inner: Arc<Inner>,
}

impl SessionsEngine {
    pub fn new(
        device_id: String,
        journal: Arc<RunJournal>,
        registry: Arc<HarnessRegistry>,
    ) -> Self {
        let (sessions_tx, _) = watch::channel(Vec::new());
        Self {
            inner: Arc::new(Inner {
                device_id,
                journal,
                registry,
                doc_host: OnceLock::new(),
                runs: Mutex::new(HashMap::new()),
                hubs: Mutex::new(HashMap::new()),
                statuses: Mutex::new(StatusCache::default()),
                sessions_tx,
                last_requests: Mutex::new(LastRequestCache::default()),
                harness_sessions: Mutex::new(HarnessSessionCache::default()),
                titles: OnceLock::new(),
            }),
        }
    }

    /// Wire the doc host (called once at engine assembly; the two services are mutually
    /// referential by design — sessions stream into docs, docs execute commands here).
    pub fn set_doc_host(&self, host: DocHost) {
        let _ = self.inner.doc_host.set(host);
    }

    /// Wire the chat auto-titler (called once at engine assembly). After each
    /// completed exchange the run task fires it for still-untitled chats.
    pub fn set_titles(&self, titles: crate::titles::TitleGenerator) {
        let _ = self.inner.titles.set(titles);
    }

    fn doc_handle(&self, chat_id: &str) -> Result<Arc<ChatDocHandle>, EngineError> {
        let host =
            self.inner.doc_host.get().ok_or_else(|| {
                EngineError::Other("doc host not wired into sessions engine".into())
            })?;
        host.open(chat_id)
    }

    /// Status watch: the full session list, re-sent on every transition.
    pub fn watch_sessions(&self) -> watch::Receiver<Vec<Session>> {
        self.inner.sessions_tx.subscribe()
    }

    pub fn session_status(&self, chat_id: &str) -> Option<Session> {
        lock(&self.inner.statuses).get(chat_id)
    }

    /// Any run currently working or blocked on input — the auto-updater's
    /// "don't restart from under a session" gate.
    pub fn any_active(&self) -> bool {
        lock(&self.inner.statuses).any_active()
    }

    /// Whether this chat has a run actively working or blocked on input.
    /// Dispatch guards use this to refuse starting a second run for a chat
    /// that is still busy — a duplicate run would collide with the live run's
    /// harness state and wedge the session.
    pub fn is_active(&self, chat_id: &str) -> bool {
        lock(&self.inner.statuses).is_active(chat_id)
    }

    /// The last request dispatched for a chat (steer→new-turn fallback).
    pub fn last_request(&self, chat_id: &str) -> Option<RunRequest> {
        lock(&self.inner.last_requests).get(chat_id)
    }

    /// Subscribe to a chat's live event stream: returns the journal replay after
    /// `after_seq` plus a live receiver. Subscribe-then-replay ordering means overlap
    /// (dedupe by seq) rather than gaps.
    pub fn subscribe(
        &self,
        chat_id: &str,
        after_seq: u64,
    ) -> Result<(Vec<JournaledEvent>, broadcast::Receiver<JournaledEvent>), EngineError> {
        self.prune_hubs();
        let rx = {
            let mut hubs = lock(&self.inner.hubs);
            hubs.entry(chat_id.to_string())
                .or_insert_with(|| broadcast::channel(1024).0)
                .subscribe()
        };
        let replay = self
            .inner
            .journal
            .replay(chat_id, after_seq)?
            .into_iter()
            .map(|(seq, event)| JournaledEvent { seq, event })
            .collect();
        Ok((replay, rx))
    }

    /// Start (or route) a run for `chat_id`.
    ///
    /// - The user message entry is written to the doc immediately (id = `message_id`).
    /// - A live steerable run receives the prompt as its next turn via the mailbox
    ///   (comet's persistent-session routing); otherwise any live run is interrupted
    ///   first — never two runtimes driving one chat.
    pub async fn dispatch(
        &self,
        chat_id: &str,
        harness_id: HarnessId,
        request: RunRequest,
        message_id: Option<String>,
    ) -> Result<String, EngineError> {
        self.dispatch_with(chat_id, harness_id, request, message_id, true)
            .await
    }

    /// [`Self::dispatch`] with resume injection controllable: the failed-resume
    /// retry re-dispatches with `inject_resume = false` so a session id the
    /// harness just rejected can never be re-injected from the journal.
    /// Boxed future: `drive_run` re-enters this for that retry, and the
    /// erasure breaks the opaque-type cycle the recursion would otherwise form.
    fn dispatch_with<'a>(
        &'a self,
        chat_id: &'a str,
        harness_id: HarnessId,
        request: RunRequest,
        message_id: Option<String>,
        inject_resume: bool,
    ) -> futures::future::BoxFuture<'a, Result<String, EngineError>> {
        Box::pin(self.dispatch_inner(chat_id, harness_id, request, message_id, inject_resume))
    }

    async fn dispatch_inner(
        &self,
        chat_id: &str,
        harness_id: HarnessId,
        mut request: RunRequest,
        message_id: Option<String>,
        inject_resume: bool,
    ) -> Result<String, EngineError> {
        let routed = lock(&self.inner.runs)
            .get(chat_id)
            .map(|h| (h.run_id.clone(), h.steerable, h.steer_tx.clone()));
        if let Some((run_id, steerable, steer_tx)) = routed {
            let message = SteerMessage {
                prompt: request.prompt.clone(),
                message_id: message_id.clone(),
            };
            if steerable && steer_tx.try_send(message).is_ok() {
                let user_id = message_id.unwrap_or_else(new_id);
                let handle = self.doc_handle(chat_id)?;
                handle.write_user_message(&user_id, &request.prompt, now_ms())?;
                // Working BEFORE the lastMessageAt bump: both ride the
                // workspace doc from this one peer, so causal order makes it
                // impossible for an observer to hold [new message, old status]
                // — that gap read as unseen-with-no-live-run = a phantom
                // "completed" flash on every remote send (2026-07-31).
                self.set_status(chat_id, SessionStatus::Working, false);
                self.inner.note_message(chat_id, &request.prompt);
                return Ok(run_id);
            }
            // Mailbox closed (runtime mid-teardown / non-steering harness): replace it.
            self.interrupt(chat_id).await?;
        }

        let harness = self.inner.registry.resolve(harness_id)?;
        let handle = self.doc_handle(chat_id)?;
        // A document `@`-ed once stays resident for the whole chat: fold every
        // mention from the transcript into this turn before the new message is
        // written (the current prompt's refs are already on the request).
        if let Err(err) =
            crate::mentions::merge_resident_document_refs_from_doc(&mut request, handle.doc())
        {
            tracing::warn!(chat = %chat_id, error = %err, "resident document scan failed");
        }
        let user_id = message_id.unwrap_or_else(new_id);
        handle.write_user_message(&user_id, &request.prompt, now_ms())?;

        // Engine-owned resume (comet sessions.ts:736 — every dispatch read the
        // chat's stored harness session): callers always send `resume: None`;
        // the engine threads the chat's prior harness session back in so a new
        // process (app restart) continues the same harness conversation.
        let mut resume_injected = false;
        if request.resume.is_none() && inject_resume {
            request.resume = self.inner.resume_for(chat_id, &request.cwd);
            resume_injected = request.resume.is_some();
        }
        lock(&self.inner.last_requests).insert(chat_id, &request);

        let run_id = new_id();
        let (steer_tx, steer_rx) = mpsc::channel::<SteerMessage>(32);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (engine_tx, engine_rx) = mpsc::channel::<AgentEvent>(INPUT_EVENT_BUFFER);
        let pending_inputs: PendingInputs = Arc::new(Mutex::new(HashMap::new()));

        // Input bridge: the harness asks questions; we mint the request id, park the
        // resolver for `respond_input`, and surface the event through the run pipeline.
        let request_input = {
            let pending = pending_inputs.clone();
            let engine_tx = engine_tx.clone();
            Box::new(move |questions: Vec<UserInputQuestion>| {
                enqueue_input_request(&pending, &engine_tx, questions)
            })
        };
        let interrupt_token = CancellationToken::new();
        let controls = RunControls {
            request_input,
            steering: steer_rx,
            interrupt: interrupt_token.clone(),
        };

        lock(&self.inner.runs).insert(
            chat_id.to_string(),
            RunHandle {
                run_id: run_id.clone(),
                steerable: harness.supports_steering(),
                steer_tx,
                interrupt_token,
                cancel: cancel_tx,
                engine_tx,
                pending_inputs,
            },
        );
        self.set_status(chat_id, SessionStatus::Working, true);
        // AFTER Working (same causal-order guarantee as the steer path): the
        // lastMessageAt bump must never be observable ahead of the live run.
        self.inner.note_message(chat_id, &request.prompt);

        // Name the chat NOW, off the first prompt — not after the first
        // exchange completes ("called New session for a long time for no
        // reason"; the titler only needs the prompt and skips titled chats;
        // the Done-time call below stays as the retry for a failed
        // generation).
        if let Some(titles) = self.inner.titles.get() {
            titles.maybe_generate(chat_id, harness_id, &request.prompt, &request.cwd);
        }

        tokio::spawn(drive_run(
            self.inner.clone(),
            chat_id.to_string(),
            run_id.clone(),
            harness,
            request,
            // Keep the host handle alive for the duration of the run. The
            // handle owns the live transcript watch and change subscription;
            // retaining only `SessionDoc` would let a Weak DocHost registry
            // create a second snapshot-backed handle while the run streams.
            // This strong reference is released as soon as `drive_run`
            // settles, so inactive chats are still reclaimable.
            handle.clone(),
            controls,
            engine_rx,
            cancel_rx,
            RunResumeState {
                user_message_id: user_id,
                resume_injected,
            },
        ));
        Ok(run_id)
    }

    /// Push a steer prompt into the live run's mailbox. `NotSteerable` when no live
    /// steerable run exists — the caller (command executor) dispatches a new turn.
    pub async fn steer(
        &self,
        chat_id: &str,
        prompt: &str,
        message_id: Option<String>,
    ) -> Result<SteerOutcome, EngineError> {
        let target = lock(&self.inner.runs)
            .get(chat_id)
            .filter(|h| h.steerable)
            .map(|h| h.steer_tx.clone());
        let Some(steer_tx) = target else {
            return Ok(SteerOutcome::NotSteerable);
        };
        let message = SteerMessage {
            prompt: prompt.to_string(),
            message_id: message_id.clone(),
        };
        if steer_tx.try_send(message).is_err() {
            return Ok(SteerOutcome::NotSteerable);
        }
        // Accepted: the steer prompt becomes a user entry immediately (client-minted id).
        let user_id = message_id.unwrap_or_else(new_id);
        let handle = self.doc_handle(chat_id)?;
        handle.write_user_message(&user_id, prompt, now_ms())?;
        self.inner.note_message(chat_id, prompt);
        Ok(SteerOutcome::Accepted)
    }

    /// Interrupt the live run, if any. The run settles with a synthetic
    /// `Done{interrupted}` and its streaming entry stamped `aborted`; this waits
    /// (bounded) for that settlement so callers observe a consistent doc.
    pub async fn interrupt(&self, chat_id: &str) -> Result<bool, EngineError> {
        let target = lock(&self.inner.runs).get(chat_id).map(|h| {
            (
                h.run_id.clone(),
                h.interrupt_token.clone(),
                h.cancel.clone(),
                h.pending_inputs.clone(),
            )
        });
        let Some((run_id, token, cancel, pending)) = target else {
            return Ok(false);
        };
        // Unpark any blocked question FIRST (mirrors comet: harness teardown can await a
        // parked question callback — a run stuck on a question would deadlock the stop).
        let parked: Vec<_> = lock(&pending).drain().map(|(_, tx)| tx).collect();
        for tx in parked {
            let _ = tx.send(Vec::new());
        }
        // Harness-level interrupt (protocol + child teardown) …
        token.cancel();
        // … plus the engine-side grace deadline in the run task, so a harness that
        // ignores its token still settles with a synthesized Done{interrupted}.
        let _ = cancel.send(true);
        // Bounded settle wait (the run task appends Done + stamps `aborted`).
        for _ in 0..500 {
            if !self.is_live(chat_id, &run_id) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok(true)
    }

    /// Resolve a pending `request_input` question set. Returns `false` when no such
    /// request is pending (unknown id, or the run already settled).
    pub fn respond_input(
        &self,
        chat_id: &str,
        request_id: &str,
        answers: Vec<UserInputAnswer>,
    ) -> Result<bool, EngineError> {
        eprintln!(
            "[Sessions] respond_input: chat_id={}, request_id={}, answers={:?}",
            chat_id, request_id, answers
        );
        let target = lock(&self.inner.runs)
            .get(chat_id)
            .map(|h| (h.pending_inputs.clone(), h.engine_tx.clone()));
        let Some((pending, engine_tx)) = target else {
            eprintln!(
                "[Sessions] respond_input: no active run for chat_id={}",
                chat_id
            );
            return Ok(false);
        };
        // Reserve the bounded bridge slot before consuming the resolver. A
        // saturated bridge leaves the question pending so the command can be
        // retried instead of delivering an answer that the transcript can no
        // longer mark resolved.
        let permit = engine_tx.try_reserve().map_err(|error| {
            EngineError::Other(format!("run input event bridge is busy: {error}"))
        })?;
        let Some(resolver) = lock(&pending).remove(request_id) else {
            eprintln!(
                "[Sessions] respond_input: no pending input for request_id={}",
                request_id
            );
            return Ok(false);
        };
        eprintln!("[Sessions] respond_input: sending answer to resolver");
        let _ = resolver.send(answers);
        permit.send(AgentEvent::InputResolved {
            request_id: request_id.to_string(),
        });
        eprintln!("[Sessions] respond_input: success");
        Ok(true)
    }

    /// Boot recovery: for every journal whose last event is not `Done` (a run died
    /// mid-stream), stamp this device's abandoned `streaming` doc entries `aborted`
    /// with a VISIBLE "Run interrupted by engine restart" error part, close the
    /// journal with a synthetic `Done{interrupted}` — and then PICK THE RUN BACK
    /// UP: a fresh crashed turn with revival budget left is re-dispatched against
    /// the remembered harness session (comet: "not just eulogized";
    /// `MAX_AUTO_RESUME` = 3 consecutive revivals, fresh = crashed < 12h ago).
    pub fn recover_stale(&self) -> Result<usize, EngineError> {
        const MAX_AUTO_RESUME: u32 = 3;
        const RESUME_FRESH_MS: i64 = 12 * 60 * 60 * 1000;

        let stale = self.inner.journal.stale_sessions()?;
        let mut recovered = 0usize;
        for chat_id in stale {
            if lock(&self.inner.runs).contains_key(&chat_id) {
                continue; // a live run owns this journal
            }
            let handle = self.doc_handle(&chat_id)?;
            // Harness continuity first: the crashed run's session id may only
            // exist in the journal (the debounced workspace-row write may
            // never have landed) — remember it so the revived run resumes the
            // same harness conversation (comet recoverDraft, sessions.ts:538).
            if let Some((session_id, cwd)) = self.inner.journal_harness_session(&chat_id) {
                self.inner
                    .remember_harness_session(&chat_id, &session_id, &cwd);
            }
            // The revival prompt: the last user message (idempotent re-dispatch
            // under the SAME id — `write_user_message` dedupes by id, so the
            // transcript never shows a duplicate).
            let prompt = handle.doc().read_entries().ok().and_then(|entries| {
                entries
                    .iter()
                    .rev()
                    .find(|e| e.role == MessageRole::User)
                    .and_then(|e| {
                        e.parts.iter().find_map(|p| match p {
                            MessagePart::Text { text, .. } => Some((e.id.clone(), text.clone())),
                            _ => None,
                        })
                    })
            });
            let attempts = self.inner.journal.resume_attempts(&chat_id);
            let fresh = handle
                .doc()
                .read_entries()
                .ok()
                .and_then(|entries| {
                    entries
                        .iter()
                        .rev()
                        .find(|e| e.status == Some(MessageStatus::Streaming))
                        .map(|e| now_ms() - e.created_at < RESUME_FRESH_MS)
                })
                .unwrap_or(false);
            let will_resume = fresh && prompt.is_some() && attempts < MAX_AUTO_RESUME;

            let note = if will_resume {
                "Run interrupted by engine restart — resuming"
            } else {
                "Run interrupted by engine restart"
            };
            let done = AgentEvent::Done {
                status: DoneStatus::Interrupted,
                result: None,
                error: Some(note.into()),
                session_id: None,
            };
            self.inner.publish(&chat_id, &done);
            let stamped = handle.mark_abandoned_streams(note)?.len();
            self.set_status(&chat_id, SessionStatus::Idle, false);
            tracing::info!(chat = %chat_id, stamped, will_resume, attempts, "recovered stale session journal");
            recovered += 1;

            if !will_resume {
                continue;
            }
            let attempt = self.inner.journal.note_resume_attempt(&chat_id);
            let (user_id, prompt_text) = prompt.expect("gated by will_resume");
            let sessions = self.clone();
            tokio::spawn(async move {
                let Some(host) = sessions.inner.doc_host.get().cloned() else {
                    return;
                };
                let request = sessions
                    .last_request(&chat_id)
                    .or_else(|| host.request_from_chat_row(&chat_id, &prompt_text))
                    // Last resort: the journal's own cwd (comet's draft config)
                    // — a crash can predate the debounced workspace-row write.
                    .or_else(|| {
                        let (_, cwd) = sessions.inner.journal_harness_session(&chat_id)?;
                        Some(RunRequest {
                            prompt: String::new(),
                            model: None,
                            reasoning: None,
                            model_options: Default::default(),
                            cwd,
                            sandbox: comet_proto::SandboxLevel::WorkspaceWrite,
                            auto_approve: false,
                            attachments: Vec::new(),
                            document_refs: Vec::new(),
                            resume: None,
                            context: Vec::new(),
                        })
                    });
                let Some(mut request) = request else {
                    tracing::warn!(chat = %chat_id, "auto-resume skipped: no run config");
                    return;
                };
                request.prompt = prompt_text;
                request.resume = None; // dispatch re-injects the remembered session
                request.attachments = Vec::new();
                // Rebuild the resident registry on the last-resort fallback
                // too; the primary `last_request` path already carries the
                // merged refs.
                if let Ok(handle) = sessions.doc_handle(&chat_id)
                    && let Err(err) = crate::mentions::merge_resident_document_refs_from_doc(
                        &mut request,
                        handle.doc(),
                    )
                {
                    tracing::warn!(
                        chat = %chat_id,
                        error = %err,
                        "resident document scan failed during auto-resume"
                    );
                }
                let harness_id = host.harness_for(&chat_id);
                match sessions
                    .dispatch(&chat_id, harness_id, request, Some(user_id))
                    .await
                {
                    Ok(_) => {
                        tracing::info!(chat = %chat_id, attempt, "auto-resumed crashed run")
                    }
                    Err(err) => {
                        tracing::warn!(chat = %chat_id, error = %err, "auto-resume dispatch failed")
                    }
                }
            });
        }
        Ok(recovered)
    }

    /// Graceful shutdown: interrupt every live run so streaming entries settle.
    pub async fn shutdown(&self) {
        let chats: Vec<String> = lock(&self.inner.runs).keys().cloned().collect();
        for chat_id in chats {
            if let Err(err) = self.interrupt(&chat_id).await {
                tracing::warn!(chat = %chat_id, error = %err, "shutdown interrupt failed");
            }
        }
    }

    fn is_live(&self, chat_id: &str, run_id: &str) -> bool {
        lock(&self.inner.runs)
            .get(chat_id)
            .is_some_and(|h| h.run_id == run_id)
    }

    fn set_status(&self, chat_id: &str, status: SessionStatus, fresh_start: bool) {
        self.inner.set_status(chat_id, status, fresh_start);
    }

    fn prune_hubs(&self) {
        self.inner.prune_hubs();
    }
}

impl Inner {
    /// Broadcast senders are useful only while a client is subscribed or a run
    /// is active. Once both are gone, dropping the sender releases its channel
    /// ring buffer (up to 1024 journaled events) and its chat-id key.
    fn prune_hubs(&self) {
        let active = lock(&self.runs)
            .keys()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let mut hubs = lock(&self.hubs);
        prune_hub_map(&mut hubs, &active);
    }

    /// Journal + broadcast one event (the two unconditional legs of the pipeline).
    fn publish(&self, chat_id: &str, event: &AgentEvent) -> u64 {
        let seq = match self.journal.append(chat_id, event) {
            Ok(seq) => seq,
            Err(err) => {
                tracing::error!(chat = %chat_id, error = %err, "journal append failed");
                0
            }
        };
        {
            let hubs = lock(&self.hubs);
            if let Some(hub) = hubs.get(chat_id) {
                let _ = hub.send(JournaledEvent {
                    seq,
                    event: event.clone(),
                });
            }
        }
        self.prune_hubs();
        seq
    }

    /// Bump the session's freshness on stream activity WITHOUT a status
    /// transition. Long silent-LOOKING stretches (thinking heartbeats, a big
    /// tool input being generated) still carry events — the UI's 45s
    /// staleness gate must not flip "Working" off mid-run. Throttled: a
    /// workspace-doc mirror per delta would be far too chatty.
    fn touch_session(&self, chat_id: &str) {
        const TOUCH_THROTTLE_MS: i64 = 10_000;
        let now = Utc::now();
        let session = {
            let mut statuses = lock(&self.statuses);
            let Some(current) = statuses.get(chat_id) else {
                return;
            };
            let age = now
                .signed_duration_since(current.updated_at)
                .num_milliseconds();
            if age < TOUCH_THROTTLE_MS {
                return;
            }
            // Update the timestamp in-place while preserving the cache's
            // recency accounting. The cache can evict only idle rows.
            if let Some(entry) = statuses.entries.get_mut(chat_id) {
                entry.session.updated_at = now;
            }
            let session = statuses.touch(chat_id).expect("status exists");
            let mut list = statuses.snapshot();
            list.sort_by(|a, b| a.chat_id.cmp(&b.chat_id));
            self.sessions_tx.send_replace(list);
            session
        };
        if let Some(ws) = self.workspace() {
            ws.record_session(&session);
        }
    }

    fn set_status(&self, chat_id: &str, status: SessionStatus, fresh_start: bool) {
        let session = {
            let mut statuses = lock(&self.statuses);
            let session = statuses.set_status(chat_id, &self.device_id, status, fresh_start);
            let mut list = statuses.snapshot();
            list.sort_by(|a, b| a.chat_id.cmp(&b.chat_id));
            // send_replace: keep the current value fresh even with no receivers,
            // so late WatchSessions subscribers see the last transition.
            self.sessions_tx.send_replace(list);
            session
        };
        // Mirror the transition into the workspace doc's session-status row so
        // remote devices' sidebars show this run (staleness-checked client-side).
        if let Some(ws) = self.workspace() {
            ws.record_session(&session);
        }
    }

    fn workspace(&self) -> Option<&crate::workspace_host::WorkspaceHost> {
        self.doc_host.get().and_then(|host| host.workspace())
    }

    /// Sidebar freshness: push a message-persist preview into the chat's workspace row.
    fn note_message(&self, chat_id: &str, text: &str) {
        if text.is_empty() {
            return;
        }
        if let Some(ws) = self.workspace() {
            ws.note_message(chat_id, text);
        }
    }

    /// Record the chat's harness-native session id (and its cwd): live-process
    /// cache plus the durable workspace chat row — the row is what survives an
    /// engine restart (comet sessions.ts:1039).
    fn remember_harness_session(&self, chat_id: &str, session_id: &str, cwd: &str) {
        if session_id.is_empty() {
            return;
        }
        lock(&self.harness_sessions).remember(
            chat_id,
            HarnessSessionRef {
                session_id: bounded_string(session_id),
                cwd: bounded_string(cwd),
            },
        );
        if let Some(ws) = self.workspace() {
            ws.set_chat_harness_session(chat_id, session_id, cwd);
        }
    }

    /// A harness rejected the stored session id: tombstone it (empty string on
    /// the row, cleared cache) so no lookup source — including the journal,
    /// which still names the dead id — can re-inject it.
    fn forget_harness_session(&self, chat_id: &str) {
        lock(&self.harness_sessions).forget(chat_id);
        if let Some(ws) = self.workspace() {
            ws.set_chat_harness_session(chat_id, "", "");
        }
    }

    /// The session id to resume for a run in `chat_id` launching from `cwd`
    /// (comet sessions.ts:736, looked up on every dispatch):
    /// live-process cache → workspace chat row → journal scan (the crash path
    /// where the debounced row write never landed — SessionStarted/Done events
    /// are journaled per event, flushed immediately). Cwd-gated throughout:
    /// harness session stores are keyed by cwd, so a session created elsewhere
    /// never rides `--resume`. An empty stored id is the explicit tombstone —
    /// no resume, no falling through to staler sources.
    fn resume_for(&self, chat_id: &str, cwd: &str) -> Option<String> {
        let cwd_ok = |session_cwd: &str| session_cwd.is_empty() || session_cwd == cwd;
        if let Some(known) = lock(&self.harness_sessions).get(chat_id) {
            return (!known.session_id.is_empty() && cwd_ok(&known.cwd))
                .then_some(known.session_id);
        }
        if let Some(ws) = self.workspace()
            && let Some((session_id, session_cwd)) = ws.chat_harness_session(chat_id)
        {
            return (!session_id.is_empty() && cwd_ok(session_cwd.as_deref().unwrap_or("")))
                .then_some(session_id);
        }
        let (session_id, session_cwd) = self.journal_harness_session(chat_id)?;
        // Cache the journal hit (memory + row) so later dispatches skip the scan.
        self.remember_harness_session(chat_id, &session_id, &session_cwd);
        cwd_ok(&session_cwd).then_some(session_id)
    }

    /// The last harness session id named anywhere in the chat's journal, with
    /// the cwd of the `SessionStarted` that governs it. `Done.session_id`
    /// inherits the cwd of the most recent `SessionStarted` (same run).
    fn journal_harness_session(&self, chat_id: &str) -> Option<(String, String)> {
        let mut current_cwd = String::new();
        let mut found: Option<(String, String)> = None;
        if let Err(err) = self.journal.scan(chat_id, |_, event| {
            match event {
                AgentEvent::SessionStarted {
                    session_id, cwd, ..
                } => {
                    current_cwd = cwd;
                    if !session_id.is_empty() {
                        found = Some((session_id, current_cwd.clone()));
                    }
                }
                AgentEvent::Done {
                    session_id: Some(session_id),
                    ..
                } if !session_id.is_empty() => {
                    found = Some((session_id, current_cwd.clone()));
                }
                _ => {}
            }
            Ok(())
        }) {
            tracing::warn!(chat = %chat_id, error = %err, "journal scan for harness session failed");
            return None;
        }
        found
    }

    fn remove_run(&self, chat_id: &str, run_id: &str) {
        let mut runs = lock(&self.runs);
        if runs.get(chat_id).is_some_and(|h| h.run_id == run_id) {
            runs.remove(chat_id);
        }
        drop(runs);
        self.prune_hubs();
    }
}

fn prune_hub_map(
    hubs: &mut HashMap<String, broadcast::Sender<JournaledEvent>>,
    active: &std::collections::HashSet<String>,
) {
    hubs.retain(|chat_id, sender| active.contains(chat_id) || sender.receiver_count() != 0);
}

// ── run task ────────────────────────────────────────────────────────────────

/// Fold the render-safe event directly into the live doc accumulator. Full
/// tool inputs have already been journaled/published by the caller; keeping
/// them in `folded` until the next 120ms commit retained large write/edit
/// payloads and forced a full parts clone on every commit.
fn fold_render_event(parts: &mut Vec<MessagePart>, event: &AgentEvent) {
    match event {
        AgentEvent::ToolCall { id, call } => {
            let render_event = AgentEvent::ToolCall {
                id: id.clone(),
                call: sanitize_tool_call(call),
            };
            fold_event_into_parts_in_place(parts, &render_event);
        }
        _ => fold_event_into_parts_in_place(parts, event),
    }
}

/// The persisted assistant text of a folded segment (workspace preview source).
fn folded_text(parts: &[MessagePart]) -> String {
    parts
        .iter()
        .filter_map(|part| match part {
            MessagePart::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn sync_segment<'a>(
    doc: &'a SessionDoc,
    writer: &mut Option<SegmentWriter<'a>>,
    entry_id: &str,
    device_id: &str,
    started_at: i64,
    folded: &[MessagePart],
) -> Result<(), DocError> {
    if folded.is_empty() {
        return Ok(());
    }
    if writer.is_none() {
        *writer = Some(SegmentWriter::begin(doc, entry_id, device_id, started_at)?);
    }
    if let Some(w) = writer.as_mut() {
        w.sync(folded)?;
    }
    Ok(())
}

fn finish_segment<'a>(
    doc: &'a SessionDoc,
    writer: Option<SegmentWriter<'a>>,
    entry_id: &str,
    device_id: &str,
    started_at: i64,
    folded: &[MessagePart],
    status: MessageStatus,
) -> Result<(), DocError> {
    match writer {
        Some(w) => w.finish(folded, status),
        None if !folded.is_empty() => {
            SegmentWriter::begin(doc, entry_id, device_id, started_at)?.finish(folded, status)
        }
        None => Ok(()),
    }
}

/// Resume bookkeeping for one run task: which user entry the run answers (so a
/// failed-resume retry re-dispatches idempotently against the same doc entry)
/// and whether `dispatch` injected the resume id itself (only engine-injected
/// resumes are retried fresh — a caller-specified resume fails loudly).
struct RunResumeState {
    user_message_id: String,
    resume_injected: bool,
}

#[allow(clippy::too_many_arguments)]
async fn drive_run(
    inner: Arc<Inner>,
    chat_id: String,
    run_id: String,
    harness: Arc<dyn Harness>,
    request: RunRequest,
    handle: Arc<ChatDocHandle>,
    controls: RunControls,
    mut engine_rx: mpsc::Receiver<AgentEvent>,
    mut cancel_rx: watch::Receiver<bool>,
    resume_state: RunResumeState,
) {
    // `handle` deliberately stays owned by this task. See the spawn site:
    // DocHost's registry is weak so this is the active-run ownership boundary.
    let doc = handle.doc_arc();
    let device_id = inner.device_id.clone();
    // Captured for post-run auto-titling (the request moves into the harness).
    let harness_id = harness.id();
    let user_prompt = request.prompt.clone();
    let run_cwd = request.cwd.clone();
    // Kept whole for the failed-resume retry (fresh session, same user entry).
    // Option so the retry branch (inside the event loop) can take ownership.
    let mut retry_request = Some(RunRequest {
        resume: None,
        ..request.clone()
    });
    let mut stream = match harness.run(request, controls).await {
        Ok(stream) => stream,
        Err(err) => {
            let message = err.to_string();
            inner.publish(
                &chat_id,
                &AgentEvent::Error {
                    message: message.clone(),
                },
            );
            inner.publish(
                &chat_id,
                &AgentEvent::Done {
                    status: DoneStatus::Errored,
                    result: None,
                    error: Some(message),
                    session_id: None,
                },
            );
            inner.remove_run(&chat_id, &run_id);
            inner.set_status(&chat_id, SessionStatus::Errored, false);
            return;
        }
    };

    let doc_ref: &SessionDoc = &doc;
    let mut folded: Vec<MessagePart> = Vec::new();
    let mut entry_id = new_id();
    let mut segment_started = now_ms();
    let mut writer: Option<SegmentWriter<'_>> = None;
    let mut dirty = false;
    let mut flush_at = tokio::time::Instant::now();
    // Set when the engine interrupts the run: the harness gets this long to end its own
    // stream (its token was cancelled); past it, a terminal Done is synthesized.
    let mut interrupt_deadline: Option<tokio::time::Instant> = None;
    let mut interrupted = false;
    let mut saw_session_started = false;
    // Liveness heartbeat: this loop RUNNING is proof the harness stream is
    // open, so freshness must not depend on events arriving. Silent stretches
    // are normal and UNBOUNDED — a long tool call, redacted thinking, an
    // agent waiting on an external process, a question parked for an hour —
    // and each starved the UI's 45s staleness gate in turn (working strip /
    // AwaitingInput dot vanishing mid-run, both user-reported). No stall
    // timeout here by design (a first port was rejected — agents may
    // legitimately be quiet for >10min): a live child means Working, dying
    // paths each carry their own error, and engine death stops these ticks
    // so the gate still catches real crashes. touch_session throttles at 10s.
    let mut live_heartbeat = tokio::time::interval(std::time::Duration::from_secs(15));
    live_heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // PERSISTENT SESSION (comet runsBySession): a completed turn on a
    // steerable harness parks here instead of ending the run — the child and
    // its steering mailbox stay warm, and the next user message (dispatch
    // routes into a live run) starts the next turn with zero respawn/resume
    // latency. `Some(when)` = idle since then; the 30-min reaper below ends
    // a session nobody comes back to (comet SESSION_IDLE_MS).
    const SESSION_IDLE: std::time::Duration = std::time::Duration::from_secs(30 * 60);
    let mut idle_since: Option<tokio::time::Instant> = None;
    let steerable = harness.supports_steering();

    let final_status = loop {
        let event: AgentEvent = tokio::select! {
            biased;
            changed = cancel_rx.changed(), if !interrupted => {
                let _ = changed;
                interrupted = true;
                interrupt_deadline = Some(
                    tokio::time::Instant::now() + std::time::Duration::from_secs(3),
                );
                continue;
            }
            _ = tokio::time::sleep_until(
                interrupt_deadline.unwrap_or_else(tokio::time::Instant::now)
            ), if interrupt_deadline.is_some() => AgentEvent::Done {
                status: DoneStatus::Interrupted,
                result: None,
                error: None,
                session_id: None,
            },
            _ = live_heartbeat.tick() => {
                inner.touch_session(&chat_id);
                continue;
            }
            // Idle reaper (comet SESSION_IDLE_MS): a parked persistent session
            // nobody returned to in 30 minutes releases its child. The turn
            // was finalized at Done, so this end is clean — no aborted stamp.
            _ = tokio::time::sleep_until(
                idle_since.map(|at| at + SESSION_IDLE).unwrap_or_else(tokio::time::Instant::now)
            ), if idle_since.is_some() => {
                tracing::info!(chat = %chat_id, "reaping idle persistent session");
                if let Some(token) = lock(&inner.runs)
                    .get(&chat_id)
                    .filter(|h| h.run_id == run_id)
                    .map(|h| h.interrupt_token.clone())
                {
                    token.cancel();
                }
                break SessionStatus::Idle;
            }
            Some(event) = engine_rx.recv() => event,
            next = stream.next() => match next {
                Some(Ok(event)) => event,
                Some(Err(err)) => AgentEvent::Done {
                    status: DoneStatus::Errored,
                    result: None,
                    error: Some(err.to_string()),
                    session_id: None,
                },
                None if interrupted => AgentEvent::Done {
                    status: DoneStatus::Interrupted,
                    result: None,
                    error: None,
                    session_id: None,
                },
                // Stream end while PARKED idle: a per-turn adapter closing
                // after its final Done — a clean end, not a crash (the turn
                // was already finalized). Persistent adapters keep the
                // stream open and never hit this.
                None if idle_since.is_some() => break SessionStatus::Idle,
                None => AgentEvent::Done {
                    status: DoneStatus::Errored,
                    result: None,
                    error: Some("harness stream ended without Done".into()),
                    session_id: None,
                },
            },
            _ = tokio::time::sleep_until(flush_at), if dirty => {
                // Coalesced STREAM_COMMIT_MS tick: one doc commit per window.
                if let Err(err) = sync_segment(
                    doc_ref, &mut writer, &entry_id, &device_id, segment_started, &folded,
                ) {
                    tracing::warn!(chat = %chat_id, error = %err, "segment sync failed");
                }
                dirty = false;
                continue;
            }
        };

        // Any stream activity proves the run is alive — keep the session's
        // freshness inside the UI's 45s staleness window (throttled).
        inner.touch_session(&chat_id);
        // First event after parking idle = the next turn beginning (a routed
        // dispatch steered in): the session is Working again.
        if idle_since.take().is_some() {
            inner.set_status(&chat_id, SessionStatus::Working, true);
        }
        // Empty reasoning deltas are PURE heartbeats: redacted thinking and
        // tool-input-generation windows stream them with no text. They fold
        // to nothing, so journaling/publishing them is only noise (hundreds
        // per long turn observed) — the touch above already did their job.
        if matches!(&event, AgentEvent::ReasoningDelta { text } if text.is_empty()) {
            continue;
        }

        // Failed-resume fallback: an engine-injected `--resume` naming a session
        // the harness no longer knows dies before ever starting (claude exits
        // without an init frame; codex falls back internally via thread/start).
        // Signature: errored Done, no SessionStarted, nothing streamed. Retry
        // ONCE as a fresh session against the same user entry — tombstone the
        // dead id first so no lookup source (journal included) re-injects it.
        if resume_state.resume_injected
            && !saw_session_started
            && folded.is_empty()
            && !interrupted
            && matches!(
                &event,
                AgentEvent::Done {
                    status: DoneStatus::Errored,
                    ..
                }
            )
            && let Some(retry) = retry_request.take()
        {
            tracing::warn!(
                chat = %chat_id,
                "harness rejected injected resume id; retrying as a fresh session"
            );
            inner.forget_harness_session(&chat_id);
            inner.remove_run(&chat_id, &run_id);
            let engine = SessionsEngine {
                inner: inner.clone(),
            };
            let chat = chat_id.clone();
            let message_id = resume_state.user_message_id.clone();
            tokio::spawn(async move {
                // `inject_resume = false`: the retry must start fresh. The user
                // entry write inside dispatch is idempotent by message id.
                if let Err(err) = engine
                    .dispatch_with(&chat, harness_id, retry, Some(message_id), false)
                    .await
                {
                    tracing::error!(chat = %chat, error = %err, "fresh-session retry dispatch failed");
                }
            });
            return;
        }

        // A steer boundary splits the assistant entry exactly where the fold resets.
        if let AgentEvent::Steered {
            next_assistant_message_id,
            ..
        } = &event
        {
            inner.publish(&chat_id, &event);
            if let Err(err) = finish_segment(
                doc_ref,
                writer.take(),
                &entry_id,
                &device_id,
                segment_started,
                &folded,
                MessageStatus::Complete,
            ) {
                tracing::warn!(chat = %chat_id, error = %err, "segment finish failed");
            }
            inner.note_message(&chat_id, &folded_text(&folded));
            folded.clear();
            dirty = false;
            entry_id = next_assistant_message_id.clone().unwrap_or_else(new_id);
            segment_started = now_ms();
            continue;
        }

        match &event {
            AgentEvent::SessionStarted {
                session_id, cwd, ..
            } => {
                saw_session_started = true;
                // The event's own cwd (where the harness actually created the
                // session) scopes the stored id, not the request's.
                inner.remember_harness_session(&chat_id, session_id, cwd);
            }
            AgentEvent::Done {
                session_id: Some(session_id),
                ..
            } => {
                inner.remember_harness_session(&chat_id, session_id, &run_cwd);
            }
            AgentEvent::InputRequested { request_id, .. } => {
                // The engine's input bridge is the sole authority on input
                // requests: it mints the id and parks the resolver BEFORE
                // emitting the event, so a legitimate id is always pending
                // here. A harness emitting its own copy (a different id no
                // resolver knows) would fold an unanswerable twin chip into
                // the doc — and answering the twin would never resume the
                // run. Drop such events.
                let pending = lock(&inner.runs)
                    .get(&chat_id)
                    .map(|h| h.pending_inputs.clone());
                let known = pending.is_some_and(|p| lock(&p).contains_key(request_id));
                if !known {
                    tracing::warn!(
                        chat = %chat_id,
                        request = %request_id,
                        "dropping harness-emitted InputRequested (unknown id; \
                         the engine input bridge owns this lifecycle)"
                    );
                    continue;
                }
                inner.set_status(&chat_id, SessionStatus::AwaitingInput, false);
            }
            AgentEvent::InputResolved { .. } => {
                inner.set_status(&chat_id, SessionStatus::Working, false);
            }
            _ => {}
        }

        inner.publish(&chat_id, &event);

        // Defensive rule from comet: a mid-run SessionStarted re-emission (Claude SDK
        // background re-invocations) must not wipe the segment being written.
        let skip_fold = matches!(&event, AgentEvent::SessionStarted { .. }) && !folded.is_empty();
        if !skip_fold {
            fold_render_event(&mut folded, &event);
        }

        if let AgentEvent::Done { status, .. } = &event {
            let message_status = match status {
                DoneStatus::Interrupted => MessageStatus::Aborted,
                DoneStatus::Completed | DoneStatus::Errored => MessageStatus::Complete,
            };
            // No dangling chips: a run that ends for ANY reason (completed,
            // errored, interrupted) terminally resolves its input parts — an
            // unresolved question must not outlive the run that asked it
            // (its resolver died with the run; an answer could never land).
            for part in folded.iter_mut() {
                if let MessagePart::Input { resolved, .. } = part {
                    *resolved = true;
                }
            }
            // A Done landing on a PARKED session with nothing streamed (the
            // idle reaper's or an interrupt's own teardown) has no entry to
            // finalize — writing one would leave an empty aborted stub.
            let nothing_streamed = writer.is_none() && folded.is_empty();
            if !nothing_streamed {
                if let Err(err) = finish_segment(
                    doc_ref,
                    writer.take(),
                    &entry_id,
                    &device_id,
                    segment_started,
                    &folded,
                    message_status,
                ) {
                    tracing::warn!(chat = %chat_id, error = %err, "final segment finish failed");
                }
                inner.note_message(&chat_id, &folded_text(&folded));
            }
            if *status == DoneStatus::Completed {
                // A cleanly completed turn resets the auto-resume revival
                // budget: only consecutive crash-revive-crash cycles spend it.
                inner.journal.clear_resume_attempts(&chat_id);
            }
            // Exchange completed on an untitled chat → name it (fire-and-forget;
            // interrupted/errored turns never trigger naming).
            if *status == DoneStatus::Completed
                && let Some(titles) = inner.titles.get()
            {
                titles.maybe_generate(&chat_id, harness_id, &user_prompt, &run_cwd);
            }
            // PERSISTENT SESSION: a cleanly completed turn on a steerable
            // harness PARKS instead of ending — child + mailbox stay warm for
            // the next routed dispatch; per-turn state resets for it.
            if *status == DoneStatus::Completed && steerable && !interrupted {
                folded.clear();
                dirty = false;
                entry_id = new_id();
                segment_started = now_ms();
                // Resume-retry is strictly a first-turn concern.
                saw_session_started = true;
                idle_since = Some(tokio::time::Instant::now());
                inner.set_status(&chat_id, SessionStatus::Idle, false);
                continue;
            }
            break match status {
                DoneStatus::Errored => SessionStatus::Errored,
                _ => SessionStatus::Idle,
            };
        }

        if !folded.is_empty() && !dirty {
            dirty = true;
            flush_at =
                tokio::time::Instant::now() + std::time::Duration::from_millis(STREAM_COMMIT_MS);
        }
    };

    inner.remove_run(&chat_id, &run_id);
    inner.set_status(&chat_id, final_status, false);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input_question(text: String) -> UserInputQuestion {
        UserInputQuestion {
            id: "question".into(),
            header: "Need input".into(),
            question: text,
            options: vec!["Yes".into(), "No".into()],
            multi_select: false,
        }
    }

    fn request() -> RunRequest {
        RunRequest {
            prompt: "prompt".into(),
            model: Some("model".into()),
            reasoning: Some(comet_proto::ReasoningLevel::Medium),
            model_options: serde_json::Map::new(),
            cwd: "/workspace".into(),
            sandbox: comet_proto::SandboxLevel::WorkspaceWrite,
            auto_approve: true,
            resume: Some("resume".into()),
            attachments: vec!["/tmp/large-image.png".into()],
            document_refs: vec![comet_proto::DocumentRef {
                node_id: "node".into(),
                content_id: "content".into(),
                title: "title".into(),
            }],
            context: vec![comet_proto::ContextTurn {
                role: "user".into(),
                content: "old context".into(),
            }],
        }
    }

    #[test]
    fn render_fold_never_retains_heavy_tool_payloads() {
        let mut parts = Vec::new();
        fold_render_event(
            &mut parts,
            &AgentEvent::ToolCall {
                id: "write".into(),
                call: comet_proto::ToolCall::WriteFile {
                    path: "/workspace/large.bin".into(),
                    content: Some("x".repeat(2 * 1024 * 1024)),
                },
            },
        );

        assert!(matches!(
            parts.as_slice(),
            [MessagePart::Tool {
                call: comet_proto::ToolCall::WriteFile { content: None, .. },
                ..
            }]
        ));
    }

    #[test]
    fn last_request_cache_keeps_only_fallback_configuration() {
        let mut cache = LastRequestCache::default();
        cache.insert("chat", &request());
        let compact = cache.get("chat").expect("cached config");

        assert!(compact.prompt.is_empty());
        assert!(compact.resume.is_none());
        assert!(compact.attachments.is_empty());
        assert!(compact.document_refs.is_empty());
        assert!(compact.context.is_empty());
        assert_eq!(compact.cwd, "/workspace");
        assert_eq!(compact.model.as_deref(), Some("model"));
        assert!(compact.auto_approve);
    }

    #[test]
    fn input_event_bridge_bounds_pending_resolvers() {
        let pending: PendingInputs = Arc::new(Mutex::new(HashMap::new()));
        let (events_tx, mut events_rx) = mpsc::channel(INPUT_EVENT_BUFFER);
        let mut receivers = Vec::new();

        for _ in 0..(MAX_PENDING_INPUTS_PER_RUN + 3) {
            receivers.push(enqueue_input_request(
                &pending,
                &events_tx,
                vec![input_question("Continue?".into())],
            ));
        }

        assert_eq!(lock(&pending).len(), MAX_PENDING_INPUTS_PER_RUN);
        let mut event_count = 0;
        while events_rx.try_recv().is_ok() {
            event_count += 1;
        }
        assert_eq!(event_count, MAX_PENDING_INPUTS_PER_RUN);
        for receiver in &mut receivers[..MAX_PENDING_INPUTS_PER_RUN] {
            assert_eq!(
                receiver.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            );
        }
        for receiver in &mut receivers[MAX_PENDING_INPUTS_PER_RUN..] {
            assert_eq!(
                receiver.try_recv(),
                Err(oneshot::error::TryRecvError::Closed)
            );
        }
    }

    #[test]
    fn input_event_bridge_rolls_back_resolver_when_queue_is_full() {
        let pending: PendingInputs = Arc::new(Mutex::new(HashMap::new()));
        let (events_tx, _events_rx) = mpsc::channel(1);
        events_tx
            .try_send(AgentEvent::Usage {
                input_tokens: 0,
                output_tokens: 0,
            })
            .unwrap();

        let mut receiver = enqueue_input_request(
            &pending,
            &events_tx,
            vec![input_question("Continue?".into())],
        );

        assert!(lock(&pending).is_empty());
        assert_eq!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        );
    }

    #[test]
    fn input_event_bridge_rejects_oversized_question_before_enqueue() {
        let pending: PendingInputs = Arc::new(Mutex::new(HashMap::new()));
        let (events_tx, mut events_rx) = mpsc::channel(INPUT_EVENT_BUFFER);
        let mut receiver = enqueue_input_request(
            &pending,
            &events_tx,
            vec![input_question("x".repeat(MAX_INPUT_FIELD_BYTES + 1))],
        );

        assert!(lock(&pending).is_empty());
        assert!(matches!(
            events_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        );
    }

    #[test]
    fn last_request_cache_is_bounded() {
        let mut cache = LastRequestCache::default();
        for index in 0..(MAX_LAST_REQUESTS + 7) {
            cache.insert(&format!("chat-{index}"), &request());
        }

        assert_eq!(cache.entries.len(), MAX_LAST_REQUESTS);
        assert!(!cache.entries.contains_key("chat-0"));
        assert!(
            cache
                .entries
                .contains_key(&format!("chat-{}", MAX_LAST_REQUESTS + 6))
        );
    }

    #[test]
    fn oversized_optional_strings_are_bounded() {
        let mut cache = LastRequestCache::default();
        let mut oversized = request();
        oversized.cwd = "中".repeat(MAX_LAST_REQUEST_STRING_BYTES);
        cache.insert("chat", &oversized);
        let compact = cache.get("chat").unwrap();
        assert!(compact.cwd.len() <= MAX_LAST_REQUEST_STRING_BYTES);
        assert!(compact.cwd.is_char_boundary(compact.cwd.len()));
    }

    #[test]
    fn harness_session_cache_is_bounded_and_refreshes_lru() {
        let mut cache = HarnessSessionCache::default();
        for index in 0..(MAX_HARNESS_SESSIONS + 3) {
            cache.remember(
                &format!("chat-{index}"),
                HarnessSessionRef {
                    session_id: format!("session-{index}"),
                    cwd: "/workspace".into(),
                },
            );
        }
        assert_eq!(cache.entries.len(), MAX_HARNESS_SESSIONS);
        assert!(cache.get("chat-0").is_none());
        assert_eq!(
            cache
                .get(&format!("chat-{}", MAX_HARNESS_SESSIONS + 2))
                .unwrap()
                .session_id,
            format!("session-{}", MAX_HARNESS_SESSIONS + 2)
        );
    }

    #[test]
    fn status_cache_never_evicts_active_sessions() {
        let mut cache = StatusCache::default();
        for index in 0..MAX_STATUS_ENTRIES {
            cache.set_status(
                &format!("idle-{index}"),
                "device",
                SessionStatus::Idle,
                false,
            );
        }
        cache.set_status("active", "device", SessionStatus::Working, true);
        cache.set_status("awaiting", "device", SessionStatus::AwaitingInput, false);

        assert!(cache.is_active("active"));
        assert!(cache.is_active("awaiting"));
        assert!(cache.entries.len() <= MAX_STATUS_ENTRIES + 2);

        for index in MAX_STATUS_ENTRIES..(MAX_STATUS_ENTRIES + 16) {
            cache.set_status(
                &format!("idle-{index}"),
                "device",
                SessionStatus::Idle,
                false,
            );
        }
        assert!(cache.is_active("active"));
        assert!(cache.is_active("awaiting"));
        assert!(cache.entries.len() <= MAX_STATUS_ENTRIES);
    }

    #[test]
    fn hub_pruning_keeps_subscribed_and_active_channels_only() {
        let (subscribed, _rx) = broadcast::channel(4);
        let (active, _active_rx) = broadcast::channel(4);
        let (idle, idle_rx) = broadcast::channel(4);
        drop(idle_rx);
        let mut hubs = HashMap::from([
            ("subscribed".into(), subscribed),
            ("active".into(), active),
            ("idle".into(), idle),
        ]);
        let active_ids = std::collections::HashSet::from(["active".to_string()]);

        prune_hub_map(&mut hubs, &active_ids);

        assert!(hubs.contains_key("subscribed"));
        assert!(hubs.contains_key("active"));
        assert!(!hubs.contains_key("idle"));
    }
}
