//! Client side: request/stream multiplexing over string frames + the WebSocket dialer.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};

use futures::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::{ClientFrame, RpcError, ServerFrame};

enum Pending {
    Call(oneshot::Sender<Result<serde_json::Value, RpcError>>),
    Stream(Arc<StreamBuffer>),
}

#[derive(Clone, Copy)]
enum StreamRetention {
    /// Preserve every item in protocol order. This is the default because an
    /// arbitrary RPC stream may be an event log whose entries cannot be lost.
    Ordered,
    /// Keep one replaceable state snapshot. Watch streams use this mode so a
    /// stalled UI cannot retain multiple complete transcripts or registries.
    Latest,
}

#[derive(Default)]
struct StreamBufferState {
    items: VecDeque<serde_json::Value>,
    closed: bool,
    receiver_alive: bool,
}

struct StreamBuffer {
    state: Mutex<StreamBufferState>,
    changed: tokio::sync::Notify,
    retention: StreamRetention,
}

impl StreamBuffer {
    fn new(retention: StreamRetention) -> Self {
        Self {
            state: Mutex::new(StreamBufferState {
                receiver_alive: true,
                ..StreamBufferState::default()
            }),
            changed: tokio::sync::Notify::new(),
            retention,
        }
    }

    /// Returns false after the receiver has been dropped. Latest-value streams
    /// discard their stale snapshot before installing the replacement.
    fn push(&self, item: serde_json::Value) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !state.receiver_alive || state.closed {
            return false;
        }
        if matches!(self.retention, StreamRetention::Latest) {
            state.items.clear();
        }
        state.items.push_back(item);
        drop(state);
        self.changed.notify_one();
        true
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.closed = true;
        drop(state);
        self.changed.notify_one();
    }

    fn drop_receiver(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.receiver_alive = false;
        state.items.clear();
        drop(state);
        self.changed.notify_one();
    }
}

/// Receiver returned by [`RpcClient::subscribe`] and
/// [`RpcClient::subscribe_latest`]. It has the same
/// `recv().await -> Option<Value>` surface as Tokio's channel receiver.
pub struct RpcStream {
    buffer: Arc<StreamBuffer>,
    id: u64,
    out: mpsc::Sender<String>,
    shared: Weak<Shared>,
}

impl RpcStream {
    pub async fn recv(&mut self) -> Option<serde_json::Value> {
        loop {
            // Register before checking state so a push between the check and
            // await cannot be missed.
            let changed = self.buffer.changed.notified();
            {
                let mut state = self
                    .buffer
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if let Some(item) = state.items.pop_front() {
                    return Some(item);
                }
                if state.closed {
                    return None;
                }
            }
            changed.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.buffer
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .closed
    }
}

impl Drop for RpcStream {
    fn drop(&mut self) {
        self.buffer.drop_receiver();
        if self.shared.upgrade().is_some_and(|shared| {
            matches!(shared.lock().remove(&self.id), Some(Pending::Stream(_)))
        }) {
            try_cancel(&self.out, self.id);
        }
    }
}

struct Shared {
    pending: Mutex<HashMap<u64, Pending>>,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Pending>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A multiplexing RPC client over any string-frame duplex ([`crate::memory_client`] or
/// [`connect_ws`]). Cheap to clone-by-Arc internally; use one per connection.
pub struct RpcClient {
    out: mpsc::Sender<String>,
    shared: Arc<Shared>,
    next_id: AtomicU64,
    reader: tokio::task::JoinHandle<()>,
}

impl RpcClient {
    /// Wrap an existing duplex: `out` carries client frames, `inbound` server frames.
    pub fn new(out: mpsc::Sender<String>, mut inbound: mpsc::Receiver<String>) -> Self {
        let shared = Arc::new(Shared {
            pending: Mutex::new(HashMap::new()),
        });
        let reader_shared = shared.clone();
        let reader_out = out.clone();
        let reader = tokio::spawn(async move {
            while let Some(payload) = inbound.recv().await {
                for line in payload.lines() {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    let frame: ServerFrame = match serde_json::from_str(line) {
                        Ok(frame) => frame,
                        Err(err) => {
                            tracing::warn!(error = %err, "rpc: dropping malformed server frame");
                            continue;
                        }
                    };
                    route_frame(&reader_shared, &reader_out, frame);
                }
            }
            // Connection closed: fail everything still pending.
            let drained: Vec<Pending> = {
                let mut pending = reader_shared.lock();
                pending.drain().map(|(_, p)| p).collect()
            };
            for entry in drained {
                match entry {
                    Pending::Call(tx) => {
                        let _ = tx.send(Err(RpcError::Closed));
                    }
                    Pending::Stream(stream) => stream.close(),
                }
            }
        });
        Self {
            out,
            shared,
            next_id: AtomicU64::new(1),
            reader,
        }
    }

    /// Unary request.
    pub async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.shared.lock().insert(id, Pending::Call(tx));
        self.send(ClientFrame {
            id,
            method: Some(method.into()),
            params,
            cancel: false,
        })
        .await
        .inspect_err(|_| {
            self.shared.lock().remove(&id);
        })?;
        rx.await.map_err(|_| RpcError::Closed)?
    }

    /// Typed unary request.
    pub async fn call_as<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, RpcError> {
        let value = self.call(method, params).await?;
        serde_json::from_value(value).map_err(|e| RpcError::BadParams(e.to_string()))
    }

    /// Lossless streaming request: items arrive in protocol order and the
    /// receiver closes when the server sends `{done}` or `{err}`, or the
    /// connection drops. Use [`Self::subscribe_latest`] for replaceable state
    /// snapshots. Dropping the receiver cancels the stream server-side.
    pub async fn subscribe(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<RpcStream, RpcError> {
        self.subscribe_with_retention(method, params, StreamRetention::Ordered)
            .await
    }

    /// State-watch request that retains at most the latest complete snapshot.
    /// This prevents a slow UI consumer from queueing multiple large copies of
    /// a transcript while preserving the current-state semantics of `watch`.
    pub async fn subscribe_latest(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<RpcStream, RpcError> {
        self.subscribe_with_retention(method, params, StreamRetention::Latest)
            .await
    }

    async fn subscribe_with_retention(
        &self,
        method: &str,
        params: serde_json::Value,
        retention: StreamRetention,
    ) -> Result<RpcStream, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let buffer = Arc::new(StreamBuffer::new(retention));
        self.shared
            .lock()
            .insert(id, Pending::Stream(buffer.clone()));
        self.send(ClientFrame {
            id,
            method: Some(method.into()),
            params,
            cancel: false,
        })
        .await
        .inspect_err(|_| {
            self.shared.lock().remove(&id);
        })?;
        Ok(RpcStream {
            buffer,
            id,
            out: self.out.clone(),
            shared: Arc::downgrade(&self.shared),
        })
    }

    async fn send(&self, frame: ClientFrame) -> Result<(), RpcError> {
        let json = serde_json::to_string(&frame)
            .map_err(|e| RpcError::Transport(format!("serialize frame: {e}")))?;
        self.out.send(json).await.map_err(|_| RpcError::Closed)
    }
}

impl Drop for RpcClient {
    fn drop(&mut self) {
        self.reader.abort();
        let drained = self
            .shared
            .lock()
            .drain()
            .map(|(_, pending)| pending)
            .collect::<Vec<_>>();
        for pending in drained {
            match pending {
                Pending::Call(sender) => {
                    let _ = sender.send(Err(RpcError::Closed));
                }
                Pending::Stream(stream) => stream.close(),
            }
        }
    }
}

fn route_frame(shared: &Arc<Shared>, out: &mpsc::Sender<String>, frame: ServerFrame) {
    let id = frame.id;
    if let Some(err) = frame.err {
        match shared.lock().remove(&id) {
            Some(Pending::Call(tx)) => {
                let _ = tx.send(Err(RpcError::Failed(err)));
            }
            Some(Pending::Stream(stream)) => {
                stream.close();
                tracing::debug!(id, %err, "rpc: stream ended with error");
            }
            None => {}
        }
        return;
    }
    if let Some(value) = frame.ok {
        if let Some(Pending::Call(tx)) = shared.lock().remove(&id) {
            let _ = tx.send(Ok(value));
        }
        return;
    }
    if let Some(item) = frame.item {
        let dead = {
            let pending = shared.lock();
            match pending.get(&id) {
                Some(Pending::Stream(stream)) => !stream.push(item),
                _ => false,
            }
        };
        if dead {
            // Receiver was dropped — cancel server-side and forget the stream.
            shared.lock().remove(&id);
            try_cancel(out, id);
        }
        return;
    }
    if frame.done {
        if let Some(Pending::Stream(stream)) = shared.lock().remove(&id) {
            stream.close();
        }
    }
}

fn try_cancel(out: &mpsc::Sender<String>, id: u64) {
    if let Ok(json) = serde_json::to_string(&ClientFrame {
        id,
        method: None,
        params: serde_json::Value::Null,
        cancel: true,
    }) {
        // The reader multiplexes every RPC. Never await a cancel when the
        // outgoing channel is saturated or unrelated replies can be
        // head-of-line blocked behind a dead stream.
        let _ = out.try_send(json);
    }
}

/// How long a dial may take before we give up.
///
/// This is localhost: a real engine answers in milliseconds. Without a bound,
/// *any* other process holding the port accepts the TCP connection and then
/// never completes the WebSocket handshake, and the caller waits forever — a
/// stranger on port 27654 would hang the app at boot rather than degrade it.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Dial a WebSocket RPC server (`ws://127.0.0.1:{ipc_port}`).
pub async fn connect_ws(url: &str) -> Result<RpcClient, RpcError> {
    let (ws, _) = tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url))
        .await
        .map_err(|_| RpcError::Transport(format!("timed out dialing {url}")))?
        .map_err(|e| RpcError::Transport(e.to_string()))?;
    let (mut sink, mut stream) = ws.split();
    let (out_tx, mut out_rx) = mpsc::channel::<String>(256);
    let (in_tx, in_rx) = mpsc::channel::<String>(256);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                frame = out_rx.recv() => match frame {
                    Some(text) => {
                        if sink.send(WsMessage::Text(text)).await.is_err() {
                            break;
                        }
                    }
                    None => {
                        let _ = sink.send(WsMessage::Close(None)).await;
                        break;
                    }
                },
                message = stream.next() => match message {
                    Some(Ok(WsMessage::Text(text))) => {
                        if in_tx.send(text).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(WsMessage::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {}
                },
            }
        }
    });
    Ok(RpcClient::new(out_tx, in_rx))
}
