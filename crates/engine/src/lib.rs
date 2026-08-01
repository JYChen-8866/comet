//! comet-engine — the local backend for the note-agent app: sessions engine,
//! doc host + command executor, run journal + crash recovery, and the IPC RPC
//! server. Account auth, edge sync, device relay, repos/diffs and terminals
//! were removed with the coding-agent features.

use std::path::{Path, PathBuf};
use std::sync::Arc;

pub use comet_proto::HarnessId;

use comet_sync::DocsStore;

pub mod doc_host;
pub mod instance_lock;
pub mod mentions;
pub mod registry;
pub mod rpc;
pub mod run_journal;
pub mod sessions;
pub mod titles;
pub mod uploads;
pub mod workspace_host;

pub use doc_host::{ChatDocHandle, DocHost, DocHostConfig};
pub use instance_lock::InstanceLock;
pub use registry::{HarnessDescriptor, HarnessRegistry, default_registry};
pub use rpc::EngineRpc;
pub use run_journal::{JournalError, RunJournal};
pub use sessions::{JournaledEvent, SessionsEngine, SteerOutcome};
pub use titles::TitleGenerator;
pub use uploads::{AttachmentChunk, Uploads};
pub use workspace_host::{WORKSPACE_DOC_ID, WorkspaceHost, WorkspaceHostConfig};

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("doc: {0}")]
    Doc(#[from] comet_doc::DocError),
    #[error("journal: {0}")]
    Journal(#[from] run_journal::JournalError),
    #[error("store: {0}")]
    Store(#[from] comet_sync::StoreError),
    #[error("harness: {0}")]
    Harness(#[from] comet_harness::HarnessError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

/// Epoch millis now — the doc/journal timestamp base.
pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub(crate) fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Data directory (default `~/.comet-native`).
    pub data_dir: PathBuf,
    /// Localhost IPC port for the UI.
    pub ipc_port: u16,
    /// Harness for doc-command runs on chats without a workspace `config` row.
    pub default_harness: HarnessId,
}

/// The assembled engine core — also constructible without the IPC server for
/// tests and the in-process (headed) mode.
pub struct EngineCore {
    pub sessions: SessionsEngine,
    pub doc_host: DocHost,
    pub workspace: WorkspaceHost,
    pub registry: Arc<HarnessRegistry>,
    pub uploads: Uploads,
    pub device_id: String,
    /// Exclusive data-dir lock — held for the engine's lifetime (single-instance).
    _instance_lock: InstanceLock,
}

impl EngineCore {
    /// Open stores under `data_dir`, wire sessions ⇄ doc host ⇄ workspace host,
    /// and recover stale journals from a previous crash.
    pub fn assemble(
        data_dir: &Path,
        registry: Arc<HarnessRegistry>,
        default_harness: HarnessId,
    ) -> Result<Self, EngineError> {
        std::fs::create_dir_all(data_dir)?;
        // Single-instance guard: two engines on one data dir would race the
        // SQLite snapshots + journals. Taken before any store opens or the IPC
        // port binds; held (and kernel-released on crash) for the engine's life.
        let lock = InstanceLock::acquire(data_dir)?;
        let device_id = load_or_create_device_id(data_dir)?;
        let store = Arc::new(DocsStore::open(data_dir)?);
        let journal = Arc::new(RunJournal::open(data_dir.join("journals"))?);
        let sessions = SessionsEngine::new(device_id.clone(), journal, registry.clone());
        let doc_host = DocHost::new(
            store.clone(),
            DocHostConfig {
                device_id: device_id.clone(),
                default_harness,
            },
        );
        let workspace = WorkspaceHost::open(
            store,
            WorkspaceHostConfig {
                device_id: device_id.clone(),
                device_name: local_device_name(),
                platform: std::env::consts::OS.to_string(),
            },
        )?;
        doc_host.set_workspace(workspace.clone());
        doc_host.set_sessions(sessions.clone());
        sessions.set_doc_host(doc_host.clone());
        match sessions.recover_stale() {
            Ok(0) => {}
            Ok(recovered) => tracing::info!(recovered, "stale sessions recovered on boot"),
            Err(err) => tracing::error!(error = %err, "stale-session recovery failed"),
        }
        let uploads = Uploads::new(data_dir);
        sessions.set_titles(TitleGenerator::new(workspace.clone()));
        Ok(Self {
            sessions,
            doc_host,
            workspace,
            registry,
            uploads,
            device_id,
            _instance_lock: lock,
        })
    }

    pub fn rpc_service(&self) -> Arc<EngineRpc> {
        Arc::new(EngineRpc::new(
            self.sessions.clone(),
            self.doc_host.clone(),
            self.workspace.clone(),
            self.registry.clone(),
            self.uploads.clone(),
        ))
    }

    /// Graceful teardown: settle live runs, flush every open doc snapshot, and
    /// stamp the workspace row.
    pub async fn shutdown(&self) {
        self.sessions.shutdown().await;
        self.doc_host.flush_all();
        self.workspace.shutdown();
    }
}

pub struct Engine {
    pub config: EngineConfig,
}

/// A fully assembled local engine, shared by headless and headed modes.
pub struct EngineRuntime {
    core: EngineCore,
}

impl EngineRuntime {
    pub fn core(&self) -> &EngineCore {
        &self.core
    }

    pub async fn shutdown(&self) {
        self.core.shutdown().await;
    }
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        Self { config }
    }

    /// Assemble the local stores and engine services.
    pub async fn assemble_runtime(config: &EngineConfig) -> anyhow::Result<EngineRuntime> {
        let core = EngineCore::assemble(
            &config.data_dir,
            Arc::new(default_registry()),
            config.default_harness,
        )?;
        tracing::info!(device_id = %core.device_id, "engine core assembled");
        Ok(EngineRuntime { core })
    }

    /// Run until ctrl-c: sessions engine + doc host + command executor + IPC server.
    pub async fn run(self) -> anyhow::Result<()> {
        let config = self.config;
        tracing::info!(data_dir = %config.data_dir.display(), "engine starting");

        std::fs::create_dir_all(&config.data_dir)?;
        let runtime = Self::assemble_runtime(&config).await?;

        // A daemon exists to serve this port, so a bind failure is fatal here —
        // unlike the headed app, which can still work over its in-process
        // transport (see `serve_ipc`).
        let server = serve_ipc(config.ipc_port, runtime.core().rpc_service()).await?;

        shutdown_signal().await?;
        tracing::info!("shutting down");
        server.abort();
        runtime.shutdown().await;
        Ok(())
    }
}

/// Ctrl-C or SIGTERM. systemd/launchd stop delivers SIGTERM — without catching
/// it the daemon dies mid-write and every stop takes the crash-recovery path
/// instead of the graceful drain.
async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = sigterm.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

/// Serve the typed RPC on the localhost IPC port. Both the headless daemon and
/// the headed app's embedded engine call this so any viewport can attach.
pub async fn serve_ipc(
    port: u16,
    service: std::sync::Arc<dyn comet_rpc::RpcService>,
) -> std::io::Result<tokio::task::JoinHandle<()>> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    tracing::info!(port, "IPC server listening");
    Ok(tokio::spawn(comet_rpc::serve_ws_listener(
        listener, service,
    )))
}

/// Best-effort human name for this device's registry row (hostname).
fn local_device_name() -> String {
    std::env::var("COMET_DEVICE_NAME")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown-device".to_string())
}

/// Stable per-installation device id, persisted at `{data_dir}/device-id`.
fn load_or_create_device_id(data_dir: &Path) -> Result<String, EngineError> {
    let path = data_dir.join("device-id");
    match std::fs::read_to_string(&path) {
        Ok(id) if !id.trim().is_empty() => Ok(id.trim().to_string()),
        Ok(_) | Err(_) => {
            let id = new_id();
            std::fs::write(&path, &id)?;
            Ok(id)
        }
    }
}
