//! Confirmation recovery regressions: the doc must flip the input part to
//! resolved even when the harness stalls after the answer, and the engine must
//! refuse a second run while the first is still live.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;

use comet_doc::{MessagePart, SessionMessageEntry};
use comet_engine::{EngineCore, HarnessRegistry};
use comet_harness::{Harness, HarnessError, RunControls};
use comet_proto::{
    AgentEvent, DoneStatus, HarnessId, Model, ReasoningLevel, RunRequest, SandboxLevel,
    SessionStatus, SteeringMode,
};

const CHAT: &str = "chat-repro";

fn run_request(prompt: &str) -> RunRequest {
    RunRequest {
        prompt: prompt.into(),
        model: None,
        reasoning: None,
        model_options: Default::default(),
        context: Vec::new(),
        cwd: "/tmp".into(),
        sandbox: SandboxLevel::WorkspaceWrite,
        auto_approve: true,
        attachments: Vec::new(),
        document_refs: Vec::new(),
        resume: None,
    }
}

struct AskThenHang;

#[async_trait]
impl Harness for AskThenHang {
    fn id(&self) -> HarnessId {
        HarnessId::Mock
    }
    fn display_name(&self) -> &str {
        "AskThenHang"
    }
    fn supports_steering(&self) -> bool {
        false
    }
    fn steering_mode(&self) -> SteeringMode {
        SteeringMode::TurnBoundary
    }
    fn reasoning_levels(&self) -> &[ReasoningLevel] {
        &[]
    }
    async fn models(&self) -> Result<Vec<Model>, HarnessError> {
        Ok(vec![])
    }
    async fn run(
        &self,
        _request: RunRequest,
        controls: RunControls,
    ) -> Result<BoxStream<'static, Result<AgentEvent, HarnessError>>, HarnessError> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<AgentEvent, HarnessError>>(16);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(AgentEvent::SessionStarted {
                    harness: HarnessId::Mock,
                    model: "mock".into(),
                    tools: vec![],
                    cwd: "/tmp".into(),
                    session_id: "hs-repro".into(),
                    assistant_message_id: "a-1".into(),
                }))
                .await;
            let answers = (controls.request_input)(vec![comet_proto::UserInputQuestion {
                id: "q1".into(),
                header: "AI 工具确认".into(),
                question: "允许？".into(),
                options: vec!["Yes".into(), "No".into()],
                multi_select: false,
            }])
            .await
            .unwrap_or_default();
            let picked = answers
                .first()
                .and_then(|a| a.labels.first().cloned())
                .unwrap_or_else(|| "none".into());
            let _ = tx
                .send(Ok(AgentEvent::ToolCall {
                    id: "tool-1".into(),
                    call: comet_proto::ToolCall::Unknown {
                        name: "hang".into(),
                        input: Some(serde_json::json!({ "picked": picked })),
                    },
                }))
                .await;
            // Never send ToolResult or Done: emulate a wedged tool call.
            let _ = controls.interrupt.cancelled().await;
            let _ = tx
                .send(Ok(AgentEvent::Done {
                    status: DoneStatus::Interrupted,
                    result: None,
                    error: None,
                    session_id: Some("hs-repro".into()),
                }))
                .await;
        });
        Ok(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        })
        .boxed())
    }
}

fn registry_with(harness: Arc<dyn Harness>) -> Arc<HarnessRegistry> {
    let registry = HarnessRegistry::new();
    registry.register(harness);
    Arc::new(registry)
}

fn assemble(dir: &std::path::Path, harness: Arc<dyn Harness>) -> EngineCore {
    EngineCore::assemble(dir, registry_with(harness), HarnessId::Mock)
        .expect("engine core assembles")
}

async fn wait_for<F>(mut predicate: F, what: &str)
where
    F: FnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !predicate() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

fn entries(core: &EngineCore) -> Vec<SessionMessageEntry> {
    core.doc_host
        .open(CHAT)
        .expect("open chat")
        .doc()
        .read_entries()
        .expect("read entries")
}

#[tokio::test]
async fn input_part_flips_resolved_while_harness_stalls() {
    let dir = tempfile::tempdir().unwrap();
    let core = assemble(dir.path(), Arc::new(AskThenHang));
    let handle = core.doc_host.open(CHAT).unwrap();
    let now = chrono::Utc::now().timestamp_millis();
    handle
        .doc()
        .queue_command(&comet_doc::SessionCommandEntry {
            id: "cmd-run".into(),
            payload: comet_doc::SessionCommandPayload::Run {
                request: run_request("ask"),
                message_id: "m-1".into(),
            },
            issued_by: "viewer".into(),
            issued_at: now,
            based_on: None,
            expires_at: None,
            status: comet_doc::SessionCommandStatus::Pending,
            resolution: None,
        })
        .unwrap();

    wait_for(
        || {
            core.sessions.session_status(CHAT).map(|s| s.status)
                == Some(SessionStatus::AwaitingInput)
        },
        "awaiting input",
    )
    .await;
    let mut request_id = String::new();
    wait_for(
        || {
            if let Some(id) = entries(&core).iter().find_map(|e| {
                e.parts.iter().find_map(|p| match p {
                    MessagePart::Input { request_id, .. } => Some(request_id.clone()),
                    _ => None,
                })
            }) {
                request_id = id;
                true
            } else {
                false
            }
        },
        "input part in doc",
    )
    .await;

    handle
        .doc()
        .queue_command(&comet_doc::SessionCommandEntry {
            id: "cmd-answer".into(),
            payload: comet_doc::SessionCommandPayload::RespondInput {
                request_id,
                answers: vec![comet_proto::UserInputAnswer {
                    question_id: "q1".into(),
                    labels: vec!["Yes".into()],
                }],
            },
            issued_by: "viewer".into(),
            issued_at: chrono::Utc::now().timestamp_millis(),
            based_on: None,
            expires_at: None,
            status: comet_doc::SessionCommandStatus::Pending,
            resolution: None,
        })
        .unwrap();

    wait_for(
        || {
            entries(&core).iter().any(|e| {
                e.parts
                    .iter()
                    .any(|p| matches!(p, MessagePart::Input { resolved: true, .. }))
            })
        },
        "input part resolved while harness stalls",
    )
    .await;
}

#[tokio::test]
async fn second_run_rejected_while_first_is_live() {
    let dir = tempfile::tempdir().unwrap();
    let core = assemble(dir.path(), Arc::new(AskThenHang));
    let handle = core.doc_host.open(CHAT).unwrap();
    let queue = |id: &str, payload: comet_doc::SessionCommandPayload| {
        handle
            .doc()
            .queue_command(&comet_doc::SessionCommandEntry {
                id: id.into(),
                payload,
                issued_by: "viewer".into(),
                issued_at: chrono::Utc::now().timestamp_millis(),
                based_on: None,
                expires_at: None,
                status: comet_doc::SessionCommandStatus::Pending,
                resolution: None,
            })
            .unwrap();
    };
    queue(
        "cmd-run-1",
        comet_doc::SessionCommandPayload::Run {
            request: run_request("first"),
            message_id: "m-1".into(),
        },
    );

    wait_for(
        || {
            core.sessions.session_status(CHAT).map(|s| s.status)
                == Some(SessionStatus::AwaitingInput)
        },
        "first run awaiting input",
    )
    .await;

    // A second prompt while the first run is live must not spawn a duplicate
    // harness run (that collides with the runtime turn and bricks the chat).
    queue(
        "cmd-run-2",
        comet_doc::SessionCommandPayload::Run {
            request: run_request("second"),
            message_id: "m-2".into(),
        },
    );
    wait_for(
        || {
            handle
                .doc()
                .read_commands()
                .expect("read commands")
                .iter()
                .find(|c| c.id == "cmd-run-2")
                .is_some_and(|c| c.status != comet_doc::SessionCommandStatus::Pending)
        },
        "second run rejected",
    )
    .await;
    assert_eq!(
        handle
            .doc()
            .read_commands()
            .expect("read commands")
            .into_iter()
            .find(|c| c.id == "cmd-run-2")
            .map(|c| c.status),
        Some(comet_doc::SessionCommandStatus::Rejected)
    );

    // The first run is untouched and still answers.
    let mut request_id = String::new();
    wait_for(
        || {
            if let Some(id) = entries(&core).iter().find_map(|e| {
                e.parts.iter().find_map(|p| match p {
                    MessagePart::Input { request_id, .. } => Some(request_id.clone()),
                    _ => None,
                })
            }) {
                request_id = id;
                true
            } else {
                false
            }
        },
        "input part in doc",
    )
    .await;
    queue(
        "cmd-answer-1",
        comet_doc::SessionCommandPayload::RespondInput {
            request_id,
            answers: vec![comet_proto::UserInputAnswer {
                question_id: "q1".into(),
                labels: vec!["Yes".into()],
            }],
        },
    );
    wait_for(
        || {
            entries(&core).iter().any(|e| {
                e.parts
                    .iter()
                    .any(|p| matches!(p, MessagePart::Input { resolved: true, .. }))
            })
        },
        "first run still resolves",
    )
    .await;
}
