//! Chat auto-titling — after a run starts on an untitled chat, name it from the
//! prompt's first words. The coding-agent titling run was removed with the
//! external harnesses; this keeps new chats readable with zero model calls.

use std::sync::Arc;

use comet_proto::HarnessId;

use crate::workspace_host::WorkspaceHost;

struct Inner {
    workspace: WorkspaceHost,
}

#[derive(Clone)]
pub struct TitleGenerator {
    inner: Arc<Inner>,
}

impl TitleGenerator {
    pub fn new(workspace: WorkspaceHost) -> Self {
        Self {
            inner: Arc::new(Inner { workspace }),
        }
    }

    /// Fire-and-forget: title `chat_id` if it's still untitled.
    pub fn maybe_generate(&self, chat_id: &str, _harness: HarnessId, prompt: &str, _cwd: &str) {
        let this = self.clone();
        let chat_id = chat_id.to_string();
        let prompt = prompt.to_string();
        tokio::spawn(async move {
            if let Err(err) = this.generate(&chat_id, &prompt).await {
                tracing::debug!(chat = %chat_id, error = %err, "chat auto-titling skipped");
            }
        });
    }

    async fn generate(&self, chat_id: &str, prompt: &str) -> Result<(), crate::EngineError> {
        let chat = self
            .inner
            .workspace
            .doc()
            .chat(chat_id)?
            .ok_or_else(|| crate::EngineError::Other("chat has no workspace row".into()))?;
        if chat.title.as_deref().is_some_and(|t| !t.trim().is_empty()) {
            return Ok(()); // already named
        }
        let fallback: String = prompt
            .split_whitespace()
            .take(7)
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(48)
            .collect();
        if fallback.is_empty() {
            return Ok(());
        }
        // Re-read after the fallback: a user may have named the chat meanwhile.
        let latest = self.inner.workspace.doc().chat(chat_id)?.unwrap_or(chat);
        if latest
            .title
            .as_deref()
            .is_some_and(|t| !t.trim().is_empty())
        {
            return Ok(());
        }
        self.inner.workspace.rename_chat(chat_id, &fallback)?;
        tracing::info!(chat = %chat_id, title = %fallback, "chat auto-titled");
        Ok(())
    }
}
