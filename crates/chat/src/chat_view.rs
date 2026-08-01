use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use comet_ui::composer::{Composer, ComposerEvent};
use comet_ui::state::{AppState, ConnectionStatus, EngineBootConfig};
use comet_ui::theme::Theme;
use comet_ui::transcript::Transcript;
use comet_ui::HarnessId;
use comet_rpc::RpcClient;
use gpui::{
    Context, Entity, EventEmitter, Render, Subscription, Window, div, prelude::*, px,
};

/// Everything a host app needs to embed the chat interface.
#[derive(Debug, Clone)]
pub struct ChatConfig {
    /// Data directory the embedded/connected engine uses.
    pub data_dir: PathBuf,
    /// Localhost IPC port. If an engine is already listening here the chat
    /// connects to it; otherwise it embeds its own engine.
    pub ipc_port: u16,
    /// Default harness for chats without a config row.
    pub default_harness: HarnessId,
    /// Chat to select on first frame (e.g. a stable "aurin-copilot" chat).
    pub initial_chat_id: Option<String>,
}

pub struct ChatView {
    state: Entity<AppState>,
    transcript: Entity<Transcript>,
    composer: Entity<Composer>,
    initial_select_done: bool,
    initial_chat_id: Option<String>,
    _state_observation: Subscription,
    _composer_events: Subscription,
}

/// Events emitted by the chat surface to its host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatEvent {
    /// The user typed `@` in the composer; hosts may open a document picker.
    MentionRequested { offset: usize },
}

impl EventEmitter<ChatEvent> for ChatView {}

impl ChatView {
    /// The AppState shared with the chat view — host panels can build the
    /// comet session-management sidebar on the same state so selections stay
    /// in sync.
    pub fn state(&self) -> Entity<AppState> {
        self.state.clone()
    }

    /// Insert host text (e.g. a picked document mention) at the composer caret.
    pub fn insert_composer_text(
        &mut self,
        text: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.composer.update(cx, |composer, cx| {
            composer.insert_text_at_cursor(text, window, cx);
        });
    }

    /// Replace the `@` that opened the mention picker with the picked token.
    pub fn insert_composer_mention(
        &mut self,
        text: &str,
        trigger_offset: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.composer.update(cx, |composer, cx| {
            composer.insert_mention_text(text, trigger_offset, window, cx);
        });
    }

    pub fn new(config: ChatConfig, cx: &mut Context<Self>) -> Self {
        // Host apps (Aurin) may not have initialized gpui's tokio bridge.
        static TOKIO_INIT: AtomicBool = AtomicBool::new(false);
        if !TOKIO_INIT.swap(true, Ordering::SeqCst) {
            gpui_tokio::init(cx);
        }
        // The comet transcript/composer render against comet-ui's own theme
        // global. Hosts (Aurin) may install a themed global before creating us;
        // only fall back to the built-in dark theme when none exists.
        if !cx.has_global::<Theme>() {
            cx.set_global(Theme::dark());
        }
        comet_ui::composer::init(cx);

        let state = cx.new(|_| AppState::new());
        AppState::bootstrap(
            state.clone(),
            EngineBootConfig {
                data_dir: config.data_dir.clone(),
                ipc_port: config.ipc_port,
                default_harness: config.default_harness,
            },
            cx,
        );
        Self::finish(state, config, cx)
    }

    /// Create the chat view from an already-connected engine RPC client.
    /// Hosts like Aurin use this with `comet_rpc::memory_client` so there is
    /// no IPC port race and no second engine instance.
    pub fn new_with_client(client: RpcClient, config: ChatConfig, cx: &mut Context<Self>) -> Self {
        cx.set_global(Theme::dark());
        comet_ui::composer::init(cx);

        let state = cx.new(|_| AppState::new());
        let handle = comet_ui::state::EngineHandle::from_client(
            client,
            "comet-chat-memory".to_string(),
        );
        state.update(cx, |state, cx| {
            state.data_dir = Some(config.data_dir.clone());
            state.attach_engine(handle, cx);
            cx.notify();
        });
        Self::finish(state, config, cx)
    }

    /// Create the chat view from an already-connected [`AppState`] (shared
    /// with a host's session-management sidebar).
    pub fn new_with_state(
        state: Entity<AppState>,
        config: ChatConfig,
        cx: &mut Context<Self>,
    ) -> Self {
        cx.set_global(Theme::dark());
        comet_ui::composer::init(cx);
        Self::finish(state, config, cx)
    }

    fn finish(
        state: Entity<AppState>,
        config: ChatConfig,
        cx: &mut Context<Self>,
    ) -> Self {
        let transcript = cx.new(|cx| Transcript::new(state.clone(), cx));
        let composer = cx.new(|cx| Composer::new(state.clone(), cx));
        let composer_events = cx.subscribe(&composer, {
            let transcript = transcript.clone();
            move |_this: &mut Self, _, event: &ComposerEvent, cx| {
                match event {
                    ComposerEvent::Sent { .. } => {
                        transcript.update(cx, |t, cx| t.on_own_send(cx));
                    }
                    ComposerEvent::MentionRequested { offset } => {
                        cx.emit(ChatEvent::MentionRequested { offset: *offset });
                    }
                }
            }
        });
        let state_observation = cx.observe(&state, |this: &mut Self, _, cx| {
            this.on_state_changed(cx);
            cx.notify();
        });
        let initial_chat_id = config.initial_chat_id;
        Self {
            state,
            transcript,
            composer,
            initial_select_done: false,
            initial_chat_id,
            _state_observation: state_observation,
            _composer_events: composer_events,
        }
    }

    fn on_state_changed(&mut self, cx: &mut Context<Self>) {
        if self.initial_select_done {
            return;
        }
        let state = self.state.read(cx);
        if !matches!(state.connection, ConnectionStatus::Ready) {
            return;
        }
        // Only select a chat when one is explicitly chosen. Without this, a
        // fresh host panel would silently reopen the first persisted chat
        // instead of starting a new session.
        let target = state
            .selected_chat
            .clone()
            .or_else(|| self.initial_chat_id.clone());
        if let Some(chat_id) = target {
            self.initial_select_done = true;
            let _ = state;
            self.state.update(cx, |s, cx| s.select_chat(Some(chat_id), cx));
        }
    }
}

impl Render for ChatView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = Theme::of(cx).clone();
        let conversation = div()
            .w_full()
            .max_w(px(760.0))
            .h_full()
            .child(self.transcript.clone());
        let composer = div()
            .w_full()
            .max_w(px(760.0))
            .child(self.composer.clone());
        div()
            .id("comet-chat")
            .size_full()
            .bg(theme.bg)
            .text_color(theme.text)
            .flex()
            .flex_col()
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .flex()
                    .justify_center()
                    .child(conversation),
            )
            .child(
                div()
                    .flex_none()
                    .flex()
                    .justify_center()
                    .px(px(16.0))
                    .pb(px(16.0))
                    .child(composer),
            )
    }
}
