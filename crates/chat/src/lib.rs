//! comet-chat — the Comet chat interface as a reusable gpui component.
//!
//! This is the chat-only surface (transcript + composer, with the left
//! MessageRail outline) extracted from the full desktop shell. No titlebar,
//! no sidebar, no settings: just the conversation and its input.

mod chat_view;

pub use chat_view::{ChatConfig, ChatEvent, ChatView, init_tokio_from_handle};
