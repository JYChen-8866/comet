//! Session doc schema over `loro` — Rust port of `packages/session-doc/src/schema.ts`.
//!
//! Container layout (MUST stay shape-compatible with the TS edge/tail materializer):
//! - `meta`:     LoroMap  { chatId: string, schemaVersion: number }         (host-only writer)
//! - `messages`: LoroList of LoroMap {
//!   id, role, parts: LoroList<part map>, createdAt, deviceId, status?, continuationOf? }
//! - `commands`: LoroList of LoroMap {
//!   id, kind, payload(json), issuedBy, issuedAt, basedOn?, expiresAt?, status, resolution? }
//!
//! Part maps: { id, kind: "text"|"tool"|"input"|"error", text?: LoroText, call?: json,
//! isError?, questions?: json, resolved?, message? }. Text bodies are **LoroText** so streaming
//! appends RLE-merge (1.03x oplog overhead vs 125x for whole-value rewrites).

use std::collections::{HashMap, VecDeque};

use loro::{ExportMode, LoroDoc, LoroError, LoroList, LoroMap, LoroText, LoroValue, ToJson};
use serde::{Deserialize, Serialize};

use crate::commands::{SessionCommandEntry, SessionCommandStatus};
use crate::constants::{SESSION_SCHEMA_VERSION, TAIL_MESSAGE_COUNT};
use crate::parts::{MessagePart, MessageStatus};

#[derive(Debug, thiserror::Error)]
pub enum DocError {
    #[error("loro: {0}")]
    Loro(#[from] LoroError),
    #[error("schema: {0}")]
    Schema(String),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageRole {
    User,
    Assistant,
    System,
}

/// One entry in the doc's `messages` list (`SessionMessageEntry` in TS).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionMessageEntry {
    pub id: String,
    pub role: MessageRole,
    pub parts: Vec<MessagePart>,
    /// Epoch millis.
    pub created_at: i64,
    pub device_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<MessageStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_of: Option<String>,
}

/// The doc-resident flat part map (`DocMessagePart` in TS). Distinct from the app-layer
/// [`MessagePart`]: input parts key on their request id, error parts store `message`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DocPartJson {
    id: String,
    kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    call: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    is_error: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    questions: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resolved: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

/// App parts → doc part json (mirror of `toDocParts`).
fn to_doc_part(part: &MessagePart) -> Result<DocPartJson, DocError> {
    Ok(match part {
        MessagePart::Text { id, text } => DocPartJson {
            id: id.clone(),
            kind: "text".into(),
            text: Some(text.clone()),
            ..Default::default()
        },
        MessagePart::Tool {
            id,
            call,
            is_error,
            resolved,
        } => DocPartJson {
            id: id.clone(),
            kind: "tool".into(),
            call: Some(serde_json::to_value(call)?),
            // TS shape parity: `isError` is written only once the tool result arrived;
            // its presence IS the resolution marker.
            is_error: if *resolved { Some(*is_error) } else { None },
            ..Default::default()
        },
        MessagePart::Input {
            id: _,
            request_id,
            questions,
            resolved,
        } => DocPartJson {
            id: request_id.clone(),
            kind: "input".into(),
            questions: Some(serde_json::to_value(questions)?),
            resolved: Some(*resolved),
            ..Default::default()
        },
        MessagePart::Error { id, message } => DocPartJson {
            id: id.clone(),
            kind: "error".into(),
            message: Some(message.clone()),
            ..Default::default()
        },
    })
}

/// Doc part json → app part (mirror of `fromDocParts`; malformed degrades to empty text).
fn from_doc_part(p: DocPartJson) -> MessagePart {
    match p.kind.as_str() {
        "tool" => match p.call.and_then(|c| serde_json::from_value(c).ok()) {
            Some(call) => MessagePart::Tool {
                id: p.id,
                call,
                is_error: p.is_error.unwrap_or(false),
                resolved: p.is_error.is_some(),
            },
            None => MessagePart::Text {
                id: p.id,
                text: String::new(),
            },
        },
        "input" => MessagePart::Input {
            id: p.id.clone(),
            request_id: p.id,
            questions: p
                .questions
                .and_then(|q| serde_json::from_value(q).ok())
                .unwrap_or_default(),
            resolved: p.resolved.unwrap_or(false),
        },
        "error" => MessagePart::Error {
            id: p.id,
            message: p.message.unwrap_or_default(),
        },
        _ => MessagePart::Text {
            id: p.id,
            text: p.text.unwrap_or_default(),
        },
    }
}

/// A session doc handle: typed access over a LoroDoc with the schema above.
pub struct SessionDoc {
    doc: LoroDoc,
}

impl SessionDoc {
    /// Wrap an existing doc (e.g. imported from a snapshot).
    pub fn from_doc(doc: LoroDoc) -> Self {
        Self { doc }
    }

    /// Create + initialize a fresh doc for `chat_id` (host-only).
    pub fn init(chat_id: &str) -> Result<Self, DocError> {
        let doc = LoroDoc::new();
        let meta = doc.get_map("meta");
        meta.insert("chatId", chat_id)?;
        meta.insert("schemaVersion", SESSION_SCHEMA_VERSION as i64)?;
        doc.commit();
        Ok(Self { doc })
    }

    pub fn doc(&self) -> &LoroDoc {
        &self.doc
    }

    pub fn chat_id(&self) -> Option<String> {
        match self.doc.get_map("meta").get("chatId") {
            Some(loro::ValueOrContainer::Value(LoroValue::String(s))) => Some(s.to_string()),
            _ => None,
        }
    }

    /// Insert a complete message entry (user/system messages, command-side inserts).
    /// Streaming assistant entries go through [`SegmentWriter`].
    pub fn push_message(&self, entry: &SessionMessageEntry) -> Result<(), DocError> {
        let messages = self.doc.get_list("messages");
        let map = messages.push_container(LoroMap::new())?;
        write_entry_scalar_fields(&map, entry)?;
        let parts = map.insert_container("parts", LoroList::new())?;
        for part in &entry.parts {
            push_part(&parts, part)?;
        }
        self.doc.commit();
        Ok(())
    }

    /// Read all entries (continuations NOT joined — see `join_continuation_entries`).
    ///
    /// Malformed entries are SKIPPED, not fatal: a torn intermediate state
    /// (an entry map imported before the update that fills its fields) or a
    /// peer on a newer schema must degrade to a missing row, never blank the
    /// whole transcript — one bad entry took down every publish for the chat
    /// (2026-07-31, "missing field `id`" during a multi-update import).
    pub fn read_entries(&self) -> Result<Vec<SessionMessageEntry>, DocError> {
        let messages = self.doc.get_list("messages");
        let mut entries = Vec::with_capacity(messages.len());
        for index in 0..messages.len() {
            let Some(map) = message_map_at(&messages, index) else {
                continue;
            };
            match entry_from_map(&map) {
                Ok(entry) => entries.push(entry),
                Err(err) => {
                    tracing::warn!(error = %err, index, "skipping malformed transcript entry");
                }
            }
        }
        Ok(entries)
    }

    /// Read only the newest `limit` entries without deep-converting the whole
    /// document.  Chat history can be much larger than the provider context;
    /// callers that only need a prompt window must not pay an allocation for
    /// every old message first.
    pub fn read_entries_tail(&self, limit: usize) -> Result<Vec<SessionMessageEntry>, DocError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let messages = self.doc.get_list("messages");
        let start = messages.len().saturating_sub(limit);
        let mut entries = Vec::with_capacity(messages.len().saturating_sub(start));
        for index in start..messages.len() {
            let Some(map) = message_map_at(&messages, index) else {
                continue;
            };
            match entry_from_map(&map) {
                Ok(entry) => entries.push(entry),
                Err(err) => {
                    tracing::warn!(error = %err, index, "skipping malformed transcript entry");
                }
            }
        }
        Ok(entries)
    }

    /// Check an idempotency key without decoding any message parts.
    pub fn contains_message_id(&self, message_id: &str) -> bool {
        let messages = self.doc.get_list("messages");
        (0..messages.len()).any(|index| {
            message_map_at(&messages, index).is_some_and(|map| {
                matches!(
                    map.get("id"),
                    Some(loro::ValueOrContainer::Value(LoroValue::String(id)))
                        if id.as_str() == message_id
                )
            })
        })
    }

    /// Valid message ids in CRDT list order. This is the command evaluator's
    /// complete turn index; retaining ids is enough for past/current checks
    /// and avoids retaining every historical body during a command drain.
    pub fn message_ids(&self) -> Vec<String> {
        let messages = self.doc.get_list("messages");
        let mut ids = Vec::with_capacity(messages.len());
        for index in 0..messages.len() {
            let Some(map) = message_map_at(&messages, index) else {
                continue;
            };
            if let Ok(metadata) = entry_metadata_from_map(&map) {
                ids.push(metadata.id);
            }
        }
        ids
    }

    /// Last valid raw message id, without materializing the entry body.
    pub fn last_message_id(&self) -> Option<String> {
        let messages = self.doc.get_list("messages");
        (0..messages.len()).rev().find_map(|index| {
            message_map_at(&messages, index)
                .and_then(|map| entry_metadata_from_map(&map).ok())
                .map(|metadata| metadata.id)
        })
    }

    /// Visit user-authored text parts one at a time. Returning false stops the
    /// scan. Callers such as the resident-document registry can therefore
    /// inspect a long history with memory bounded by its largest individual
    /// text part and stop as soon as their own result budget is full.
    pub fn visit_user_text_parts(
        &self,
        mut visitor: impl FnMut(&str) -> bool,
    ) -> Result<(), DocError> {
        let messages = self.doc.get_list("messages");
        for entry_index in 0..messages.len() {
            let Some(entry) = message_map_at(&messages, entry_index) else {
                continue;
            };
            if !matches!(
                optional_map_string(&entry, "role"),
                Ok(Some(role)) if role == "user"
            ) {
                continue;
            }
            let Some(loro::ValueOrContainer::Container(loro::Container::List(parts))) =
                entry.get("parts")
            else {
                continue;
            };
            for part_index in 0..parts.len() {
                let Some(loro::ValueOrContainer::Container(loro::Container::Map(part))) =
                    parts.get(part_index)
                else {
                    continue;
                };
                if !matches!(
                    optional_map_string(&part, "kind"),
                    Ok(Some(kind)) if kind == "text"
                ) {
                    continue;
                }
                let text = match part.get("text") {
                    Some(loro::ValueOrContainer::Container(loro::Container::Text(text))) => {
                        text.to_string()
                    }
                    Some(loro::ValueOrContainer::Value(LoroValue::String(text))) => {
                        text.to_string()
                    }
                    None | Some(loro::ValueOrContainer::Value(LoroValue::Null)) => String::new(),
                    Some(_) => continue,
                };
                if !visitor(&text) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Read the newest `limit` entries after continuation joining without
    /// materializing old message bodies. The first pass reads only entry ids,
    /// continuation links, and schema types to identify the final tail. The
    /// second pass decodes text/tool payloads only for roots that survived in
    /// that tail (and continuations targeting those roots).
    pub fn read_joined_entries_tail(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionMessageEntry>, DocError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.read_joined_tail_with_total(limit)
            .map(|(entries, _)| entries)
    }

    pub(crate) fn read_joined_tail_with_total(
        &self,
        limit: usize,
    ) -> Result<(Vec<SessionMessageEntry>, usize), DocError> {
        #[derive(Clone, Copy)]
        struct TailSlot {
            ordinal: usize,
        }

        let messages = self.doc.get_list("messages");
        let mut roots = HashMap::<String, usize>::new();
        let mut tail = VecDeque::<TailSlot>::with_capacity(limit.min(messages.len()));
        let mut total = 0usize;

        // Pass one never materializes a message body. It only resolves which
        // joined roots occupy the final tail and validates the same schema
        // types that a full entry decode requires.
        for index in 0..messages.len() {
            let Some(map) = message_map_at(&messages, index) else {
                continue;
            };
            let metadata = match entry_metadata_from_map(&map) {
                Ok(metadata) => metadata,
                Err(err) => {
                    tracing::warn!(error = %err, index, "skipping malformed transcript entry");
                    continue;
                }
            };
            let continuation_target = metadata
                .continuation_of
                .as_deref()
                .and_then(|root_id| roots.get(root_id).copied());
            if continuation_target.is_some() {
                continue;
            }

            // An orphan continuation is surfaced as its own joined entry, but
            // (matching `join_continuation_entries`) it does not become a root
            // that later continuation entries can target.
            if metadata.continuation_of.is_none() {
                roots.insert(metadata.id, total);
            }
            tail.push_back(TailSlot { ordinal: total });
            total = total.saturating_add(1);
            if tail.len() > limit {
                tail.pop_front();
            }
        }

        let selected = tail
            .iter()
            .enumerate()
            .map(|(tail_index, slot)| (slot.ordinal, tail_index))
            .collect::<HashMap<_, _>>();
        let mut output: Vec<Option<SessionMessageEntry>> = vec![None; tail.len()];

        // Pass two decodes only the selected roots and continuations that
        // contribute parts to them. Old text/tool bodies are never allocated.
        roots.clear();
        let mut ordinal = 0usize;
        for index in 0..messages.len() {
            let Some(map) = message_map_at(&messages, index) else {
                continue;
            };
            let Ok(metadata) = entry_metadata_from_map(&map) else {
                continue;
            };
            let continuation_target = metadata
                .continuation_of
                .as_deref()
                .and_then(|root_id| roots.get(root_id).copied());
            if let Some(target) = continuation_target {
                if let Some(&tail_index) = selected.get(&target)
                    && let Ok(continuation) = entry_from_map(&map)
                    && let Some(root) = output[tail_index].as_mut()
                {
                    root.parts.extend(continuation.parts);
                }
                continue;
            }

            if metadata.continuation_of.is_none() {
                roots.insert(metadata.id, ordinal);
            }
            if let Some(&tail_index) = selected.get(&ordinal)
                && let Ok(entry) = entry_from_map(&map)
            {
                output[tail_index] = Some(entry);
            }
            ordinal = ordinal.saturating_add(1);
        }

        Ok((output.into_iter().flatten().collect(), total))
    }

    /// Read the commands ledger.
    ///
    /// Same skip-not-fail policy as `read_entries`: any device can append
    /// here, and one malformed entry must not wedge command draining for the
    /// chat forever (an unparseable command can't be executed anyway).
    pub fn read_commands(&self) -> Result<Vec<SessionCommandEntry>, DocError> {
        let commands = self.doc.get_list("commands");
        let mut entries = Vec::with_capacity(commands.len());
        for index in 0..commands.len() {
            let Some(loro::ValueOrContainer::Container(loro::Container::Map(map))) =
                commands.get(index)
            else {
                continue;
            };
            // Commands contain an arbitrary tagged payload, so retain serde at
            // this boundary, but deep-convert one command map at a time rather
            // than cloning the complete document and command ledger first.
            match serde_json::from_value(map.get_deep_value().to_json_value()) {
                Ok(entry) => entries.push(entry),
                Err(err) => {
                    tracing::warn!(error = %err, index, "skipping malformed command entry");
                }
            }
        }
        Ok(entries)
    }

    /// Append a command entry (rule 1: own entries only, append-only).
    pub fn queue_command(&self, entry: &SessionCommandEntry) -> Result<(), DocError> {
        let commands = self.doc.get_list("commands");
        let map = commands.push_container(LoroMap::new())?;
        map.insert("id", entry.id.as_str())?;
        map.insert(
            "kind",
            serde_json::to_value(entry.kind())?
                .as_str()
                .ok_or_else(|| DocError::Schema("kind not a string".into()))?,
        )?;
        map.insert(
            "payload",
            loro_value_from_json(&serde_json::to_value(&entry.payload)?),
        )?;
        map.insert("issuedBy", entry.issued_by.as_str())?;
        map.insert("issuedAt", entry.issued_at)?;
        if let Some(based_on) = &entry.based_on {
            map.insert(
                "basedOn",
                loro_value_from_json(&serde_json::to_value(based_on)?),
            )?;
        }
        if let Some(expires_at) = entry.expires_at {
            map.insert("expiresAt", expires_at)?;
        }
        map.insert(
            "status",
            serde_json::to_value(entry.status)?
                .as_str()
                .ok_or_else(|| DocError::Schema("status not a string".into()))?,
        )?;
        self.doc.commit();
        Ok(())
    }

    /// Rule 2: host (or the issuing composer, for `cancelled`) writes an outcome.
    pub fn set_command_status(
        &self,
        command_id: &str,
        status: SessionCommandStatus,
        resolution: Option<&str>,
    ) -> Result<(), DocError> {
        let commands = self.doc.get_list("commands");
        for i in 0..commands.len() {
            if let Some(loro::ValueOrContainer::Container(loro::Container::Map(map))) =
                commands.get(i)
            {
                let id_matches = matches!(
                    map.get("id"),
                    Some(loro::ValueOrContainer::Value(LoroValue::String(s))) if s.as_str() == command_id
                );
                if id_matches {
                    map.insert(
                        "status",
                        serde_json::to_value(status)?
                            .as_str()
                            .ok_or_else(|| DocError::Schema("status not a string".into()))?,
                    )?;
                    if let Some(r) = resolution {
                        map.insert("resolution", r)?;
                    }
                    self.doc.commit();
                    return Ok(());
                }
            }
        }
        Err(DocError::Schema(format!("command {command_id} not found")))
    }

    /// Stamp a terminal status on an existing message entry by id (recovery:
    /// abandoned `streaming` entries from a dead run are stamped `aborted`).
    /// Returns `false` when no entry with that id exists.
    pub fn set_message_status(
        &self,
        message_id: &str,
        status: MessageStatus,
    ) -> Result<bool, DocError> {
        let messages = self.doc.get_list("messages");
        for i in 0..messages.len() {
            if let Some(loro::ValueOrContainer::Container(loro::Container::Map(map))) =
                messages.get(i)
            {
                let id_matches = matches!(
                    map.get("id"),
                    Some(loro::ValueOrContainer::Value(LoroValue::String(s))) if s.as_str() == message_id
                );
                if id_matches {
                    map.insert("status", status_str(status))?;
                    self.doc.commit();
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Append an error part to an existing entry (crash recovery: the aborted
    /// entry must SAY why it ended — "Run interrupted by engine restart…" —
    /// not just truncate silently). Returns `false` when no entry matches.
    pub fn append_error_part(
        &self,
        message_id: &str,
        part_id: &str,
        message: &str,
    ) -> Result<bool, DocError> {
        let messages = self.doc.get_list("messages");
        for i in 0..messages.len() {
            let Some(loro::ValueOrContainer::Container(loro::Container::Map(entry))) =
                messages.get(i)
            else {
                continue;
            };
            let id_matches = matches!(
                entry.get("id"),
                Some(loro::ValueOrContainer::Value(LoroValue::String(s))) if s.as_str() == message_id
            );
            if !id_matches {
                continue;
            }
            let Some(loro::ValueOrContainer::Container(loro::Container::List(parts))) =
                entry.get("parts")
            else {
                continue;
            };
            // Idempotent per part id (recovery may re-run on a crash loop).
            for j in 0..parts.len() {
                if let Some(loro::ValueOrContainer::Container(loro::Container::Map(part))) =
                    parts.get(j)
                    && matches!(
                        part.get("id"),
                        Some(loro::ValueOrContainer::Value(LoroValue::String(s))) if s.as_str() == part_id
                    )
                {
                    return Ok(true);
                }
            }
            push_part(
                &parts,
                &MessagePart::Error {
                    id: part_id.to_string(),
                    message: message.to_string(),
                },
            )?;
            self.doc.commit();
            return Ok(true);
        }
        Ok(false)
    }

    /// Mark the input part carrying `request_id` resolved, wherever it lives
    /// (input parts store the request id as their part id). The live-run path
    /// resolves through the entry fold; this direct write is for answers to a
    /// question whose run already died — no fold owns the entry anymore.
    /// Returns `false` when no such part exists.
    pub fn resolve_input(&self, request_id: &str) -> Result<bool, DocError> {
        let messages = self.doc.get_list("messages");
        for i in 0..messages.len() {
            let Some(loro::ValueOrContainer::Container(loro::Container::Map(entry))) =
                messages.get(i)
            else {
                continue;
            };
            let Some(loro::ValueOrContainer::Container(loro::Container::List(parts))) =
                entry.get("parts")
            else {
                continue;
            };
            for j in 0..parts.len() {
                let Some(loro::ValueOrContainer::Container(loro::Container::Map(part))) =
                    parts.get(j)
                else {
                    continue;
                };
                let is_input = matches!(
                    part.get("kind"),
                    Some(loro::ValueOrContainer::Value(LoroValue::String(s))) if s.as_str() == "input"
                );
                let id_matches = matches!(
                    part.get("id"),
                    Some(loro::ValueOrContainer::Value(LoroValue::String(s))) if s.as_str() == request_id
                );
                if is_input && id_matches {
                    part.insert("resolved", true)?;
                    self.doc.commit();
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Export a snapshot (persistence) — `ExportMode::Snapshot`.
    pub fn export_snapshot(&self) -> Result<Vec<u8>, DocError> {
        self.doc
            .export(ExportMode::Snapshot)
            .map_err(|e| DocError::Schema(e.to_string()))
    }
}

fn write_entry_scalar_fields(map: &LoroMap, entry: &SessionMessageEntry) -> Result<(), DocError> {
    map.insert("id", entry.id.as_str())?;
    map.insert(
        "role",
        match entry.role {
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::System => "system",
        },
    )?;
    map.insert("createdAt", entry.created_at)?;
    map.insert("deviceId", entry.device_id.as_str())?;
    if let Some(status) = entry.status {
        map.insert("status", status_str(status))?;
    }
    if let Some(continuation_of) = &entry.continuation_of {
        map.insert("continuationOf", continuation_of.as_str())?;
    }
    Ok(())
}

fn status_str(status: MessageStatus) -> &'static str {
    match status {
        MessageStatus::Streaming => "streaming",
        MessageStatus::Complete => "complete",
        MessageStatus::Aborted => "aborted",
    }
}

/// Append one part map to a parts list; text bodies become LoroText containers.
fn push_part(parts: &LoroList, part: &MessagePart) -> Result<(), DocError> {
    let map = parts.push_container(LoroMap::new())?;
    let doc_part = to_doc_part(part)?;
    map.insert("id", doc_part.id.as_str())?;
    map.insert("kind", doc_part.kind.as_str())?;
    if let Some(text) = &doc_part.text {
        let t = map.insert_container("text", LoroText::new())?;
        t.insert(0, text)?;
    }
    if let Some(call) = &doc_part.call {
        map.insert("call", loro_value_from_json(call))?;
    }
    if let Some(is_error) = doc_part.is_error {
        map.insert("isError", is_error)?;
    }
    if let Some(questions) = &doc_part.questions {
        map.insert("questions", loro_value_from_json(questions))?;
    }
    if let Some(resolved) = doc_part.resolved {
        map.insert("resolved", resolved)?;
    }
    if let Some(message) = &doc_part.message {
        map.insert("message", message.as_str())?;
    }
    Ok(())
}

#[derive(Debug)]
struct EntryMetadata {
    id: String,
    continuation_of: Option<String>,
}

fn message_map_at(messages: &LoroList, index: usize) -> Option<LoroMap> {
    match messages.get(index) {
        Some(loro::ValueOrContainer::Container(loro::Container::Map(map))) => Some(map),
        _ => None,
    }
}

fn optional_map_string(map: &LoroMap, key: &str) -> Result<Option<String>, DocError> {
    match map.get(key) {
        None | Some(loro::ValueOrContainer::Value(LoroValue::Null)) => Ok(None),
        Some(loro::ValueOrContainer::Value(LoroValue::String(value))) => {
            Ok(Some(value.to_string()))
        }
        Some(_) => Err(DocError::Schema(format!("`{key}` is not a string"))),
    }
}

fn required_map_i64(map: &LoroMap, key: &str) -> Result<i64, DocError> {
    match map.get(key) {
        Some(loro::ValueOrContainer::Value(LoroValue::I64(value))) => Ok(value),
        _ => Err(DocError::Schema(format!("missing field `{key}`"))),
    }
}

fn optional_map_bool(map: &LoroMap, key: &str) -> Result<Option<bool>, DocError> {
    match map.get(key) {
        None | Some(loro::ValueOrContainer::Value(LoroValue::Null)) => Ok(None),
        Some(loro::ValueOrContainer::Value(LoroValue::Bool(value))) => Ok(Some(value)),
        Some(_) => Err(DocError::Schema(format!("`{key}` is not a bool"))),
    }
}

fn map_json(map: &LoroMap, key: &str) -> Option<serde_json::Value> {
    match map.get(key) {
        None | Some(loro::ValueOrContainer::Value(LoroValue::Null)) => None,
        Some(value) => Some(value.get_deep_value().to_json_value()),
    }
}

fn required_map_string(map: &LoroMap, key: &str) -> Result<String, DocError> {
    optional_map_string(map, key)?.ok_or_else(|| DocError::Schema(format!("missing field `{key}`")))
}

fn validate_parts_shape(entry: &LoroMap) -> Result<(), DocError> {
    let parts = match entry.get("parts") {
        None => return Ok(()),
        Some(loro::ValueOrContainer::Container(loro::Container::List(parts))) => parts,
        Some(_) => return Err(DocError::Schema("parts is not a list".into())),
    };
    for index in 0..parts.len() {
        let Some(loro::ValueOrContainer::Container(loro::Container::Map(part))) = parts.get(index)
        else {
            return Err(DocError::Schema(format!("malformed part at {index}")));
        };
        required_map_string(&part, "id")?;
        required_map_string(&part, "kind")?;
        match part.get("text") {
            None
            | Some(loro::ValueOrContainer::Value(LoroValue::Null))
            | Some(loro::ValueOrContainer::Value(LoroValue::String(_)))
            | Some(loro::ValueOrContainer::Container(loro::Container::Text(_))) => {}
            Some(_) => return Err(DocError::Schema("text is not a string".into())),
        }
        optional_map_bool(&part, "isError")?;
        optional_map_bool(&part, "resolved")?;
        optional_map_string(&part, "message")?;
    }
    Ok(())
}

fn entry_metadata_from_map(map: &LoroMap) -> Result<EntryMetadata, DocError> {
    let id = required_map_string(map, "id")?;
    match required_map_string(map, "role")?.as_str() {
        "user" | "assistant" | "system" => {}
        other => return Err(DocError::Schema(format!("unknown role `{other}`"))),
    }
    required_map_i64(map, "createdAt")?;
    required_map_string(map, "deviceId")?;
    match optional_map_string(map, "status")?.as_deref() {
        None | Some("streaming" | "complete" | "aborted") => {}
        Some(other) => return Err(DocError::Schema(format!("unknown status `{other}`"))),
    }
    validate_parts_shape(map)?;
    Ok(EntryMetadata {
        id,
        continuation_of: optional_map_string(map, "continuationOf")?,
    })
}

fn entry_from_map(map: &LoroMap) -> Result<SessionMessageEntry, DocError> {
    let id = required_map_string(map, "id")?;
    let role = match required_map_string(map, "role")?.as_str() {
        "user" => MessageRole::User,
        "assistant" => MessageRole::Assistant,
        "system" => MessageRole::System,
        other => return Err(DocError::Schema(format!("unknown role `{other}`"))),
    };
    let created_at = required_map_i64(map, "createdAt")?;
    let device_id = required_map_string(map, "deviceId")?;
    let status = match optional_map_string(map, "status")?.as_deref() {
        None => None,
        Some("streaming") => Some(MessageStatus::Streaming),
        Some("complete") => Some(MessageStatus::Complete),
        Some("aborted") => Some(MessageStatus::Aborted),
        Some(other) => return Err(DocError::Schema(format!("unknown status `{other}`"))),
    };
    let parts = match map.get("parts") {
        None => Vec::new(),
        Some(loro::ValueOrContainer::Container(loro::Container::List(parts))) => {
            let mut decoded = Vec::with_capacity(parts.len());
            for index in 0..parts.len() {
                let Some(loro::ValueOrContainer::Container(loro::Container::Map(part))) =
                    parts.get(index)
                else {
                    return Err(DocError::Schema(format!("malformed part at {index}")));
                };
                decoded.push(part_from_map(&part)?);
            }
            decoded
        }
        Some(_) => return Err(DocError::Schema("parts is not a list".into())),
    };
    Ok(SessionMessageEntry {
        id,
        role,
        parts,
        created_at,
        device_id,
        status,
        continuation_of: optional_map_string(map, "continuationOf")?,
    })
}

fn part_from_map(map: &LoroMap) -> Result<MessagePart, DocError> {
    let id = required_map_string(map, "id")?;
    let kind = required_map_string(map, "kind")?;
    let text = match map.get("text") {
        Some(loro::ValueOrContainer::Container(loro::Container::Text(text))) => {
            Some(text.to_string())
        }
        Some(loro::ValueOrContainer::Value(LoroValue::String(text))) => Some(text.to_string()),
        Some(loro::ValueOrContainer::Value(LoroValue::Null)) | None => None,
        Some(_) => return Err(DocError::Schema("text is not a string".into())),
    };
    let doc_part = DocPartJson {
        id,
        kind,
        text,
        call: map_json(map, "call"),
        is_error: optional_map_bool(map, "isError")?,
        questions: map_json(map, "questions"),
        resolved: optional_map_bool(map, "resolved")?,
        message: optional_map_string(map, "message")?,
    };
    Ok(from_doc_part(doc_part))
}

/// Render-time continuation join at the entry level (`joinContinuations` in TS):
/// concatenate continuation entries' parts onto their root, in list order.
pub fn join_continuation_entries(entries: Vec<SessionMessageEntry>) -> Vec<SessionMessageEntry> {
    if !entries.iter().any(|e| e.continuation_of.is_some()) {
        return entries;
    }
    let mut out: Vec<SessionMessageEntry> = Vec::with_capacity(entries.len());
    let mut root_index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for entry in entries {
        match &entry.continuation_of {
            Some(root_id) => {
                if let Some(&at) = root_index.get(root_id) {
                    out[at].parts.extend(entry.parts);
                } else {
                    // Orphan continuation — surface as its own entry rather than dropping.
                    out.push(entry);
                }
            }
            None => {
                root_index.insert(entry.id.clone(), out.len());
                out.push(entry);
            }
        }
    }
    out
}

/// Compact mirror of the fields already written to Loro.
///
/// In particular, text/error bodies and input questions are not retained here:
/// the caller already owns the folded parts and Loro owns the durable copy. A
/// second full `MessagePart` mirror doubled the steady live-reply footprint and
/// cloned the accumulated text on every commit tick.
#[derive(Debug, Clone, PartialEq)]
enum WrittenPart {
    Text {
        id: String,
        byte_len: usize,
    },
    Tool {
        id: String,
        call: comet_proto::ToolCall,
        is_error: bool,
        resolved: bool,
    },
    Input {
        id: String,
        resolved: bool,
    },
    Error {
        id: String,
        byte_len: usize,
    },
}

impl WrittenPart {
    fn from_part(part: &MessagePart) -> Self {
        match part {
            MessagePart::Text { id, text } => Self::Text {
                id: id.clone(),
                byte_len: text.len(),
            },
            MessagePart::Tool {
                id,
                call,
                is_error,
                resolved,
            } => Self::Tool {
                id: id.clone(),
                call: call.clone(),
                is_error: *is_error,
                resolved: *resolved,
            },
            MessagePart::Input { id, resolved, .. } => Self::Input {
                id: id.clone(),
                resolved: *resolved,
            },
            MessagePart::Error { id, message } => Self::Error {
                id: id.clone(),
                byte_len: message.len(),
            },
        }
    }

    fn matches(&self, part: &MessagePart) -> bool {
        match (self, part) {
            (Self::Text { id, byte_len }, MessagePart::Text { id: next_id, text }) => {
                id == next_id && *byte_len == text.len()
            }
            (
                Self::Tool {
                    id,
                    call,
                    is_error,
                    resolved,
                },
                MessagePart::Tool {
                    id: next_id,
                    call: next_call,
                    is_error: next_error,
                    resolved: next_resolved,
                },
            ) => {
                id == next_id
                    && call == next_call
                    && is_error == next_error
                    && resolved == next_resolved
            }
            (
                Self::Input { id, resolved },
                MessagePart::Input {
                    id: next_id,
                    resolved: next_resolved,
                    ..
                },
            ) => id == next_id && resolved == next_resolved,
            (
                Self::Error { id, byte_len },
                MessagePart::Error {
                    id: next_id,
                    message,
                },
            ) => id == next_id && *byte_len == message.len(),
            _ => false,
        }
    }
}

/// Incremental streaming writer for one assistant entry.
///
/// Port of comet's `DocSegmentWriter` diff discipline: called with the *folded* parts of the
/// live segment (from `fold_event_into_parts`) at each commit tick, it diffs against what's in
/// the doc and writes only the delta:
/// - trailing text growth → `LoroText` append (RLE-merged),
/// - new parts → pushed,
/// - tool call refresh / resolution / input resolution → in-place map updates.
///
/// Invariant relied upon: the fold only ever APPENDS parts or grows the trailing text; earlier
/// text never mutates. Tool/input parts may update fields in place.
pub struct SegmentWriter<'a> {
    doc: &'a SessionDoc,
    /// Index of this entry in the `messages` list.
    entry_index: usize,
    /// Compact mirror of what has been written so far.
    written: Vec<WrittenPart>,
}

impl<'a> SegmentWriter<'a> {
    /// Begin a streaming assistant entry: pushes the entry with `status: streaming`, no parts.
    pub fn begin(
        doc: &'a SessionDoc,
        entry_id: &str,
        device_id: &str,
        created_at: i64,
    ) -> Result<Self, DocError> {
        let messages = doc.doc.get_list("messages");
        let entry_index = messages.len();
        let map = messages.push_container(LoroMap::new())?;
        write_entry_scalar_fields(
            &map,
            &SessionMessageEntry {
                id: entry_id.into(),
                role: MessageRole::Assistant,
                parts: vec![],
                created_at,
                device_id: device_id.into(),
                status: Some(MessageStatus::Streaming),
                continuation_of: None,
            },
        )?;
        map.insert_container("parts", LoroList::new())?;
        doc.doc.commit();
        Ok(Self {
            doc,
            entry_index,
            written: Vec::new(),
        })
    }

    fn entry_map(&self) -> Result<LoroMap, DocError> {
        let messages = self.doc.doc.get_list("messages");
        match messages.get(self.entry_index) {
            Some(loro::ValueOrContainer::Container(loro::Container::Map(map))) => Ok(map),
            _ => Err(DocError::Schema("streaming entry map missing".into())),
        }
    }

    fn parts_list(&self) -> Result<LoroList, DocError> {
        match self.entry_map()?.get("parts") {
            Some(loro::ValueOrContainer::Container(loro::Container::List(list))) => Ok(list),
            _ => Err(DocError::Schema(
                "streaming entry parts list missing".into(),
            )),
        }
    }

    /// Diff `folded` (the full folded segment so far) into the doc.
    pub fn sync(&mut self, folded: &[MessagePart]) -> Result<(), DocError> {
        let parts = self.parts_list()?;
        let mut dirty = false;

        for (i, part) in folded.iter().enumerate() {
            match self.written.get(i) {
                None => {
                    push_part(&parts, part)?;
                    self.written.push(WrittenPart::from_part(part));
                    dirty = true;
                }
                Some(prev) if prev.matches(part) => {}
                Some(prev) => {
                    match (prev, part) {
                        (
                            WrittenPart::Text {
                                id,
                                byte_len: old_len,
                            },
                            MessagePart::Text {
                                id: next_id,
                                text: new,
                            },
                        ) if id == next_id
                            && *old_len <= new.len()
                            && new.is_char_boundary(*old_len) =>
                        {
                            // Trailing-text growth: append the suffix into the LoroText.
                            let delta = &new[*old_len..];
                            if !delta.is_empty() {
                                let part_map = part_map_at(&parts, i)?;
                                match part_map.get("text") {
                                    Some(loro::ValueOrContainer::Container(
                                        loro::Container::Text(t),
                                    )) => {
                                        let len = t.len_unicode();
                                        t.insert(len, delta)?;
                                    }
                                    _ => {
                                        return Err(DocError::Schema(
                                            "text part missing LoroText".into(),
                                        ));
                                    }
                                }
                                dirty = true;
                            }
                        }
                        _ => {
                            // Field-level update (tool refresh/resolve, input resolve, or a
                            // non-append text rewrite, which the fold shouldn't produce —
                            // rewrite the part map fields defensively).
                            let part_map = part_map_at(&parts, i)?;
                            update_part_fields(&part_map, part)?;
                            dirty = true;
                        }
                    }
                    self.written[i] = WrittenPart::from_part(part);
                }
            }
        }

        if dirty {
            self.doc.doc.commit();
        }
        Ok(())
    }

    /// Finish the stream: sync final parts and stamp a terminal status.
    pub fn finish(mut self, folded: &[MessagePart], status: MessageStatus) -> Result<(), DocError> {
        self.sync(folded)?;
        let map = self.entry_map()?;
        map.insert("status", status_str(status))?;
        self.doc.doc.commit();
        Ok(())
    }
}

fn part_map_at(parts: &LoroList, index: usize) -> Result<LoroMap, DocError> {
    match parts.get(index) {
        Some(loro::ValueOrContainer::Container(loro::Container::Map(map))) => Ok(map),
        _ => Err(DocError::Schema(format!("part map missing at {index}"))),
    }
}

/// In-place field refresh for tool/input parts (and defensive text rewrite).
fn update_part_fields(map: &LoroMap, part: &MessagePart) -> Result<(), DocError> {
    let doc_part = to_doc_part(part)?;
    if let Some(call) = &doc_part.call {
        map.insert("call", loro_value_from_json(call))?;
    }
    if let Some(is_error) = doc_part.is_error {
        map.insert("isError", is_error)?;
    }
    if let Some(questions) = &doc_part.questions {
        map.insert("questions", loro_value_from_json(questions))?;
    }
    if let Some(resolved) = doc_part.resolved {
        map.insert("resolved", resolved)?;
    }
    if let Some(message) = &doc_part.message {
        map.insert("message", message.as_str())?;
    }
    if let Some(text) = &doc_part.text {
        // Defensive path only — the fold never rewrites earlier text.
        if let Some(loro::ValueOrContainer::Container(loro::Container::Text(t))) = map.get("text") {
            t.update(text, Default::default())
                .map_err(|e| DocError::Schema(e.to_string()))?;
        }
    }
    Ok(())
}

fn loro_value_from_json(v: &serde_json::Value) -> LoroValue {
    LoroValue::from(v.clone())
}

/// Tail sidecar shape (`SessionTail` in TS).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionTail {
    pub chat_id: String,
    pub schema_version: u32,
    pub messages: Vec<SessionMessageEntry>,
    pub total_messages: usize,
    pub updated_at: i64,
}

/// Materialize the last-N joined messages (`materializeTail` in TS).
pub fn materialize_tail(
    doc: &SessionDoc,
    now: i64,
    tail_count: usize,
) -> Result<SessionTail, DocError> {
    let limit = if tail_count == 0 {
        TAIL_MESSAGE_COUNT
    } else {
        tail_count
    };
    let (messages, total) = doc.read_joined_tail_with_total(limit)?;
    Ok(SessionTail {
        chat_id: doc.chat_id().unwrap_or_default(),
        schema_version: SESSION_SCHEMA_VERSION,
        messages,
        total_messages: total,
        updated_at: now,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parts::fold_event_into_parts;
    use comet_proto::{AgentEvent, ToolCall};

    fn user_entry(id: &str, text: &str) -> SessionMessageEntry {
        SessionMessageEntry {
            id: id.into(),
            role: MessageRole::User,
            parts: vec![MessagePart::Text {
                id: "t0".into(),
                text: text.into(),
            }],
            created_at: 1,
            device_id: "dev-a".into(),
            status: Some(MessageStatus::Complete),
            continuation_of: None,
        }
    }

    #[test]
    fn round_trips_message_entries() {
        let doc = SessionDoc::init("chat-1").unwrap();
        doc.push_message(&user_entry("m1", "hello")).unwrap();
        let entries = doc.read_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "m1");
        assert_eq!(
            entries[0].parts,
            vec![MessagePart::Text {
                id: "t0".into(),
                text: "hello".into()
            }]
        );
        assert_eq!(doc.chat_id().as_deref(), Some("chat-1"));
    }

    #[test]
    fn read_entries_tail_reads_only_newest_entries() {
        let doc = SessionDoc::init("chat-1").unwrap();
        for i in 0..5 {
            doc.push_message(&user_entry(&format!("m{i}"), &format!("msg {i}")))
                .unwrap();
        }
        let entries = doc.read_entries_tail(2).unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            ["m3", "m4"]
        );
    }

    #[test]
    fn message_id_metadata_reads_do_not_decode_bodies() {
        let doc = SessionDoc::init("chat-1").unwrap();
        doc.push_message(&user_entry("m0", &"x".repeat(1024 * 1024)))
            .unwrap();
        doc.push_message(&user_entry("m1", "tail")).unwrap();

        assert!(doc.contains_message_id("m0"));
        assert!(!doc.contains_message_id("missing"));
        assert_eq!(doc.message_ids(), ["m0", "m1"]);
        assert_eq!(doc.last_message_id().as_deref(), Some("m1"));
    }

    #[test]
    fn resolve_input_stamps_the_part_in_place() {
        let doc = SessionDoc::init("chat-1").unwrap();
        doc.push_message(&SessionMessageEntry {
            id: "m1".into(),
            role: MessageRole::Assistant,
            parts: vec![MessagePart::Input {
                id: "r1".into(),
                request_id: "r1".into(),
                questions: vec![],
                resolved: false,
            }],
            created_at: 1,
            device_id: "dev-a".into(),
            // The orphan case: the run died and recovery stamped the entry.
            status: Some(MessageStatus::Aborted),
            continuation_of: None,
        })
        .unwrap();
        assert!(!doc.resolve_input("nope").unwrap());
        assert!(doc.resolve_input("r1").unwrap());
        let entries = doc.read_entries().unwrap();
        assert!(matches!(
            &entries[0].parts[0],
            MessagePart::Input { resolved: true, .. }
        ));
    }

    #[test]
    fn snapshot_round_trips_between_docs() {
        let doc = SessionDoc::init("chat-1").unwrap();
        doc.push_message(&user_entry("m1", "hello")).unwrap();
        let bytes = doc.export_snapshot().unwrap();

        let other = LoroDoc::new();
        other.import(&bytes).unwrap();
        let restored = SessionDoc::from_doc(other);
        assert_eq!(
            restored.read_entries().unwrap(),
            doc.read_entries().unwrap()
        );
    }

    #[test]
    fn two_peers_converge_on_concurrent_inserts() {
        let a = SessionDoc::init("chat-1").unwrap();
        let b = SessionDoc::from_doc({
            let d = LoroDoc::new();
            d.import(&a.export_snapshot().unwrap()).unwrap();
            d
        });
        a.push_message(&user_entry("m-a", "from a")).unwrap();
        b.push_message(&user_entry("m-b", "from b")).unwrap();

        // Cross-import updates.
        let a_update = a
            .doc()
            .export(ExportMode::updates(&b.doc().oplog_vv()))
            .unwrap();
        let b_update = b
            .doc()
            .export(ExportMode::updates(&a.doc().oplog_vv()))
            .unwrap();
        b.doc().import(&a_update).unwrap();
        a.doc().import(&b_update).unwrap();

        let ea = a.read_entries().unwrap();
        let eb = b.read_entries().unwrap();
        assert_eq!(ea, eb);
        assert_eq!(ea.len(), 2); // one insert from each peer, converged in the same order
    }

    #[test]
    fn segment_writer_streams_text_incrementally() {
        let doc = SessionDoc::init("chat-1").unwrap();
        let mut writer = SegmentWriter::begin(&doc, "a1", "dev-a", 5).unwrap();

        let mut folded = Vec::new();
        folded = fold_event_into_parts(&folded, &AgentEvent::TextDelta { text: "Hel".into() });
        writer.sync(&folded).unwrap();
        folded = fold_event_into_parts(&folded, &AgentEvent::TextDelta { text: "lo".into() });
        writer.sync(&folded).unwrap();
        folded = fold_event_into_parts(
            &folded,
            &AgentEvent::ToolCall {
                id: "tool-1".into(),
                call: ToolCall::Exec {
                    command: "ls".into(),
                },
            },
        );
        writer.sync(&folded).unwrap();
        folded = fold_event_into_parts(
            &folded,
            &AgentEvent::ToolResult {
                id: "tool-1".into(),
                is_error: false,
            },
        );
        writer.sync(&folded).unwrap();
        writer.finish(&folded, MessageStatus::Complete).unwrap();

        let entries = doc.read_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].status, Some(MessageStatus::Complete));
        assert_eq!(entries[0].parts.len(), 2);
        match &entries[0].parts[0] {
            MessagePart::Text { text, .. } => assert_eq!(text, "Hello"),
            other => panic!("unexpected {other:?}"),
        }
        match &entries[0].parts[1] {
            MessagePart::Tool {
                resolved, is_error, ..
            } => {
                assert!(*resolved);
                assert!(!*is_error);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn segment_writer_keeps_only_text_length_in_its_mirror() {
        let doc = SessionDoc::init("chat-1").unwrap();
        let mut writer = SegmentWriter::begin(&doc, "a1", "dev-a", 5).unwrap();
        let mut text = "x".repeat(512 * 1024);
        let mut folded = vec![MessagePart::Text {
            id: "t0".into(),
            text: text.clone(),
        }];
        writer.sync(&folded).unwrap();
        assert!(matches!(
            writer.written.as_slice(),
            [WrittenPart::Text { byte_len, .. }] if *byte_len == text.len()
        ));

        text.push_str(" tail");
        let MessagePart::Text { text: live, .. } = &mut folded[0] else {
            unreachable!();
        };
        live.push_str(" tail");
        writer.sync(&folded).unwrap();
        assert!(matches!(
            writer.written.as_slice(),
            [WrittenPart::Text { byte_len, .. }] if *byte_len == text.len()
        ));

        writer.finish(&folded, MessageStatus::Complete).unwrap();
        let entries = doc.read_entries().unwrap();
        assert!(matches!(
            &entries[0].parts[0],
            MessagePart::Text { text: persisted, .. } if persisted == &text
        ));
    }

    #[test]
    fn set_message_status_stamps_existing_entry() {
        let doc = SessionDoc::init("chat-1").unwrap();
        let mut entry = user_entry("m1", "hello");
        entry.role = MessageRole::Assistant;
        entry.status = Some(MessageStatus::Streaming);
        doc.push_message(&entry).unwrap();

        assert!(
            doc.set_message_status("m1", MessageStatus::Aborted)
                .unwrap()
        );
        assert!(
            !doc.set_message_status("nope", MessageStatus::Aborted)
                .unwrap()
        );
        let entries = doc.read_entries().unwrap();
        assert_eq!(entries[0].status, Some(MessageStatus::Aborted));
    }

    #[test]
    fn command_queue_and_outcome_round_trip() {
        use crate::commands::{SessionCommandPayload, SessionCommandStatus};
        let doc = SessionDoc::init("chat-1").unwrap();
        let entry = SessionCommandEntry {
            id: "c1".into(),
            payload: SessionCommandPayload::Steer {
                prompt: "focus".into(),
                message_id: None,
            },
            issued_by: "dev-b".into(),
            issued_at: 10,
            based_on: None,
            expires_at: None,
            status: SessionCommandStatus::Pending,
            resolution: None,
        };
        doc.queue_command(&entry).unwrap();
        doc.set_command_status("c1", SessionCommandStatus::Applied, None)
            .unwrap();
        let commands = doc.read_commands().unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].status, SessionCommandStatus::Applied);
        assert_eq!(commands[0].payload, entry.payload);
    }

    #[test]
    fn tail_materializes_last_n_joined() {
        let doc = SessionDoc::init("chat-1").unwrap();
        for i in 0..5 {
            doc.push_message(&user_entry(&format!("m{i}"), &format!("msg {i}")))
                .unwrap();
        }
        let tail = materialize_tail(&doc, 99, 2).unwrap();
        assert_eq!(tail.total_messages, 5);
        assert_eq!(tail.messages.len(), 2);
        assert_eq!(tail.messages[1].id, "m4");
        assert_eq!(tail.chat_id, "chat-1");
    }

    #[test]
    fn joined_tail_decodes_only_selected_roots_and_their_continuations() {
        let doc = SessionDoc::init("chat-1").unwrap();
        let push = |id: &str, text: &str, continuation_of: Option<&str>| {
            let mut entry = user_entry(id, text);
            entry.continuation_of = continuation_of.map(str::to_owned);
            doc.push_message(&entry).unwrap();
        };
        push("m0", "zero", None);
        push("m1", "one", None);
        push("c0", "zero tail", Some("m0"));
        push("c1", "one tail", Some("m1"));
        push("m2", "two", None);

        let tail = materialize_tail(&doc, 99, 2).unwrap();
        assert_eq!(tail.total_messages, 3);
        assert_eq!(
            tail.messages
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            ["m1", "m2"]
        );
        assert_eq!(tail.messages[0].parts.len(), 2);
        assert!(matches!(
            &tail.messages[0].parts[1],
            MessagePart::Text { text, .. } if text == "one tail"
        ));
        assert_eq!(tail.messages[1].parts.len(), 1);
    }

    #[test]
    fn joined_tail_matches_orphan_and_duplicate_root_semantics() {
        let doc = SessionDoc::init("chat-1").unwrap();
        let push = |id: &str, text: &str, continuation_of: Option<&str>| {
            let mut entry = user_entry(id, text);
            entry.continuation_of = continuation_of.map(str::to_owned);
            doc.push_message(&entry).unwrap();
        };
        // An orphan continuation is its own joined row and does not become a
        // root. The next continuation targeting its id is therefore another
        // orphan row.
        push("orphan", "first", Some("missing"));
        push("orphan-child", "second", Some("orphan"));
        // Duplicate root ids are legal under convergence; subsequent
        // continuations target the most recently inserted duplicate.
        push("dup", "old", None);
        push("dup", "new", None);
        push("dup-tail", "new tail", Some("dup"));

        let expected = join_continuation_entries(doc.read_entries().unwrap());
        let actual = doc.read_joined_entries_tail(3).unwrap();
        assert_eq!(actual, expected[expected.len() - 3..]);
        assert_eq!(actual[2].parts.len(), 2);
        assert!(matches!(
            &actual[2].parts[1],
            MessagePart::Text { text, .. } if text == "new tail"
        ));
    }
}
