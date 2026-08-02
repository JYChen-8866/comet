//! Resident Aurin document references.
//!
//! Once a chat message `@`'s a workspace document, that reference stays
//! attached to every later run in the same chat: users pin a document once
//! instead of re-@-ing it on each turn. The persisted message text
//! (`@[title](aurin://doc/{node_id}/{content_id})`) is the source of truth,
//! so restarts and crash recovery rebuild the same registry without extra
//! state.

use comet_doc::{MessagePart, MessageRole, SessionMessageEntry};
use comet_proto::{DocumentRef, RunRequest, document_refs_from_text};

/// Fold every `@`-ed document reference found in `entries` into `request`.
///
/// References attached to the current request (or parsed from its prompt)
/// take precedence on `content_id` conflicts; older mentions are appended in
/// transcript order and deduplicated by `content_id`.
pub(crate) fn merge_resident_document_refs(
    request: &mut RunRequest,
    entries: &[SessionMessageEntry],
) {
    let mut merged: Vec<DocumentRef> = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for reference in document_refs_from_text(&request.prompt)
        .into_iter()
        .chain(request.document_refs.iter().cloned())
    {
        if seen.insert(reference.content_id.clone()) {
            merged.push(reference);
        }
    }

    for entry in entries {
        if entry.role != MessageRole::User {
            continue;
        }
        for part in &entry.parts {
            let MessagePart::Text { text, .. } = part else {
                continue;
            };
            for reference in document_refs_from_text(text) {
                if seen.insert(reference.content_id.clone()) {
                    merged.push(reference);
                }
            }
        }
    }

    request.document_refs = merged;
}

#[cfg(test)]
mod tests {
    use comet_doc::{MessageStatus, SessionMessageEntry};
    use comet_proto::SandboxLevel;

    use super::*;

    fn text_entry(id: &str, role: MessageRole, text: &str) -> SessionMessageEntry {
        SessionMessageEntry {
            id: id.to_owned(),
            role,
            parts: vec![MessagePart::Text {
                id: "p".into(),
                text: text.to_owned(),
            }],
            created_at: 0,
            device_id: "test".into(),
            status: Some(MessageStatus::Complete),
            continuation_of: None,
        }
    }

    fn request(prompt: &str) -> RunRequest {
        RunRequest {
            prompt: prompt.into(),
            model: None,
            reasoning: None,
            model_options: Default::default(),
            context: Vec::new(),
            cwd: "/tmp".into(),
            sandbox: SandboxLevel::WorkspaceWrite,
            auto_approve: false,
            attachments: Vec::new(),
            document_refs: Vec::new(),
            resume: None,
        }
    }

    #[test]
    fn keeps_prior_mentions_resident() {
        let mut request = request("再改一下");
        let entries = vec![
            text_entry(
                "u1",
                MessageRole::User,
                "@[需求文档](aurin://doc/n1/c1) 帮我改一下",
            ),
            text_entry("a1", MessageRole::Assistant, "改好了"),
            text_entry(
                "u2",
                MessageRole::User,
                "@[周报](aurin://doc/n2/c2) 也看一下",
            ),
        ];

        merge_resident_document_refs(&mut request, &entries);

        let ids: Vec<_> = request
            .document_refs
            .iter()
            .map(|reference| reference.content_id.as_str())
            .collect();
        assert_eq!(ids, vec!["c1", "c2"]);
        assert_eq!(request.document_refs[0].title, "需求文档");
        assert_eq!(request.document_refs[1].node_id, "n2");
    }

    #[test]
    fn current_refs_win_and_dedupe() {
        let mut request = request("按新标题处理");
        request.document_refs = vec![
            DocumentRef {
                node_id: "n1".into(),
                content_id: "c1".into(),
                title: "需求文档-新".into(),
            },
            DocumentRef {
                node_id: "n3".into(),
                content_id: "c3".into(),
                title: "会议纪要".into(),
            },
        ];
        let entries = vec![
            text_entry(
                "u1",
                MessageRole::User,
                "@[需求文档-旧](aurin://doc/n1/c1) 帮我改一下",
            ),
            text_entry(
                "u2",
                MessageRole::User,
                "@[周报](aurin://doc/n2/c2) 也看一下",
            ),
        ];

        merge_resident_document_refs(&mut request, &entries);

        let refs = request.document_refs;
        assert_eq!(refs[0].title, "需求文档-新");
        assert_eq!(refs[0].content_id, "c1");
        assert_eq!(refs[1].content_id, "c3");
        assert_eq!(refs[2].content_id, "c2");
    }

    #[test]
    fn parses_prompt_tokens_when_structured_refs_missing() {
        let mut request = request("@[需求文档](aurin://doc/n1/c1) 帮我改一下");
        let entries = vec![text_entry(
            "u1",
            MessageRole::User,
            "@[周报](aurin://doc/n2/c2) 也看一下",
        )];

        merge_resident_document_refs(&mut request, &entries);

        assert_eq!(request.document_refs.len(), 2);
        assert_eq!(request.document_refs[0].content_id, "c1");
        assert_eq!(request.document_refs[1].content_id, "c2");
    }
}
