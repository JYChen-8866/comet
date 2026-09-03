//! Per-session on-disk event journal (port of comet's `run-journal.ts`, JSONL-shaped).
//!
//! One append-only JSONL file per chat under `{data_dir}/journals/{chat_id}.jsonl`; each
//! line is `{"seq": n, "event": AgentEvent}` with a monotonically increasing `seq`. The
//! journal is the durable replay source for live streams (`Subscribe` = replay then tail
//! the broadcast hub) and the crash-recovery gauge: a journal whose LAST event is not
//! `Done` belongs to a run that died mid-stream — boot recovery stamps its doc entry
//! `aborted` and closes the journal with a synthetic `Done`.
//!
//! Bounded-window compaction is deferred (whole file kept for now, per M2 scope); a torn
//! trailing line from a crash mid-write is tolerated everywhere.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use serde::{Deserialize, Serialize};

use comet_proto::AgentEvent;

#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("journal {what} exceeds the {limit} limit")]
    Limit { what: &'static str, limit: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalLine {
    seq: u64,
    event: AgentEvent,
}

#[derive(Serialize)]
struct JournalLineRef<'a> {
    seq: u64,
    event: &'a AgentEvent,
}

struct ChatJournal {
    file: File,
    next_seq: u64,
    event_count: usize,
    file_bytes: u64,
    /// Monotonic access stamp used by the bounded descriptor cache. The journal
    /// itself remains durable on disk; this only controls which idle `File`
    /// handles stay open between appends.
    last_used: u64,
    /// True when the file ends without a newline (torn write) — the next append
    /// starts with one so the torn line stays isolated.
    needs_newline: bool,
}

/// Append-only JSONL journal store, one file per chat.
pub struct RunJournal {
    dir: PathBuf,
    open_files: Mutex<HashMap<String, ChatJournal>>,
    access_clock: Mutex<u64>,
}

// A document can have thousands of chats, but keeping one descriptor and one
// kernel file object for every chat is unnecessary. Keep a modest hot set and
// reopen cold journals on demand. This bounds FD-backed kernel memory without
// changing the on-disk journal or replay semantics.
const MAX_OPEN_JOURNALS: usize = 64;
/// Journals are diagnostic/replay state; the durable SessionDoc remains the
/// source of truth for the transcript. Refuse pathological files before they
/// can turn a replay or crash-recovery scan into an unbounded allocation.
const MAX_JOURNAL_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_JOURNAL_LINE_BYTES: usize = 4 * 1024 * 1024;
const MAX_JOURNAL_EVENTS: usize = 100_000;

impl RunJournal {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, JournalError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            open_files: Mutex::new(HashMap::new()),
            access_clock: Mutex::new(0),
        })
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, ChatJournal>> {
        self.open_files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn path_for(&self, chat_id: &str) -> PathBuf {
        self.dir.join(format!("{}.jsonl", sanitize_id(chat_id)))
    }

    fn attempts_path(&self, chat_id: &str) -> PathBuf {
        self.dir.join(format!("{}.resume", sanitize_id(chat_id)))
    }

    fn next_access_stamp(&self) -> u64 {
        let mut clock = self
            .access_clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *clock = clock.saturating_add(1);
        *clock
    }

    fn trim_open_files(files: &mut HashMap<String, ChatJournal>, current_chat: &str) {
        while files.len() > MAX_OPEN_JOURNALS {
            let Some(oldest) = files
                .iter()
                .filter(|(chat_id, _)| chat_id.as_str() != current_chat)
                .min_by_key(|(_, journal)| journal.last_used)
                .map(|(chat_id, _)| chat_id.clone())
            else {
                // `current_chat` is always present, so this is only a defensive
                // guard against a future caller changing the insertion order.
                break;
            };
            files.remove(&oldest);
        }
    }

    /// Auto-resume revival budget (comet `resumeAttempt`/`MAX_AUTO_RESUME`):
    /// persisted beside the journal so a run that CRASHES THE ENGINE cannot
    /// revive itself in an infinite boot loop.
    pub fn resume_attempts(&self, chat_id: &str) -> u32 {
        std::fs::read_to_string(self.attempts_path(chat_id))
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    }

    pub fn note_resume_attempt(&self, chat_id: &str) -> u32 {
        let next = self.resume_attempts(chat_id) + 1;
        if let Err(err) = std::fs::write(self.attempts_path(chat_id), next.to_string()) {
            tracing::warn!(chat = %chat_id, error = %err, "resume-attempt ledger write failed");
        }
        next
    }

    /// A cleanly completed turn resets the budget — only consecutive
    /// crash-revive-crash cycles exhaust it.
    pub fn clear_resume_attempts(&self, chat_id: &str) {
        let _ = std::fs::remove_file(self.attempts_path(chat_id));
    }

    /// Append one event; returns its journal seq.
    pub fn append(&self, chat_id: &str, event: &AgentEvent) -> Result<u64, JournalError> {
        let mut files = self.lock();
        let access_stamp = self.next_access_stamp();
        if !files.contains_key(chat_id) {
            let path = self.path_for(chat_id);
            let (next_seq, event_count, file_bytes, needs_newline) = scan_tail(&path)?;
            let file = OpenOptions::new().create(true).append(true).open(&path)?;
            files.insert(
                chat_id.to_string(),
                ChatJournal {
                    file,
                    next_seq,
                    event_count,
                    file_bytes,
                    needs_newline,
                    last_used: access_stamp,
                },
            );
            // Limit failures below return before the success-path trim. Bound
            // descriptors immediately so repeated rejected first appends for
            // distinct chats cannot grow the kernel/file-handle hot set.
            Self::trim_open_files(&mut files, chat_id);
        }
        // Entry guaranteed present; avoid unwrap in a library path regardless.
        let Some(journal) = files.get_mut(chat_id) else {
            return Err(JournalError::Io(std::io::Error::other(
                "journal entry vanished under lock",
            )));
        };
        let seq = journal.next_seq;
        let mut buf = Vec::with_capacity(4096);
        if journal.needs_newline {
            buf.push(b'\n');
        }
        let record_start = buf.len();
        serde_json::to_writer(&mut buf, &JournalLineRef { seq, event })?;
        if buf.len().saturating_sub(record_start).saturating_add(1) > MAX_JOURNAL_LINE_BYTES {
            return Err(JournalError::Limit {
                what: "line",
                limit: format!("{} MiB", MAX_JOURNAL_LINE_BYTES / (1024 * 1024)),
            });
        }
        buf.push(b'\n');
        if journal.event_count >= MAX_JOURNAL_EVENTS {
            return Err(JournalError::Limit {
                what: "events",
                limit: MAX_JOURNAL_EVENTS.to_string(),
            });
        }
        if journal.file_bytes.saturating_add(buf.len() as u64) > MAX_JOURNAL_FILE_BYTES {
            return Err(JournalError::Limit {
                what: "file",
                limit: format!("{} MiB", MAX_JOURNAL_FILE_BYTES / (1024 * 1024)),
            });
        }
        journal.file.write_all(&buf)?;
        journal.file.flush()?;
        journal.needs_newline = false;
        journal.next_seq = seq + 1;
        journal.event_count += 1;
        journal.file_bytes = journal.file_bytes.saturating_add(buf.len() as u64);
        journal.last_used = access_stamp;
        Self::trim_open_files(&mut files, chat_id);
        Ok(seq)
    }

    /// Events with `seq > after_seq`, in order. A cursor ahead of the last issued seq is
    /// from a previous era (file replaced) — falls back to a full replay, mirroring comet.
    pub fn replay(
        &self,
        chat_id: &str,
        after_seq: u64,
    ) -> Result<Vec<(u64, AgentEvent)>, JournalError> {
        let path = self.path_for(chat_id);
        let mut last_seq = 0;
        self.scan_path(&path, |seq, _| {
            last_seq = seq;
            Ok(())
        })?;
        let from = if after_seq > last_seq { 0 } else { after_seq };
        let mut replay = Vec::new();
        self.scan_path(&path, |seq, event| {
            if seq > from {
                replay.push((seq, event));
            }
            Ok(())
        })?;
        Ok(replay)
    }

    /// The last event in a chat's journal, if any (ignores a torn tail line).
    pub fn last_event(&self, chat_id: &str) -> Result<Option<(u64, AgentEvent)>, JournalError> {
        let path = self.path_for(chat_id);
        let mut last = None;
        self.scan_path(&path, |seq, event| {
            last = Some((seq, event));
            Ok(())
        })?;
        Ok(last)
    }

    /// Crash-recovery scan: chat ids whose journal's last event is NOT a `Done` — their
    /// runs died mid-stream and need recovery (stamp `aborted`, close the journal).
    pub fn stale_sessions(&self) -> Result<Vec<String>, JournalError> {
        let mut stale = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                continue;
            }
            let Some(chat_id) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let mut last = None;
            self.scan_path(&path, |seq, event| {
                last = Some((seq, event));
                Ok(())
            })?;
            match last {
                Some((_, AgentEvent::Done { .. })) | None => {}
                Some(_) => stale.push(chat_id.to_string()),
            }
        }
        stale.sort();
        Ok(stale)
    }

    /// Remove a chat's journal file entirely (tests / future compaction).
    pub fn discard(&self, chat_id: &str) -> Result<(), JournalError> {
        self.lock().remove(chat_id);
        let path = self.path_for(chat_id);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    /// Visit valid journal events without materializing the complete history.
    /// This is used by crash recovery and resume lookup, where only a small
    /// amount of state is needed from an arbitrarily long journal.
    pub(crate) fn scan<F>(&self, chat_id: &str, visitor: F) -> Result<(), JournalError>
    where
        F: FnMut(u64, AgentEvent) -> Result<(), JournalError>,
    {
        self.scan_path(&self.path_for(chat_id), visitor)
    }

    fn scan_path<F>(&self, path: &Path, mut visitor: F) -> Result<(), JournalError>
    where
        F: FnMut(u64, AgentEvent) -> Result<(), JournalError>,
    {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let metadata = file.metadata()?;
        if metadata.len() > MAX_JOURNAL_FILE_BYTES {
            return Err(JournalError::Limit {
                what: "file",
                limit: format!("{} MiB", MAX_JOURNAL_FILE_BYTES / (1024 * 1024)),
            });
        }
        let mut reader = BufReader::new(file.take(MAX_JOURNAL_FILE_BYTES + 1));
        let mut line = Vec::with_capacity(4096);
        let mut total_bytes = 0u64;
        let mut event_count = 0usize;
        while let Some((read_bytes, oversized)) = read_bounded_line(&mut reader, &mut line)? {
            total_bytes = total_bytes.saturating_add(read_bytes as u64);
            if total_bytes > MAX_JOURNAL_FILE_BYTES {
                return Err(JournalError::Limit {
                    what: "file",
                    limit: format!("{} MiB", MAX_JOURNAL_FILE_BYTES / (1024 * 1024)),
                });
            }
            if oversized {
                tracing::warn!(path = %path.display(), "journal: skipping oversized line");
                continue;
            }
            if line.iter().all(|byte| byte.is_ascii_whitespace()) {
                continue;
            }
            let parsed = match serde_json::from_slice::<JournalLine>(&line) {
                Ok(parsed) => parsed,
                Err(error) => {
                    tracing::warn!(path = %path.display(), error = %error, "journal: skipping malformed line");
                    continue;
                }
            };
            event_count = event_count.saturating_add(1);
            if event_count > MAX_JOURNAL_EVENTS {
                return Err(JournalError::Limit {
                    what: "events",
                    limit: MAX_JOURNAL_EVENTS.to_string(),
                });
            }
            visitor(parsed.seq, parsed.event)?;
        }
        Ok(())
    }
}

/// Read one newline-delimited record while never allocating more than the
/// per-line budget. Oversized records are consumed and reported to the caller
/// so a hostile/torn line cannot retain a multi-megabyte temporary `String`.
fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    line: &mut Vec<u8>,
) -> std::io::Result<Option<(usize, bool)>> {
    line.clear();
    let mut consumed = 0usize;
    let mut oversized = false;
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            return if consumed == 0 {
                Ok(None)
            } else {
                Ok(Some((consumed, oversized)))
            };
        }
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(buffer.len(), |position| position + 1);
        if !oversized {
            let remaining = MAX_JOURNAL_LINE_BYTES.saturating_sub(line.len());
            let copy = take.min(remaining.saturating_add(1));
            line.extend_from_slice(&buffer[..copy]);
            if line.len() > MAX_JOURNAL_LINE_BYTES {
                oversized = true;
                line.clear();
            }
        }
        reader.consume(take);
        consumed = consumed.saturating_add(take);
        if newline.is_some() {
            return Ok(Some((consumed, oversized)));
        }
    }
}

/// Next seq, valid event count, physical byte length, and torn-tail state.
fn scan_tail(path: &Path) -> Result<(u64, usize, u64, bool), JournalError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((1, 0, 0, false)),
        Err(error) => return Err(error.into()),
    };
    let metadata = file.metadata()?;
    if metadata.len() > MAX_JOURNAL_FILE_BYTES {
        return Err(JournalError::Limit {
            what: "file",
            limit: format!("{} MiB", MAX_JOURNAL_FILE_BYTES / (1024 * 1024)),
        });
    }
    let needs_newline = if metadata.len() == 0 {
        false
    } else {
        let mut file = file;
        file.seek(SeekFrom::End(-1))?;
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte)?;
        byte[0] != b'\n'
    };
    let mut next_seq = 1;
    let mut event_count = 0usize;
    let mut reader = BufReader::new(File::open(path)?.take(MAX_JOURNAL_FILE_BYTES + 1));
    let mut line = Vec::with_capacity(4096);
    let mut total_bytes = 0u64;
    while let Some((read_bytes, oversized)) = read_bounded_line(&mut reader, &mut line)? {
        total_bytes = total_bytes.saturating_add(read_bytes as u64);
        if total_bytes > MAX_JOURNAL_FILE_BYTES {
            return Err(JournalError::Limit {
                what: "file",
                limit: format!("{} MiB", MAX_JOURNAL_FILE_BYTES / (1024 * 1024)),
            });
        }
        if oversized {
            return Err(JournalError::Limit {
                what: "line",
                limit: format!("{} MiB", MAX_JOURNAL_LINE_BYTES / (1024 * 1024)),
            });
        }
        if let Ok(parsed) = serde_json::from_slice::<JournalLine>(&line) {
            next_seq = parsed.seq.saturating_add(1);
            event_count = event_count.saturating_add(1);
            if event_count > MAX_JOURNAL_EVENTS {
                return Err(JournalError::Limit {
                    what: "events",
                    limit: MAX_JOURNAL_EVENTS.to_string(),
                });
            }
        }
    }
    Ok((next_seq, event_count, metadata.len(), needs_newline))
}

/// Chat ids become file names; anything outside a conservative set is replaced so a
/// hostile id cannot traverse paths. (Ids are uuids in practice.)
fn sanitize_id(chat_id: &str) -> String {
    chat_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use comet_proto::DoneStatus;

    fn text(s: &str) -> AgentEvent {
        AgentEvent::TextDelta { text: s.into() }
    }

    fn done() -> AgentEvent {
        AgentEvent::Done {
            status: DoneStatus::Completed,
            result: None,
            error: None,
            session_id: None,
        }
    }

    #[test]
    fn appends_are_monotonic_and_replayable() {
        let dir = tempfile::tempdir().unwrap();
        let journal = RunJournal::open(dir.path()).unwrap();
        assert_eq!(journal.append("chat-1", &text("a")).unwrap(), 1);
        assert_eq!(journal.append("chat-1", &text("b")).unwrap(), 2);
        assert_eq!(journal.append("chat-1", &done()).unwrap(), 3);

        let all = journal.replay("chat-1", 0).unwrap();
        assert_eq!(all.len(), 3);
        let after = journal.replay("chat-1", 2).unwrap();
        assert_eq!(after.len(), 1);
        assert!(matches!(after[0].1, AgentEvent::Done { .. }));
        // Era fallback: cursor ahead of last seq replays everything.
        assert_eq!(journal.replay("chat-1", 99).unwrap().len(), 3);
    }

    #[test]
    fn seq_continues_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let journal = RunJournal::open(dir.path()).unwrap();
            journal.append("chat-1", &text("a")).unwrap();
        }
        let journal = RunJournal::open(dir.path()).unwrap();
        assert_eq!(journal.append("chat-1", &text("b")).unwrap(), 2);
    }

    #[test]
    fn stale_scan_flags_journals_without_terminal_done() {
        let dir = tempfile::tempdir().unwrap();
        let journal = RunJournal::open(dir.path()).unwrap();
        journal.append("dead", &text("partial")).unwrap();
        journal.append("clean", &text("full")).unwrap();
        journal.append("clean", &done()).unwrap();
        assert_eq!(journal.stale_sessions().unwrap(), vec!["dead".to_string()]);
        // Closing the stale journal with a Done clears the flag.
        journal.append("dead", &done()).unwrap();
        assert!(journal.stale_sessions().unwrap().is_empty());
    }

    #[test]
    fn torn_tail_line_is_tolerated() {
        let dir = tempfile::tempdir().unwrap();
        {
            let journal = RunJournal::open(dir.path()).unwrap();
            journal.append("chat-1", &text("a")).unwrap();
        }
        // Simulate a crash mid-write: garbage with no trailing newline.
        let path = dir.path().join("chat-1.jsonl");
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"{\"seq\":2,\"event\":{\"type\":\"textD")
            .unwrap();
        drop(f);

        let journal = RunJournal::open(dir.path()).unwrap();
        assert_eq!(journal.replay("chat-1", 0).unwrap().len(), 1);
        assert_eq!(journal.append("chat-1", &text("b")).unwrap(), 2);
        let all = journal.replay("chat-1", 0).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[1].0, 2);
    }

    #[test]
    fn open_file_cache_is_bounded_by_hot_journal_limit() {
        let dir = tempfile::tempdir().unwrap();
        let journal = RunJournal::open(dir.path()).unwrap();
        for index in 0..(MAX_OPEN_JOURNALS + 11) {
            journal
                .append(&format!("chat-{index}"), &text("event"))
                .unwrap();
        }

        let files = journal.lock();
        assert_eq!(files.len(), MAX_OPEN_JOURNALS);
        assert!(!files.contains_key("chat-0"));
        assert!(files.contains_key(&format!("chat-{}", MAX_OPEN_JOURNALS + 10)));
    }

    #[test]
    fn scan_visits_events_without_requiring_a_replay_vector() {
        let dir = tempfile::tempdir().unwrap();
        let journal = RunJournal::open(dir.path()).unwrap();
        journal.append("chat-1", &text("a")).unwrap();
        journal.append("chat-1", &done()).unwrap();

        let mut seen = Vec::new();
        journal
            .scan("chat-1", |seq, event| {
                seen.push((seq, event));
                Ok(())
            })
            .unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].0, 1);
        assert!(matches!(seen[1].1, AgentEvent::Done { .. }));
    }

    #[test]
    fn oversized_line_is_rejected_before_append_reopens_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chat-1.jsonl");
        std::fs::write(&path, vec![b'x'; MAX_JOURNAL_LINE_BYTES + 1]).unwrap();
        let journal = RunJournal::open(dir.path()).unwrap();
        assert!(matches!(
            journal.append("chat-1", &text("event")),
            Err(JournalError::Limit { what: "line", .. })
        ));
    }

    #[test]
    fn append_rejects_oversized_serialized_event_without_mutating_journal() {
        let dir = tempfile::tempdir().unwrap();
        let journal = RunJournal::open(dir.path()).unwrap();
        let path = dir.path().join("chat-1.jsonl");
        assert_eq!(journal.append("chat-1", &text("first")).unwrap(), 1);
        let before = std::fs::metadata(&path).unwrap().len();

        let oversized = text(&"x".repeat(MAX_JOURNAL_LINE_BYTES));
        assert!(matches!(
            journal.append("chat-1", &oversized),
            Err(JournalError::Limit { what: "line", .. })
        ));

        assert_eq!(std::fs::metadata(&path).unwrap().len(), before);
        assert_eq!(journal.append("chat-1", &text("second")).unwrap(), 2);
    }

    #[test]
    fn append_count_and_file_limits_fail_before_write_or_seq_increment() {
        let dir = tempfile::tempdir().unwrap();
        let journal = RunJournal::open(dir.path()).unwrap();
        let path = dir.path().join("chat-1.jsonl");
        assert_eq!(journal.append("chat-1", &text("first")).unwrap(), 1);
        let physical_bytes = std::fs::metadata(&path).unwrap().len();

        {
            let mut files = journal.lock();
            files.get_mut("chat-1").unwrap().event_count = MAX_JOURNAL_EVENTS;
        }
        assert!(matches!(
            journal.append("chat-1", &text("blocked-by-count")),
            Err(JournalError::Limit { what: "events", .. })
        ));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), physical_bytes);
        {
            let mut files = journal.lock();
            let state = files.get_mut("chat-1").unwrap();
            assert_eq!(state.next_seq, 2);
            state.event_count = 1;
            state.file_bytes = MAX_JOURNAL_FILE_BYTES;
        }

        assert!(matches!(
            journal.append("chat-1", &text("blocked-by-bytes")),
            Err(JournalError::Limit { what: "file", .. })
        ));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), physical_bytes);
        {
            let mut files = journal.lock();
            let state = files.get_mut("chat-1").unwrap();
            assert_eq!(state.next_seq, 2);
            state.file_bytes = physical_bytes;
        }
        assert_eq!(journal.append("chat-1", &text("second")).unwrap(), 2);
    }
}
