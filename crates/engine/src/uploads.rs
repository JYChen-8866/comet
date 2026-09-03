//! Uploads — local attachment staging for the note agent.
//!
//! The UI streams a file as base64 chunks (~60KB, sized for the relay when the
//! target device is remote); chunks stage on disk under `{data_dir}/uploads/tmp/
//! {uploadId}/{seq}.b64` (surviving an engine restart mid-upload, unlike comet's
//! in-memory buffers), and `commit` assembles them into
//! `{data_dir}/uploads/{id8}-{name}` and returns the absolute path, which the
//! composer appends to the prompt so the agent can read the file from disk.
//!
//! `read_chunk` serves transcript images back in 45KB base64 chunks. Path jail:
//! only files under the uploads dir or a workspace-known chat cwd are readable
//! (the RPC layer supplies the cwd roots) — and only supported image types, as
//! in comet.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::Serialize;

use crate::EngineError;

/// A pending upload must finish within this window (covers slow mesh links).
const STAGING_TTL: Duration = Duration::from_secs(10 * 60);
/// Hard cap on an assembled file.
const MAX_BYTES: u64 = 32 * 1024 * 1024;
/// Bound one staged base64 chunk so a compromised client cannot make
/// `commit` allocate an arbitrarily large temporary string. Normal relay
/// chunks are roughly 60 KiB, so this leaves ample headroom while keeping the
/// decoder's working set predictable.
const MAX_UPLOAD_CHUNK_B64_BYTES: u64 = 1024 * 1024;
/// At the normal ~60 KiB relay size, a maximum-size upload needs fewer than
/// 750 chunks. Keep modest retry/headroom while preventing a staging directory
/// full of zero-byte chunks from allocating an unbounded path vector.
const MAX_UPLOAD_CHUNKS: usize = 1024;
/// Maximum encoded representation of a file at `MAX_BYTES` (without optional
/// surrounding whitespace, which is rejected/conservatively counted).
const MAX_ENCODED_BYTES: u64 = MAX_BYTES.div_ceil(3) * 4;
/// Multiple of 3 so independent base64 chunks concatenate losslessly.
const READ_CHUNK_BYTES: u64 = 45_000;

/// `ReadAttachmentChunk` reply.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentChunk {
    pub name: String,
    pub mime_type: String,
    /// Base64 of this chunk's byte range.
    pub data: String,
    pub next_offset: u64,
    pub done: bool,
}

struct UploadsInner {
    /// Durable home for committed attachments (`{data_dir}/uploads`).
    dir: PathBuf,
    /// Chunk staging (`{data_dir}/uploads/tmp/{uploadId}/`).
    tmp: PathBuf,
}

#[derive(Clone)]
pub struct Uploads {
    inner: Arc<UploadsInner>,
}

impl Uploads {
    pub fn new(data_dir: &Path) -> Self {
        let dir = data_dir.join("uploads");
        Self {
            inner: Arc::new(UploadsInner {
                tmp: dir.join("tmp"),
                dir,
            }),
        }
    }

    /// The durable uploads dir (a path-jail root).
    pub fn dir(&self) -> &Path {
        &self.inner.dir
    }

    /// Stage one base64 chunk. Positional (`seq`) writes are IDEMPOTENT: a client
    /// retrying a chunk whose ack was lost overwrites the same slot instead of
    /// double-appending. Callers without `seq` get append-only behavior.
    pub fn append(&self, upload_id: &str, data: &str, seq: Option<u64>) -> Result<(), EngineError> {
        let dir = self.staging_dir(upload_id)?;
        if data.len() as u64 > MAX_UPLOAD_CHUNK_B64_BYTES {
            return Err(EngineError::Other("Upload chunk is too large".into()));
        }
        self.sweep();
        std::fs::create_dir_all(&dir)?;
        let at = match seq {
            Some(seq) => seq,
            None => next_free_seq(&dir)?,
        };
        if at > 1_000_000 {
            return Err(EngineError::Other("Invalid chunk index".into()));
        }
        // Base64 inflates by ~4/3; bound the staged payload against the file cap.
        let mut staged = 0u64;
        let parts = chunk_files(&dir)?;
        if parts.len() >= MAX_UPLOAD_CHUNKS && !parts.iter().any(|(seq, _)| *seq == at) {
            return Err(EngineError::Other("Upload has too many chunks".into()));
        }
        for (seq, path) in parts {
            if seq == at {
                continue;
            }
            let size = std::fs::metadata(path)?.len();
            staged = staged
                .checked_add(size)
                .ok_or_else(|| EngineError::Other("Upload too large".into()))?;
        }
        let staged_with_chunk = staged
            .checked_add(data.len() as u64)
            .ok_or_else(|| EngineError::Other("Upload too large".into()))?;
        if staged_with_chunk > MAX_ENCODED_BYTES {
            let _ = std::fs::remove_dir_all(&dir);
            return Err(EngineError::Other("Upload too large".into()));
        }
        std::fs::write(dir.join(format!("{at:06}.b64")), data)?;
        Ok(())
    }

    /// Assemble the staged chunks into a durable file and return its absolute
    /// path.
    pub fn commit(&self, upload_id: &str, file_name: &str) -> Result<String, EngineError> {
        let dir = self.staging_dir(upload_id)?;
        let mut parts = chunk_files(&dir)?;
        if parts.is_empty() {
            return Err(EngineError::Other("Unknown or expired upload".into()));
        }
        parts.sort_by_key(|(seq, _)| *seq);
        // Positional appends may leave holes if a chunk never arrived — joining
        // around them would silently corrupt the file.
        for (i, (seq, _path)) in parts.iter().enumerate() {
            if *seq != i as u64 {
                return Err(EngineError::Other("Upload is missing a chunk".into()));
            }
        }
        std::fs::create_dir_all(&self.inner.dir)?;
        let name = sanitize(file_name);
        let id8: String = upload_id.chars().take(8).collect();
        let path = self.inner.dir.join(format!("{id8}-{name}"));
        // Decode incrementally instead of joining every base64 chunk and then
        // allocating a second full decoded `Vec`. The staged input is already
        // bounded to 32 MiB, but the old approach still held roughly 75 MiB of
        // transient buffers for one upload. A small carry preserves quartets
        // that straddle chunk boundaries.
        let temporary = self
            .inner
            .dir
            .join(format!(".{id8}-{name}.{}.part", std::process::id()));
        let result = decode_chunks_to_file(&parts, &temporary);
        if let Err(error) = result {
            let _ = std::fs::remove_file(&temporary);
            // Invalid or oversized input is not recoverable by appending more
            // chunks. Drop the staging directory so a failed upload cannot
            // pin its full allowance until the TTL sweep runs.
            let _ = std::fs::remove_dir_all(&dir);
            return Err(error);
        }
        // Replace the destination only after the complete decoded file is
        // durable. This avoids exposing a partially decoded attachment to a
        // concurrent agent read when validation fails.
        if let Err(error) = std::fs::rename(&temporary, &path) {
            // `rename` replaces an existing file atomically on Unix but fails
            // with `AlreadyExists` on Windows. Preserve the old retry/overwrite
            // behavior for a reused upload id while keeping the normal path
            // atomic on platforms that support replacement.
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                if let Err(replace_error) = (|| -> Result<(), std::io::Error> {
                    std::fs::remove_file(&path)?;
                    std::fs::rename(&temporary, &path)
                })() {
                    let _ = std::fs::remove_file(&temporary);
                    return Err(replace_error.into());
                }
            } else {
                let _ = std::fs::remove_file(&temporary);
                return Err(error.into());
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
        Ok(path.to_string_lossy().to_string())
    }

    /// Read one 45KB chunk of an attachment. `extra_roots` are the workspace's
    /// known chat cwds — together with the uploads dir they form the path jail.
    pub fn read_chunk(
        &self,
        path: &str,
        offset: u64,
        extra_roots: &[PathBuf],
    ) -> Result<AttachmentChunk, EngineError> {
        use std::io::{Read, Seek};
        let file = self.inspect(path, extra_roots)?;
        let size = file.size;
        let start = offset.min(size);
        let next_offset = (start + READ_CHUNK_BYTES).min(size);
        // Read ONLY this chunk's byte range — never the whole file per chunk.
        let mut buf = vec![0u8; (next_offset - start) as usize];
        let mut handle = std::fs::File::open(&file.resolved)?;
        handle.seek(std::io::SeekFrom::Start(start))?;
        let mut read = 0usize;
        while read < buf.len() {
            let n = handle.read(&mut buf[read..])?;
            if n == 0 {
                break;
            }
            read += n;
        }
        buf.truncate(read);
        Ok(AttachmentChunk {
            name: file.name,
            mime_type: file.mime_type,
            data: BASE64.encode(&buf),
            next_offset,
            done: next_offset >= size,
        })
    }

    // ── internals ───────────────────────────────────────────────────────────

    fn staging_dir(&self, upload_id: &str) -> Result<PathBuf, EngineError> {
        // The id becomes a directory name — jail it to a safe charset.
        let ok = !upload_id.is_empty()
            && upload_id.len() <= 64
            && upload_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'));
        if !ok {
            return Err(EngineError::Other("Invalid upload id".into()));
        }
        Ok(self.inner.tmp.join(upload_id))
    }

    /// Reclaim staging dirs whose newest chunk is older than the TTL (an upload
    /// abandoned mid-stream must not hold up to 32MB forever).
    fn sweep(&self) {
        let Ok(entries) = std::fs::read_dir(&self.inner.tmp) else {
            return;
        };
        for entry in entries.flatten() {
            let newest = std::fs::read_dir(entry.path())
                .ok()
                .into_iter()
                .flatten()
                .flatten()
                .filter_map(|f| f.metadata().ok()?.modified().ok())
                .max();
            let expired = match newest {
                Some(at) => at.elapsed().map(|age| age > STAGING_TTL).unwrap_or(false),
                None => true, // empty dir — reclaim
            };
            if expired {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }

    fn inspect(&self, path: &str, extra_roots: &[PathBuf]) -> Result<InspectedFile, EngineError> {
        let outside = || EngineError::Other("Attachment is outside the upload cache".into());
        // Canonicalize BOTH sides so `..` segments and symlinks can't escape.
        let resolved = std::fs::canonicalize(path).map_err(|_| outside())?;
        let allowed = std::iter::once(&self.inner.dir)
            .chain(extra_roots.iter())
            .filter_map(|root| std::fs::canonicalize(root).ok())
            .any(|root| resolved.starts_with(&root) && resolved != root);
        if !allowed {
            return Err(outside());
        }
        let meta = std::fs::metadata(&resolved)?;
        if !meta.is_file() {
            return Err(EngineError::Other("Attachment is not a file".into()));
        }
        if meta.len() > MAX_BYTES {
            return Err(EngineError::Other("Attachment is too large".into()));
        }
        let mime_type = mime_by_ext(&resolved)
            .ok_or_else(|| EngineError::Other("Attachment is not a supported image".into()))?;
        Ok(InspectedFile {
            name: resolved
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "attachment".into()),
            mime_type: mime_type.to_string(),
            size: meta.len(),
            resolved,
        })
    }
}

fn decode_chunks_to_file(parts: &[(u64, PathBuf)], destination: &Path) -> Result<(), EngineError> {
    // Validate metadata before opening the destination. This keeps malformed
    // staging files from creating a partial output and bounds every
    // subsequent `read_to_string` allocation.
    let mut encoded_total = 0u64;
    for (_, path) in parts {
        let size = std::fs::metadata(path)?.len();
        if size > MAX_UPLOAD_CHUNK_B64_BYTES {
            return Err(EngineError::Other("Upload chunk is too large".into()));
        }
        encoded_total = encoded_total
            .checked_add(size)
            .ok_or_else(|| EngineError::Other("Upload too large".into()))?;
        if encoded_total > MAX_ENCODED_BYTES {
            return Err(EngineError::Other("Upload too large".into()));
        }
    }
    let mut output = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(destination)?;
    let mut carry = String::new();
    let mut decoded_total = 0u64;

    for (_, path) in parts {
        let chunk = std::fs::read_to_string(path)?;
        carry.push_str(chunk.trim());
        let complete_len = carry.len() / 4 * 4;
        if complete_len == 0 {
            continue;
        }
        let decoded = BASE64
            .decode(carry.as_bytes().get(..complete_len).unwrap_or_default())
            .map_err(|error| EngineError::Other(format!("upload is not valid base64: {error}")))?;
        decoded_total = decoded_total.saturating_add(decoded.len() as u64);
        if decoded_total > MAX_BYTES {
            return Err(EngineError::Other("Upload too large".into()));
        }
        output.write_all(&decoded)?;
        carry.drain(..complete_len);
    }

    if !carry.is_empty() {
        let decoded = BASE64
            .decode(carry.as_bytes())
            .map_err(|error| EngineError::Other(format!("upload is not valid base64: {error}")))?;
        decoded_total = decoded_total.saturating_add(decoded.len() as u64);
        if decoded_total > MAX_BYTES {
            return Err(EngineError::Other("Upload too large".into()));
        }
        output.write_all(&decoded)?;
    }
    output.flush()?;
    Ok(())
}

struct InspectedFile {
    resolved: PathBuf,
    name: String,
    mime_type: String,
    size: u64,
}

fn chunk_files(dir: &Path) -> Result<Vec<(u64, PathBuf)>, EngineError> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Ok(Vec::new());
    };
    let mut files = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let seq = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok());
        if let Some(seq) = seq
            && path.extension().and_then(|e| e.to_str()) == Some("b64")
        {
            if files.len() >= MAX_UPLOAD_CHUNKS {
                return Err(EngineError::Other("Upload has too many chunks".into()));
            }
            files.push((seq, path));
        }
    }
    Ok(files)
}

fn next_free_seq(dir: &Path) -> Result<u64, EngineError> {
    let files = chunk_files(dir)?;
    if files.len() >= MAX_UPLOAD_CHUNKS {
        return Err(EngineError::Other("Upload has too many chunks".into()));
    }
    files.iter().try_fold(0, |next, (seq, _)| {
        let candidate = seq
            .checked_add(1)
            .ok_or_else(|| EngineError::Other("Invalid chunk index".into()))?;
        Ok(next.max(candidate))
    })
}

fn sanitize(file_name: &str) -> String {
    let base = Path::new(file_name)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    let cleaned: String = base
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let tail: String = cleaned
        .chars()
        .rev()
        .take(80)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    if tail.is_empty() {
        "upload".into()
    } else {
        tail
    }
}

fn mime_by_ext(path: &Path) -> Option<&'static str> {
    match path.extension()?.to_str()?.to_ascii_lowercase().as_str() {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        "svg" => Some("image/svg+xml"),
        "bmp" => Some("image/bmp"),
        "tif" | "tiff" => Some("image/tiff"),
        "avif" => Some("image/avif"),
        "heic" => Some("image/heic"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn sanitize_names() {
        assert_eq!(sanitize("../../etc/passwd"), "passwd");
        assert_eq!(sanitize("my photo (1).png"), "my_photo__1_.png");
        assert_eq!(sanitize(""), "upload");
    }

    #[test]
    fn decode_chunks_streams_across_non_quartet_boundaries() {
        let directory = tempdir().unwrap();
        let first = directory.path().join("000000.b64");
        let second = directory.path().join("000001.b64");
        // "hello world" encoded as two deliberately awkward fragments.
        fs::write(&first, "aGVsb").unwrap();
        fs::write(&second, "G8gd29ybGQ=").unwrap();
        let destination = directory.path().join("decoded.bin");

        decode_chunks_to_file(&[(0, first), (1, second)], &destination).unwrap();

        assert_eq!(fs::read(destination).unwrap(), b"hello world");
    }

    #[test]
    fn decode_chunks_rejects_invalid_base64_without_panicking() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("000000.b64");
        fs::write(&source, "not base64!").unwrap();
        let error =
            decode_chunks_to_file(&[(0, source)], &directory.path().join("out")).unwrap_err();
        assert!(error.to_string().contains("not valid base64"));
    }

    #[test]
    fn commit_decodes_staged_chunks_without_joining_the_input() {
        let directory = tempdir().unwrap();
        let uploads = Uploads::new(directory.path());
        let data = b"streamed upload payload";
        let encoded = BASE64.encode(data);
        let split = 7;
        uploads
            .append("upload-1", &encoded[..split], Some(0))
            .unwrap();
        uploads
            .append("upload-1", &encoded[split..], Some(1))
            .unwrap();

        let path = uploads.commit("upload-1", "payload.bin").unwrap();
        assert_eq!(fs::read(path).unwrap(), data);
        assert!(!directory.path().join("uploads/tmp/upload-1").exists());
    }

    #[test]
    fn append_rejects_an_oversized_chunk_before_creating_staging() {
        let directory = tempdir().unwrap();
        let uploads = Uploads::new(directory.path());
        let oversized = "A".repeat(MAX_UPLOAD_CHUNK_B64_BYTES as usize + 1);

        let error = uploads
            .append("too-large", &oversized, Some(0))
            .unwrap_err();

        assert!(error.to_string().contains("chunk is too large"));
        assert!(!directory.path().join("uploads/tmp/too-large").exists());
    }

    #[test]
    fn commit_cleans_invalid_staging_after_decode_failure() {
        let directory = tempdir().unwrap();
        let uploads = Uploads::new(directory.path());
        uploads
            .append("bad-upload", "not base64!", Some(0))
            .unwrap();

        let error = uploads.commit("bad-upload", "payload.bin").unwrap_err();

        assert!(error.to_string().contains("not valid base64"));
        assert!(!directory.path().join("uploads/tmp/bad-upload").exists());
    }

    #[test]
    fn commit_rejects_oversized_staged_chunk_and_cleans_staging() {
        let directory = tempdir().unwrap();
        let uploads = Uploads::new(directory.path());
        let staging = directory.path().join("uploads/tmp/forged-upload");
        fs::create_dir_all(&staging).unwrap();
        fs::write(
            staging.join("000000.b64"),
            vec![b'A'; MAX_UPLOAD_CHUNK_B64_BYTES as usize + 1],
        )
        .unwrap();

        let error = uploads.commit("forged-upload", "payload.bin").unwrap_err();

        assert!(error.to_string().contains("chunk is too large"));
        assert!(!staging.exists());
    }

    #[test]
    fn staged_chunk_count_is_hard_bounded_but_existing_slots_remain_idempotent() {
        let directory = tempdir().unwrap();
        let uploads = Uploads::new(directory.path());
        let staging = directory.path().join("uploads/tmp/many-chunks");
        fs::create_dir_all(&staging).unwrap();
        for seq in 0..MAX_UPLOAD_CHUNKS {
            fs::write(staging.join(format!("{seq:06}.b64")), []).unwrap();
        }

        // An implicit append would allocate another path and must stop at the
        // hard count, while retrying/replacing an existing positional slot is
        // still idempotent at the boundary.
        assert!(next_free_seq(&staging).is_err());
        uploads
            .append("many-chunks", "YQ==", Some(0))
            .expect("replace existing slot at count limit");
        let error = uploads
            .append("many-chunks", "Yg==", Some(MAX_UPLOAD_CHUNKS as u64))
            .unwrap_err();
        assert!(error.to_string().contains("too many chunks"));

        // A forged extra file is rejected during commit enumeration before a
        // path vector can grow past the same bound. Removing it restores a
        // valid exact-boundary upload.
        let extra = staging.join(format!("{:06}.b64", MAX_UPLOAD_CHUNKS));
        fs::write(&extra, []).unwrap();
        assert!(
            uploads
                .commit("many-chunks", "payload.bin")
                .unwrap_err()
                .to_string()
                .contains("too many chunks")
        );
        fs::remove_file(extra).unwrap();

        let path = uploads.commit("many-chunks", "payload.bin").unwrap();
        assert_eq!(fs::read(path).unwrap(), b"a");
    }
}
