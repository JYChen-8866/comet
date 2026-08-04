//! Attachments (feature-inventory §1.7/§1.8): the composer's staged images,
//! the chunked upload to the chat's host device, the plain-text attachment-ref
//! transport that rides the prompt, the transcript read-back cache, and the
//! full-size preview lightbox.
//!
//! Ports of comet's `composer/use-attachments.ts` (staging/upload),
//! `control/message-attachments.ts` (the `withAttachments` /
//! `parseUserMessageImages` text transport — attachment refs are embedded in
//! the user message's plain text, which is exactly what persists in the doc),
//! and `lib/transcript-attachment-cache.ts` (decoded-image cache keyed by
//! `(deviceId, path)`, seeded locally after a send so own bubbles never
//! round-trip).

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use gpui::{
    AnyElement, App, BackgroundExecutor, Image, ImageFormat, ObjectFit, SharedString, Size,
    StyledImage as _, Window, div, img, prelude::*, px,
};

use crate::state::EngineHandle;
use crate::theme::white_alpha;
use comet_rpc::methods;

/// use-attachments.ts `MAX_ATTACHMENT_BYTES`.
pub const MAX_ATTACHMENT_BYTES: u64 = 24 * 1024 * 1024;
/// Base64 chars per `UploadChunk` (comet state.ts `UPLOAD_CHUNK` — sized for
/// the relay when the target device is remote).
pub const UPLOAD_CHUNK_B64_CHARS: usize = 60_000;
/// state.ts `MAX_ATTACHMENT_READ_CHUNKS` — bounds the read-back loop.
const MAX_READ_CHUNKS: usize = 1_000;

// ---------------------------------------------------------------------------
// Text transport (message-attachments.ts)
// ---------------------------------------------------------------------------

/// The body used for image-only sends (`use-attachments.ts`).
pub const ATTACHMENT_ONLY_TEXT: &str = "See the attached image(s).";

/// How attachments ride the prompt (use-attachments.ts `withAttachments`):
/// plain local paths appended to the text — the files are staged on the device
/// that runs the agent, so the agent can open them with its own tools; the
/// same text is what persists as the user doc entry.
pub fn with_attachments(text: &str, paths: &[String]) -> String {
    if paths.is_empty() {
        return text.to_string();
    }
    let refs: Vec<String> = paths.iter().map(|p| format!("- {p}")).collect();
    let body = if text.is_empty() {
        ATTACHMENT_ONLY_TEXT
    } else {
        text
    };
    format!(
        "{body}\n\nAttached images (local files — open them to view):\n{}",
        refs.join("\n")
    )
}

/// An attachment ref parsed back out of a user message's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserImageAttachment {
    pub id: String,
    pub path: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedUserMessage {
    /// The visible prompt (the refs trailer stripped; empty for image-only sends).
    pub text: String,
    pub attachments: Vec<UserImageAttachment>,
}

fn name_from_path(path: &str) -> String {
    let name = path
        .rsplit(['/', '\\'])
        .next()
        .map(str::trim)
        .unwrap_or_default();
    if name.is_empty() {
        "image".to_string()
    } else {
        name.to_string()
    }
}

/// Find the refs trailer: a blank line, then a line starting (case-insensitive)
/// with `Attached images (local files` and ending `):`. Returns
/// `(body_end, refs_start)` byte offsets — the tolerant equivalent of comet's
/// `ATTACHED_IMAGES_RE`.
fn find_refs_marker(content: &str) -> Option<(usize, usize)> {
    let lower = content.to_ascii_lowercase();
    let needle = "\n\nattached images (local files";
    let mut from = 0usize;
    while let Some(rel) = lower[from..].find(needle) {
        let gap = from + rel;
        let line_start = gap + 2;
        let line_end = content[line_start..]
            .find('\n')
            .map(|p| line_start + p)
            .unwrap_or(content.len());
        let line = content[line_start..line_end].trim_end_matches('\r');
        if line.ends_with("):") {
            let refs_start = (line_end + 1).min(content.len());
            return Some((gap, refs_start));
        }
        from = line_start;
    }
    None
}

/// message-attachments.ts `parseUserMessageImages`: split the visible prompt
/// from its attachment-ref trailer.
pub fn parse_user_message_images(content: &str) -> ParsedUserMessage {
    let Some((body_end, refs_start)) = find_refs_marker(content) else {
        return ParsedUserMessage {
            text: content.to_string(),
            attachments: Vec::new(),
        };
    };
    let body = content[..body_end].trim_end();
    let attachments: Vec<UserImageAttachment> = content[refs_start..]
        .lines()
        .filter_map(|line| {
            let path = line.trim_start().strip_prefix("- ")?.trim();
            (!path.is_empty()).then(|| path.to_string())
        })
        .enumerate()
        .map(|(index, path)| UserImageAttachment {
            id: format!("{index}:{path}"),
            name: name_from_path(&path),
            path,
        })
        .collect();
    if attachments.is_empty() {
        return ParsedUserMessage {
            text: content.to_string(),
            attachments,
        };
    }
    ParsedUserMessage {
        text: if body.trim() == ATTACHMENT_ONLY_TEXT {
            String::new()
        } else {
            body.to_string()
        },
        attachments,
    }
}

/// message-attachments.ts `userMessageRailText`: what the rail/sidebar shows
/// for a user message ("Attached image" / "N attached images" when image-only).
pub fn user_message_rail_text(content: &str) -> String {
    let parsed = parse_user_message_images(content);
    if !parsed.text.trim().is_empty() {
        return parsed.text;
    }
    match parsed.attachments.len() {
        0 => content.to_string(),
        1 => "Attached image".to_string(),
        n => format!("{n} attached images"),
    }
}

// ---------------------------------------------------------------------------
// Staging (use-attachments.ts intake)
// ---------------------------------------------------------------------------

/// An image staged in the composer, before upload. The raw bytes live inside
/// the [`Image`] (gpui decodes them at paint; the same Arc feeds thumbnails,
/// the lightbox, the upload, and the post-send cache seed).
#[derive(Clone)]
pub struct StagedAttachment {
    pub id: String,
    /// File name with a type-matching extension (use-attachments.ts
    /// `ensureExtension` — agents sniff images by extension).
    pub name: String,
    pub image: Arc<Image>,
    /// A bounded raster used by the composer strip and transcript bubble.
    pub thumbnail: Arc<Image>,
}

impl StagedAttachment {
    pub fn bytes(&self) -> &[u8] {
        &self.image.bytes
    }
}

/// Image formats the whole pipeline supports: intersection of gpui's decoders
/// and the engine's `mime_by_ext` read-back jail.
pub fn format_by_extension(path: &Path) -> Option<ImageFormat> {
    match path.extension()?.to_str()?.to_ascii_lowercase().as_str() {
        "png" => Some(ImageFormat::Png),
        "jpg" | "jpeg" => Some(ImageFormat::Jpeg),
        "gif" => Some(ImageFormat::Gif),
        "webp" => Some(ImageFormat::Webp),
        "svg" => Some(ImageFormat::Svg),
        "bmp" => Some(ImageFormat::Bmp),
        "tif" | "tiff" => Some(ImageFormat::Tiff),
        _ => None,
    }
}

/// use-attachments.ts `ensureExtension`: pasted screenshots often arrive as a
/// bare "image" — make sure the staged name carries a type-matching extension.
pub fn ensure_extension(name: &str, format: ImageFormat) -> String {
    let has_ext = name
        .rsplit_once('.')
        .map(|(stem, ext)| {
            !stem.is_empty()
                && (2..=5).contains(&ext.len())
                && ext.chars().all(|c| c.is_ascii_alphanumeric())
        })
        .unwrap_or(false);
    if has_ext {
        name.to_string()
    } else {
        format!("{name}.{}", image_format_extension(format))
    }
}

fn image_format_extension(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Png => "png",
        ImageFormat::Jpeg => "jpg",
        ImageFormat::Webp => "webp",
        ImageFormat::Gif => "gif",
        ImageFormat::Svg => "svg",
        ImageFormat::Bmp => "bmp",
        ImageFormat::Tiff => "tiff",
        ImageFormat::Ico => "ico",
        _ => "png",
    }
}

/// Stage a file from disk (picker / drop / pasted path). `Err` carries the
/// user-facing message (mirrors the old `onError` copy).
pub fn stage_file(path: &Path) -> Result<StagedAttachment, String> {
    let display_name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "image".to_string());
    let Some(format) = format_by_extension(path) else {
        return Err(format!("{display_name} is not a supported image."));
    };
    let meta = std::fs::metadata(path).map_err(|_| format!("{display_name} could not be read."))?;
    if meta.len() > MAX_ATTACHMENT_BYTES {
        return Err(format!("{display_name} is too large (24 MB max)."));
    }
    let bytes = std::fs::read(path).map_err(|_| format!("{display_name} could not be read."))?;
    let image = Arc::new(Image::from_bytes(format, bytes));
    let thumbnail = attachment_thumbnail(&image);
    Ok(StagedAttachment {
        id: uuid::Uuid::new_v4().to_string(),
        name: ensure_extension(&display_name, format),
        image,
        thumbnail,
    })
}

/// Stage an image pasted from the clipboard.
pub fn stage_clipboard_image(image: Image) -> StagedAttachment {
    let format = image.format;
    let image = Arc::new(image);
    let thumbnail = attachment_thumbnail(&image);
    StagedAttachment {
        id: uuid::Uuid::new_v4().to_string(),
        name: ensure_extension("image", format),
        image,
        thumbnail,
    }
}

// ---------------------------------------------------------------------------
// Upload (state.ts uploadAttachment) + read-back (state.ts readAttachmentImage)
// ---------------------------------------------------------------------------

fn with_target(mut params: serde_json::Value, target_device_id: Option<&str>) -> serde_json::Value {
    if let (Some(target), Some(map)) = (target_device_id, params.as_object_mut()) {
        map.insert("targetDeviceId".into(), target.into());
    }
    params
}

/// Per-call deadlines (desktop state.ts): a stalled-but-open relay link never
/// fails an RPC on its own, so every attachment call races a timer. The first
/// chunk gets 90s (a cold dial to a remote device), later chunks 30s; commit
/// 150s (it must outlast the engine's cross-device assemble); reads 20s.
const FIRST_CHUNK_TIMEOUT: Duration = Duration::from_secs(90);
const CHUNK_TIMEOUT: Duration = Duration::from_secs(30);
const COMMIT_TIMEOUT: Duration = Duration::from_secs(150);
const READ_CHUNK_TIMEOUT: Duration = Duration::from_secs(20);

/// Race an RPC against `timeout` on the gpui background executor (these
/// futures run under `cx.spawn`, so tokio's timer reactor isn't available).
async fn call_with_timeout(
    engine: &EngineHandle,
    executor: &BackgroundExecutor,
    method: &str,
    params: serde_json::Value,
    timeout: Duration,
) -> Result<serde_json::Value, String> {
    let call = engine.client().call(method, params);
    let timer = executor.timer(timeout);
    futures::pin_mut!(call);
    match futures::future::select(call, timer).await {
        futures::future::Either::Left((result, _)) => result.map_err(|e| e.to_string()),
        futures::future::Either::Right(_) => Err(format!("{method} timed out")),
    }
}

/// Chunked upload: base64 the bytes, `UploadChunk{uploadId,seq,data}` per 60KB
/// slice (positional `seq` makes the cheap retry idempotent), then
/// `UploadCommit{uploadId,fileName}` → the durable absolute path on the target
/// device. Errors return the raw cause (the composer shows friendly copy).
pub async fn upload_attachment(
    engine: &EngineHandle,
    executor: &BackgroundExecutor,
    target_device_id: Option<&str>,
    attachment: &StagedAttachment,
) -> Result<String, String> {
    let b64 = BASE64.encode(attachment.bytes());
    let upload_id = uuid::Uuid::new_v4().to_string();
    let mut start = 0usize;
    let mut seq = 0u64;
    loop {
        let end = (start + UPLOAD_CHUNK_B64_CHARS).min(b64.len());
        let params = with_target(
            serde_json::json!({ "uploadId": upload_id, "seq": seq, "data": &b64[start..end] }),
            target_device_id,
        );
        let timeout = if seq == 0 {
            FIRST_CHUNK_TIMEOUT
        } else {
            CHUNK_TIMEOUT
        };
        // One transient blip must not abort a ~400-chunk upload; `seq` slots
        // are idempotent engine-side, so a blind re-send is safe (timeouts
        // retry too, like the original's per-chunk `withTimeout` + retry ×2).
        let mut attempt = 0u32;
        loop {
            match call_with_timeout(
                engine,
                executor,
                methods::UPLOAD_CHUNK,
                params.clone(),
                timeout,
            )
            .await
            {
                Ok(_) => break,
                Err(err) if attempt < 2 => {
                    attempt += 1;
                    tracing::debug!(error = %err, seq, "upload chunk retry");
                }
                Err(err) => return Err(err),
            }
        }
        start = end;
        seq += 1;
        if start >= b64.len() {
            break;
        }
    }
    let params = with_target(
        serde_json::json!({ "uploadId": upload_id, "fileName": attachment.name }),
        target_device_id,
    );
    let reply = call_with_timeout(
        engine,
        executor,
        methods::UPLOAD_COMMIT,
        params,
        COMMIT_TIMEOUT,
    )
    .await?;
    reply
        .get("path")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| "upload commit returned no path".to_string())
}

/// A transcript image read back from the owning device.
pub struct LoadedAttachmentImage {
    pub name: String,
    pub image: Arc<Image>,
    pub thumbnail: Arc<Image>,
}

/// `ReadAttachmentChunk` loop: 45KB base64 chunks until `done` (bounded, with
/// the same stuck-offset guard as comet's `readAttachmentImage`).
pub async fn read_attachment_image(
    engine: &EngineHandle,
    executor: &BackgroundExecutor,
    target_device_id: Option<&str>,
    path: &str,
) -> Option<LoadedAttachmentImage> {
    let mut name = String::new();
    let mut mime = String::new();
    let mut bytes = Vec::new();
    let mut offset = 0u64;
    let mut done = false;
    for _ in 0..MAX_READ_CHUNKS {
        let params = with_target(
            serde_json::json!({ "path": path, "offset": offset }),
            target_device_id,
        );
        let chunk = call_with_timeout(
            engine,
            executor,
            methods::READ_ATTACHMENT_CHUNK,
            params,
            READ_CHUNK_TIMEOUT,
        )
        .await
        .ok()?;
        name = chunk.get("name")?.as_str()?.to_string();
        mime = chunk.get("mimeType")?.as_str()?.to_string();
        BASE64
            .decode_vec(chunk.get("data")?.as_str()?.as_bytes(), &mut bytes)
            .ok()?;
        if bytes.len() as u64 > MAX_ATTACHMENT_BYTES {
            return None;
        }
        done = chunk.get("done")?.as_bool()?;
        if done {
            break;
        }
        let next = chunk.get("nextOffset")?.as_u64()?;
        if next <= offset {
            return None;
        }
        offset = next;
    }
    if !done || bytes.is_empty() {
        return None;
    }
    let format = ImageFormat::from_mime_type(&mime).unwrap_or(ImageFormat::Png);
    let image = Arc::new(Image::from_bytes(format, bytes));
    let thumbnail = attachment_thumbnail(&image);
    Some(LoadedAttachmentImage {
        name: if name.is_empty() {
            name_from_path(path)
        } else {
            name
        },
        image,
        thumbnail,
    })
}

const ATTACHMENT_THUMB_MAX_WIDTH: u32 = 224;
const ATTACHMENT_THUMB_MAX_HEIGHT: u32 = 160;

fn attachment_thumbnail(image: &Arc<Image>) -> Arc<Image> {
    make_attachment_thumbnail(image.format, &image.bytes)
        .map(Arc::new)
        .unwrap_or_else(|| image.clone())
}

fn make_attachment_thumbnail(format: ImageFormat, bytes: &[u8]) -> Option<Image> {
    if format == ImageFormat::Png
        && let Some(thumbnail) = decode_streaming_png_thumbnail(bytes)
    {
        return Some(thumbnail);
    }
    let decoded = if format == ImageFormat::Jpeg {
        decode_scaled_jpeg(bytes)?
    } else {
        let image_format = match format {
            ImageFormat::Gif => image::ImageFormat::Gif,
            ImageFormat::Webp => image::ImageFormat::WebP,
            ImageFormat::Bmp => image::ImageFormat::Bmp,
            ImageFormat::Tiff => image::ImageFormat::Tiff,
            _ => return None,
        };
        image::load_from_memory_with_format(bytes, image_format).ok()?
    };
    encode_thumbnail(decoded)
}

/// Non-interlaced PNGs can be sampled row-by-row, so a large screenshot never
/// needs a width × height decode buffer just to produce a chat thumbnail.
fn decode_streaming_png_thumbnail(bytes: &[u8]) -> Option<Image> {
    let mut decoder = png::Decoder::new(Cursor::new(bytes));
    decoder.set_transformations(png::Transformations::EXPAND | png::Transformations::STRIP_16);
    let mut reader = decoder.read_info().ok()?;
    let info = reader.info();
    if info.interlaced {
        return None;
    }
    let (source_width, source_height) = (info.width, info.height);
    let (width, height) = thumbnail_dimensions(source_width, source_height);
    let (color_type, _) = reader.output_color_type();
    let channels = match color_type {
        png::ColorType::Grayscale => 1,
        png::ColorType::Rgb => 3,
        png::ColorType::GrayscaleAlpha => 2,
        png::ColorType::Rgba => 4,
        png::ColorType::Indexed => return None,
    };
    let mut rgba = vec![0; width as usize * height as usize * 4];
    let mut source_y = 0u32;
    let mut target_y = 0u32;
    while let Some(row) = reader.next_row().ok()? {
        while target_y < height && target_y.saturating_mul(source_height) / height == source_y {
            let output_row = &mut rgba[target_y as usize * width as usize * 4
                ..(target_y + 1) as usize * width as usize * 4];
            for target_x in 0..width {
                let source_x = target_x.saturating_mul(source_width) / width;
                let input = source_x as usize * channels;
                let output = target_x as usize * 4;
                match color_type {
                    png::ColorType::Grayscale => {
                        output_row[output..output + 3].fill(row.data()[input]);
                        output_row[output + 3] = 255;
                    }
                    png::ColorType::Rgb => {
                        output_row[output..output + 3]
                            .copy_from_slice(&row.data()[input..input + 3]);
                        output_row[output + 3] = 255;
                    }
                    png::ColorType::GrayscaleAlpha => {
                        output_row[output..output + 3].fill(row.data()[input]);
                        output_row[output + 3] = row.data()[input + 1];
                    }
                    png::ColorType::Rgba => {
                        output_row[output..output + 4]
                            .copy_from_slice(&row.data()[input..input + 4]);
                    }
                    png::ColorType::Indexed => unreachable!(),
                }
            }
            target_y += 1;
        }
        source_y += 1;
    }
    let image = (target_y == height).then(|| image::RgbaImage::from_raw(width, height, rgba))??;
    encode_thumbnail(image::DynamicImage::ImageRgba8(image))
}

fn thumbnail_dimensions(width: u32, height: u32) -> (u32, u32) {
    let scale = (ATTACHMENT_THUMB_MAX_WIDTH as f64 / f64::from(width.max(1)))
        .min(ATTACHMENT_THUMB_MAX_HEIGHT as f64 / f64::from(height.max(1)))
        .min(1.0);
    (
        (f64::from(width) * scale).round().max(1.0) as u32,
        (f64::from(height) * scale).round().max(1.0) as u32,
    )
}

/// JPEG supports 1/2, 1/4 and 1/8 IDCT output. Using that before the final
/// resize keeps a 20 MP photo below roughly one megapixel during thumbnailing.
fn decode_scaled_jpeg(bytes: &[u8]) -> Option<image::DynamicImage> {
    use jpeg_decoder::PixelFormat;

    let mut decoder = jpeg_decoder::Decoder::new(Cursor::new(bytes));
    decoder
        .scale(
            ATTACHMENT_THUMB_MAX_WIDTH.try_into().ok()?,
            ATTACHMENT_THUMB_MAX_HEIGHT.try_into().ok()?,
        )
        .ok()?;
    let pixels = decoder.decode().ok()?;
    let info = decoder.info()?;
    let (width, height) = (u32::from(info.width), u32::from(info.height));
    match info.pixel_format {
        PixelFormat::L8 => {
            image::GrayImage::from_raw(width, height, pixels).map(image::DynamicImage::ImageLuma8)
        }
        PixelFormat::RGB24 => {
            image::RgbImage::from_raw(width, height, pixels).map(image::DynamicImage::ImageRgb8)
        }
        PixelFormat::CMYK32 => {
            let mut rgb = Vec::with_capacity(width as usize * height as usize * 3);
            for cmyk in pixels.chunks_exact(4) {
                let k = u16::from(cmyk[3]);
                rgb.push(255u16.saturating_sub(u16::from(cmyk[0]) + k) as u8);
                rgb.push(255u16.saturating_sub(u16::from(cmyk[1]) + k) as u8);
                rgb.push(255u16.saturating_sub(u16::from(cmyk[2]) + k) as u8);
            }
            image::RgbImage::from_raw(width, height, rgb).map(image::DynamicImage::ImageRgb8)
        }
        PixelFormat::L16 => None,
    }
}

fn encode_thumbnail(decoded: image::DynamicImage) -> Option<Image> {
    let thumbnail = decoded.thumbnail(ATTACHMENT_THUMB_MAX_WIDTH, ATTACHMENT_THUMB_MAX_HEIGHT);
    let mut encoded = Cursor::new(Vec::new());
    thumbnail
        .write_to(&mut encoded, image::ImageFormat::Png)
        .ok()?;
    Some(Image::from_bytes(ImageFormat::Png, encoded.into_inner()))
}

// ---------------------------------------------------------------------------
// Transcript image cache (transcript-attachment-cache.ts)
// ---------------------------------------------------------------------------

/// A decoded transcript image, ready for `img(...)`.
#[derive(Clone)]
pub struct CachedAttachmentImage {
    pub name: SharedString,
    /// Original encoded bytes. GPUI only decodes this when the lightbox opens.
    pub image: Arc<Image>,
    /// Bounded image used for every transcript paint.
    pub thumbnail: Arc<Image>,
}

/// What a render pass sees for one `(deviceId, path)` source.
#[derive(Clone)]
pub enum AttachmentSnapshot {
    Loading,
    Loaded(CachedAttachmentImage),
    /// Load failed; `retry_in` is how long until [`begin_load`] would hand out
    /// another attempt (the exponential 2s→15s ladder from user-attachments.tsx).
    Error {
        retry_in: Duration,
    },
}

enum CacheEntry {
    Loading { attempts: u32 },
    Loaded(CachedAttachmentImage),
    Error { attempts: u32, at: Instant },
}

const ATTACHMENT_CACHE_MAX_ENTRIES: usize = 128;
const ATTACHMENT_CACHE_MAX_ENCODED_BYTES: usize = 64 * 1024 * 1024;

struct AttachmentCache {
    entries: HashMap<(String, String), CacheEntry>,
    lru: VecDeque<(String, String)>,
    encoded_bytes: usize,
    retired: Vec<Arc<Image>>,
    max_entries: usize,
    max_encoded_bytes: usize,
}

impl AttachmentCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            lru: VecDeque::new(),
            encoded_bytes: 0,
            retired: Vec::new(),
            max_entries: ATTACHMENT_CACHE_MAX_ENTRIES,
            max_encoded_bytes: ATTACHMENT_CACHE_MAX_ENCODED_BYTES,
        }
    }

    #[cfg(test)]
    fn with_limits(max_entries: usize, max_encoded_bytes: usize) -> Self {
        Self {
            max_entries,
            max_encoded_bytes,
            ..Self::new()
        }
    }

    fn touch(&mut self, key: &(String, String)) {
        if let Some(index) = self.lru.iter().position(|candidate| candidate == key) {
            self.lru.remove(index);
        }
        self.lru.push_back(key.clone());
    }

    fn insert(&mut self, key: (String, String), entry: CacheEntry) {
        let entry = self.canonicalize(entry);
        if let Some(previous) = self.entries.insert(key.clone(), entry) {
            self.retire_if_uncached(previous);
        }
        self.recalculate_encoded_bytes();
        self.touch(&key);
        self.trim();
    }

    fn canonicalize(&self, mut entry: CacheEntry) -> CacheEntry {
        let CacheEntry::Loaded(candidate) = &mut entry else {
            return entry;
        };
        if let Some(existing) = self.entries.values().find_map(|entry| match entry {
            CacheEntry::Loaded(existing) if existing.image.id() == candidate.image.id() => {
                Some(existing)
            }
            _ => None,
        }) {
            candidate.image = existing.image.clone();
            candidate.thumbnail = existing.thumbnail.clone();
        }
        entry
    }

    fn recalculate_encoded_bytes(&mut self) {
        let mut seen = HashSet::new();
        self.encoded_bytes = self
            .entries
            .values()
            .filter_map(|entry| match entry {
                CacheEntry::Loaded(image) => Some(image),
                CacheEntry::Loading { .. } | CacheEntry::Error { .. } => None,
            })
            .flat_map(|image| [&image.image, &image.thumbnail])
            .filter(|image| seen.insert(image.id()))
            .map(|image| image.bytes.len())
            .sum();
    }

    fn trim(&mut self) {
        while self.entries.len() > self.max_entries || self.encoded_bytes > self.max_encoded_bytes {
            let Some(oldest) = self.lru.pop_front() else {
                break;
            };
            let Some(removed) = self.entries.remove(&oldest) else {
                continue;
            };
            self.retire_if_uncached(removed);
            self.recalculate_encoded_bytes();
        }
    }

    fn retire_if_uncached(&mut self, entry: CacheEntry) {
        let CacheEntry::Loaded(image) = entry else {
            return;
        };
        for candidate in [image.image, image.thumbnail] {
            let candidate_id = candidate.id();
            if self.entries.values().any(|entry| {
                matches!(entry, CacheEntry::Loaded(image)
                    if image.image.id() == candidate_id || image.thumbnail.id() == candidate_id)
            }) || self.retired.iter().any(|image| image.id() == candidate_id)
            {
                continue;
            }
            self.retired.push(candidate);
        }
    }

    fn take_retireable(&mut self) -> Vec<Arc<Image>> {
        let mut ready = Vec::new();
        let pending = std::mem::take(&mut self.retired);
        for image in pending {
            let is_cached = self.entries.values().any(|entry| {
                matches!(entry, CacheEntry::Loaded(cached)
                    if cached.image.id() == image.id() || cached.thumbnail.id() == image.id())
            });
            if is_cached || Arc::strong_count(&image) != 1 {
                self.retired.push(image);
            } else {
                ready.push(image);
            }
        }
        ready
    }
}

fn retry_delay(attempts: u32) -> Duration {
    Duration::from_millis((2_000u64 << attempts.min(3)).min(15_000))
}

fn cache() -> &'static Mutex<AttachmentCache> {
    static CACHE: OnceLock<Mutex<AttachmentCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(AttachmentCache::new()))
}

/// Releases GPUI's decoded CPU image and sprite-atlas tile for attachment
/// entries that have left the process-wide LRU and are no longer referenced by
/// a preview, composer send, or another cache key.
pub fn retire_evicted_images(window: &mut Window, cx: &mut App) {
    let images = cache()
        .lock()
        .map(|mut cache| cache.take_retireable())
        .unwrap_or_default();
    if images.is_empty() {
        return;
    }
    let mut decoded = Vec::new();
    for image in images {
        if let Some(render_image) = image.clone().get_render_image(window, cx) {
            decoded.push(render_image);
        }
        image.remove_asset(cx);
    }
    if decoded.is_empty() {
        return;
    }
    cx.defer(move |cx| {
        for image in decoded {
            cx.drop_image(image, None);
        }
    });
}

fn key(device_id: &str, path: &str) -> (String, String) {
    (device_id.to_string(), path.to_string())
}

pub fn attachment_snapshot(device_id: &str, path: &str) -> AttachmentSnapshot {
    let key = key(device_id, path);
    let mut cache = cache().lock().unwrap();
    if cache.entries.contains_key(&key) {
        cache.touch(&key);
    }
    match cache.entries.get(&key) {
        Some(CacheEntry::Loaded(image)) => AttachmentSnapshot::Loaded(image.clone()),
        Some(CacheEntry::Error { attempts, at }) => AttachmentSnapshot::Error {
            retry_in: retry_delay(attempts.saturating_sub(1)).saturating_sub(at.elapsed()),
        },
        _ => AttachmentSnapshot::Loading,
    }
}

/// Claim the load for a source: `true` ⇒ the caller should start fetching now
/// (the entry is marked Loading so concurrent renders don't double-fetch).
/// Errored sources hand out a retry only after their backoff has elapsed.
pub fn begin_load(device_id: &str, path: &str) -> bool {
    let mut cache = cache().lock().unwrap();
    let key = key(device_id, path);
    cache.touch(&key);
    let entry = cache.entries.entry(key);
    match entry {
        std::collections::hash_map::Entry::Vacant(v) => {
            v.insert(CacheEntry::Loading { attempts: 0 });
            cache.trim();
            true
        }
        std::collections::hash_map::Entry::Occupied(mut o) => match o.get() {
            CacheEntry::Error { attempts, at }
                if at.elapsed() >= retry_delay(attempts.saturating_sub(1)) =>
            {
                let attempts = *attempts;
                o.insert(CacheEntry::Loading { attempts });
                true
            }
            _ => false,
        },
    }
}

pub fn store_loaded(
    device_id: &str,
    path: &str,
    name: SharedString,
    image: Arc<Image>,
    thumbnail: Arc<Image>,
) {
    cache().lock().unwrap().insert(
        key(device_id, path),
        CacheEntry::Loaded(CachedAttachmentImage {
            name,
            image,
            thumbnail,
        }),
    );
}

pub fn store_error(device_id: &str, path: &str) {
    let mut cache = cache().lock().unwrap();
    let key = key(device_id, path);
    let attempts = match cache.entries.get(&key) {
        Some(CacheEntry::Loading { attempts }) => attempts + 1,
        Some(CacheEntry::Error { attempts, .. }) => *attempts,
        _ => 1,
    };
    cache.insert(
        key,
        CacheEntry::Error {
            attempts,
            at: Instant::now(),
        },
    );
}

/// Seed the cache after a successful upload (composer send path) so the just-
/// sent bubble's thumbnails render from local bytes instead of a round-trip.
pub fn seed_attachment(
    device_id: &str,
    path: &str,
    name: &str,
    image: Arc<Image>,
    thumbnail: Arc<Image>,
) {
    store_loaded(device_id, path, name.to_string().into(), image, thumbnail);
}

// ---------------------------------------------------------------------------
// Preview lightbox (attachment-ui.tsx AttachmentPreviewDialog)
// ---------------------------------------------------------------------------

/// A full-size preview target (staged strip or transcript thumbnail).
#[derive(Clone)]
pub struct PreviewImage {
    pub name: SharedString,
    pub image: Arc<Image>,
}

/// The bare lightbox: dim scrim, the image at ≤85vh/90vw, the file name under
/// it. Any click closes (the whole dialog is the close button, as in the
/// original's `cursor-zoom-out` figure).
pub fn lightbox(
    viewport: Size<gpui::Pixels>,
    preview: &PreviewImage,
    on_close: impl Fn(&mut gpui::Window, &mut gpui::App) + 'static,
) -> AnyElement {
    let max_h = px(f32::from(viewport.height) * 0.85);
    let max_w = px(f32::from(viewport.width) * 0.9);
    gpui::deferred(
        gpui::anchored()
            .position(gpui::point(px(0.0), px(0.0)))
            .child(
                div()
                    .id("attachment-lightbox")
                    .occlude()
                    .w(viewport.width)
                    .h(viewport.height)
                    .bg(gpui::hsla(0.0, 0.0, 0.0, 0.7))
                    .flex()
                    .flex_col()
                    .items_center()
                    .justify_center()
                    .gap(px(12.0))
                    .cursor_pointer()
                    .on_click(move |_, window, cx| on_close(window, cx))
                    .child(
                        img(preview.image.clone())
                            .object_fit(ObjectFit::Contain)
                            .max_h(max_h)
                            .max_w(max_w)
                            .rounded(px(6.0))
                            .shadow_2xl(),
                    )
                    .child(
                        div()
                            .max_w(max_w)
                            .overflow_hidden()
                            .text_size(px(11.0))
                            .text_color(white_alpha(0.45))
                            .child(preview.name.clone()),
                    ),
            ),
    )
    .priority(3)
    .into_any_element()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_attachments_round_trips_through_parse() {
        let paths = vec!["/data/uploads/ab-cat.png".to_string(), "/x/dog.jpg".into()];
        let content = with_attachments("look at these", &paths);
        let parsed = parse_user_message_images(&content);
        assert_eq!(parsed.text, "look at these");
        assert_eq!(parsed.attachments.len(), 2);
        assert_eq!(parsed.attachments[0].path, "/data/uploads/ab-cat.png");
        assert_eq!(parsed.attachments[0].name, "ab-cat.png");
        assert_eq!(parsed.attachments[1].name, "dog.jpg");
        assert_eq!(parsed.attachments[0].id, "0:/data/uploads/ab-cat.png");
    }

    #[test]
    fn image_only_send_hides_placeholder_body() {
        let content = with_attachments("", &["/a/b.png".to_string()]);
        assert!(content.starts_with(ATTACHMENT_ONLY_TEXT));
        let parsed = parse_user_message_images(&content);
        assert_eq!(parsed.text, "");
        assert_eq!(parsed.attachments.len(), 1);
    }

    #[test]
    fn plain_text_passes_through_unchanged() {
        assert_eq!(with_attachments("hello", &[]), "hello");
        let parsed = parse_user_message_images("hello\n\nno images here");
        assert!(parsed.attachments.is_empty());
        assert_eq!(parsed.text, "hello\n\nno images here");
    }

    #[test]
    fn marker_is_case_insensitive_and_requires_ref_lines() {
        let parsed = parse_user_message_images(
            "hi\n\nATTACHED IMAGES (local files — open them to view):\n- /p/q.png",
        );
        assert_eq!(parsed.attachments.len(), 1);
        // A trailer with no valid `- path` lines is left as plain text.
        let empty = parse_user_message_images(
            "hi\n\nAttached images (local files — open them to view):\nnothing",
        );
        assert!(empty.attachments.is_empty());
        assert!(empty.text.contains("Attached images"));
    }

    #[test]
    fn rail_text_summarizes_image_only_sends() {
        let one = with_attachments("", &["/a/b.png".to_string()]);
        assert_eq!(user_message_rail_text(&one), "Attached image");
        let two = with_attachments("", &["/a/b.png".to_string(), "/c/d.png".into()]);
        assert_eq!(user_message_rail_text(&two), "2 attached images");
        let with_text = with_attachments("fix this", &["/a/b.png".to_string()]);
        assert_eq!(user_message_rail_text(&with_text), "fix this");
        assert_eq!(user_message_rail_text("plain"), "plain");
    }

    #[test]
    fn ensure_extension_matches_browser_heuristic() {
        assert_eq!(ensure_extension("shot.png", ImageFormat::Png), "shot.png");
        assert_eq!(ensure_extension("image", ImageFormat::Png), "image.png");
        assert_eq!(
            ensure_extension("photo.j", ImageFormat::Jpeg),
            "photo.j.jpg"
        );
        assert_eq!(
            ensure_extension("archive.tar.gz", ImageFormat::Png),
            "archive.tar.gz"
        );
    }

    #[test]
    fn supported_formats_match_engine_jail() {
        for (ext, expect) in [
            ("png", Some(ImageFormat::Png)),
            ("JPG", Some(ImageFormat::Jpeg)),
            ("webp", Some(ImageFormat::Webp)),
            ("svg", Some(ImageFormat::Svg)),
            ("ico", None),
            ("txt", None),
        ] {
            assert_eq!(
                format_by_extension(Path::new(&format!("f.{ext}"))),
                expect,
                "ext {ext}"
            );
        }
    }

    #[test]
    fn retry_ladder_is_2s_doubling_capped_at_15s() {
        assert_eq!(retry_delay(0), Duration::from_millis(2_000));
        assert_eq!(retry_delay(1), Duration::from_millis(4_000));
        assert_eq!(retry_delay(2), Duration::from_millis(8_000));
        assert_eq!(retry_delay(3), Duration::from_millis(15_000));
        assert_eq!(retry_delay(9), Duration::from_millis(15_000));
    }

    #[test]
    fn jpeg_thumbnail_is_bounded_before_gpui_paints_it() {
        let source = image::RgbImage::from_pixel(800, 1200, image::Rgb([30, 80, 160]));
        let mut encoded = Cursor::new(Vec::new());
        source
            .write_to(&mut encoded, image::ImageFormat::Jpeg)
            .unwrap();

        let thumbnail = make_attachment_thumbnail(ImageFormat::Jpeg, encoded.get_ref()).unwrap();
        let decoded = image::load_from_memory(&thumbnail.bytes).unwrap();

        assert!(decoded.width() <= ATTACHMENT_THUMB_MAX_WIDTH);
        assert!(decoded.height() <= ATTACHMENT_THUMB_MAX_HEIGHT);
        assert_eq!(thumbnail.format, ImageFormat::Png);
    }

    #[test]
    fn png_thumbnail_is_bounded_before_gpui_paints_it() {
        let source = image::RgbaImage::from_pixel(800, 1200, image::Rgba([30, 80, 160, 255]));
        let mut encoded = Cursor::new(Vec::new());
        source
            .write_to(&mut encoded, image::ImageFormat::Png)
            .unwrap();

        let thumbnail = make_attachment_thumbnail(ImageFormat::Png, encoded.get_ref()).unwrap();
        let decoded = image::load_from_memory(&thumbnail.bytes).unwrap();

        assert_eq!((decoded.width(), decoded.height()), (107, 160));
        assert_eq!(thumbnail.format, ImageFormat::Png);
    }

    #[test]
    fn cache_canonicalizes_identical_content_across_device_keys() {
        let mut cache = AttachmentCache::new();
        for device in ["one", "two"] {
            cache.insert(
                (device.into(), "/same.jpg".into()),
                CacheEntry::Loaded(CachedAttachmentImage {
                    name: "same.jpg".into(),
                    image: Arc::new(Image::from_bytes(ImageFormat::Jpeg, vec![1, 2, 3])),
                    thumbnail: Arc::new(Image::from_bytes(ImageFormat::Png, vec![4, 5])),
                }),
            );
        }

        let first = match cache.entries.get(&("one".into(), "/same.jpg".into())) {
            Some(CacheEntry::Loaded(image)) => image,
            _ => panic!("first image missing"),
        };
        let second = match cache.entries.get(&("two".into(), "/same.jpg".into())) {
            Some(CacheEntry::Loaded(image)) => image,
            _ => panic!("second image missing"),
        };
        assert!(Arc::ptr_eq(&first.image, &second.image));
        assert!(Arc::ptr_eq(&first.thumbnail, &second.thumbnail));
        assert_eq!(cache.encoded_bytes, 5);
    }

    #[test]
    fn transcript_attachment_cache_evicts_by_encoded_byte_budget() {
        let mut cache = AttachmentCache::with_limits(128, 16);
        let image_bytes = 9;
        for index in 0..2 {
            let thumbnail = Arc::new(Image::from_bytes(
                ImageFormat::Png,
                vec![100 + index as u8; 2],
            ));
            cache.insert(
                ("device".into(), format!("/{index}.png")),
                CacheEntry::Loaded(CachedAttachmentImage {
                    name: format!("{index}.png").into(),
                    image: Arc::new(Image::from_bytes(
                        ImageFormat::Png,
                        vec![index as u8; image_bytes],
                    )),
                    thumbnail,
                }),
            );
        }
        assert_eq!(cache.entries.len(), 1);
        assert!(
            !cache
                .entries
                .contains_key(&("device".into(), "/0.png".into()))
        );
        assert!(
            cache
                .entries
                .contains_key(&("device".into(), "/1.png".into()))
        );
        assert_eq!(cache.encoded_bytes, image_bytes + 2);
        assert_eq!(cache.retired.len(), 2);
        assert_eq!(cache.take_retireable().len(), 2);
    }

    #[test]
    fn transcript_attachment_cache_evicts_by_entry_budget() {
        let mut cache = AttachmentCache::with_limits(4, usize::MAX);
        for index in 0..=4 {
            cache.insert(
                ("device".into(), format!("/{index}.png")),
                CacheEntry::Error {
                    attempts: 1,
                    at: Instant::now(),
                },
            );
        }
        assert_eq!(cache.entries.len(), 4);
        assert!(
            !cache
                .entries
                .contains_key(&("device".into(), "/0.png".into()))
        );
    }

    #[test]
    fn transcript_attachment_cache_defers_retirement_while_image_is_in_use() {
        let mut cache = AttachmentCache::new();
        let image = Arc::new(Image::from_bytes(ImageFormat::Png, vec![1, 2, 3]));
        cache.retired.push(image.clone());

        assert!(cache.take_retireable().is_empty());
        assert_eq!(cache.retired.len(), 1);

        drop(image);
        assert_eq!(cache.take_retireable().len(), 1);
        assert!(cache.retired.is_empty());
    }
}
