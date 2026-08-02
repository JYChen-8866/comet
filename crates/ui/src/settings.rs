//! UI settings persisted to a small JSON file in the data dir — sidebar width,
//! space/session order, sound, and shortcut bindings.
//!
//! Loaded once at boot; saved debounced by the shell ([`SAVE_DEBOUNCE_MS`]).
//! Corrupt or missing files fall back to defaults; loaded values are clamped so a
//! hand-edited file can't wedge the layout.

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub mod archived;
pub mod composer;
pub mod shortcuts;
pub mod widgets;

/// Sidebar drag-resize bounds (px).
pub const SIDEBAR_MIN: f32 = 208.0;
pub const SIDEBAR_MAX: f32 = 400.0;
pub const SIDEBAR_DEFAULT: f32 = 256.0;

/// Debounce for settings writes after a drag/toggle.
pub const SAVE_DEBOUNCE_MS: u64 = 400;

const FILE_NAME: &str = "ui-settings.json";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct UiSettings {
    pub sidebar_width: f32,
    pub sidebar_collapsed: bool,
    /// Legacy: the grouped-by-project toggle predates spaces (which group by
    /// folder inherently). Kept for file compatibility; no longer read.
    pub sidebar_grouped: bool,
    /// The last selected space — restored on boot when the row still exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_space_id: Option<String>,
    /// Manual session-tab order per space (drag-reorder; device-local).
    /// Missing chats are skipped; new chats append in creation order.
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub tab_order: std::collections::HashMap<String, Vec<String>>,
    /// Manual sidebar space order (drag-reorder; device-local). Missing spaces
    /// are skipped; new spaces append in creation order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub space_order: Vec<String>,
    /// Session notification chimes (done / awaiting-input). `COMET_DISABLE_SOUND`
    /// overrides.
    pub sound_enabled: bool,
    /// Customizable shortcut combos (feature-inventory §1.4).
    pub keymap: KeymapConfig,
}

impl Default for UiSettings {
    fn default() -> Self {
        Self {
            sidebar_width: SIDEBAR_DEFAULT,
            sidebar_collapsed: false,
            sidebar_grouped: false,
            last_space_id: None,
            tab_order: std::collections::HashMap::new(),
            space_order: Vec::new(),
            sound_enabled: true,
            keymap: KeymapConfig::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Keymap (customizable shortcuts, §1.4)
// ---------------------------------------------------------------------------

/// The rebindable app shortcuts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShortcutId {
    ToggleSidebar,
}

impl ShortcutId {
    pub const ALL: [ShortcutId; 1] = [ShortcutId::ToggleSidebar];

    /// Row label (comet lib/shortcuts.ts `SHORTCUT_DEFINITIONS`, verbatim).
    pub fn label(self) -> &'static str {
        match self {
            ShortcutId::ToggleSidebar => "Toggle left sidebar",
        }
    }

    pub fn default_combo(self) -> &'static str {
        match self {
            ShortcutId::ToggleSidebar => "mod-s",
        }
    }
}

/// Persisted shortcut combos. Stored platform-neutral ("mod-s"); translated to
/// "cmd-s"/"ctrl-s" at bind time by [`platform_combo`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct KeymapConfig {
    pub toggle_sidebar: String,
}

impl Default for KeymapConfig {
    fn default() -> Self {
        Self {
            toggle_sidebar: ShortcutId::ToggleSidebar.default_combo().into(),
        }
    }
}

impl KeymapConfig {
    pub fn get(&self, id: ShortcutId) -> &str {
        match id {
            ShortcutId::ToggleSidebar => &self.toggle_sidebar,
        }
    }

    pub fn set(&mut self, id: ShortcutId, combo: String) {
        match id {
            ShortcutId::ToggleSidebar => self.toggle_sidebar = combo,
        }
    }

    pub fn reset(&mut self, id: ShortcutId) {
        self.set(id, id.default_combo().to_string());
    }
}

/// Build a combo string from a recorded keystroke. The primary modifier
/// (cmd on macOS, ctrl elsewhere — either recorded key maps in) becomes "mod";
/// bare modifier presses record nothing.
pub fn combo_from_keystroke(
    ctrl: bool,
    alt: bool,
    shift: bool,
    cmd: bool,
    key: &str,
) -> Option<String> {
    let key = key.trim().to_lowercase();
    if key.is_empty()
        || matches!(
            key.as_str(),
            "ctrl" | "control" | "alt" | "shift" | "cmd" | "platform" | "fn"
        )
    {
        return None;
    }
    let mut parts: Vec<&str> = Vec::new();
    if ctrl || cmd {
        parts.push("mod");
    }
    if alt {
        parts.push("alt");
    }
    if shift {
        parts.push("shift");
    }
    parts.push(&key);
    Some(parts.join("-"))
}

/// Shortcut ids whose combos collide with another shortcut (conflict detection).
pub fn conflicted_shortcuts(keymap: &KeymapConfig) -> Vec<ShortcutId> {
    ShortcutId::ALL
        .into_iter()
        .filter(|&id| {
            let combo = keymap.get(id);
            !combo.is_empty()
                && ShortcutId::ALL
                    .into_iter()
                    .any(|other| other != id && keymap.get(other) == combo)
        })
        .collect()
}

/// Translate a stored combo into a bindable keystroke for this platform.
pub fn platform_combo(combo: &str) -> String {
    let primary = if cfg!(target_os = "macos") {
        "cmd"
    } else {
        "ctrl"
    };
    combo
        .split('-')
        .map(|part| if part == "mod" { primary } else { part })
        .collect::<Vec<_>>()
        .join("-")
}

/// Human-readable combo for the shortcuts table ("mod-s" → "Cmd+S"/"Ctrl+S").
pub fn display_combo(combo: &str) -> String {
    combo
        .split('-')
        .map(|part| match part {
            "mod" => {
                if cfg!(target_os = "macos") {
                    "Cmd".to_string()
                } else {
                    "Ctrl".to_string()
                }
            }
            "alt" => "Alt".to_string(),
            "shift" => "Shift".to_string(),
            other => {
                let mut chars = other.chars();
                match chars.next() {
                    Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                    None => String::new(),
                }
            }
        })
        .collect::<Vec<_>>()
        .join("+")
}

impl UiSettings {
    /// Clamp widths into their legal ranges (also heals NaN to defaults).
    pub fn clamped(mut self) -> Self {
        self.sidebar_width = clamp_or(
            self.sidebar_width,
            SIDEBAR_MIN,
            SIDEBAR_MAX,
            SIDEBAR_DEFAULT,
        );
        self
    }

    /// Load from `{data_dir}/ui-settings.json`; defaults on any failure.
    pub fn load(data_dir: &Path) -> Self {
        match std::fs::read_to_string(Self::path(data_dir)) {
            Ok(text) => match serde_json::from_str::<UiSettings>(&text) {
                Ok(settings) => settings.clamped(),
                Err(err) => {
                    tracing::warn!(error = %err, "ui-settings corrupt; using defaults");
                    Self::default()
                }
            },
            Err(_) => Self::default(),
        }
    }

    /// Write atomically (temp file + rename) so a crash mid-write never corrupts.
    pub fn save(&self, data_dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(data_dir)?;
        let path = Self::path(data_dir);
        let tmp = path.with_extension("json.tmp");
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        std::fs::write(&tmp, json)?;
        std::fs::rename(&tmp, &path)
    }

    pub fn path(data_dir: &Path) -> PathBuf {
        data_dir.join(FILE_NAME)
    }
}

fn clamp_or(value: f32, min: f32, max: f32, default: f32) -> f32 {
    if value.is_finite() {
        value.clamp(min, max)
    } else {
        default
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let settings = UiSettings {
            sidebar_width: 300.0,
            sidebar_collapsed: true,
            sidebar_grouped: true,
            last_space_id: Some("space-1".into()),
            tab_order: std::collections::HashMap::from([(
                "space-1".to_string(),
                vec!["b".to_string(), "a".to_string()],
            )]),
            space_order: vec!["space-2".to_string(), "space-1".to_string()],
            sound_enabled: false,
            keymap: KeymapConfig {
                toggle_sidebar: "mod-shift-s".into(),
                ..KeymapConfig::default()
            },
        };
        settings.save(dir.path()).unwrap();
        assert_eq!(UiSettings::load(dir.path()), settings);
    }

    #[test]
    fn missing_and_corrupt_files_yield_defaults() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(UiSettings::load(dir.path()), UiSettings::default());
        std::fs::write(UiSettings::path(dir.path()), "{not json").unwrap();
        assert_eq!(UiSettings::load(dir.path()), UiSettings::default());
    }

    #[test]
    fn loaded_values_are_clamped() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            UiSettings::path(dir.path()),
            r#"{"sidebarWidth": 10000, "rightPaneWidth": 1}"#,
        )
        .unwrap();
        let loaded = UiSettings::load(dir.path());
        assert_eq!(loaded.sidebar_width, SIDEBAR_MAX);
    }

    #[test]
    fn nan_heals_to_default() {
        let healed = UiSettings {
            sidebar_width: f32::NAN,
            ..Default::default()
        }
        .clamped();
        assert_eq!(healed.sidebar_width, SIDEBAR_DEFAULT);
    }

    #[test]
    fn defaults_match_comet() {
        let d = UiSettings::default();
        assert_eq!(d.sidebar_width, 256.0);
        assert!(!d.sidebar_collapsed);
    }

    #[test]
    fn keymap_defaults_and_reset() {
        let mut keymap = KeymapConfig::default();
        assert_eq!(keymap.get(ShortcutId::ToggleSidebar), "mod-s");
        keymap.set(ShortcutId::ToggleSidebar, "mod-shift-x".into());
        assert_eq!(keymap.get(ShortcutId::ToggleSidebar), "mod-shift-x");
        keymap.reset(ShortcutId::ToggleSidebar);
        assert_eq!(keymap.get(ShortcutId::ToggleSidebar), "mod-s");
    }

    #[test]
    fn combo_recording() {
        // Primary modifier (ctrl or cmd) normalizes to "mod".
        assert_eq!(
            combo_from_keystroke(true, false, false, false, "s"),
            Some("mod-s".into())
        );
        assert_eq!(
            combo_from_keystroke(false, false, false, true, "s"),
            Some("mod-s".into())
        );
        assert_eq!(
            combo_from_keystroke(true, true, true, false, "K"),
            Some("mod-alt-shift-k".into())
        );
        // Plain keys record without modifiers (Esc is filtered by the caller).
        assert_eq!(
            combo_from_keystroke(false, false, false, false, "f5"),
            Some("f5".into())
        );
        // Bare modifier presses record nothing.
        assert_eq!(
            combo_from_keystroke(true, false, false, false, "ctrl"),
            None
        );
        assert_eq!(
            combo_from_keystroke(false, false, true, false, "shift"),
            None
        );
        assert_eq!(combo_from_keystroke(false, false, false, false, ""), None);
    }

    #[test]
    fn conflict_detection() {
        let mut keymap = KeymapConfig::default();
        assert!(conflicted_shortcuts(&keymap).is_empty());
        keymap.set(ShortcutId::ToggleSidebar, "mod-s".into());
        keymap.reset(ShortcutId::ToggleSidebar);
        assert!(conflicted_shortcuts(&keymap).is_empty());
    }

    #[test]
    fn combo_translation() {
        let primary = if cfg!(target_os = "macos") {
            "cmd"
        } else {
            "ctrl"
        };
        assert_eq!(platform_combo("mod-s"), format!("{primary}-s"));
        assert_eq!(platform_combo("alt-f4"), "alt-f4");
        let display_primary = if cfg!(target_os = "macos") {
            "Cmd"
        } else {
            "Ctrl"
        };
        assert_eq!(
            display_combo("mod-shift-s"),
            format!("{display_primary}+Shift+S")
        );
        assert_eq!(display_combo("f5"), "F5");
    }

    #[test]
    fn keymap_survives_old_settings_files() {
        // Files written before the keymap existed load with defaults.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(UiSettings::path(dir.path()), r#"{"sidebarWidth": 300}"#).unwrap();
        let loaded = UiSettings::load(dir.path());
        assert_eq!(loaded.keymap, KeymapConfig::default());
        assert!(!loaded.sidebar_grouped);
    }
}
