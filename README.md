# Comet

A local note-taking agent. The desktop app is one chat box: a transcript and a
composer over a small engine that keeps conversations in a local Loro/SQLite
store. No account, cloud sync, sidebar, or self-update surface — everything
stays on this device.

The chat surface is the original Comet chat UI (session tabs, transcript with
the left MessageRail outline, composer) with the left sidebar removed. The
first message auto-creates a default local space so no sidebar is needed.

## Run

```bash
cargo run                       # desktop UI (embeds the engine)
cargo run -- headless           # engine only
cargo run -- daemon install     # install the headless service
```

The engine stores chats under `~/.comet-native` (override with `COMET_DATA_DIR`).
The first message auto-creates a default local space there.

Licensed under the [MIT License](LICENSE).
