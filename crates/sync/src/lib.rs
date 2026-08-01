//! comet-sync — local persistence for the note-agent engine.
//!
//! [`DocsStore`] keeps SQLite doc snapshots and the processed-command ledger
//! with mark-BEFORE-execute semantics. The Loro room client and edge sync were
//! removed with the multi-device features.

mod store;

pub use store::{DocsStore, StoreError};
