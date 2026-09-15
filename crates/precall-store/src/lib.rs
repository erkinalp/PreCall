// SPDX-License-Identifier: GPL-2.0-only
//! Precall storage layer.
//!
//! Three stores, mirroring Windows Recall's persistence:
//!
//! * [`ukg::Ukg`] — metadata SQLite database (`ukg.db`), byte-compatible
//!   schema with Recall plus Precall's own sync/config tables.
//! * [`semantic::SemanticIndex`] — `si_*` DiskANN-schema stores
//!   (`SemanticTextStore.db` / `SemanticImageStore.db`). Embeddings and graph
//!   rows follow Recall's layout; neighbour search is a flat cosine scan until
//!   a real DiskANN index is layered on (schema is already compatible).
//! * [`objects::ObjectStore`] — screenshot/audio object storage.

mod crypto;
pub mod objects;
mod schema;
pub mod semantic;
pub mod ukg;

pub use crypto::EnvelopeCipher;
pub use objects::ObjectStore;
pub use schema::UKG_SCHEMA;
pub use semantic::{EmbeddingHit, SemanticIndex};
pub use ukg::{
    AppDwellEntry, SearchHit, SearchOptions, TimelineEntry, Ukg, WebDwellEntry,
};
