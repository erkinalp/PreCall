// SPDX-License-Identifier: GPL-2.0-only
//! `si_*` semantic vector store — schema-compatible with Recall's
//! `SemanticTextStore.db` / `SemanticImageStore.db`.
//!
//! Embeddings are stored as little-endian `f32` blobs in `si_diskann_graph`,
//! keyed by auto-increment ids; `si_embedding_metadata` maps an embedding back
//! to its owning item (a `WindowCapture` id in the `si_items` table) and
//! optional region. Neighbour search currently runs a flat cosine scan — for
//! the capture volumes a workstation produces this is comfortably fast; the
//! `outbound_ids`/`si_diskann_*` columns are already populated so a real
//! DiskANN index can be grafted on without a schema migration.

use crate::schema::SI_SCHEMA;
use rusqlite::{params, Connection};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SemanticError {
    #[error("sqlite: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimMismatch { expected: usize, got: usize },
    #[error("item id must be 16 bytes")]
    BadItemId,
}

#[derive(Debug, Clone)]
pub struct EmbeddingHit {
    /// The `si_items` id — for Precall this is the 16-byte encoding of the
    /// owning `WindowCapture` id (see [`SemanticIndex::item_id_for_capture`]).
    pub item_id: Vec<u8>,
    pub region_id: Option<String>,
    pub score: f32,
}

/// One `si_*` database (text or image).
pub struct SemanticIndex {
    conn: Connection,
    dim: usize,
}

impl SemanticIndex {
    /// Open (or create) a semantic store with embedding dimension `dim`.
    pub fn open(path: &Path, dim: usize, space_id: &str) -> Result<Self, SemanticError> {
        let conn = Connection::open(path)?;
        conn.execute_batch(SI_SCHEMA)?;
        let idx = Self { conn, dim };
        idx.ensure_info(space_id)?;
        Ok(idx)
    }

    pub fn open_in_memory(dim: usize, space_id: &str) -> Result<Self, SemanticError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SI_SCHEMA)?;
        let idx = Self { conn, dim };
        idx.ensure_info(space_id)?;
        Ok(idx)
    }

    fn ensure_info(&self, space_id: &str) -> Result<(), SemanticError> {
        self.conn.execute(
            "INSERT INTO \"si_diskann_info\" (\"graph_table_name\",\"dimension\",\"vector_space_id\")
             SELECT 'si_diskann_graph', ?, ?
             WHERE NOT EXISTS (SELECT 1 FROM \"si_diskann_info\")",
            params![self.dim as i64, space_id],
        )?;
        self.conn.execute(
            "INSERT INTO \"si_diskann_config\" (\"graph_table_name\",\"max_degree\",\"alpha\")
             SELECT 'si_diskann_graph', 32, 1.2
             WHERE NOT EXISTS (SELECT 1 FROM \"si_diskann_config\")",
            [],
        )?;
        Ok(())
    }

    /// Deterministic `si_items` id for a `WindowCapture` row — id in the low
    /// bytes of a 16-byte buffer, matching Recall's blob-keyed items.
    pub fn item_id_for_capture(capture_id: i64) -> Vec<u8> {
        let mut v = vec![0u8; 16];
        v[8..16].copy_from_slice(&capture_id.to_le_bytes());
        v
    }

    /// Insert `embedding` (len must equal `dim`) under `item_id` with optional
    /// region linkage. Returns the `si_diskann_graph` row id.
    pub fn insert(
        &self,
        item_id: &[u8],
        region_id: Option<&str>,
        metadata_json: Option<&str>,
        embedding: &[f32],
    ) -> Result<i64, SemanticError> {
        if embedding.len() != self.dim {
            return Err(SemanticError::DimMismatch { expected: self.dim, got: embedding.len() });
        }
        if item_id.len() != 16 {
            return Err(SemanticError::BadItemId);
        }
        let blob = f32s_to_bytes(embedding);
        self.conn.execute("INSERT OR IGNORE INTO \"si_items\" (\"id\") VALUES (?)", params![item_id])?;
        self.conn.execute(
            "INSERT INTO \"si_diskann_graph\" (\"embedding\",\"outbound_ids\") VALUES (?, '')",
            params![blob],
        )?;
        let graph_id = self.conn.last_insert_rowid();
        self.conn.execute(
            "INSERT INTO \"si_embedding_metadata\" (\"embedding_id\",\"item_id\",\"region_id\",\"metadata_json\")
             VALUES (?,?,?,?)",
            params![graph_id, item_id, region_id, metadata_json],
        )?;
        Ok(graph_id)
    }

    /// Flat cosine search — returns the `k` nearest items, best first.
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<EmbeddingHit>, SemanticError> {
        if query.len() != self.dim {
            return Err(SemanticError::DimMismatch { expected: self.dim, got: query.len() });
        }
        let mut stmt = self.conn.prepare(
            "SELECT g.\"id\", g.\"embedding\", m.\"item_id\", m.\"region_id\"
             FROM \"si_diskann_graph\" g
             JOIN \"si_embedding_metadata\" m ON m.\"embedding_id\" = g.\"id\"",
        )?;
        let qnorm = norm(query);
        let mut hits = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let blob: Vec<u8> = row.get(1)?;
            let emb = bytes_to_f32s(&blob);
            if emb.len() != self.dim {
                continue;
            }
            let score = cosine(query, qnorm, &emb);
            hits.push(EmbeddingHit {
                item_id: row.get(2)?,
                region_id: row.get(3)?,
                score,
            });
        }
        hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        hits.truncate(k);
        Ok(hits)
    }

    pub fn count(&self) -> Result<i64, SemanticError> {
        Ok(self.conn.query_row("SELECT COUNT(*) FROM \"si_diskann_graph\"", [], |r| r.get(0))?)
    }

    /// Decode a `si_items` id back to a `WindowCapture` id (inverse of
    /// [`SemanticIndex::item_id_for_capture`]).
    pub fn capture_id_for_item(item_id: &[u8]) -> Option<i64> {
        if item_id.len() != 16 || item_id[..8].iter().any(|&b| b != 0) {
            return None;
        }
        Some(i64::from_le_bytes(item_id[8..16].try_into().ok()?))
    }
}

fn f32s_to_bytes(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn bytes_to_f32s(b: &[u8]) -> Vec<f32> {
    b.chunks_exact(4).map(|c| f32::from_le_bytes(c.try_into().unwrap())).collect()
}

fn norm(v: &[f32]) -> f32 {
    v.iter().map(|x| x * x).sum::<f32>().sqrt().max(f32::EPSILON)
}

fn cosine(query: &[f32], qnorm: f32, other: &[f32]) -> f32 {
    let dot: f32 = query.iter().zip(other.iter()).map(|(a, b)| a * b).sum();
    dot / (qnorm * norm(other))
}
