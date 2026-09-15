# SPDX-License-Identifier: GPL-2.0-only
"""Embedding/topic providers.

Design rules (kept strict — see project notes):

* Hardware control is explicit: ``PRECALL_AI_DEVICE`` (``cpu``/``cuda``/
  ``rocm``/``mps``) or ``--force-cpu``. Torch never silently grabs a GPU.
* Model deps live in *separate* optional groups (``ai-cpu``, ``ai-cuda``,
  ``ai-rocm``) so the fallback service installs without a gigabyte of wheels.
* After every batch: ``torch.cuda.empty_cache()`` + ``gc.collect()`` so VRAM
  doesn't ratchet up over a long-running enrichment pass.
* When the torch stack is absent the service still answers with a
  deterministic hash embedding — the Rust side then exercises the exact
  same si_* plumbing (that's the point of the fallback; it is NOT a real
  semantic model and must not be presented as one).
"""

from __future__ import annotations

import gc
import hashlib
import io
import os
import re
from dataclasses import dataclass
from typing import Optional

import numpy as np

DIM = 384  # all-MiniLM-L6-v2 output size; si_* stores are provisioned for this
_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


def _requested_device() -> str:
    return os.environ.get("PRECALL_AI_DEVICE", "cpu").lower()


@dataclass
class TextEmbedder:
    """Sentence-transformers when installed; deterministic hash otherwise."""

    device: str = "cpu"

    def __post_init__(self) -> None:
        self._model = None
        self.backend = "hash"
        if self.device in {"cuda", "rocm"}:
            self.device = "cuda"
        try:
            import torch  # noqa: F401

            if self.device == "cuda" and not torch.cuda.is_available():
                self.device = "cpu"
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer(_MODEL_NAME, device=self.device)
            self.backend = "sentence-transformers"
        except Exception:  # torch not installed / model unavailable offline
            self._model = None
            self.backend = "hash"

    def embed(self, texts: list[str]) -> np.ndarray:
        if self._model is not None:
            emb = self._model.encode(
                texts,
                convert_to_numpy=True,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            self._cleanup()
            return np.asarray(emb, dtype=np.float32)
        return np.stack([hash_embedding(t) for t in texts]).astype(np.float32)

    @staticmethod
    def _cleanup() -> None:
        try:
            import torch

            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        except Exception:
            pass
        gc.collect()


@dataclass
class ImageEmbedder:
    """CLIP image encoder when installed; content-hash otherwise."""

    device: str = "cpu"

    def __post_init__(self) -> None:
        self._model = None
        self._preprocess = None
        self.backend = "hash"
        if self.device in {"cuda", "rocm"}:
            self.device = "cuda"
        try:
            import torch
            from transformers import CLIPModel, CLIPProcessor

            if self.device == "cuda" and not torch.cuda.is_available():
                self.device = "cpu"
            self._model = CLIPModel.from_pretrained(
                "openai/clip-vit-base-patch32"
            ).to(self.device)
            self._preprocess = CLIPProcessor.from_pretrained(
                "openai/clip-vit-base-patch32"
            )
            self.backend = "clip"
        except Exception:
            self._model = None
            self.backend = "hash"

    def embed(self, jpeg: bytes) -> np.ndarray:
        if self._model is not None and self._preprocess is not None:
            import torch
            from PIL import Image

            img = Image.open(io.BytesIO(jpeg)).convert("RGB")
            inputs = self._preprocess(images=img, return_tensors="pt").to(self.device)
            with torch.no_grad():
                feats = self._model.get_image_features(**inputs)
            v = feats[0].detach().float().cpu().numpy()
            v /= max(float(np.linalg.norm(v)), 1e-6)
            TextEmbedder._cleanup()
            # CLIP ViT-B/32 outputs 512 — project down to DIM by truncation
            # + renormalize so it fits the si_* store dimension.
            if v.shape[0] != DIM:
                v = v[:DIM] / max(float(np.linalg.norm(v[:DIM])), 1e-6)
            return v.astype(np.float32)
        return hash_embedding_bytes(jpeg)


class TopicExtractor:
    """Keyword-frequency topics — deterministic, no model dependency.

    (Recall uses KeyBERT-style extraction upstream; the deterministic
    version keeps the shape — [(label, score)] — so the Topic table and
    the Topics API stay exercised.)
    """

    STOP = {
        "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "is",
        "are", "was", "were", "be", "been", "at", "by", "it", "its", "as",
        "with", "that", "this", "from", "not", "but", "you", "your", "we",
    }

    def topics(self, text: str, k: int = 5) -> list[tuple[str, float]]:
        words = re.findall(r"[a-zA-Z][a-zA-Z0-9_\-]{2,}", text.lower())
        freq: dict[str, int] = {}
        for w in words:
            if w in self.STOP:
                continue
            freq[w] = freq.get(w, 0) + 1
        if not freq:
            return []
        top = sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))[:k]
        m = top[0][1]
        return [(w, round(c / m, 3)) for w, c in top]


def hash_embedding(text: str, dim: int = DIM) -> np.ndarray:
    """Deterministic bag-of-words hashing — mirrors the Rust Local embedder."""
    v = np.zeros(dim, dtype=np.float32)
    for pos, word in enumerate(text.split()):
        h = hashlib.sha256(word.lower().encode()).digest()
        idx = int.from_bytes(h[:4], "little") % dim
        sign = 1.0 if h[4] & 1 else -1.0
        v[idx] += sign * (1.0 / (1.0 + pos * 0.05))
    n = float(np.linalg.norm(v))
    if n > 0:
        v /= n
    return v


def hash_embedding_bytes(data: bytes, dim: int = DIM) -> np.ndarray:
    acc = np.zeros(32, dtype=np.uint8)
    for i in range(0, len(data), 32):
        h = hashlib.sha256(data[i : i + 32]).digest()
        acc = np.frombuffer(
            bytes(a ^ b for a, b in zip(acc, np.frombuffer(h, dtype=np.uint8))),
            dtype=np.uint8,
        )
    v = (acc.astype(np.float32) / 255.0) - 0.5
    if v.shape[0] < dim:
        v = np.resize(v, dim)
    return (v / max(float(np.linalg.norm(v)), 1e-6)).astype(np.float32)
