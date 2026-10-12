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
CLIP_MODEL_NAME = "openai/clip-vit-base-patch32"


def _requested_device() -> str:
    return os.environ.get("PRECALL_AI_DEVICE", "cpu").lower()


@dataclass
class TextEmbedder:
    """Sentence-transformers when installed; deterministic hash otherwise."""

    device: str = "cpu"

    def __post_init__(self) -> None:
        self._model = None
        self.backend = "hash"
        self.device = normalize_device(self.device)
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
        self.device = normalize_device(self.device)
        loaded = _load_clip(self.device)
        if loaded is None:
            self._model = None
            self._preprocess = None
            self.backend = "hash"
        else:
            self._model, self._preprocess, self.device = loaded
            self.backend = "clip"

    def embed(self, jpeg: bytes) -> np.ndarray:
        if self._model is not None and self._preprocess is not None:
            import torch
            from PIL import Image

            img = Image.open(io.BytesIO(jpeg)).convert("RGB")
            inputs = self._preprocess(images=img, return_tensors="pt").to(self.device)
            with torch.no_grad():
                feats = self._model.get_image_features(**inputs)
            v = feats[0].detach().float().cpu().numpy()
            TextEmbedder._cleanup()
            return project_clip(v, DIM)
        return hash_embedding_bytes(jpeg)


def normalize_device(device: str) -> str:
    """Map our device vocabulary onto what PyTorch accepts.

    ``rocm`` is *our* name for the extra; PyTorch exposes ROCm through the CUDA
    device API, so ``.to("rocm")`` raises and the caller would silently fall
    back to the hash backend on exactly the hardware the ROCm extra is for.
    """
    return "cuda" if device in {"cuda", "rocm"} else device


def _load_clip(device: str):
    """Load the CLIP checkpoint once; ``None`` when the stack is absent.

    Both the image encoder and the image-space *text* encoder MUST be built
    from this one function. Two different checkpoints (or two different
    preprocessing pipelines) do not share a space, and text→image search would
    then return confident nonsense instead of failing.
    """
    try:
        import torch
        from transformers import CLIPModel, CLIPProcessor

        device = normalize_device(device)
        if device == "cuda" and not torch.cuda.is_available():
            device = "cpu"
        model = CLIPModel.from_pretrained(CLIP_MODEL_NAME).to(device)
        processor = CLIPProcessor.from_pretrained(CLIP_MODEL_NAME)
        return model, processor, device
    except Exception:
        return None


def project_clip(v: np.ndarray, dim: int = DIM) -> np.ndarray:
    """Project a CLIP feature into the ``si_*`` store width, L2-normalised.

    Truncate-then-renormalise, applied identically to text and image features —
    that shared transform is the whole reason the two projections are
    comparable. It is lossy and crude: a learned 512→dim projection would
    preserve far more cross-modal structure, and nothing here should be
    presented as one.
    """
    v = np.asarray(v, dtype=np.float32).ravel()
    if v.shape[0] > dim:
        v = v[:dim]
    return (v / max(float(np.linalg.norm(v)), 1e-6)).astype(np.float32)


class ImageTextUnavailable(RuntimeError):
    """No CLIP stack — text cannot be placed in the image space."""


@dataclass
class ImageTextEmbedder:
    """Text → image space (the CLIP text tower), sharing the image encoder's
    checkpoint and preprocessing.

    Deliberately has **no** hash fallback: hashed words and hashed pixels live
    in unrelated spaces, so a fallback embedding would make text→image search
    return ranked noise while looking like a working feature. Without CLIP the
    service answers 501 and the caller reports the leg as skipped.
    """

    model: object = None
    preprocess: object = None
    device: str = "cpu"

    @classmethod
    def from_encoder(cls, encoder: ImageEmbedder) -> "ImageTextEmbedder":
        """Reuse the loaded image encoder — same checkpoint, one copy in RAM."""
        return cls(
            model=encoder._model,
            preprocess=encoder._preprocess,
            device=encoder.device,
        )

    @property
    def available(self) -> bool:
        return self.model is not None and self.preprocess is not None

    @property
    def backend(self) -> str:
        return "clip" if self.available else "hash"

    def embed(self, text: str) -> np.ndarray:
        if not self.available:
            raise ImageTextUnavailable(
                "text→image search needs the CLIP stack; install the ai-cpu/ai-cuda extra"
            )
        import torch

        inputs = self.preprocess(text=[text], return_tensors="pt", padding=True)
        inputs = inputs.to(self.device)
        with torch.no_grad():
            feats = self.model.get_text_features(**inputs)
        v = feats[0].detach().float().cpu().numpy()
        TextEmbedder._cleanup()
        return project_clip(v, DIM)


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
