# SPDX-License-Identifier: GPL-2.0-only
"""Service tests — run against the deterministic hash backend so they pass
without model downloads (the model path is exercised in CI only when the
ai-cpu extra is installed)."""

import os

os.environ.setdefault("PRECALL_AI_DEVICE", "cpu")

import numpy as np
import pytest
from fastapi.testclient import TestClient

from precall_ai.providers import (
    DIM,
    ImageEmbedder,
    ImageTextEmbedder,
    hash_embedding,
    project_clip,
    TopicExtractor,
)
from precall_ai.server import app


@pytest.fixture(scope="module")
def client() -> TestClient:
    with TestClient(app) as c:
        yield c


def test_health(client: TestClient) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["dim"] == 384


def test_embed_text_shape_and_determinism(client: TestClient) -> None:
    r1 = client.post("/embed/text", json={"text": "kerberos tickets"})
    r2 = client.post("/embed/text", json={"text": "kerberos tickets"})
    assert r1.status_code == 200
    a, b = np.array(r1.json()["embedding"]), np.array(r2.json()["embedding"])
    assert a.shape == (384,)
    np.testing.assert_allclose(a, b)
    # Normalized
    assert abs(float(np.linalg.norm(a)) - 1.0) < 1e-3


def test_embed_batch(client: TestClient) -> None:
    r = client.post(
        "/embed/text/batch", json={"texts": ["alpha", "beta", "gamma"]}
    )
    assert r.status_code == 200
    embs = r.json()["embeddings"]
    assert len(embs) == 3 and len(embs[0]) == 384


def test_similar_texts_score_higher_than_unrelated() -> None:
    a = hash_embedding("kerberos ticket renewal")
    b = hash_embedding("kerberos ticket policy")
    c = hash_embedding("banana bread recipe")
    assert float(a @ b) > float(a @ c)


def test_topics(client: TestClient) -> None:
    r = client.post(
        "/topics",
        json={
            "text": "kerberos kerberos tickets tickets renewal policy policy",
            "k": 3,
        },
    )
    assert r.status_code == 200
    topics = r.json()["topics"]
    assert topics and topics[0]["label"] in {"kerberos", "tickets", "policy"}
    assert all(0 < t["score"] <= 1.0 for t in topics)


def test_topic_extractor_deterministic() -> None:
    t = TopicExtractor()
    assert t.topics("alpha alpha beta") == t.topics("alpha alpha beta")


def test_embed_image(client: TestClient) -> None:
    # Minimal JPEG SOI+EOI is enough for the hash backend; CLIP path would
    # need a real image and is covered only when the model is installed.
    r = client.post(
        "/embed/image", content=b"\xff\xd8\xff\xd9", headers={"content-type": "image/jpeg"}
    )
    assert r.status_code == 200
    emb = np.array(r.json()["embedding"])
    assert emb.shape == (384,)


def test_health_reports_the_image_text_backend(client: TestClient) -> None:
    body = client.get("/health").json()
    assert body["image_text_backend"] in {"hash", "clip"}
    assert body["image_backend"] in {"hash", "clip"}


def test_image_text_is_refused_without_clip(client: TestClient) -> None:
    """No CLIP stack ⇒ 501, never a hash embedding.

    A hash "text embedding" does not share a space with the hashed pixels in
    the image store, so answering 200 here would silently turn text→image
    search into a random ranker. The Rust caller relies on the non-2xx status
    to report the leg as skipped.
    """
    original = app.state.image_text_embedder
    app.state.image_text_embedder = ImageTextEmbedder()  # hash-backed
    try:
        assert app.state.image_text_embedder.backend == "hash"
        r = client.post("/embed/image-text", json={"text": "a red bicycle"})
        assert r.status_code == 501, r.text
        assert "CLIP" in r.json()["error"]
    finally:
        app.state.image_text_embedder = original


def test_image_text_encoder_shares_the_image_checkpoint() -> None:
    """One checkpoint for both towers — two different CLIP builds would not
    share a space. Pinned here so a future refactor cannot load a second one."""
    enc = ImageEmbedder()
    ite = ImageTextEmbedder.from_encoder(enc)
    assert ite.model is enc._model
    assert ite.preprocess is enc._preprocess
    assert ite.backend == enc.backend


def test_project_clip_truncates_and_renormalises() -> None:
    full = np.arange(512, dtype=np.float32) + 1.0
    p = project_clip(full, DIM)
    assert p.shape == (DIM,)
    assert abs(float(np.linalg.norm(p)) - 1.0) < 1e-5
    # Same transform for text and image features: cosine in the projected
    # space equals cosine in the truncated subspace (documenting the loss).
    other = (np.arange(512, dtype=np.float32) % 7) + 1.0
    q = project_clip(other, DIM)
    a, b = full[:DIM], other[:DIM]
    expected = float(a @ b) / (float(np.linalg.norm(a)) * float(np.linalg.norm(b)))
    assert abs(float(p @ q) - expected) < 1e-5
    # Anything past `dim` is discarded — the projection is lossy by design.
    tail_changed = full.copy()
    tail_changed[DIM:] += 100.0
    np.testing.assert_allclose(project_clip(tail_changed, DIM), p)
