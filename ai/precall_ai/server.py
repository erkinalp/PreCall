# SPDX-License-Identifier: GPL-2.0-only
"""FastAPI service: /embed/text, /embed/image, /topics, /transcribe, /health.

Run:  ``uv run uvicorn precall_ai.server:app --port 8090``
Device selection: ``PRECALL_AI_DEVICE=cpu|cuda|rocm`` (default cpu) — or the
CLI ``--force-cpu`` flag in ``__main__``.
"""

from __future__ import annotations

import argparse
import os
from contextlib import asynccontextmanager

import numpy as np
from fastapi import FastAPI, Request
from pydantic import BaseModel, Field

from .providers import DIM, ImageEmbedder, TextEmbedder, TopicExtractor


@asynccontextmanager
async def lifespan(app: FastAPI):
    dev = _device()
    app.state.text_embedder = TextEmbedder(device=dev)
    app.state.image_embedder = ImageEmbedder(device=dev)
    app.state.topics = TopicExtractor()
    yield


app = FastAPI(title="precall-ai", version="0.1.0", lifespan=lifespan)


class EmbedTextReq(BaseModel):
    text: str


class EmbedTextBatchReq(BaseModel):
    texts: list[str] = Field(min_length=1, max_length=256)


class TopicsReq(BaseModel):
    text: str
    k: int = 5


def _device() -> str:
    return os.environ.get("PRECALL_AI_DEVICE", "cpu")


def _text_embedder() -> TextEmbedder:
    return app.state.text_embedder


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "text_backend": app.state.text_embedder.backend,
        "image_backend": app.state.image_embedder.backend,
        "dim": DIM,
        "device": app.state.text_embedder.device,
    }


@app.post("/embed/text")
def embed_text(req: EmbedTextReq) -> dict:
    emb = _text_embedder().embed([req.text])[0]
    return {"embedding": emb.tolist(), "dim": int(emb.shape[0])}


@app.post("/embed/text/batch")
def embed_text_batch(req: EmbedTextBatchReq) -> dict:
    emb = _text_embedder().embed(req.texts)
    return {"embeddings": emb.tolist(), "dim": int(emb.shape[1])}


@app.post("/embed/image")
async def embed_image(request: Request) -> dict:
    body = await request.body()
    if not body:
        return {"embedding": [], "dim": DIM}
    emb = app.state.image_embedder.embed(body)
    return {"embedding": emb.tolist(), "dim": int(emb.shape[0])}


@app.post("/topics")
def topics(req: TopicsReq) -> dict:
    ts = app.state.topics.topics(req.text, k=req.k)
    return {"topics": [{"label": w, "score": s} for w, s in ts]}


@app.post("/transcribe")
async def transcribe(request: Request) -> dict:
    """PCM16 audio → text. Requires the `whisper` extra; otherwise 501."""
    body = await request.body()
    try:
        import whisper  # noqa: F401
    except Exception:
        return {"text": "", "error": "transcription backend not installed"}
    # Whisper wants float32 mono 16kHz.
    pcm = np.frombuffer(body, dtype=np.int16).astype(np.float32) / 32768.0
    model = getattr(app.state, "whisper", None)
    if model is None:
        import whisper

        model = whisper.load_model("base", device=_device())
        app.state.whisper = model
    result = model.transcribe(pcm, fp16=False)
    return {"text": result.get("text", "")}


def main() -> None:
    parser = argparse.ArgumentParser(prog="precall-ai")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument(
        "--force-cpu",
        action="store_true",
        help="force CPU inference regardless of installed accelerators",
    )
    args = parser.parse_args()
    if args.force_cpu:
        os.environ["PRECALL_AI_DEVICE"] = "cpu"
    import uvicorn

    uvicorn.run("precall_ai.server:app", host=args.host, port=args.port)


if __name__ == "__main__":
    main()
