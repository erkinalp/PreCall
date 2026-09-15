# SPDX-License-Identifier: GPL-2.0-only
# Central AI service (embeddings / topics / whisper). This is where the
# "inference at a central server" counterfactual actually happens.
FROM python:3.12-slim AS base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml ./
COPY precall_ai/ ./precall_ai/
# Install with CPU torch by default; override extras for GPU hosts.
ARG EXTRAS=""
RUN uv pip install --system ".${EXTRAS:+[$EXTRAS]}"
EXPOSE 3000
ENV PRECALL_AI_HOST=0.0.0.0 \
    PRECALL_AI_PORT=3000
CMD ["python", "-m", "precall_ai.server"]
