# syntax=docker/dockerfile:1.6
# =============================================================================
# Personal Finance Pipeline — runtime image
# =============================================================================
# Single-stage build. Houses run_pipeline.py, migrate.py, and inspect tools.
# Invoked as a one-shot container by docker-compose `run --rm pipeline ...`.
# See docs/08_STRATEGIC_ROADMAP.md §3.3 (Phase A.5) for the design rationale.
# =============================================================================

FROM python:3.12-slim-bookworm

# --- OS layer ---------------------------------------------------------------
# tzdata is needed for the TZ env var to actually resolve a zone.
# ca-certificates lets requests/urllib3 validate TLS against the BudgetBakers API.
# No build toolchain installed: all Python deps ship as manylinux wheels.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        tzdata \
        ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# --- Python runtime hygiene -------------------------------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Default timezone. Overridable via compose `environment: TZ: ...`.
ENV TZ=Europe/Sofia

WORKDIR /app

# --- Python deps (cached layer) ---------------------------------------------
# requirements.txt is COPYed before source so source edits don't bust the
# pip-install layer.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Application code -------------------------------------------------------
# Separate COPY steps so that changes in scripts/ or tests/ don't bust the
# src/ layer (and vice-versa).
COPY src/     ./src/
COPY scripts/ ./scripts/
COPY tests/   ./tests/
COPY docker/pipeline-entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# --- Non-root user ----------------------------------------------------------
# UID 1000 matches the typical developer UID on Linux hosts, giving sane
# ownership on bind-mounted ./data, ./logs, ./backups. Windows Docker Desktop
# handles UIDs transparently regardless.
RUN useradd --create-home --shell /bin/bash --uid 1000 pipeline \
 && mkdir -p /app/data /app/logs /app/backups \
 && chown -R pipeline:pipeline /app
USER pipeline

# --- Entrypoint -------------------------------------------------------------
# Dispatcher maps the first arg to a Python script. See docker/pipeline-entrypoint.sh.
# Signal handling (SIGTERM forwarding, zombie reaping) is delegated to Docker
# via `init: true` in docker-compose.yml — no tini needed in the image.
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["pipeline", "--mode", "incremental"]
