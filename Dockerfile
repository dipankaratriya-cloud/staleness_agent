FROM python:3.12-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── Install pi CLI (AI coding agent — @earendil-works/pi-coding-agent) ─────────
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*
RUN npm install -g @earendil-works/pi-coding-agent

# ── Python deps ────────────────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium --with-deps

# ── Source ─────────────────────────────────────────────────────────────────────
COPY *.py ./
COPY Provenance.csv ./
COPY Provenance_clean.csv ./
COPY Provenance_high.csv ./

CMD ["python3", "pipeline_cloudrun.py"]
