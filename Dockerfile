FROM python:3.12-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── Install pi CLI ─────────────────────────────────────────────────────────────
# Replace this with however your pi CLI is distributed:
#   npm:    RUN npm install -g @your-org/pi-cli
#   binary: COPY bin/pi /usr/local/bin/pi && chmod +x /usr/local/bin/pi
COPY bin/pi /usr/local/bin/pi
RUN chmod +x /usr/local/bin/pi

# ── Python deps ────────────────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium --with-deps

# ── Source ─────────────────────────────────────────────────────────────────────
COPY *.py ./
COPY Provenance.csv ./

CMD ["python3", "pipeline_cloudrun.py"]
