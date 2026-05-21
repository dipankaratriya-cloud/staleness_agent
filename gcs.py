"""GCS helpers — upload/download results and dataset files."""

import json
import os
from google.cloud import storage

BUCKET = os.environ.get("GCS_BUCKET", "staleness-pipeline")
_client = None


def _bucket():
    global _client
    if _client is None:
        _client = storage.Client()
    return _client.bucket(BUCKET)


# ── JSON ──────────────────────────────────────────────────────────────────────

def upload_json(data: dict, gcs_path: str):
    blob = _bucket().blob(gcs_path)
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    print(f"  [gcs] uploaded → gs://{BUCKET}/{gcs_path}")


def download_json(gcs_path: str) -> dict | None:
    blob = _bucket().blob(gcs_path)
    if not blob.exists():
        return None
    return json.loads(blob.download_as_text())


# ── Files ─────────────────────────────────────────────────────────────────────

def upload_file(local_path: str, gcs_path: str):
    blob = _bucket().blob(gcs_path)
    blob.upload_from_filename(local_path)
    print(f"  [gcs] uploaded → gs://{BUCKET}/{gcs_path}")


def download_file(gcs_path: str, local_path: str) -> bool:
    blob = _bucket().blob(gcs_path)
    if not blob.exists():
        return False
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    blob.download_to_filename(local_path)
    return True


# ── Run snapshots ─────────────────────────────────────────────────────────────

def upload_run_artifacts(run_id: str, base_dir: str):
    """Upload all phase result JSONs for this run."""
    for fname in ("phase1_results.json", "phase23_results.json",
                  "phase4_results.json", "staleness_report.json"):
        local = os.path.join(base_dir, fname)
        if os.path.exists(local):
            upload_json(json.load(open(local)), f"runs/{run_id}/{fname}")


def get_previous_run_results(current_run_id: str) -> dict | None:
    """Return phase4_results from the most recent prior run (for delta computation)."""
    blobs = list(_bucket().list_blobs(prefix="runs/"))
    run_ids = sorted(
        set(b.name.split("/")[1] for b in blobs if b.name.split("/")[1] != current_run_id),
        reverse=True,
    )
    for rid in run_ids:
        data = download_json(f"runs/{rid}/phase4_results.json")
        if data:
            print(f"  [gcs] using previous run for delta: {rid}")
            return data
    return None


# ── Dataset files (for row-level delta) ──────────────────────────────────────

def upload_dataset_file(dataset_id: str, run_id: str, local_path: str):
    """Store downloaded dataset file keyed by dataset + run for future diff."""
    import re
    slug = re.sub(r"[^a-z0-9]+", "_", dataset_id.lower())[:60]
    ext = os.path.splitext(local_path)[1] or ".bin"
    gcs_path = f"datasets/{slug}/{run_id}/data{ext}"
    upload_file(local_path, gcs_path)
    return gcs_path


def get_previous_dataset_file(dataset_id: str, current_run_id: str,
                               local_dest: str) -> bool:
    """Download the previous run's dataset file for row-count comparison."""
    import re
    slug = re.sub(r"[^a-z0-9]+", "_", dataset_id.lower())[:60]
    blobs = list(_bucket().list_blobs(prefix=f"datasets/{slug}/"))
    run_ids = sorted(
        set(b.name.split("/")[2] for b in blobs if b.name.split("/")[2] != current_run_id),
        reverse=True,
    )
    for rid in run_ids:
        for blob in blobs:
            if f"/{rid}/" in blob.name:
                blob.download_to_filename(local_dest)
                return True
    return False
