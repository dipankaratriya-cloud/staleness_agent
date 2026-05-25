"""Cloud Run entrypoint — runs all phases then pushes results to BigQuery.

Required env vars:
  GCP_PROJECT     GCP project ID
  BQ_DATASET      BigQuery dataset (default: staleness)
  GITHUB_TOKEN    GitHub API token
  GROQ_API_KEY    Groq API key
  GEMINI_API_KEY  Gemini API key

Optional:
  WORKERS         parallel workers per phase (default: 4)
  RESUME          set to "true" to resume a partial run
  INPUT_FILE      path to Provenance CSV or urls.txt (default: Provenance.csv)
"""

import json
import os
import subprocess
import sys
from datetime import datetime, date, timezone

import bq
import gcs

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def run_phase(label, cmd):
    print(f"\n{'='*55}\n[pipeline] ▶ {label}\n{'='*55}")
    r = subprocess.run(cmd, cwd=BASE_DIR)
    if r.returncode != 0:
        print(f"[pipeline] ✗ {label} failed — aborting")
        sys.exit(r.returncode)
    print(f"[pipeline] ✓ {label} done")


def load_json(path):
    if path and os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def count_rows(filepath: str) -> int | None:
    """Count rows without loading the full file into memory."""
    if not filepath or not os.path.exists(filepath):
        return None
    ext = os.path.splitext(filepath)[1].lower()
    try:
        if ext in (".csv", ".tsv"):
            # Stream line-by-line — never loads full file into RAM
            with open(filepath, "rb") as f:
                return sum(1 for _ in f) - 1  # subtract header row
        elif ext == ".gz":
            import gzip
            with gzip.open(filepath, "rb") as f:
                return sum(1 for _ in f) - 1
        elif ext in (".xlsx", ".xls"):
            import openpyxl
            wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
            ws = wb.active
            count = ws.max_row - 1
            wb.close()
            return count
        elif ext == ".parquet":
            import pyarrow.parquet as pq
            return pq.read_metadata(filepath).num_rows
    except Exception:
        pass
    return None


def parse_date_to_days(date_str: str | None) -> int | None:
    """Convert a date string (YYYY or YYYY-MM or YYYY-MM-DD) to days since epoch."""
    if not date_str or date_str == "not_possible":
        return None
    try:
        from datetime import date as dt
        s = str(date_str).strip()
        if len(s) == 4:
            d = dt(int(s), 12, 31)
        elif len(s) == 7:
            import calendar
            y, m = int(s[:4]), int(s[5:7])
            last_day = calendar.monthrange(y, m)[1]
            d = dt(y, m, last_day)
        else:
            d = dt.fromisoformat(s[:10])
        return (d - dt(1970, 1, 1)).days
    except Exception:
        return None


def compute_delta(run_id: str, phase23: dict, phase4: dict,
                  prev_phase4: dict | None) -> list[dict]:
    """Compute delta metrics for every successfully processed dataset."""
    today = date.today()
    today_days = (today - date(1970, 1, 1)).days
    rows = []

    for dataset_id, p4 in phase4.items():
        if p4.get("last_obs_date") in (None, "not_possible"):
            continue

        p23 = phase23.get(dataset_id, {})
        filepath  = p23.get("file")
        file_size = os.path.getsize(filepath) if filepath and os.path.exists(filepath) else None
        row_count_current = count_rows(filepath)

        # Previous run data
        prev = (prev_phase4 or {}).get(dataset_id, {})
        prev_date_str = prev.get("last_obs_date")
        prev_row_count = prev.get("row_count_current")  # stored from last run

        # Date delta
        curr_days = parse_date_to_days(p4.get("last_obs_date"))
        prev_days = parse_date_to_days(prev_date_str)
        date_delta = (curr_days - prev_days) if (curr_days and prev_days) else None

        # Freshness = days from last observation date to today
        freshness = (today_days - curr_days) if curr_days else None

        # Row delta
        row_additions = None
        row_deletions = None
        if row_count_current is not None and prev_row_count is not None:
            diff = row_count_current - prev_row_count
            row_additions = max(0, diff)
            row_deletions = max(0, -diff)

        rows.append({
            "dataset_id":          dataset_id,
            "source_url":          p4.get("source_url", p23.get("url", "")),
            "last_obs_date":       p4.get("last_obs_date"),
            "prev_last_obs_date":  prev_date_str,
            "date_delta_days":     date_delta,
            "data_freshness_days": freshness,
            "row_count_current":   row_count_current,
            "row_count_previous":  prev_row_count,
            "row_additions":       row_additions,
            "row_deletions":       row_deletions,
            "file_size_bytes":     file_size,
        })

    return rows


def latest_phase4_file():
    results_dir = os.path.join(BASE_DIR, "results")
    if not os.path.isdir(results_dir):
        return None
    candidates = [
        os.path.join(results_dir, d, "phase4_results.json")
        for d in os.listdir(results_dir)
    ]
    candidates = [p for p in candidates if os.path.exists(p)]
    return max(candidates, key=os.path.getmtime) if candidates else None


def main():
    run_id  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    workers = os.environ.get("WORKERS", "4")
    resume  = os.environ.get("RESUME", "false").lower() == "true"
    input_f = os.environ.get("INPUT_FILE", os.path.join(BASE_DIR, "Provenance.csv"))

    print(f"[pipeline] run_id : {run_id}")
    print(f"[pipeline] input  : {input_f}")
    print(f"[pipeline] workers: {workers}  resume: {resume}")

    # ── Ensure BQ tables exist ─────────────────────────────────────────────────
    bq.ensure_tables()

    # ── Phase 1 ───────────────────────────────────────────────────────────────
    cmd = ["python3", "run_phase1.py", input_f, "--workers", workers]
    if resume:
        cmd.append("--resume")
    run_phase("Phase 1 — URL → repo match", cmd)

    print(f"\n[pipeline] pushing Phase 1 results to BigQuery...")
    bq.write_phase1(
        run_id = run_id,
        phase1 = load_json(os.path.join(BASE_DIR, "phase1_results.json")),
    )
    print(f"[pipeline] ✓ Phase 1 → BigQuery done")

    # ── Phase 2+3 ─────────────────────────────────────────────────────────────
    cmd = ["python3", "run_phase23.py", "phase1_results.json", "--workers", workers]
    if resume:
        cmd.append("--resume")
    run_phase("Phase 2+3 — dataset download", cmd)

    # ── Enrich phase23 with file size + row count, upload files to GCS ───────
    phase23_data = load_json(os.path.join(BASE_DIR, "phase23_results.json"))
    print(f"\n[pipeline] enriching Phase 2+3 with file sizes and row counts...")
    for dataset_id, entry in phase23_data.items():
        if entry.get("status") != "success":
            continue
        fp = entry.get("file")
        if fp and os.path.exists(fp):
            entry["file_size_bytes"] = os.path.getsize(fp)
            entry["row_count"] = count_rows(fp)
            # Upload dataset file to GCS for future delta comparisons
            try:
                gcs.upload_dataset_file(dataset_id, run_id, fp)
            except Exception as e:
                print(f"  [gcs] upload skipped for {dataset_id}: {e}")

    print(f"\n[pipeline] pushing Phase 2+3 results to BigQuery...")
    bq.write_phase23(run_id=run_id, phase23=phase23_data)
    print(f"[pipeline] ✓ Phase 2+3 → BigQuery done")

    # ── Phase 4 ───────────────────────────────────────────────────────────────
    cmd = ["python3", "run_phase4.py", "--workers", workers]
    if resume:
        cmd.append("--resume")
    run_phase("Phase 4 — observation date extraction", cmd)

    phase4_data = load_json(latest_phase4_file())

    print(f"\n[pipeline] pushing Phase 4 results to BigQuery...")
    bq.write_phase4(run_id=run_id, phase4=phase4_data)
    print(f"[pipeline] ✓ Phase 4 → BigQuery done")

    # ── Fetch previous run from GCS and compute delta metrics ─────────────────
    print(f"\n[pipeline] computing delta metrics vs previous run...")
    try:
        prev_phase4 = gcs.get_previous_run_results(run_id)
    except Exception as e:
        print(f"  [gcs] could not fetch previous run (first run?): {e}")
        prev_phase4 = None

    delta_rows = compute_delta(run_id, phase23_data, phase4_data, prev_phase4)
    print(f"[pipeline] computed delta for {len(delta_rows)} datasets")
    bq.write_delta(run_id=run_id, delta=delta_rows)
    print(f"[pipeline] ✓ delta_results → BigQuery done")

    # ── Upload run artifacts to GCS for future reference ──────────────────────
    try:
        gcs.upload_run_artifacts(run_id, BASE_DIR)
        print(f"[pipeline] ✓ run artifacts uploaded to GCS")
    except Exception as e:
        print(f"  [gcs] artifact upload skipped: {e}")

    print(f"\n[pipeline] ✅ complete — run_id: {run_id}")
    print(f"  BigQuery tables:")
    print(f"    {bq.PROJECT}.{bq.DATASET}.phase1_results")
    print(f"    {bq.PROJECT}.{bq.DATASET}.phase23_results  (with file_size + row_count)")
    print(f"    {bq.PROJECT}.{bq.DATASET}.phase4_results")
    print(f"    {bq.PROJECT}.{bq.DATASET}.delta_results    (staleness + additions/deletions)")


if __name__ == "__main__":
    main()
