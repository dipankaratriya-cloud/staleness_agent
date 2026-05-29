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
import threading
import time
from datetime import datetime, date, timezone

import bq
import gcs

BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
REFRESH_DATES_FILE = os.path.join(BASE_DIR, "provenance_refresh_dates.json")
OBS_INPUT_CSV      = os.path.join(BASE_DIR, "obs_input.csv")
REFRESH_INPUT_CSV  = os.path.join(BASE_DIR, "refresh_input.csv")


def run_phase(label, cmd) -> float:
    """Run a pipeline phase subprocess. Returns elapsed seconds."""
    print(f"\n{'='*55}\n[pipeline] ▶ {label}\n{'='*55}")
    t0 = time.time()
    r = subprocess.run(cmd, cwd=BASE_DIR)
    elapsed = round(time.time() - t0, 1)
    if r.returncode != 0:
        print(f"[pipeline] ✗ {label} failed — aborting")
        sys.exit(r.returncode)
    print(f"[pipeline] ✓ {label} done  ({elapsed}s)")
    return elapsed


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


def staleness_label(freshness_days: int | None) -> str:
    """Bucket freshness into human-readable label."""
    if freshness_days is None:
        return "No Date"
    if freshness_days < 30:
        return "Fresh"
    if freshness_days < 180:
        return "Recent"
    if freshness_days < 365:
        return "Stale"
    return "Very Stale"


def compute_delta(run_id: str, phase23: dict, phase4: dict,
                  refresh_dates: dict,
                  prev_results: dict,
                  phase_timings: dict | None = None) -> list[dict]:
    """Compute delta metrics for every dataset with an obs date OR a refresh date.

    prev_results: {dataset_id: {last_obs_date, last_refresh_date, row_count_current}}
                  from bq.get_previous_results() — empty dict on first run.
    refresh_dates: {dataset_id: {last_refresh_date, ...}}
                   from provenance_refresh_dates.json.
    """
    today = date.today()
    today_days = (today - date(1970, 1, 1)).days
    phase_timings = phase_timings or {}

    # Union of all dataset_ids that have at least one date signal
    all_ids = (
        {did for did, p4 in phase4.items()
         if p4.get("last_obs_date") not in (None, "not_possible")}
        |
        {did for did, rd in refresh_dates.items()
         if rd.get("last_refresh_date")}
    )

    rows = []
    for dataset_id in all_ids:
        p4  = phase4.get(dataset_id, {})
        p23 = phase23.get(dataset_id, {})
        rd  = refresh_dates.get(dataset_id, {})

        # Match against datcom_import_list: try dataset_id first, fall back to URL
        source_url = p4.get("source_url") or rd.get("url") or p23.get("url", "")
        prev = (prev_results["by_id"].get(dataset_id)
                or prev_results["by_url"].get(source_url)
                or {})

        # ── Obs date ──────────────────────────────────────────────────────────
        curr_obs      = p4.get("last_obs_date") if p4.get("last_obs_date") != "not_possible" else None
        prev_obs      = prev.get("last_obs_date")
        curr_obs_days = parse_date_to_days(curr_obs)
        prev_obs_days = parse_date_to_days(prev_obs)
        obs_delta     = (curr_obs_days - prev_obs_days) if (curr_obs_days and prev_obs_days) else None
        freshness     = (today_days - curr_obs_days) if curr_obs_days else None

        # ── Refresh date ──────────────────────────────────────────────────────
        curr_refresh      = rd.get("last_refresh_date")
        prev_refresh      = prev.get("last_refresh_date")
        curr_refresh_days = parse_date_to_days(curr_refresh)
        prev_refresh_days = parse_date_to_days(prev_refresh)
        refresh_delta     = (curr_refresh_days - prev_refresh_days) \
                            if (curr_refresh_days and prev_refresh_days) else None

        # ── File / row metrics (only for datasets with downloaded files) ──────
        filepath  = p23.get("file")
        file_size = os.path.getsize(filepath) if filepath and os.path.exists(filepath) else None
        row_count_current = count_rows(filepath)
        prev_row_count    = prev.get("row_count_current")

        row_additions = row_deletions = None
        if row_count_current is not None and prev_row_count is not None:
            diff = row_count_current - prev_row_count
            row_additions = max(0, diff)
            row_deletions = max(0, -diff)

        file_format = p23.get("file_format", "")
        if not file_format and filepath:
            file_format = os.path.splitext(filepath)[1].lstrip(".").lower()

        rows.append({
            "dataset_id":              dataset_id,
            "source_url":              p4.get("source_url") or rd.get("url") or p23.get("url", ""),
            # Obs date metrics
            "last_obs_date":           curr_obs,
            "prev_last_obs_date":      prev_obs,
            "date_delta_days":         obs_delta,
            "data_freshness_days":     freshness,
            "staleness_label":         staleness_label(freshness),
            # Refresh date metrics
            "last_refresh_date":       curr_refresh,
            "prev_last_refresh_date":  prev_refresh,
            "refresh_date_delta_days": refresh_delta,
            # Row / file metrics
            "row_count_current":       row_count_current,
            "row_count_previous":      prev_row_count,
            "row_additions":           row_additions,
            "row_deletions":           row_deletions,
            "file_size_bytes":         file_size,
            "file_format":             file_format,
            # Download metrics
            "download_strategy":       p23.get("download_strategy", ""),
            "download_time_sec":       p23.get("download_time_sec"),
            # Extraction metrics
            "extraction_time_sec":     p4.get("extraction_time_sec"),
            # Phase timing
            "phase1_time_sec":         phase_timings.get("phase1"),
            "phase23_time_sec":        phase_timings.get("phase23"),
            "phase4_time_sec":         phase_timings.get("phase4"),
            "total_pipeline_time_sec": phase_timings.get("total"),
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


def _write_filtered_csv(datasets: dict[str, str], path: str):
    """Write a Provenance-style CSV from {dataset_id: source_url}."""
    import csv as _csv
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = _csv.DictWriter(f, fieldnames=["id", "provenance_url"])
        w.writeheader()
        for did, url in datasets.items():
            w.writerow({"id": did, "provenance_url": url})
    print(f"  [pipeline] filtered input → {os.path.basename(path)}  ({len(datasets)} datasets)")


def _local_obs_datasets() -> dict[str, str]:
    """Build {dataset_id: url} from latest local phase4_results — datasets with a known obs date."""
    p4_path = latest_phase4_file()
    if not p4_path:
        return {}
    p4 = load_json(p4_path)
    return {
        did: v["source_url"]
        for did, v in p4.items()
        if v.get("last_obs_date") not in (None, "not_possible") and v.get("source_url")
    }


def _local_refresh_datasets() -> dict[str, str]:
    """Build {dataset_id: url} from local provenance_refresh_dates.json — URLs with a known refresh date."""
    rd = load_json(REFRESH_DATES_FILE)
    return {
        did: v["url"]
        for did, v in rd.items()
        if v.get("last_refresh_date") and v.get("url")
    }


def _run_refresh_pipeline(run_id: str, input_csv: str, resume: bool):
    """Runs in a background thread: extract refresh dates → upload to BQ."""
    print(f"\n[refresh] ▶ starting provenance refresh date extraction")
    t0 = time.time()
    cmd = [
        "python3", "provenance_refresh_extractor.py",
        "--csv",    input_csv,
        "--output", REFRESH_DATES_FILE,
        "--resume",
    ]
    r = subprocess.run(cmd, cwd=BASE_DIR)
    elapsed = round(time.time() - t0, 1)
    if r.returncode != 0:
        print(f"[refresh] ✗ extractor failed (exit {r.returncode}) — skipping BQ upload")
        return
    print(f"[refresh] ✓ extraction done ({elapsed}s) — uploading to BigQuery...")
    refresh_data = load_json(REFRESH_DATES_FILE)
    bq.write_refresh_dates(run_id=run_id, refresh=refresh_data)
    found = sum(1 for v in refresh_data.values() if v.get("last_refresh_date"))
    print(f"[refresh] ✓ {found} refresh dates → BQ refresh_dates table  ({elapsed}s total)")


def main():
    run_id      = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    workers     = os.environ.get("WORKERS", "4")
    resume      = os.environ.get("RESUME", "false").lower() == "true"
    input_f     = os.environ.get("INPUT_FILE", os.path.join(BASE_DIR, "Provenance.csv"))
    pipeline_t0 = time.time()

    print(f"[pipeline] run_id : {run_id}")
    print(f"[pipeline] input  : {input_f}")
    print(f"[pipeline] workers: {workers}  resume: {resume}")

    # ── Ensure BQ tables exist ─────────────────────────────────────────────────
    bq.ensure_tables()

    # ── Build scoped inputs: BQ first, fall back to local result files ──────────
    print(f"\n[pipeline] resolving scoped inputs...")

    obs_datasets = bq.get_successful_obs_datasets() or _local_obs_datasets()
    if obs_datasets:
        _write_filtered_csv(obs_datasets, OBS_INPUT_CSV)
        obs_csv = OBS_INPUT_CSV
    else:
        raise RuntimeError("No known-working obs datasets found in BQ or local phase4 results. "
                           "Run the full pipeline at least once first.")

    refresh_datasets = bq.get_successful_refresh_datasets() or _local_refresh_datasets()
    if refresh_datasets:
        _write_filtered_csv(refresh_datasets, REFRESH_INPUT_CSV)
        refresh_csv = REFRESH_INPUT_CSV
    else:
        raise RuntimeError("No known-working refresh datasets found in BQ or local provenance_refresh_dates.json. "
                           "Run provenance_refresh_extractor.py at least once first.")

    # ── Phase 5 (refresh dates) — starts immediately, runs in parallel ─────────
    refresh_thread = threading.Thread(
        target=_run_refresh_pipeline,
        args=(run_id, refresh_csv, resume),
        daemon=True,
        name="refresh-dates",
    )
    refresh_thread.start()
    print(f"[pipeline] ↗ refresh pipeline started in background  ({len(refresh_datasets) or 'all'} datasets)")

    # ── Phase 1 ───────────────────────────────────────────────────────────────
    cmd = ["python3", "run_phase1.py", obs_csv, "--workers", workers]
    if resume:
        cmd.append("--resume")
    phase1_time = run_phase("Phase 1 — URL → repo match", cmd)

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
    phase23_time = run_phase("Phase 2+3 — dataset download", cmd)

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
    phase4_time = run_phase("Phase 4 — observation date extraction", cmd)

    phase4_data = load_json(latest_phase4_file())

    print(f"\n[pipeline] pushing Phase 4 results to BigQuery...")
    bq.write_phase4(run_id=run_id, phase4=phase4_data)
    print(f"[pipeline] ✓ Phase 4 → BigQuery done")

    # ── Wait for refresh pipeline before computing delta ──────────────────────
    if refresh_thread.is_alive():
        print(f"\n[pipeline] ⏳ waiting for refresh date extraction before delta...")
        refresh_thread.join()
    refresh_data = load_json(REFRESH_DATES_FILE)

    # ── Fetch previous run from BQ and compute delta metrics ──────────────────
    phase_timings = {
        "phase1":  phase1_time,
        "phase23": phase23_time,
        "phase4":  phase4_time,
        "total":   round(time.time() - pipeline_t0, 1),
    }

    print(f"\n[pipeline] querying datcom_import_list for previous obs + refresh dates...")
    prev_results = bq.get_datcom_previous()

    delta_rows = compute_delta(run_id, phase23_data, phase4_data,
                               refresh_data, prev_results, phase_timings)
    print(f"[pipeline] computed delta for {len(delta_rows)} datasets "
          f"({sum(1 for r in delta_rows if r.get('last_obs_date'))} with obs date, "
          f"{sum(1 for r in delta_rows if r.get('last_refresh_date'))} with refresh date)")
    bq.write_delta(run_id=run_id, delta=delta_rows)
    print(f"[pipeline] ✓ delta_results → BigQuery done")

    # ── Upload run artifacts to GCS for future reference ──────────────────────
    try:
        gcs.upload_run_artifacts(run_id, BASE_DIR)
        print(f"[pipeline] ✓ run artifacts uploaded to GCS")
    except Exception as e:
        print(f"  [gcs] artifact upload skipped: {e}")

    total_time = round(time.time() - pipeline_t0, 1)
    print(f"\n[pipeline] ✅ complete — run_id: {run_id}  total_time: {total_time}s")
    print(f"  Phase timings:  P1={phase1_time}s  P2+3={phase23_time}s  P4={phase4_time}s")
    print(f"  BigQuery tables:")
    print(f"    {bq.PROJECT}.{bq.DATASET}.phase1_results")
    print(f"    {bq.PROJECT}.{bq.DATASET}.phase23_results")
    print(f"    {bq.PROJECT}.{bq.DATASET}.phase4_results")
    print(f"    {bq.PROJECT}.{bq.DATASET}.delta_results    ← full report")
    print(f"    {bq.PROJECT}.{bq.DATASET}.refresh_dates    ← provenance refresh dates")


if __name__ == "__main__":
    main()
