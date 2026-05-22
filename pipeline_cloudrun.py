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
from datetime import datetime, timezone

import bq

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

    print(f"\n[pipeline] pushing Phase 2+3 results to BigQuery...")
    bq.write_phase23(
        run_id  = run_id,
        phase23 = load_json(os.path.join(BASE_DIR, "phase23_results.json")),
    )
    print(f"[pipeline] ✓ Phase 2+3 → BigQuery done")

    # ── Phase 4 ───────────────────────────────────────────────────────────────
    cmd = ["python3", "run_phase4.py", "--workers", workers]
    if resume:
        cmd.append("--resume")
    run_phase("Phase 4 — observation date extraction", cmd)

    print(f"\n[pipeline] pushing Phase 4 results to BigQuery...")
    bq.write_phase4(
        run_id = run_id,
        phase4 = load_json(latest_phase4_file()),
    )
    print(f"[pipeline] ✓ Phase 4 → BigQuery done")

    print(f"\n[pipeline] ✅ complete — run_id: {run_id}")
    print(f"  BigQuery: {bq.PROJECT}.{bq.DATASET}.[phase1|phase23|phase4]_results")


if __name__ == "__main__":
    main()
