"""Staleness pipeline: URLs → Phase 1 → Phase 2+3 → Phase 4.

Usage:
  python3 pipeline.py urls.txt                        # run all three phases
  python3 pipeline.py urls.txt --workers 4            # parallel workers per phase
  python3 pipeline.py urls.txt --limit 10             # first 10 URLs only
  python3 pipeline.py urls.txt --resume               # skip already-processed datasets
  python3 pipeline.py urls.txt --start-from phase23   # skip phase 1, start from phase 2+3
  python3 pipeline.py urls.txt --start-from phase4    # skip phase 1+2+3, only run phase 4

urls.txt format — one URL per line, blank lines and # comments ignored:
  https://www.epa.gov/ghgreporting
  https://ec.europa.eu/eurostat/web/health/database
  # this line is a comment
"""

import argparse
import csv
import os
import re
import subprocess
import sys
from datetime import datetime
from urllib.parse import urlparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def url_to_id(url: str) -> str:
    """Stable dataset ID derived from the URL (same slug logic as phase23_downloader)."""
    p = urlparse(url)
    slug = re.sub(r"[^a-z0-9]+", "_", (p.netloc + p.path).lower()).strip("_")
    return slug[:80] or "dataset"


def make_provenance_csv(urls: list[str], path: str):
    """Write a Provenance CSV that run_phase1.py accepts."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "provenance_url"])
        writer.writeheader()
        for url in urls:
            writer.writerow({"id": url_to_id(url), "provenance_url": url})


def run_phase(label: str, cmd: list[str]):
    """Run a phase command, streaming output. Exit pipeline on failure."""
    print(f"\n{'='*60}")
    print(f"[pipeline] ▶ {label}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, cwd=BASE_DIR)
    if result.returncode != 0:
        print(f"\n[pipeline] ✗ {label} failed (exit {result.returncode}) — aborting")
        sys.exit(result.returncode)
    print(f"[pipeline] ✓ {label} complete")


def main():
    parser = argparse.ArgumentParser(description="End-to-end staleness pipeline")
    parser.add_argument("urls_file", help="Text file with one URL per line")
    parser.add_argument("--workers", type=int, default=2, help="Workers per phase (default: 2)")
    parser.add_argument("--limit", type=int, default=None, help="Cap on number of URLs to process")
    parser.add_argument("--resume", action="store_true", help="Skip already-processed datasets in each phase")
    parser.add_argument("--start-from", choices=["phase1", "phase23", "phase4"],
                        default="phase1", help="Start pipeline from this phase (default: phase1)")
    args = parser.parse_args()

    # ── Read URLs ──────────────────────────────────────────────────────────────
    if not os.path.exists(args.urls_file):
        print(f"[pipeline] ✗ file not found: {args.urls_file}")
        sys.exit(1)

    with open(args.urls_file) as f:
        urls = [l.strip() for l in f if l.strip() and not l.startswith("#")]

    if args.limit:
        urls = urls[: args.limit]

    print(f"[pipeline] started  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[pipeline] URLs     : {len(urls)}")
    print(f"[pipeline] workers  : {args.workers}")
    print(f"[pipeline] start-from: {args.start_from}")

    start = args.start_from

    # ── Phase 1: URL → repo match ──────────────────────────────────────────────
    if start == "phase1":
        csv_path = os.path.join(BASE_DIR, "pipeline_input.csv")
        make_provenance_csv(urls, csv_path)
        cmd = ["python3", "run_phase1.py", csv_path, "--workers", str(args.workers)]
        if args.resume:
            cmd.append("--resume")
        run_phase("Phase 1 — URL → repo match", cmd)

    # ── Phase 2+3: download datasets ───────────────────────────────────────────
    if start in ("phase1", "phase23"):
        cmd = ["python3", "run_phase23.py", "phase1_results.json",
               "--workers", str(args.workers)]
        if args.resume:
            cmd.append("--resume")
        run_phase("Phase 2+3 — dataset download", cmd)

    # ── Phase 4: extract observation dates ────────────────────────────────────
    cmd = ["python3", "run_phase4.py", "--workers", str(args.workers)]
    if args.resume:
        cmd.append("--resume")
    run_phase("Phase 4 — observation date extraction", cmd)

    print(f"\n[pipeline] ✅ all phases complete — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[pipeline] results → results/<timestamp>/phase4_results.json")


if __name__ == "__main__":
    main()
