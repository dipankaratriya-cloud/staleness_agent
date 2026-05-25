"""Run Phase 2+3: download datasets from phase1 results.

Usage:
  python3 run_phase23.py phase1_top50.json            # all entries in file
  python3 run_phase23.py phase1_results.json --limit 10
  python3 run_phase23.py phase1_results.json --resume --workers 3
  python3 run_phase23.py Provenance.csv               # CSV with url column
"""

import argparse
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from phase23_downloader import download_dataset

_save_lock = threading.Lock()

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PHASE23_RESULTS_FILE = os.path.join(BASE_DIR, "phase23_results.json")


def load_json(path: str) -> dict:
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_json(path: str, data: dict):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def load_entries(input_path: str) -> list[dict]:
    """Load entries from a phase1 results JSON or Provenance CSV."""
    entries = []
    if input_path.endswith(".csv"):
        import csv
        with open(input_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                dataset_id = (row.get("dataset_id") or row.get("id") or row.get("prov_id") or "").strip()
                url = (row.get("provenance_url") or row.get("url") or "").strip()
                if dataset_id and url:
                    entries.append({"dataset_id": dataset_id, "url": url,
                                    "matched_folder": ""})
    else:
        data = load_json(input_path)
        for val in data.values():
            if not isinstance(val, dict):
                continue
            dataset_id = val.get("dataset_id", "")
            url = val.get("url", "")
            matched_folder = val.get("matched_folder") or ""
            if dataset_id and url:
                entries.append({"dataset_id": dataset_id, "url": url,
                                "matched_folder": matched_folder})
    return entries


def run_one(entry: dict) -> dict:
    dataset_id = entry["dataset_id"]
    url = entry["url"]
    repo_folder = entry.get("matched_folder") or ""

    print(f"[{dataset_id}]")
    print(f"  url    : {url}")
    print(f"  folder : {repo_folder or '(none)'}")
    print(f"  status : running pi agent...")

    result = download_dataset(dataset_id, repo_folder, url)

    if result.get("status") == "success":
        print(f"  done   : {result.get('file')}")
    else:
        print(f"  failed : {result.get('failure_code')} — {result.get('reason','')[:80]}")

    return {"dataset_id": dataset_id, "url": url, **result}


def main():
    parser = argparse.ArgumentParser(description="Phase 2+3: dataset download")
    parser.add_argument("input", help="phase1_results.json or Provenance.csv")
    parser.add_argument("--limit", type=int, default=None, help="Process only first N")
    parser.add_argument("--workers", type=int, default=2, help="Parallel pi sessions (default: 2)")
    parser.add_argument("--resume", action="store_true", help="Skip already-processed datasets")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"[error] file not found: {args.input}")
        sys.exit(1)

    entries = load_entries(args.input)
    results = load_json(PHASE23_RESULTS_FILE)

    if args.resume:
        done = {r.get("dataset_id") for r in results.values() if isinstance(r, dict)}
        entries = [e for e in entries if e["dataset_id"] not in done]
        print(f"[resume] {len(entries)} remaining after skipping already-processed")

    if args.limit:
        entries = entries[:args.limit]

    print(f"[phase23] processing {len(entries)} datasets with {args.workers} workers\n")

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_one, entry): entry for entry in entries}
        for future in as_completed(futures):
            entry = futures[future]
            try:
                result = future.result()
                did = result["dataset_id"]
            except Exception as e:
                result = {
                    "dataset_id": entry["dataset_id"],
                    "url": entry["url"],
                    "status": "failure",
                    "failure_code": "exception",
                    "reason": str(e),
                }
                did = entry["dataset_id"]
            with _save_lock:
                results[did] = result
                save_json(PHASE23_RESULTS_FILE, results)

    succeeded = sum(1 for r in results.values()
                    if isinstance(r, dict) and r.get("status") == "success")
    print(f"\n[phase23] done — {succeeded}/{len(entries)} downloaded")
    print(f"[phase23] results saved to: {PHASE23_RESULTS_FILE}")


if __name__ == "__main__":
    main()
