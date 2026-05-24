"""Run Phase 1: URL → repo folder matching for datasets in a Provenance CSV."""

import argparse
import csv
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from phase1_url_matcher import match_url

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PHASE1_RESULTS_FILE = os.path.join(BASE_DIR, "phase1_results.json")


def load_results() -> dict:
    if os.path.exists(PHASE1_RESULTS_FILE):
        with open(PHASE1_RESULTS_FILE) as f:
            return json.load(f)
    return {}


def save_results(results: dict):
    with open(PHASE1_RESULTS_FILE, "w") as f:
        json.dump(results, f, indent=2)


def load_provenance(csv_path: str) -> list[dict]:
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            dataset_id = row.get("dataset_id") or row.get("id") or row.get("prov_id") or ""
            url = row.get("provenance_url") or row.get("url") or ""
            if dataset_id and url:
                rows.append({"dataset_id": dataset_id.strip(), "url": url.strip()})
    return rows


def run_one(entry: dict, results: dict) -> dict:
    dataset_id = entry["dataset_id"]
    url = entry["url"]

    print(f"[{dataset_id}] url    : {url}")
    print(f"[{dataset_id}] status : running pi agent...")

    result = match_url(url, dataset_id=dataset_id)

    print(f"[{dataset_id}] match  : {result.get('matched_folder')}  ({result.get('confidence')})")
    print(f"[{dataset_id}] name   : {result.get('dataset_name')}")
    print(f"[{dataset_id}] reason : {result.get('reason')}")

    return {"dataset_id": dataset_id, "url": url, **result}


def main():
    parser = argparse.ArgumentParser(description="Phase 1: URL → repo matching")
    parser.add_argument("csv", help="Path to Provenance CSV file")
    parser.add_argument("--limit", type=int, default=None, help="Process only first N datasets")
    parser.add_argument("--workers", type=int, default=3, help="Parallel pi sessions (default: 3)")
    parser.add_argument("--resume", action="store_true", help="Skip already-processed datasets")
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        print(f"[error] file not found: {args.csv}")
        sys.exit(1)

    rows = load_provenance(args.csv)
    results = load_results()

    if args.resume:
        rows = [r for r in rows if r["dataset_id"] not in results]
        print(f"[resume] {len(rows)} remaining after skipping already-processed")

    if args.limit:
        rows = rows[:args.limit]

    print(f"[phase1] processing {len(rows)} datasets with {args.workers} workers\n")

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_one, entry, results): entry for entry in rows}
        for future in as_completed(futures):
            try:
                result = future.result()
                results[result["dataset_id"]] = result
                save_results(results)  # save after each completion
            except Exception as e:
                entry = futures[future]
                print(f"[error] {entry['dataset_id']}: {e}")
                results[entry["dataset_id"]] = {
                    "dataset_id": entry["dataset_id"],
                    "url": entry["url"],
                    "matched_folder": None,
                    "confidence": "none",
                    "dataset_name": "",
                    "reason": f"exception: {e}",
                }
                save_results(results)

    matched = sum(1 for r in results.values() if r.get("matched_folder"))
    print(f"\n[phase1] done — {matched}/{len(rows)} matched")
    print(f"[phase1] results saved to: {PHASE1_RESULTS_FILE}")


if __name__ == "__main__":
    main()
