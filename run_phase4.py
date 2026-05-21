"""Run Phase 4: observation date extraction for all successfully downloaded datasets.

Usage:
  python3 run_phase4.py                          # all successful datasets in phase23_results.json
  python3 run_phase4.py --limit 10               # first 10 only
  python3 run_phase4.py --resume                 # skip already-processed in latest results/ folder
  python3 run_phase4.py --workers 4              # parallel workers (default: 2)
  python3 run_phase4.py --input my_results.json  # use different phase23 results file

Each run creates a new timestamped folder:
  results/20260521_034500/
    phase4_results.json   ← all extracted dates
    summary.json          ← counts + success rate
"""

import argparse
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dotenv import load_dotenv

from pi_date_extractor import extract_date_with_pi

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PHASE23_RESULTS = os.path.join(BASE_DIR, "phase23_results.json")
RESULTS_ROOT = os.path.join(BASE_DIR, "results")
GROUND_TRUTH_FILE = os.path.join(BASE_DIR, "ground_truth.json")

# File-type priority: lower index = higher priority when sampling across types.
PRIORITY_EXTS = [".csv", ".xlsx", ".xls", ".tsv", ".json",
                 ".parquet", ".gz", ".zip", ".nc", ".dat"]
SKIP_EXTS = {".py", ".sh", ".log", ".md", ".rst", ".html", ".htm",
             ".js", ".css", ".ipynb", ".cfg", ".ini", ".toml", ".yaml", ".yml"}
SKIP_NAMES = {"manifest.json", "manifest.yaml", "package.json", "requirements.txt",
              "readme.md", "readme.txt", "config.json", "config.yaml", "schema.json"}
SKIP_PATH_PARTS = {"venv", ".venv", "node_modules", "__pycache__",
                   ".git", "site-packages", "dist-packages"}

MIN_DATA_SIZE  = 2 * 1024    # 2 KB minimum
MAX_FILES_SENT = 10          # max files forwarded to the pi agent per dataset

_YEAR_RE = re.compile(r"(20\d{2}|19\d{2})")


# ─── File selection ───────────────────────────────────────────────────────────

def _candidate_year(path: str) -> int:
    """Largest year found in the file's path — used to prefer recent files."""
    years = _YEAR_RE.findall(path)
    return max(int(y) for y in years) if years else 0


def pick_best_files(all_files: list[str],
                    max_files: int = MAX_FILES_SENT) -> list[str]:
    """Return up to `max_files` representative data files, ranked so the agent
    finds the most recent dates first.

    Ranking:
      1. Most recent year embedded in filename/path (e.g. hate_crime_2022 > 2018)
      2. Highest extension priority (.csv beats .xlsx beats .json …)
      3. Largest size (for files with no year hint)
    """
    candidates = []
    for f in all_files:
        if not os.path.exists(f):
            continue
        # Skip virtual-env / package directories
        parts = set(os.path.normpath(f).split(os.sep))
        if parts & SKIP_PATH_PARTS:
            continue
        fname = os.path.basename(f).lower()
        if fname in SKIP_NAMES:
            continue
        ext = os.path.splitext(f)[1].lower()
        if ext in SKIP_EXTS:
            continue
        try:
            size = os.path.getsize(f)
        except OSError:
            continue
        if size < MIN_DATA_SIZE:
            continue

        year = _candidate_year(f)
        try:
            ext_rank = PRIORITY_EXTS.index(ext)
        except ValueError:
            ext_rank = len(PRIORITY_EXTS)   # unknown ext → lowest priority

        candidates.append((f, ext_rank, year, size))

    if not candidates:
        return []

    # Sort: recent year first → best extension → largest size
    candidates.sort(key=lambda x: (-x[2], x[1], -x[3]))

    # Deduplicate by path just in case
    seen, result = set(), []
    for f, *_ in candidates:
        if f not in seen:
            seen.add(f)
            result.append(f)
        if len(result) == max_files:
            break

    return result


def resolve_files_for_dataset(entry: dict) -> list[str]:
    """Given a phase23 result entry, return the ranked list of files to run phase4 on."""
    all_files = list(entry.get("all_files") or [])

    # Make sure the primary file is included
    primary = entry.get("file")
    if primary and primary not in all_files:
        all_files = [primary] + all_files

    files = pick_best_files(all_files)
    if files:
        return files

    # Fallback: scan the output_dir on disk
    output_dir = entry.get("output_dir")
    if output_dir and os.path.isdir(output_dir):
        disk_files = []
        for root, _, fnames in os.walk(output_dir):
            for fn in fnames:
                disk_files.append(os.path.join(root, fn))
        return pick_best_files(disk_files)

    return []


# ─── Results management ───────────────────────────────────────────────────────

def make_results_dir() -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder = os.path.join(RESULTS_ROOT, ts)
    os.makedirs(folder, exist_ok=True)
    return folder


def latest_results_dir() -> str | None:
    if not os.path.isdir(RESULTS_ROOT):
        return None
    subdirs = sorted(
        [d for d in os.listdir(RESULTS_ROOT)
         if os.path.isdir(os.path.join(RESULTS_ROOT, d))],
        reverse=True,
    )
    return os.path.join(RESULTS_ROOT, subdirs[0]) if subdirs else None


def load_json(path: str) -> dict:
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_json(path: str, data: dict):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def write_summary(results_dir: str, results: dict):
    total = len(results)
    extracted = sum(1 for r in results.values()
                    if r.get("last_obs_date") not in (None, "not_possible"))
    failed = total - extracted
    correct = sum(1 for r in results.values() if r.get("match") is True)
    wrong   = sum(1 for r in results.values() if r.get("match") is False)

    summary = {
        "generated_at": datetime.now().isoformat(),
        "total_datasets": total,
        "extracted": extracted,
        "failed": failed,
        "correct_vs_ground_truth": correct,
        "wrong_vs_ground_truth": wrong,
        "success_rate": f"{extracted/total*100:.1f}%" if total else "0%",
    }
    save_json(os.path.join(results_dir, "summary.json"), summary)
    return summary


# ─── Per-dataset worker ───────────────────────────────────────────────────────

def run_one(dataset_id: str, entry: dict, ground_truth: dict,
            results_dir: str) -> dict:
    files = resolve_files_for_dataset(entry)

    if not files:
        print(f"[{dataset_id}]  ⚠  no data file found — skipping")
        return {
            "dataset_id": dataset_id,
            "last_obs_date": "not_possible",
            "column_used": "none",
            "files_checked": 0,
            "reason": "no data file found on disk",
            "run_at": datetime.now().isoformat(),
        }

    print(f"[{dataset_id}]")
    if len(files) == 1:
        print(f"  file  : {os.path.relpath(files[0], BASE_DIR)}")
    else:
        print(f"  files : {len(files)} files selected")
        for f in files[:3]:
            print(f"    • {os.path.relpath(f, BASE_DIR)}")
        if len(files) > 3:
            print(f"    … and {len(files)-3} more")
    print(f"  status: running phase 4...")

    actual = ground_truth.get(dataset_id, {}).get("actual_last_obs_date")
    try:
        date, col, n_checked = extract_date_with_pi(
            files if len(files) > 1 else files[0],
            ground_truth=actual,
        )
    except Exception as e:
        date, col, n_checked = "not_possible", "exception", 0
        print(f"  error : {e}")

    match = (str(date) == str(actual)) if actual else None
    if actual:
        verdict = "✅ CORRECT" if match else f"❌ WRONG (actual={actual})"
        print(f"  check : {verdict}")
    print(f"  result: last_obs_date={date}  column={col}  files_checked={n_checked}")

    result = {
        "dataset_id": dataset_id,
        "files": [os.path.relpath(f, BASE_DIR) for f in files],
        "files_checked": n_checked,
        "last_obs_date": date,
        "column_used": col,
        "actual_last_obs_date": actual,
        "match": match,
        "source_url": entry.get("url", ""),
        "run_at": datetime.now().isoformat(),
    }

    # Persist incrementally — partial results survive a crash
    results_file = os.path.join(results_dir, "phase4_results.json")
    all_results = load_json(results_file)
    all_results[dataset_id] = result
    save_json(results_file, all_results)

    return result


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Phase 4: observation date extraction")
    parser.add_argument("--input", default=PHASE23_RESULTS,
                        help="phase23_results.json path (default: phase23_results.json)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Process only first N datasets")
    parser.add_argument("--workers", type=int, default=2,
                        help="Parallel workers (default: 2)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume: skip datasets already in the latest results/ folder")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"[error] file not found: {args.input}")
        sys.exit(1)

    phase23      = load_json(args.input)
    successes    = {k: v for k, v in phase23.items() if v.get("status") == "success"}
    ground_truth = load_json(GROUND_TRUTH_FILE)

    if args.resume:
        results_dir = latest_results_dir()
        if results_dir:
            existing = load_json(os.path.join(results_dir, "phase4_results.json"))
            done_ids = set(existing.keys())
            successes = {k: v for k, v in successes.items() if k not in done_ids}
            print(f"[resume] folder : {os.path.relpath(results_dir, BASE_DIR)}")
            print(f"[resume] {len(successes)} remaining (skipped {len(done_ids)} done)")
        else:
            results_dir = make_results_dir()
            print(f"[resume] no prior run — starting fresh: {os.path.relpath(results_dir, BASE_DIR)}")
    else:
        results_dir = make_results_dir()
        print(f"[phase4] results folder: {os.path.relpath(results_dir, BASE_DIR)}")

    if args.limit:
        successes = dict(list(successes.items())[:args.limit])

    entries = list(successes.items())
    print(f"[phase4] {len(entries)} datasets  •  {args.workers} workers\n")

    all_results = load_json(os.path.join(results_dir, "phase4_results.json"))

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(run_one, did, entry, ground_truth, results_dir): did
            for did, entry in entries
        }
        for future in as_completed(futures):
            did = futures[future]
            try:
                all_results[did] = future.result()
            except Exception as e:
                all_results[did] = {
                    "dataset_id": did,
                    "last_obs_date": "not_possible",
                    "column_used": "exception",
                    "files_checked": 0,
                    "reason": str(e),
                    "run_at": datetime.now().isoformat(),
                }

    save_json(os.path.join(results_dir, "phase4_results.json"), all_results)
    summary = write_summary(results_dir, all_results)

    print(f"\n[phase4] ── DONE ──────────────────────────────")
    print(f"  Results folder : {os.path.relpath(results_dir, BASE_DIR)}")
    print(f"  Total processed: {summary['total_datasets']}")
    print(f"  Dates extracted: {summary['extracted']}  ({summary['success_rate']})")
    print(f"  Failed         : {summary['failed']}")
    if summary["correct_vs_ground_truth"] or summary["wrong_vs_ground_truth"]:
        print(f"  vs ground truth: {summary['correct_vs_ground_truth']} correct"
              f" / {summary['wrong_vs_ground_truth']} wrong")
    print(f"  Output         : {os.path.relpath(results_dir, BASE_DIR)}/phase4_results.json")
    print(f"                   {os.path.relpath(results_dir, BASE_DIR)}/summary.json")


if __name__ == "__main__":
    main()
