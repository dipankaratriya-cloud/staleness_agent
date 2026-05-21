"""Run observation date extraction for one or all datasets."""

import json
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from pi_date_extractor import extract_date_with_pi

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_FILE = os.path.join(BASE_DIR, "results.json")
GROUND_TRUTH_FILE = os.path.join(BASE_DIR, "ground_truth.json")
DATA_EXTS = (".csv", ".xlsx", ".xls")
SKIP_STEMS = {"pvmap", "metadata", "places_resolved", "pv_map"}


def find_data_file(dataset_dir: str) -> str | None:
    candidates = []
    for fname in os.listdir(dataset_dir):
        stem = os.path.splitext(fname)[0].lower()
        if fname.endswith(DATA_EXTS) and not any(s in stem for s in SKIP_STEMS):
            candidates.append(os.path.join(dataset_dir, fname))
    candidates.sort(key=os.path.getsize, reverse=True)
    return candidates[0] if candidates else None


def load_results() -> dict:
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE) as f:
            return json.load(f)
    return {}


def save_results(results: dict):
    with open(RESULTS_FILE, "w") as f:
        json.dump(results, f, indent=2)


def load_ground_truth() -> dict:
    if os.path.exists(GROUND_TRUTH_FILE):
        with open(GROUND_TRUTH_FILE) as f:
            return json.load(f)
    return {}


def run(dataset_name: str):
    dataset_dir = os.path.join(BASE_DIR, dataset_name)
    if not os.path.isdir(dataset_dir):
        print(f"[error] dataset folder not found: {dataset_dir}")
        sys.exit(1)

    filepath = find_data_file(dataset_dir)
    if not filepath:
        print(f"[error] no data file found in {dataset_dir}")
        sys.exit(1)

    print(f"[{dataset_name}] file  : {os.path.basename(filepath)}")
    print(f"[{dataset_name}] status: running pi agent...")

    ground_truth = load_ground_truth()
    actual = ground_truth.get(dataset_name, {}).get("actual_last_obs_date")
    date, col = extract_date_with_pi(filepath, ground_truth=actual)

    print(f"[{dataset_name}] result: last_obs_date={date}  column={col}")

    match = (str(date) == str(actual)) if actual else None
    if actual:
        status = "CORRECT" if match else f"WRONG (actual={actual})"
        print(f"[{dataset_name}] check : {status}")

    # update results.json
    results = load_results()
    results[dataset_name] = {
        "file": os.path.basename(filepath),
        "last_obs_date": date,
        "column_used": col,
        "actual_last_obs_date": actual,
        "match": match,
        "run_at": datetime.now().isoformat(),
    }
    save_results(results)
    print(f"[{dataset_name}] saved : results.json")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        datasets = [
            d for d in os.listdir(BASE_DIR)
            if os.path.isdir(os.path.join(BASE_DIR, d)) and not d.startswith(".")
        ]
        for ds in sorted(datasets):
            run(ds)
    else:
        run(sys.argv[1])
