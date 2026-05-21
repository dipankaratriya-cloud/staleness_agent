"""Generate staleness report: delta change, additions/deletions vs previous run."""

import os
import re
from datetime import date, datetime


def _parse_date(s: str) -> datetime | None:
    if not s or s == "not_possible":
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(s.strip()[:len(fmt.replace("%Y","0000").replace("%m","00").replace("%d","00"))], fmt)
        except ValueError:
            continue
    return None


def _delta_days(current: str, previous: str) -> int | None:
    c, p = _parse_date(current), _parse_date(previous)
    if c and p:
        return (c - p).days
    return None


def _count_rows(filepath: str) -> int | None:
    """Fast row count — reads only line count, never loads full file into memory."""
    if not filepath or not os.path.exists(filepath):
        return None
    ext = os.path.splitext(filepath)[1].lower()
    try:
        if ext in (".csv", ".tsv", ".txt", ""):
            with open(filepath, "rb") as f:
                return sum(1 for _ in f) - 1   # subtract header
        if ext in (".xlsx", ".xls"):
            import openpyxl
            wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
            return sum(ws.max_row - 1 for ws in wb.worksheets)
        if ext == ".parquet":
            import pyarrow.parquet as pq
            return pq.read_metadata(filepath).num_rows
        if ext == ".json":
            import json
            data = json.load(open(filepath))
            return len(data) if isinstance(data, list) else None
    except Exception:
        pass
    return None


def generate(
    run_id: str,
    phase23_results: dict,
    phase4_results: dict,
    previous_bq: dict,          # dataset_id → {last_obs_date, file_row_count}
    base_dir: str,
) -> list[dict]:
    """Build one report row per dataset and return as list of dicts (BQ-ready)."""

    today = str(date.today())
    rows = []

    all_ids = set(phase23_results) | set(phase4_results)

    for dataset_id in all_ids:
        p23 = phase23_results.get(dataset_id, {})
        p4  = phase4_results.get(dataset_id, {})
        prev = previous_bq.get(dataset_id, {})

        current_obs  = p4.get("last_obs_date")
        previous_obs = prev.get("last_obs_date")
        delta_days   = _delta_days(current_obs, previous_obs)

        # Row count from the downloaded file
        file_path = p23.get("file") or (p4.get("files", [None]) or [None])[0]
        if file_path and not os.path.isabs(file_path):
            file_path = os.path.join(base_dir, file_path)
        current_rows  = _count_rows(file_path)
        previous_rows = prev.get("file_row_count")

        row_additions = None
        row_deletions = None
        if current_rows is not None and previous_rows is not None:
            diff = current_rows - previous_rows
            row_additions = max(0, diff)
            row_deletions = max(0, -diff)

        rows.append({
            "run_id":                run_id,
            "run_date":              today,
            "dataset_id":            dataset_id,
            "source_url":            p23.get("url") or p4.get("source_url", ""),
            "download_status":       p23.get("status", "unknown"),
            "download_failure_code": p23.get("failure_code"),
            "last_obs_date":         current_obs,
            "prev_last_obs_date":    previous_obs,
            "obs_date_delta_days":   delta_days,
            "refresh_date":          today,
            "file_row_count":        current_rows,
            "prev_file_row_count":   previous_rows,
            "row_additions":         row_additions,
            "row_deletions":         row_deletions,
            "column_used":           p4.get("column_used"),
            "files_checked":         p4.get("files_checked"),
        })

    rows.sort(key=lambda r: r["dataset_id"])
    return rows


def print_summary(report_rows: list[dict]):
    total      = len(report_rows)
    with_date  = sum(1 for r in report_rows if r.get("last_obs_date") not in (None, "not_possible"))
    with_delta = sum(1 for r in report_rows if r.get("obs_date_delta_days") is not None)
    refreshed  = sum(1 for r in report_rows if (r.get("obs_date_delta_days") or 0) > 0)
    stale      = sum(1 for r in report_rows if (r.get("obs_date_delta_days") or 0) == 0
                     and r.get("prev_last_obs_date"))

    print(f"\n{'='*55}")
    print(f"  STALENESS REPORT  —  {report_rows[0]['run_date'] if report_rows else 'n/a'}")
    print(f"{'='*55}")
    print(f"  Total datasets    : {total}")
    print(f"  Dates extracted   : {with_date}")
    print(f"  Compared to prev  : {with_delta}")
    print(f"  Data refreshed ✅  : {refreshed}  (obs date moved forward)")
    print(f"  Data stale ⚠️     : {stale}   (obs date unchanged)")
    print(f"{'='*55}")
    print(f"  {'DATASET':<45} {'CURRENT':>10}  {'PREV':>10}  {'ΔDAYS':>6}  {'ROWS±':>8}")
    print(f"  {'-'*45} {'-'*10}  {'-'*10}  {'-'*6}  {'-'*8}")
    for r in report_rows:
        delta   = f"{r['obs_date_delta_days']:+d}" if r["obs_date_delta_days"] is not None else "  n/a"
        rows_pm = ""
        if r["row_additions"] is not None:
            rows_pm = f"+{r['row_additions']}" if r["row_additions"] else ""
        if r["row_deletions"]:
            rows_pm += f"-{r['row_deletions']}"
        print(f"  {r['dataset_id'][:45]:<45} "
              f"{(r['last_obs_date'] or '-')[:10]:>10}  "
              f"{(r['prev_last_obs_date'] or '-')[:10]:>10}  "
              f"{delta:>6}  "
              f"{rows_pm:>8}")
    print(f"{'='*55}\n")
