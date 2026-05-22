"""Write pipeline results to BigQuery."""

import os
from datetime import date
from google.cloud import bigquery

PROJECT  = os.environ.get("GCP_PROJECT", "your-gcp-project")
DATASET  = os.environ.get("BQ_DATASET", "staleness")
_client  = None


def _bq():
    global _client
    if _client is None:
        _client = bigquery.Client(project=PROJECT)
    return _client


def _table(name):
    return f"{PROJECT}.{DATASET}.{name}"


def ensure_tables():
    client = _bq()
    ds = bigquery.Dataset(f"{PROJECT}.{DATASET}")
    ds.location = "US"
    client.create_dataset(ds, exists_ok=True)

    tables = {
        "phase1_results": [
            bigquery.SchemaField("run_id",           "STRING"),
            bigquery.SchemaField("run_date",         "DATE"),
            bigquery.SchemaField("dataset_id",       "STRING"),
            bigquery.SchemaField("url",              "STRING"),
            bigquery.SchemaField("matched_folder",   "STRING"),
            bigquery.SchemaField("confidence",       "STRING"),
            bigquery.SchemaField("dataset_name",     "STRING"),
        ],
        "phase23_results": [
            bigquery.SchemaField("run_id",           "STRING"),
            bigquery.SchemaField("run_date",         "DATE"),
            bigquery.SchemaField("dataset_id",       "STRING"),
            bigquery.SchemaField("url",              "STRING"),
            bigquery.SchemaField("status",           "STRING"),
            bigquery.SchemaField("failure_code",     "STRING"),
            bigquery.SchemaField("file",             "STRING"),
            bigquery.SchemaField("rounds_taken",     "INT64"),
            bigquery.SchemaField("file_size_bytes",  "INT64"),
            bigquery.SchemaField("row_count",        "INT64"),
        ],
        "phase4_results": [
            bigquery.SchemaField("run_id",           "STRING"),
            bigquery.SchemaField("run_date",         "DATE"),
            bigquery.SchemaField("dataset_id",       "STRING"),
            bigquery.SchemaField("source_url",       "STRING"),
            bigquery.SchemaField("last_obs_date",    "STRING"),
            bigquery.SchemaField("column_used",      "STRING"),
            bigquery.SchemaField("files_checked",    "INT64"),
        ],
        # ── Delta / staleness report ──────────────────────────────────────────
        "delta_results": [
            bigquery.SchemaField("run_id",              "STRING"),
            bigquery.SchemaField("run_date",            "DATE"),
            bigquery.SchemaField("dataset_id",          "STRING"),
            bigquery.SchemaField("source_url",          "STRING"),
            # Date metrics
            bigquery.SchemaField("last_obs_date",       "STRING"),
            bigquery.SchemaField("prev_last_obs_date",  "STRING"),
            bigquery.SchemaField("date_delta_days",     "INT64"),   # positive = data got newer
            bigquery.SchemaField("data_freshness_days", "INT64"),   # days from last_obs_date to today
            # Row / file metrics
            bigquery.SchemaField("row_count_current",   "INT64"),
            bigquery.SchemaField("row_count_previous",  "INT64"),
            bigquery.SchemaField("row_additions",       "INT64"),   # current - previous (if positive)
            bigquery.SchemaField("row_deletions",       "INT64"),   # previous - current (if positive)
            bigquery.SchemaField("file_size_bytes",     "INT64"),
        ],
    }

    for table_name, schema in tables.items():
        t = bigquery.Table(_table(table_name), schema=schema)
        t.time_partitioning = bigquery.TimePartitioning(field="run_date")
        t.clustering_fields = ["dataset_id"]
        client.create_table(t, exists_ok=True)
        print(f"  [bq] table ready: {_table(table_name)}")


def _insert(table_name, rows):
    if not rows:
        return
    errors = _bq().insert_rows_json(_table(table_name), rows)
    if errors:
        print(f"  [bq] insert errors in {table_name}: {errors[:3]}")
    else:
        print(f"  [bq] ✓ {len(rows)} rows → {table_name}")


def write_phase1(run_id: str, phase1: dict):
    """Push Phase 1 results to BigQuery immediately after Phase 1 completes."""
    today = str(date.today())
    _insert("phase1_results", [
        {
            "run_id":         run_id,
            "run_date":       today,
            "dataset_id":     v.get("dataset_id", k),
            "url":            v.get("url", ""),
            "matched_folder": v.get("matched_folder", ""),
            "confidence":     v.get("confidence", ""),
            "dataset_name":   v.get("dataset_name", ""),
        }
        for k, v in phase1.items()
    ])


def write_phase23(run_id: str, phase23: dict):
    """Push Phase 2+3 results to BigQuery immediately after Phase 2+3 completes."""
    today = str(date.today())
    _insert("phase23_results", [
        {
            "run_id":           run_id,
            "run_date":         today,
            "dataset_id":       v.get("dataset_id", k),
            "url":              v.get("url", ""),
            "status":           v.get("status", ""),
            "failure_code":     v.get("failure_code"),
            "file":             str(v.get("file") or ""),
            "rounds_taken":     v.get("rounds_taken"),
            "file_size_bytes":  v.get("file_size_bytes"),
            "row_count":        v.get("row_count"),
        }
        for k, v in phase23.items()
    ])


def write_phase4(run_id: str, phase4: dict):
    """Push Phase 4 results to BigQuery immediately after Phase 4 completes."""
    today = str(date.today())
    _insert("phase4_results", [
        {
            "run_id":        run_id,
            "run_date":      today,
            "dataset_id":    v.get("dataset_id", k),
            "source_url":    v.get("source_url", ""),
            "last_obs_date": v.get("last_obs_date"),
            "column_used":   v.get("column_used"),
            "files_checked": v.get("files_checked"),
        }
        for k, v in phase4.items()
    ])


def write_delta(run_id: str, delta: list[dict]):
    """Push delta/staleness metrics to BigQuery after Phase 4 completes."""
    if not delta:
        return
    today = str(date.today())
    _insert("delta_results", [
        {
            "run_id":               run_id,
            "run_date":             today,
            "dataset_id":           row.get("dataset_id", ""),
            "source_url":           row.get("source_url", ""),
            "last_obs_date":        row.get("last_obs_date"),
            "prev_last_obs_date":   row.get("prev_last_obs_date"),
            "date_delta_days":      row.get("date_delta_days"),
            "data_freshness_days":  row.get("data_freshness_days"),
            "row_count_current":    row.get("row_count_current"),
            "row_count_previous":   row.get("row_count_previous"),
            "row_additions":        row.get("row_additions"),
            "row_deletions":        row.get("row_deletions"),
            "file_size_bytes":      row.get("file_size_bytes"),
        }
        for row in delta
    ])


def write_all(run_id: str, phase1: dict, phase23: dict, phase4: dict):
    """Push all phases at once (kept for backward compatibility)."""
    write_phase1(run_id, phase1)
    write_phase23(run_id, phase23)
    write_phase4(run_id, phase4)
