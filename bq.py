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
            bigquery.SchemaField("run_id",              "STRING"),
            bigquery.SchemaField("run_date",            "DATE"),
            bigquery.SchemaField("dataset_id",          "STRING"),
            bigquery.SchemaField("url",                 "STRING"),
            bigquery.SchemaField("status",              "STRING"),
            bigquery.SchemaField("failure_code",        "STRING"),
            bigquery.SchemaField("file",                "STRING"),
            bigquery.SchemaField("rounds_taken",        "INT64"),
            bigquery.SchemaField("file_size_bytes",     "INT64"),
            bigquery.SchemaField("row_count",           "INT64"),
            bigquery.SchemaField("download_time_sec",   "FLOAT64"),
            bigquery.SchemaField("download_strategy",   "STRING"),
            bigquery.SchemaField("file_format",         "STRING"),
        ],
        "phase4_results": [
            bigquery.SchemaField("run_id",               "STRING"),
            bigquery.SchemaField("run_date",             "DATE"),
            bigquery.SchemaField("dataset_id",           "STRING"),
            bigquery.SchemaField("source_url",           "STRING"),
            bigquery.SchemaField("last_obs_date",        "STRING"),
            bigquery.SchemaField("column_used",          "STRING"),
            bigquery.SchemaField("files_checked",        "INT64"),
            bigquery.SchemaField("extraction_time_sec",  "FLOAT64"),
        ],
        # ── Full report table ─────────────────────────────────────────────────
        "delta_results": [
            bigquery.SchemaField("run_id",                 "STRING"),
            bigquery.SchemaField("run_date",               "DATE"),
            bigquery.SchemaField("dataset_id",             "STRING"),
            bigquery.SchemaField("source_url",             "STRING"),
            # ── Date metrics ──────────────────────────────────────────────────
            bigquery.SchemaField("last_obs_date",          "STRING"),
            bigquery.SchemaField("prev_last_obs_date",     "STRING"),
            bigquery.SchemaField("date_delta_days",        "INT64"),
            bigquery.SchemaField("data_freshness_days",    "INT64"),
            bigquery.SchemaField("staleness_label",        "STRING"),   # Fresh/Recent/Stale/Very Stale
            # ── Row / file metrics ────────────────────────────────────────────
            bigquery.SchemaField("row_count_current",      "INT64"),
            bigquery.SchemaField("row_count_previous",     "INT64"),
            bigquery.SchemaField("row_additions",          "INT64"),
            bigquery.SchemaField("row_deletions",          "INT64"),
            bigquery.SchemaField("file_size_bytes",        "INT64"),
            bigquery.SchemaField("file_format",            "STRING"),
            # ── Download metrics ──────────────────────────────────────────────
            bigquery.SchemaField("download_strategy",      "STRING"),
            bigquery.SchemaField("download_time_sec",      "FLOAT64"),
            # ── Extraction metrics ────────────────────────────────────────────
            bigquery.SchemaField("extraction_time_sec",    "FLOAT64"),
            # ── Pipeline timing ───────────────────────────────────────────────
            bigquery.SchemaField("phase1_time_sec",        "FLOAT64"),
            bigquery.SchemaField("phase23_time_sec",       "FLOAT64"),
            bigquery.SchemaField("phase4_time_sec",        "FLOAT64"),
            bigquery.SchemaField("total_pipeline_time_sec","FLOAT64"),
        ],
    }

    for table_name, schema in tables.items():
        table_ref = _table(table_name)
        try:
            existing = client.get_table(table_ref)
            # Add any new columns without destroying existing data
            existing_fields = {f.name for f in existing.schema}
            new_fields = [f for f in schema if f.name not in existing_fields]
            if new_fields:
                existing.schema = list(existing.schema) + new_fields
                client.update_table(existing, ["schema"])
                print(f"  [bq] schema updated: {table_name} (+{len(new_fields)} new columns)")
            else:
                print(f"  [bq] table ready: {table_ref}")
        except Exception:
            # Table doesn't exist — create fresh
            t = bigquery.Table(table_ref, schema=schema)
            t.time_partitioning = bigquery.TimePartitioning(field="run_date")
            t.clustering_fields = ["dataset_id"]
            client.create_table(t, exists_ok=True)
            print(f"  [bq] table created: {table_ref}")


def _insert(table_name, rows):
    if not rows:
        return
    errors = _bq().insert_rows_json(_table(table_name), rows)
    if errors:
        print(f"  [bq] insert errors in {table_name}: {errors[:3]}")
    else:
        print(f"  [bq] ✓ {len(rows)} rows → {table_name}")


def write_phase1(run_id: str, phase1: dict):
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
    today = str(date.today())
    _insert("phase23_results", [
        {
            "run_id":             run_id,
            "run_date":           today,
            "dataset_id":         v.get("dataset_id", k),
            "url":                v.get("url", ""),
            "status":             v.get("status", ""),
            "failure_code":       v.get("failure_code"),
            "file":               str(v.get("file") or ""),
            "rounds_taken":       v.get("rounds_taken"),
            "file_size_bytes":    v.get("file_size_bytes"),
            "row_count":          v.get("row_count"),
            "download_time_sec":  v.get("download_time_sec"),
            "download_strategy":  v.get("download_strategy", ""),
            "file_format":        v.get("file_format", ""),
        }
        for k, v in phase23.items()
    ])


def write_phase4(run_id: str, phase4: dict):
    today = str(date.today())
    _insert("phase4_results", [
        {
            "run_id":              run_id,
            "run_date":            today,
            "dataset_id":          v.get("dataset_id", k),
            "source_url":          v.get("source_url", ""),
            "last_obs_date":       v.get("last_obs_date"),
            "column_used":         v.get("column_used"),
            "files_checked":       v.get("files_checked"),
            "extraction_time_sec": v.get("extraction_time_sec"),
        }
        for k, v in phase4.items()
    ])


def write_delta(run_id: str, delta: list[dict]):
    if not delta:
        return
    today = str(date.today())
    _insert("delta_results", [
        {
            "run_id":                  run_id,
            "run_date":                today,
            "dataset_id":              row.get("dataset_id", ""),
            "source_url":              row.get("source_url", ""),
            # Date metrics
            "last_obs_date":           row.get("last_obs_date"),
            "prev_last_obs_date":      row.get("prev_last_obs_date"),
            "date_delta_days":         row.get("date_delta_days"),
            "data_freshness_days":     row.get("data_freshness_days"),
            "staleness_label":         row.get("staleness_label"),
            # Row / file metrics
            "row_count_current":       row.get("row_count_current"),
            "row_count_previous":      row.get("row_count_previous"),
            "row_additions":           row.get("row_additions"),
            "row_deletions":           row.get("row_deletions"),
            "file_size_bytes":         row.get("file_size_bytes"),
            "file_format":             row.get("file_format", ""),
            # Download metrics
            "download_strategy":       row.get("download_strategy", ""),
            "download_time_sec":       row.get("download_time_sec"),
            # Extraction metrics
            "extraction_time_sec":     row.get("extraction_time_sec"),
            # Pipeline timing
            "phase1_time_sec":         row.get("phase1_time_sec"),
            "phase23_time_sec":        row.get("phase23_time_sec"),
            "phase4_time_sec":         row.get("phase4_time_sec"),
            "total_pipeline_time_sec": row.get("total_pipeline_time_sec"),
        }
        for row in delta
    ])


def write_all(run_id: str, phase1: dict, phase23: dict, phase4: dict):
    """Kept for backward compatibility."""
    write_phase1(run_id, phase1)
    write_phase23(run_id, phase23)
    write_phase4(run_id, phase4)
