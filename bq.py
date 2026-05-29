"""Write pipeline results to BigQuery."""

import os
from datetime import date
from google.cloud import bigquery

PROJECT      = os.environ.get("GCP_PROJECT", "datcom-infosys-dev")
DATASET      = os.environ.get("BQ_DATASET",  "staleness")
# Full path to the data-engineering team's import list table in datcom-store.
# Set DATCOM_DATASET to the BQ dataset name inside datcom-store that contains
# datcom_import_list (e.g. "dc", "imports", "datacommons" — check with your DE team).
_DATCOM_DS   = os.environ.get("DATCOM_DATASET", "dc_kg_latest")
DATCOM_TABLE = os.environ.get("DATCOM_IMPORT_TABLE",
                               f"datcom-store.{_DATCOM_DS}.datcom_import_list")
_client      = None


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
        "refresh_dates": [
            bigquery.SchemaField("run_id",             "STRING"),
            bigquery.SchemaField("run_date",           "DATE"),
            bigquery.SchemaField("dataset_id",         "STRING"),
            bigquery.SchemaField("provenance_url",     "STRING"),
            bigquery.SchemaField("last_refresh_date",  "STRING"),
            bigquery.SchemaField("date_source",        "STRING"),
            bigquery.SchemaField("tier_used",          "INT64"),
            bigquery.SchemaField("refresh_confidence", "STRING"),
        ],
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
            # ── Refresh date metrics ──────────────────────────────────────────
            bigquery.SchemaField("last_refresh_date",      "STRING"),
            bigquery.SchemaField("prev_last_refresh_date", "STRING"),
            bigquery.SchemaField("refresh_date_delta_days","INT64"),
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
            "last_obs_date":            row.get("last_obs_date"),
            "prev_last_obs_date":       row.get("prev_last_obs_date"),
            "date_delta_days":          row.get("date_delta_days"),
            "data_freshness_days":      row.get("data_freshness_days"),
            "staleness_label":          row.get("staleness_label"),
            # Refresh date metrics
            "last_refresh_date":        row.get("last_refresh_date"),
            "prev_last_refresh_date":   row.get("prev_last_refresh_date"),
            "refresh_date_delta_days":  row.get("refresh_date_delta_days"),
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


def get_datcom_previous() -> dict:
    """Query datcom_import_list for the latest obs + refresh date per dataset.

    Returns two lookup maps so compute_delta can match by whichever key works:
      {
        "by_id":  {dataset_id:    {last_obs_date, last_refresh_date}},
        "by_url": {provenance_url: {last_obs_date, last_refresh_date}},
      }

    Uses QUALIFY ROW_NUMBER() so it's safe whether the table has one row or
    many rows per dataset — always picks the most recent.
    Cross-project query: datcom-store → runs fine as long as the service
    account for datcom-infosys-dev has BigQuery Data Viewer on datcom-store.
    """
    query = f"""
        SELECT
            dataset_id,
            provenance_url,
            latestObservationDate,
            lastDataRefreshDate
        FROM `{DATCOM_TABLE}`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY dataset_id
            ORDER BY latestObservationDate DESC NULLS LAST
        ) = 1
    """
    by_id  = {}
    by_url = {}
    try:
        for row in _bq().query(query).result():
            entry = {
                "last_obs_date":     row.latestObservationDate,
                "last_refresh_date": row.lastDataRefreshDate,
            }
            if row.dataset_id:
                by_id[row.dataset_id] = entry
            if row.provenance_url:
                by_url[row.provenance_url] = entry
        print(f"  [bq] datcom_import_list: {len(by_id)} datasets loaded for comparison")
    except Exception as e:
        print(f"  [bq] get_datcom_previous failed — check DATCOM_DATASET env var: {e}")
    return {"by_id": by_id, "by_url": by_url}


def get_successful_obs_datasets() -> dict[str, str]:
    """Return {dataset_id: source_url} for datasets that have a confirmed obs date.
    Drawn from the most recent row per dataset in delta_results.
    Empty dict on first run or if BQ is unreachable.
    """
    query = f"""
        SELECT dataset_id, source_url
        FROM `{_table("delta_results")}`
        WHERE last_obs_date IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY run_date DESC) = 1
    """
    try:
        return {row.dataset_id: row.source_url for row in _bq().query(query).result()}
    except Exception as e:
        print(f"  [bq] get_successful_obs_datasets failed: {e}")
        return {}


def get_successful_refresh_datasets() -> dict[str, str]:
    """Return {dataset_id: source_url} for datasets that have a confirmed refresh date.
    Drawn from the most recent row per dataset in delta_results.
    Empty dict on first run or if BQ is unreachable.
    """
    query = f"""
        SELECT dataset_id, source_url
        FROM `{_table("delta_results")}`
        WHERE last_refresh_date IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY run_date DESC) = 1
    """
    try:
        return {row.dataset_id: row.source_url for row in _bq().query(query).result()}
    except Exception as e:
        print(f"  [bq] get_successful_refresh_datasets failed: {e}")
        return {}


def get_previous_results() -> dict:
    """Query delta_results for the most recent row per dataset_id.
    Returns {dataset_id: {last_obs_date, last_refresh_date, row_count_current}}.
    """
    query = f"""
        SELECT
            dataset_id,
            last_obs_date,
            last_refresh_date,
            row_count_current
        FROM `{_table("delta_results")}`
        QUALIFY ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY run_date DESC) = 1
    """
    try:
        rows = list(_bq().query(query).result())
        return {
            row.dataset_id: {
                "last_obs_date":      row.last_obs_date,
                "last_refresh_date":  row.last_refresh_date,
                "row_count_current":  row.row_count_current,
            }
            for row in rows
        }
    except Exception as e:
        print(f"  [bq] get_previous_results failed (first run?): {e}")
        return {}


def write_refresh_dates(run_id: str, refresh: dict):
    today = str(date.today())
    tier_confidence = {1: "high", 2: "high", 3: "medium", 4: "medium", 5: "medium"}
    _insert("refresh_dates", [
        {
            "run_id":             run_id,
            "run_date":           today,
            "dataset_id":         k,
            "provenance_url":     v.get("url", ""),
            "last_refresh_date":  v.get("last_refresh_date"),
            "date_source":        v.get("date_source"),
            "tier_used":          v.get("tier_used"),
            "refresh_confidence": tier_confidence.get(v.get("tier_used")),
        }
        for k, v in refresh.items()
        if v.get("last_refresh_date")   # only upload rows where we found a date
    ])


def write_all(run_id: str, phase1: dict, phase23: dict, phase4: dict):
    """Kept for backward compatibility."""
    write_phase1(run_id, phase1)
    write_phase23(run_id, phase23)
    write_phase4(run_id, phase4)
