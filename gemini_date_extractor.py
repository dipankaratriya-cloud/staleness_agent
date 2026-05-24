"""Direct Gemini API date extractor — replaces the pi-based approach.

Reads sample data from files and asks Gemini to find the last observation date.
No subprocess / pi CLI involved — uses google-generativeai SDK directly.
"""

import json
import os
import re
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import google.generativeai as genai

_API_KEY = os.environ.get("GEMINI_API_KEY", "")
if _API_KEY:
    genai.configure(api_key=_API_KEY)

LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")

_YEAR_RE = re.compile(r"\b(20[012]\d|19\d{2})\b")

MAX_SAMPLE_ROWS = 5
MAX_SAMPLE_BYTES = 8_000  # send at most 8 KB of file content per file


# ─── File sampling ────────────────────────────────────────────────────────────

def _sample_file(path: str) -> str | None:
    """Return a text snippet from a data file: column names + first few rows."""
    ext = os.path.splitext(path)[1].lower()
    size = os.path.getsize(path) if os.path.exists(path) else 0
    if size == 0:
        return None
    try:
        if ext in (".csv", ".tsv", ".txt"):
            sep = "\t" if ext == ".tsv" else ","
            import pandas as pd
            df = pd.read_csv(path, sep=sep, nrows=MAX_SAMPLE_ROWS,
                             on_bad_lines="skip", low_memory=False)
            return df.to_string(index=False)
        elif ext in (".xlsx", ".xls"):
            import pandas as pd
            df = pd.read_excel(path, nrows=MAX_SAMPLE_ROWS)
            return df.to_string(index=False)
        elif ext == ".parquet":
            import pandas as pd
            df = pd.read_parquet(path).head(MAX_SAMPLE_ROWS)
            return df.to_string(index=False)
        elif ext in (".json", ".geojson"):
            with open(path, "rb") as f:
                raw = f.read(MAX_SAMPLE_BYTES).decode("utf-8", errors="replace")
            return raw
        elif ext in (".gz",):
            import gzip, pandas as pd
            with gzip.open(path, "rb") as f:
                sample = f.read(MAX_SAMPLE_BYTES).decode("utf-8", errors="replace")
            return sample[:MAX_SAMPLE_BYTES]
        else:
            with open(path, "rb") as f:
                raw = f.read(MAX_SAMPLE_BYTES).decode("utf-8", errors="replace")
            return raw[:MAX_SAMPLE_BYTES]
    except Exception as e:
        return f"(could not read: {e})"


# ─── Prompt ───────────────────────────────────────────────────────────────────

_PROMPT = """\
You are analyzing dataset files to find the LAST OBSERVATION DATE — the most recent date the data DESCRIBES (not when it was published or downloaded).

## Column priority rules
1. PREFER columns: Year, Date, Period, Observation_Date, Ref_Date, Reference_Period, Time_Period
2. IGNORE columns: Source_Year, Source Year, Publication_Year, Reported_Year, Data_Year, Access_Date
3. Wide format (year values are column headers like 2018, 2019, 2020) → last numeric column header is the date
4. Long format (a date/year column) → max value in that column

## Dataset files and their samples:

{file_sections}

## Instructions
1. Look at the column names and sample values above.
2. Find the column (or header) that represents the observation period.
3. Report the MAXIMUM (most recent) date found, as YYYY or YYYY-MM or YYYY-MM-DD.
4. If no date column exists or it cannot be determined, say not_possible.

Output ONLY this JSON (no markdown, no explanation):
{{"last_obs_date": "YYYY or YYYY-MM or YYYY-MM-DD or not_possible", "column_used": "column_name", "files_checked": {n}}}
"""


# ─── Main extraction ──────────────────────────────────────────────────────────

def extract_date_with_gemini(
    files: list[str],
    dataset_id: str = "unknown",
    timeout: int = 60,
) -> tuple[str, str, int]:
    """Extract last observation date from data files using Gemini Flash.

    Returns (last_obs_date, column_used, files_checked).
    """
    if not _API_KEY:
        return "not_possible", "no_api_key", 0

    if not files:
        return "not_possible", "no_files", 0

    # Build file sections
    file_sections = []
    files_checked = 0
    for i, path in enumerate(files[:6]):
        if not os.path.exists(path):
            continue
        sample = _sample_file(path)
        if sample is None:
            continue
        # Truncate sample
        if len(sample) > MAX_SAMPLE_BYTES:
            sample = sample[:MAX_SAMPLE_BYTES] + "\n...(truncated)"
        file_sections.append(
            f"### File {i+1}: {os.path.basename(path)}\n```\n{sample}\n```"
        )
        files_checked += 1

    if not file_sections:
        return "not_possible", "no_readable_files", 0

    prompt = _PROMPT.format(
        file_sections="\n\n".join(file_sections),
        n=files_checked,
    )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(LOGS_DIR, exist_ok=True)
    safe_id = re.sub(r"[/\\: ]", "_", dataset_id)
    log_path = os.path.join(LOGS_DIR, f"gemini_{safe_id}_{ts}.log")

    try:
        model = genai.GenerativeModel("gemini-2.0-flash")
        resp = model.generate_content(
            prompt,
            generation_config={"temperature": 0, "max_output_tokens": 200},
            request_options={"timeout": timeout},
        )
        raw = resp.text.strip()
    except Exception as e:
        with open(log_path, "w") as lf:
            lf.write(f"API error: {e}\n")
        return "not_possible", f"api_error: {e}", 0

    # Log
    with open(log_path, "w") as lf:
        lf.write(f"dataset: {dataset_id}\nfiles: {files}\n\nresponse:\n{raw}\n")

    # Parse JSON from response
    try:
        # Strip any markdown fences
        clean = re.sub(r"```(?:json)?\s*|\s*```", "", raw).strip()
        # Find the JSON object
        m = re.search(r"\{.*\}", clean, re.DOTALL)
        if m:
            result = json.loads(m.group())
            date = str(result.get("last_obs_date", "not_possible")).strip()
            col = str(result.get("column_used", "gemini")).strip()
            n = int(result.get("files_checked", files_checked))
            # Validate date format
            if date != "not_possible":
                if not _YEAR_RE.search(date):
                    date = "not_possible"
            return date, col, n
    except Exception:
        pass

    # Fallback: extract any year from the response
    years = _YEAR_RE.findall(raw)
    if years:
        return max(years), "gemini_fallback", files_checked

    return "not_possible", "parse_error", files_checked
