# Staleness Agent — Pipeline Design

## Overview

The Staleness Agent pipeline takes a list of dataset URLs from `Provenance.csv` and
finds the **last observation date** for each dataset — the most recent date for which
data was actually recorded. The pipeline is fully agent-driven using pi coding agent
(Gemini 2.5 Pro) with Groq Compound as a browser fallback for JS-rendered pages.

---

## Folder Structure

```
Staleness Agent/
├── .env                      # API keys (Gemini, GitHub, Groq)
├── Provenance.csv            # Input: dataset IDs + source URLs
├── ground_truth.json         # Known correct last observation dates (for validation)
├── results.json              # Output: agent-found dates per dataset
├── PIPELINE.md               # This file
│
├── pi_date_extractor.py      # Phase 4 implementation (done)
├── run_extraction.py         # Runner for Phase 4 (done)
│
├── <dataset>/                # One folder per dataset (downloaded files land here)
│   └── <data_file>.csv
│
└── logs/                     # One log file per agent run, per dataset
    └── <dataset>_<timestamp>.log
```

---

## Tools & APIs

| Tool | Purpose |
|---|---|
| pi coding agent (Gemini 2.5 Pro) | Core agent for all phases — inspects, writes scripts, runs them, iterates |
| Groq Compound Beta | Browser fallback for JS-rendered pages in Phase 1 |
| GitHub API (GITHUB_TOKEN) | Searching and reading the datacommonsorg/data repo |
| Groq API (GROQ_API_KEY) | Groq Compound web browsing |
| Gemini API (GEMINI_API_KEY) | Powers the pi agent |

---

## Phase 1 — URL → Dataset Name + Repo Match

### Goal
Given a source URL, find the matching folder in the
`datacommonsorg/data` GitHub repo (`statvar_imports/` or `scripts/`).

### How it works

Pi runs a single multi-turn session that handles the full flow:

**Step 1 — Fetch the URL**
- Pi runs `curl` on the source URL
- Parses the response for dataset name signals:
  - Page `<title>` or `<h1>`
  - OpenGraph / JSON-LD metadata
  - URL path segments

**Step 2 — Handle JS-rendered pages**
- If curl returns thin/empty HTML (no readable content), Pi detects this
- Hands the URL to **Groq Compound** which renders the full JS page
- Groq Compound returns the visible page text and metadata
- Pi extracts the dataset name from that text

**Step 3 — Repo search and matching**
- Pi uses the GitHub **recursive tree API** (`/git/trees/HEAD?recursive=1`) to get
  every folder at every depth in one call — no matter how deeply nested
- Fuzzy-matches the extracted dataset name against the **full folder path**
  (e.g. `statvar_imports/health/eurostat/bmi`), not just the top-level name
- If the tree response is truncated (repo too large), Pi manually recurses
  folder by folder up to depth 5
- If the top match is ambiguous, Pi reads the README of the top 2–3 candidates
  and picks the best one
- If no match found from name, Pi uses the URL domain/path as a signal
  (e.g. `sidra.ibge.gov.br` → search for `SIDRA` or `IBGE` in repo)

**Output**
- Matched repo folder path + confidence score
- OR `no_match` with best guess and reason

**Iterative loop**
```
curl URL → detect JS? → Groq Compound render
       ↓
Extract name → GitHub API search → score candidates
       ↓
Ambiguous? → Read READMEs → pick best
       ↓
No match? → Try URL domain/path signals → retry search
       ↓
Stop when confident match found or all strategies exhausted
```

---

## Phase 2+3 — Dataset Download (combined, no categories)

### Goal
Get the dataset file onto local disk by any means possible.
No fixed timeout — runs until the file is downloaded or a clear hard error occurs.
Download progress is streamed to terminal and log in real time.

### How it works

Pi runs a single session that tries every available strategy in order,
iterating within each strategy before moving to the next.

**Step 1 — Try repo scripts**
- Pi checks the matched repo folder for `manifest.json`, `download.sh`,
  or any Python download script
- Executes the script
- On failure: reads the error, patches the script, retries
- Loop: `run → read error → patch → run` until success or no more fixable errors

**Step 2 — Try direct URL download**
- If no repo script exists OR all patches failed:
- Pi inspects the source URL directly
- Detects if there is a direct file link (`.csv`, `.zip`, `.xlsx`, `.json`)
- Downloads with `wget` or `curl`, streaming progress to terminal

**Step 3 — Try API endpoint**
- If no direct file link found:
- Pi looks for detectable API patterns in the page source or network behavior
- Writes a Python script to call the API and save the response
- Runs it, checks the output file, retries on error

**Step 4 — Try Playwright scraping**
- If no API found:
- Pi writes a Playwright script to:
  - Open the page in a headless browser
  - Find and click the download button
  - Wait for the file to land on disk
- Runs the script, monitors the download folder

**Step 5 — All strategies exhausted**
- Records a specific, actionable failure reason:

| Code | Meaning |
|---|---|
| `auth_required` | Download needs login or API key not publicly available |
| `paywall` | Data is behind a paid subscription |
| `no_direct_link` | No downloadable file found anywhere on the page |
| `js_only_no_api` | Fully dynamic site with no accessible API or file endpoint |
| `rate_limited` | Server blocked repeated requests |
| `broken_script` | Manifest/script exists but has unfixable errors |
| `network_error` | Site unreachable or consistently returns errors |

**Progress output (no hard timeout)**
- File size and download speed streamed in real time to terminal
- Periodic log entries showing bytes downloaded / total
- Continues until file is complete or a hard error is hit

**Iterative loop**
```
Repo script found?
  YES → run → success? done : patch → retry loop
  NO  ↓
Direct file link?
  YES → wget/curl → success? done : retry
  NO  ↓
API endpoint detectable?
  YES → write + run API script → success? done : fix → retry
  NO  ↓
Playwright download?
  YES → write + run script → success? done : fix → retry
  NO  ↓
Record failure reason → stop
```

---

## Phase 4 — Observation Date Extraction (implemented)

### Goal
Find the **last observation date** in the downloaded dataset file —
the most recent date for which data was recorded.

### How it works

Pi (Gemini 2.5 Pro) runs a multi-turn session per dataset file:

**Step 1 — Inspect schema**
- Runs `head` and column inspection via bash
- Identifies all date-related columns

**Column priority rules**
- PREFER: `Year`, `Date`, `Period`, `Observation_Date`, `Ref_Date`, `Time_Period`
- IGNORE: `Source_Year`, `Publication_Year`, `Access_Date`, `Download_Date`
- If both `Year` and `Source Year` exist → use only `Year`

**Step 2 — Write and run extraction script**
- Pi writes a Python script to extract `max()` of the identified date column
- Handles: wide format (year as header), long format (single date column),
  split format (year + month in separate columns), mixed types

**Step 3 — Verify and iterate**
- Checks result is a plausible year/date (1900–2030)
- If not: re-examines columns, writes revised script, retries

**Step 4 — Retry with correction (if ground truth available)**
- If ground truth is provided and result is wrong:
- Sends correction message in the same pi session
- Agent re-examines with the feedback and tries again
- Up to 3 attempts total

**Output saved to `results.json`**
```json
{
  "dataset_name": {
    "file": "filename.csv",
    "last_obs_date": "2025",
    "column_used": "Year",
    "actual_last_obs_date": "2025",
    "match": true,
    "run_at": "2026-05-19T..."
  }
}
```

**Log file per run**
- Every tool call, bash execution, script output, and iteration logged to `logs/<dataset>_<timestamp>.log`

---

## Phase 5 — Skipped (for now)

---

## End-to-End Flow

```
Provenance.csv
     │
     ▼
Phase 1: pi agent
  URL → fetch (curl or Groq Compound) → extract name → GitHub repo search → match
     │
     ▼ matched repo folder + source URL
     │
Phase 2+3: pi agent
  Repo script → direct download → API → Playwright → failure reason
     │
     ▼ local dataset file
     │
Phase 4: pi agent (Gemini 2.5 Pro)
  Schema inspect → column pick → script → max date → verify → iterate
     │
     ▼
results.json  +  logs/<dataset>_<timestamp>.log
```

---

## Validation Loop (ground truth driven)

For datasets where the correct answer is known:

1. Add entry to `ground_truth.json`
2. Run `python3 run_extraction.py <dataset>`
3. If wrong: agent automatically retries with correction message (same session)
4. If still wrong after 3 attempts: analyze the log → identify failure pattern →
   add new rule or example to the prompt → re-run
5. Repeat until CORRECT

This progressively hardens the prompt without any model retraining.
