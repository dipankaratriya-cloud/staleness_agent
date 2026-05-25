"""Phase 2+3: pi agent downloads a dataset file using every available strategy.

Changes vs original:
- Self-correction feedback loop: agent reads its own failure log and retries
  with corrected approach (up to MAX_CORRECTION_ROUNDS total attempts).
- Extended file-format detection: .nc, .dat, .geojson, extensionless data files,
  any file >100KB that's not a known script type.
- Strategy 5: write a fully custom download script from scratch.
- manifest.json missing downloadUrl is NOT a reason to give up — agent must
  write its own script using repo metadata + source URL inspection.
- Better parse_error recovery: scan full agent output, not just last 300 chars.
"""

import gc
import json
import os
import re
import subprocess
import threading
from collections import deque
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASETS_DIR = os.path.join(BASE_DIR, "datasets")   # all downloaded data lives here
LOGS_DIR = os.path.join(BASE_DIR, "logs")

MAX_CORRECTION_ROUNDS = 3   # total agent invocations per dataset (1 initial + 2 corrections)
ROUND_TIMEOUT = 600         # seconds per round

# Files with these extensions AND >1KB are considered successfully downloaded data.
DATA_EXTS = {
    ".csv", ".json", ".xlsx", ".zip", ".tsv", ".xls", ".parquet",
    ".gz", ".bz2", ".tar", ".nc", ".dat", ".geojson", ".arrow",
    ".feather", ".h5", ".hdf5", ".xml", ".ndjson", ".jsonl",
}
# Files with these extensions are never counted as data (scripts, docs, logs).
SKIP_EXTS = {
    ".py", ".sh", ".bash", ".log", ".md", ".rst", ".html", ".htm",
    ".js", ".css", ".ipynb", ".cfg", ".ini", ".toml", ".yaml", ".yml",
}

# ─── Prompts ──────────────────────────────────────────────────────────────────

_BASE_TASK = """\
Dataset ID  : {dataset_id}
Repo folder : {repo_folder}
Source URL  : {source_url}
Output dir  : {output_dir}

Env vars available in every bash call:
  $GITHUB_TOKEN — GitHub API access
  $GROQ_API_KEY — Groq compound-beta (JS page rendering + live browsing)

TASK: Download the dataset file(s) to {output_dir}/ using the strategies below IN ORDER.
For each strategy: retry up to 5 times (run → read error → fix → run) before moving on.
Stop as soon as ANY file >1 KB lands in {output_dir}/ (data can have any extension or none).
After each attempt: ls -lh {output_dir}/

━━━ STRATEGY 1 — Repo scripts ━━━

List the repo folder:
  curl -s -H "Authorization: token $GITHUB_TOKEN" \\
    "https://api.github.com/repos/datacommonsorg/data/contents/{repo_folder}" \\
    | python3 -c "import json,sys; [print(x['name']) for x in json.load(sys.stdin) if isinstance(x,dict)]"

A) If manifest.json exists — fetch it:
  curl -s -H "Authorization: token $GITHUB_TOKEN" \\
    "https://api.github.com/repos/datacommonsorg/data/contents/{repo_folder}/manifest.json" \\
    | python3 -c "import json,sys,base64; d=json.load(sys.stdin); print(base64.b64decode(d.get('content','')).decode()[:4000])"

  ✅ If downloadUrls / importUrl / dataDownloadUrl found → wget/curl each to {output_dir}/.

  ⚠️  IF manifest.json HAS NO DOWNLOAD URLS — this is NOT a dead end.
  You must write a custom download script. Follow these steps:
    1. Read ALL other files in the repo folder: README, config.py, *.json, *.yaml, util scripts.
       They tell you what API or source the importer expects.
    2. Fetch {source_url} (curl -sL; use Groq compound-beta if HTML_LEN < 2000).
       Find the data API, bulk download link, or paginated endpoint.
    3. Write a Python script that fetches and saves the data to {output_dir}/data.csv (or .json).
       Handle pagination. Handle auth if needed (check env vars).
    4. Run it. Read stdout+stderr. Fix the specific error. Run again. Repeat ×5.
    5. Only move to Strategy 2 if the script fails all 5 attempts — and log the exact error.

B) If a Python/shell download script exists — fetch and run it:
  curl -s -H "Authorization: token $GITHUB_TOKEN" \\
    "https://api.github.com/repos/datacommonsorg/data/contents/{repo_folder}/<script>" \\
    | python3 -c "import json,sys,base64; d=json.load(sys.stdin); print(base64.b64decode(d.get('content','')).decode())" \\
    > /tmp/dl_script_{slug}.py
  cd "{output_dir}" && python3 /tmp/dl_script_{slug}.py

  On import error → pip3 install <package>, re-run.
  On path/config error → read the script, provide expected files or env vars, re-run.
  On logic error → patch the specific failing line, re-run.
  Loop up to 5 times.

━━━ STRATEGY 2 — Direct file link from source URL ━━━

  curl -sL --max-time 30 "{source_url}" | python3 -c "
import sys, re
html = sys.stdin.read()
print('HTML_LEN:', len(html))
for pat in [
    r'href=[\"\\x27]([^\"\\x27>]+\\.(?:csv|zip|xlsx|json|tsv|gz|xls|nc|dat|parquet)[^\"\\x27>]*)',
    r'(https?://[^\\s\"\\x27>]+\\.(?:csv|zip|xlsx|json|tsv|gz|xls|nc|dat|parquet))',
    r'(https?://[^\\s\"\\x27>]*(?:download|export|bulk)[^\\s\"\\x27>]*)',
]:
    for m in re.findall(pat, html, re.I):
        print('FILE_LINK:', m)
"

If HTML_LEN < 2000 (JS-rendered), use Groq compound-beta:
  python3 -c "
import os
from groq import Groq
r = Groq(api_key=os.environ['GROQ_API_KEY']).chat.completions.create(
    model='compound-beta',
    messages=[{{'role':'user','content':
        'Visit {source_url} and find ALL direct download links for data files '
        '(CSV, ZIP, XLSX, JSON, TSV, NC, Parquet, or any bulk data). '
        'Also look for API endpoints that return bulk data. '
        'List each complete URL on its own line starting with FILE_LINK:'
    }}]
)
print(r.choices[0].message.content)
"

Download: wget --progress=dot:mega -P {output_dir} "<url>"
Retry up to 5 times (use -c to resume partials).

━━━ STRATEGY 3 — API endpoint ━━━

Identify API patterns from URL structure and page source:
- REST: /api/v1/data, /datasets/download?format=csv
- OData: ?$format=json&$top=10000&$skip=0  (paginate with $skip)
- SDMX: /data/<flow>/all?format=csv
- ArcGIS: /FeatureServer/0/query?where=1%3D1&outFields=*&f=csv&resultOffset=0
- Socrata: /api/views/<id>/rows.csv?accessType=DOWNLOAD
- CKAN: /api/3/action/datastore_search?resource_id=<id>&limit=100000
- Eurostat: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/<code>/?format=SDMX-CSV

Write and run a Python script. If paginated, loop until all pages saved. Retry ×5.

━━━ STRATEGY 4 — Playwright browser download ━━━

  python3 << 'PYEOF'
import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(accept_downloads=True)
        page = await ctx.new_page()
        await page.goto('{source_url}', wait_until='networkidle', timeout=60000)
        await page.wait_for_timeout(3000)
        await page.screenshot(path='/tmp/pw_{slug}.png')
        print('screenshot → /tmp/pw_{slug}.png  (inspect to find correct selector)')
        for sel in [
            'a[href*="download"]', 'button:has-text("Download")',
            'a:has-text("CSV")', 'a:has-text("Export")',
            'a[href*=".csv"]', '[data-format="csv"]',
            'a:has-text("Download data")', 'button:has-text("Export")',
            'a:has-text("Download CSV")', 'button:has-text("Download CSV")',
            '[aria-label*="download" i]', 'a[download]',
            'a:has-text("Download file")', '.download-btn',
        ]:
            try:
                async with page.expect_download(timeout=30000) as dl:
                    await page.click(sel)
                download = await dl.value
                await download.save_as('{output_dir}/' + (download.suggested_filename or 'data.csv'))
                print('downloaded:', download.suggested_filename)
                await browser.close()
                return
            except Exception as e:
                print(f'sel {{sel!r}} → {{e}}')
        await browser.close()

asyncio.run(run())
PYEOF

Retry up to 3 times. On each retry, read /tmp/pw_{slug}.png screenshot and adjust selectors.

━━━ STRATEGY 5 — Custom script from scratch ━━━

Last resort — synthesise a purpose-built downloader:
  1. Use Groq compound-beta to browse {source_url}, understand its data-access mechanism.
  2. Write a fully custom Python script that targets this specific site's structure
     (hidden APIs, authenticated endpoints, scraping paginated tables, decoding obfuscated URLs).
  3. Run → read errors → fix → run. Repeat ×5. Be creative and persistent.

━━━ STOPPING CRITERIA ━━━

After each strategy attempt, run: ls -lh {output_dir}/
If ANY file >1 KB exists (any extension, including no extension) → SUCCESS.
Data files do not always have standard extensions (.nc, .dat, extensionless TSV — all valid).

━━━ FINAL OUTPUT (exactly one JSON line, no markdown fences) ━━━
{{"status": "success", "file": "<filename>", "strategy": "<1|2|3|4|5>"}}
  or
{{"status": "failure", "file": null, "failure_code": "<auth_required|paywall|no_direct_link|rate_limited|network_error|broken_script>", "reason": "<one sentence explaining why all strategies failed>"}}
"""

_CORRECTION_PREFIX = """\
⚠️  SELF-CORRECTION ROUND {round_num} / {max_rounds}

Your previous attempt on this dataset FAILED. Study the evidence below, identify
the ROOT CAUSE, and try a fundamentally different approach. Do NOT repeat steps
that already failed.

━━━ PREVIOUS FAILURE ━━━
Failure code : {failure_code}
Failure reason: {reason}

Key log excerpt from previous attempt:
---
{log_excerpt}
---

Files currently in output_dir ({output_dir}):
{current_files}

━━━ CORRECTION CHECKLIST ━━━
Before you start, pick the applicable fix:

□ Script had import/module error       → pip3 install <pkg> first, then re-run
□ Script had path/config error         → read the script to see what it expects, provide it
□ manifest.json had no downloadUrls    → write a custom download script (see Strategy 1 ⚠️ instructions)
□ curl/wget returned HTML or 403/404   → find the real API endpoint, not the landing page
□ Playwright selectors didn't match    → take a screenshot first, read the actual DOM, use correct selectors
□ A file landed but wrong extension    → run ls -lh; if >1KB it IS a success, report it
□ Agent stopped too early              → push through all 5 strategies before giving up

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{base_task}
"""


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _check_output_dir(output_dir: str) -> list[str]:
    """Return paths of files in output_dir that look like downloaded data (>1 KB)."""
    found = []
    for root, _, files in os.walk(output_dir):
        for f in files:
            if f.startswith("."):
                continue
            full = os.path.join(root, f)
            ext = os.path.splitext(f)[1].lower()
            try:
                size = os.path.getsize(full)
            except OSError:
                continue
            if size <= 1024:
                continue
            if ext in DATA_EXTS:
                found.append(full)
            elif ext in SKIP_EXTS:
                continue  # definitely a script / doc
            else:
                # Unknown or no extension: include if large enough to be real data
                if size > 10 * 1024:  # >10 KB
                    found.append(full)
    return found


def _list_output_dir(output_dir: str) -> str:
    """Human-readable listing of output_dir for the correction prompt."""
    lines = []
    for root, _, files in os.walk(output_dir):
        for f in sorted(files):
            if f.startswith("."):
                continue
            full = os.path.join(root, f)
            try:
                size = os.path.getsize(full)
                rel = os.path.relpath(full, output_dir)
                lines.append(f"  {rel}  ({size:,} bytes)")
            except OSError:
                pass
    return "\n".join(lines) if lines else "  (empty)"


def _log_tail(log_path: str, chars: int = 4000) -> str:
    """Return the last `chars` characters of a log file, keeping only readable lines."""
    try:
        with open(log_path) as f:
            text = f.read()
        # Keep only human-readable lines written by _format_event
        lines = [l for l in text.splitlines()
                 if l.startswith(("  [text]", "  [tool]", "    →", "▶", "■", "──", "===", "["))]
        readable = "\n".join(lines)
        return readable[-chars:] if len(readable) > chars else readable
    except Exception:
        return "(log unavailable)"


def _parse_result(text: str) -> dict:
    """Extract JSON result from agent output — scan full text, not just tail."""
    text = re.sub(r"```[a-z]*\n?", "", text)
    # Prefer the last status JSON found in the text
    matches = list(re.finditer(r'\{[^{}]*"status"[^{}]*\}', text, re.DOTALL))
    for m in reversed(matches):
        try:
            return json.loads(m.group())
        except Exception:
            pass
    for line in reversed(text.splitlines()):
        if '"status"' in line:
            try:
                return json.loads(line.strip())
            except Exception:
                pass
    return {
        "status": "failure",
        "file": None,
        "failure_code": "parse_error",
        "reason": text[-500:],   # more context than before
    }


def _format_event(event: dict) -> str | None:
    etype = event.get("type", "")
    if etype == "agent_start":  return "▶ agent started"
    if etype == "turn_start":   return "── turn ──"
    if etype == "agent_end":    return f"■ agent_end ({len(event.get('messages', []))} messages)"
    if etype == "message_update":
        ae = event.get("assistantMessageEvent", {})
        if ae.get("type") == "text_delta":
            return f"  [text] {ae.get('delta', '')}"
        if ae.get("type") == "tool_start":
            return f"  [tool] {ae.get('toolName') or ae.get('partial', {}).get('name', '')}"
        if ae.get("type") == "tool_result":
            c = ae.get("content", "")
            if isinstance(c, list):
                c = " ".join(x.get("text", "") for x in c if isinstance(x, dict))
            return f"    → {str(c)[:400].replace(chr(10), ' ')}"
    return None


_MAX_STREAM_BYTES = 512 * 1024  # cap streamed text at 512 KB to prevent memory growth

def _drain(proc, log, counter: list) -> str:
    # Use a deque to cap total streamed text in memory
    streamed_chunks: deque[str] = deque()
    streamed_len = 0

    for line in proc.stdout:
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue

        ae = event.get("assistantMessageEvent", {})
        etype = event.get("type", "")

        if etype == "message_update":
            if ae.get("type") == "tool_start":
                counter[0] += 1
                log.write(f"\n[tool call #{counter[0]}]\n")
            elif ae.get("type") == "text_delta":
                delta = ae.get("delta", "")
                if streamed_len < _MAX_STREAM_BYTES:
                    streamed_chunks.append(delta)
                    streamed_len += len(delta)

        readable = _format_event(event)
        if readable:
            log.write(readable + "\n")
            log.flush()

        if etype != "agent_end":
            continue

        full = "".join(streamed_chunks)
        if '"status"' not in full:
            parts = []
            for msg in event.get("messages", []):
                if msg.get("role") == "assistant":
                    for block in msg.get("content") or []:
                        if isinstance(block, dict) and block.get("type") == "text":
                            parts.append(block["text"])
            full = full + "\n" + "\n".join(parts)
        return full

    return "".join(streamed_chunks)


# ─── Core runner ──────────────────────────────────────────────────────────────

def _run_agent(prompt: str, log, counter: list, timeout: int) -> str:
    """Spawn one pi agent session, feed `prompt`, return raw text output."""
    proc = subprocess.Popen(
        ["pi", "--mode", "rpc", "--no-session",
         "--provider", "google", "--model", "gemini-2.5-pro"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, text=True,
        env=os.environ, cwd=BASE_DIR,
    )
    timer = threading.Timer(timeout, proc.kill)
    try:
        timer.start()
        proc.stdin.write(json.dumps({"type": "prompt", "message": prompt}) + "\n")
        proc.stdin.flush()
        return _drain(proc, log, counter)
    finally:
        timer.cancel()
        proc.kill()
        proc.wait()


# ─── Public API ───────────────────────────────────────────────────────────────

def download_dataset(dataset_id: str, repo_folder: str, source_url: str,
                     timeout: int = ROUND_TIMEOUT) -> dict:
    os.makedirs(LOGS_DIR, exist_ok=True)
    os.makedirs(DATASETS_DIR, exist_ok=True)

    slug = re.sub(r"[^a-z0-9]+", "_", dataset_id.lower())[:50]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(LOGS_DIR, f"phase23_{slug}_{ts}.log")

    output_dir = os.path.join(DATASETS_DIR, slug)   # ← inside datasets/
    os.makedirs(output_dir, exist_ok=True)

    base_task = _BASE_TASK.format(
        dataset_id=dataset_id,
        repo_folder=repo_folder or "unknown",
        source_url=source_url,
        output_dir=output_dir,
        slug=slug,
    )

    result = {}
    counter = [0]

    with open(log_path, "w") as log:
        log.write(
            f"=== phase 2+3 log ===\n"
            f"dataset_id : {dataset_id}\n"
            f"repo_folder: {repo_folder}\n"
            f"source_url : {source_url}\n"
            f"output_dir : {output_dir}\n"
            f"max_rounds : {MAX_CORRECTION_ROUNDS}\n"
            f"started    : {datetime.now().isoformat()}\n"
            f"{'='*40}\n\n"
        )

        for round_num in range(1, MAX_CORRECTION_ROUNDS + 1):
            log.write(f"\n{'='*40}\n[ROUND {round_num}/{MAX_CORRECTION_ROUNDS}] "
                      f"started {datetime.now().isoformat()}\n{'='*40}\n\n")

            # Build prompt: first round = base task, subsequent = correction prefix + base task
            if round_num == 1:
                prompt = base_task
            else:
                prev = result  # result from the previous round
                prompt = _CORRECTION_PREFIX.format(
                    round_num=round_num,
                    max_rounds=MAX_CORRECTION_ROUNDS,
                    failure_code=prev.get("failure_code", "unknown"),
                    reason=prev.get("reason", "unknown"),
                    log_excerpt=_log_tail(log_path),
                    output_dir=output_dir,
                    current_files=_list_output_dir(output_dir),
                    base_task=base_task,
                )

            final_text = _run_agent(prompt, log, counter, timeout)
            result = _parse_result(final_text)

            log.write(f"\n[round {round_num} agent result] {json.dumps(result)}\n")

            # ── Disk override: trust the filesystem over the agent's claim ──
            downloaded = _check_output_dir(output_dir)
            if downloaded:
                result = {
                    "status": "success",
                    "file": downloaded[0],
                    "all_files": downloaded,
                    "strategy": result.get("strategy", f"round_{round_num}"),
                    "rounds_taken": round_num,
                }
                log.write(f"[disk-check] ✅ found {len(downloaded)} file(s) — overriding to success\n")
                break  # done

            if result.get("status") == "success":
                # Agent claimed success but nothing on disk
                result = {
                    "status": "failure",
                    "file": None,
                    "failure_code": "no_file_downloaded",
                    "reason": "agent claimed success but no data file >1KB found on disk",
                    "rounds_taken": round_num,
                }
                log.write("[disk-check] ❌ agent said success but disk is empty — continuing\n")
                # Don't break — try another correction round
            else:
                result["rounds_taken"] = round_num

            if round_num < MAX_CORRECTION_ROUNDS:
                log.write(f"[feedback-loop] ⟳ round {round_num} failed — spawning correction round {round_num+1}\n")
            else:
                log.write(f"[feedback-loop] ✗ all {MAX_CORRECTION_ROUNDS} rounds exhausted\n")

            gc.collect()  # release memory between rounds

        log.write(
            f"\n{'='*40}\n"
            f"final result: {json.dumps(result)}\n"
            f"ended       : {datetime.now().isoformat()}\n"
        )

    print(f"  [log] {log_path}")
    return {**result, "output_dir": output_dir, "dataset_id": dataset_id}
