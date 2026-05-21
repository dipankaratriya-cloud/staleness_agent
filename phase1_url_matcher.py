"""Phase 1: pi agent matches a source URL to a datacommonsorg/data repo folder."""

import json
import os
import re
import subprocess
import threading
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs")

# ── prompts ────────────────────────────────────────────────────────────────────

_PROMPT_ATTEMPT1 = """\
Source URL : {url}
Dataset ID : {dataset_id}

Env vars available in every bash call:
  $GITHUB_TOKEN  — GitHub API access
  $GROQ_API_KEY  — Groq compound-beta (JS page rendering)

TASK: Find the single best-matching folder in the datacommonsorg/data GitHub repo
      (under statvar_imports/ OR scripts/) for the dataset at this URL.

━━━ STEP 1 — fetch page and extract dataset name ━━━
  curl -sL --max-time 15 "{url}" | python3 -c "
import sys, re
html = sys.stdin.read()
for pat in [r'<title[^>]*>(.*?)</title>', r'<h1[^>]*>(.*?)</h1>',
            r'og:title.*?content=.([^\"]+)', r'\"name\"\\s*:\\s*\"([^\"]+)\"']:
    m = re.search(pat, html, re.I|re.S)
    if m: print('NAME:', m.group(1).strip()); break
else: print('THIN_HTML len=', len(html))
"

━━━ STEP 2 — if THIN_HTML, use Groq compound-beta to render the page ━━━
  python3 -c "
import os
from groq import Groq
r = Groq(api_key=os.environ['GROQ_API_KEY']).chat.completions.create(
    model='compound-beta',
    messages=[{{'role':'user','content':'Visit {url} and return ONLY the dataset/page title'}}]
)
print(r.choices[0].message.content.strip())
"

━━━ STEP 3 — list ALL repo folders at every depth (recursive tree) ━━━
  curl -s -H "Authorization: token $GITHUB_TOKEN" \
    "https://api.github.com/repos/datacommonsorg/data/git/trees/HEAD?recursive=1" \
    | python3 -c "
import json,sys
data=json.load(sys.stdin)
for x in data.get('tree',[]):
    if x['type']=='tree' and (x['path'].startswith('statvar_imports/') or x['path'].startswith('scripts/')):
        print(x['path'])
"

━━━ STEP 4 — fuzzy-match name against ALL folder paths ━━━
Score each path against: extracted page title, dataset_id tokens, URL path tokens.
If ambiguous, read README of top 2-3 candidates:
  curl -s -H "Authorization: token $GITHUB_TOKEN" \
    "https://api.github.com/repos/datacommonsorg/data/contents/<path>/README.md" \
    | python3 -c "import json,sys,base64; d=json.load(sys.stdin); print(base64.b64decode(d.get('content','')).decode()[:400])"

Output ONLY this JSON on one line (no markdown, no extra text):
{{"matched_folder": "statvar_imports/x", "confidence": "high", "dataset_name": "name", "reason": "why"}}
If no match: {{"matched_folder": null, "confidence": "none", "dataset_name": "best guess", "reason": "why"}}
"""

_PROMPT_ATTEMPT2 = """\
Your previous attempt returned no confident match.
Try these additional strategies now:

━━━ STRATEGY A — use GitHub Search API (searches file contents) ━━━
Extract 2-3 keywords from the dataset ID: {dataset_id}
Then search code in the repo:
  curl -s -H "Authorization: token $GITHUB_TOKEN" \
    "https://api.github.com/search/code?q=<keyword>+repo:datacommonsorg/data" \
    | python3 -c "import json,sys; [print(x['path']) for x in json.load(sys.stdin).get('items',[])]"

Try each keyword separately. Look at which files/folders appear repeatedly.

━━━ STRATEGY B — search for the URL domain in repo files ━━━
Extract the domain from {url} (e.g. 'eurostat', 'census', 'who', 'worldbank'):
  curl -s -H "Authorization: token $GITHUB_TOKEN" \
    "https://api.github.com/search/code?q=<domain>+repo:datacommonsorg/data+filename:manifest.json" \
    | python3 -c "import json,sys; [print(x['path']) for x in json.load(sys.stdin).get('items',[])]"

━━━ STRATEGY C — direct dataset_id token matching ━━━
Strip dc/base/ from the dataset_id, split on underscores/capitals, try each token:
Dataset ID tokens: {dataset_id_tokens}
Search the full repo tree for any path containing these tokens.

Output ONLY this JSON on one line:
{{"matched_folder": "statvar_imports/x", "confidence": "high", "dataset_name": "name", "reason": "why"}}
If still no match: {{"matched_folder": null, "confidence": "none", "dataset_name": "best guess", "reason": "why"}}
"""

_PROMPT_ATTEMPT3 = """\
Still no match. Final attempt — be more aggressive:

━━━ STRATEGY D — search manifest.json files for the source URL ━━━
  curl -s -H "Authorization: token $GITHUB_TOKEN" \
    "https://api.github.com/search/code?q={url_domain}+repo:datacommonsorg/data" \
    | python3 -c "
import json,sys
items = json.load(sys.stdin).get('items',[])
for x in items: print(x['repository']['full_name'], x['path'])
"
Then read those files to confirm the source URL matches.

━━━ STRATEGY E — broad keyword scan ━━━
Try the shortest/most distinctive word from the dataset name or URL.
Accept a MEDIUM confidence match if the folder topic is clearly related,
even if the folder name doesn't exactly match.

━━━ STRATEGY F — infer from dataset_id structure ━━━
Dataset IDs like CensusACS5YearSurvey_SubjectTables_S1901 → look for census/acs folders.
Dataset IDs like WikidataGeo_France → look for geo/wikidata folders.
Use the prefix pattern to navigate the repo structure.

If you find ANY plausible folder, return it with confidence "low".
Only return matched_folder=null if there is truly NOTHING related in the entire repo.

Output ONLY this JSON on one line:
{{"matched_folder": "statvar_imports/x", "confidence": "low", "dataset_name": "name", "reason": "why"}}
If truly nothing: {{"matched_folder": null, "confidence": "none", "dataset_name": "best guess", "reason": "exhausted all strategies"}}
"""


# ── helpers ───────────────────────────────────────────────────────────────────

def _dataset_id_tokens(dataset_id: str) -> str:
    """Split dc/base/CensusACS5Year → census, acs, year, etc."""
    raw = re.sub(r"^dc/base/", "", dataset_id)
    tokens = re.sub(r"([A-Z])", r" \1", raw).lower()
    tokens = re.sub(r"[_\-]+", " ", tokens)
    return " ".join(dict.fromkeys(tokens.split()))  # deduplicated


def _url_domain(url: str) -> str:
    m = re.search(r"https?://(?:www\.)?([^/]+)", url)
    return m.group(1).split(".")[0] if m else ""


def _parse_result(text: str) -> dict:
    text = re.sub(r"```[a-z]*\n?", "", text)
    # multi-line JSON block
    for m in re.finditer(r'\{[^{}]*"matched_folder"[^{}]*\}', text, re.DOTALL):
        try:
            return json.loads(m.group())
        except Exception:
            pass
    # line-by-line fallback
    for line in reversed(text.splitlines()):
        if '"matched_folder"' in line:
            try:
                return json.loads(line.strip())
            except Exception:
                pass
    return {"matched_folder": None, "confidence": "none", "dataset_name": "", "reason": "parse_error"}


def _format_event(event: dict) -> str | None:
    etype = event.get("type", "")
    if etype == "agent_start":  return "▶ agent started"
    if etype == "turn_start":   return "── turn ──"
    if etype == "agent_end":    return f"■ agent_end ({len(event.get('messages', []))} messages)"
    if etype == "message_update":
        ae = event.get("assistantMessageEvent", {})
        if ae.get("type") == "text_delta": return f"  [text] {ae.get('delta', '')}"
        if ae.get("type") == "tool_start":
            return f"  [tool] {ae.get('toolName') or ae.get('partial',{}).get('name','')}"
        if ae.get("type") == "tool_result":
            c = ae.get("content", "")
            if isinstance(c, list): c = " ".join(x.get("text","") for x in c if isinstance(x,dict))
            return f"    → {str(c)[:200].replace(chr(10),' ')}"
    return None


def _wait_for_agent_end(proc, log, counter: list) -> str:
    streamed_text = []
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
                log.write(f"\n[tool #{counter[0]}]\n")
            elif ae.get("type") == "text_delta":
                streamed_text.append(ae.get("delta", ""))

        readable = _format_event(event)
        if readable:
            log.write(readable + "\n")
            log.flush()

        if etype != "agent_end":
            continue

        full = "".join(streamed_text)
        if '"matched_folder"' not in full:
            parts = []
            for msg in event.get("messages", []):
                if msg.get("role") == "assistant":
                    for block in msg.get("content") or []:
                        if isinstance(block, dict) and block.get("type") == "text":
                            parts.append(block["text"])
            full = full + "\n" + "\n".join(parts)
        return full

    return "".join(streamed_text)


# ── main function ─────────────────────────────────────────────────────────────

def match_url(url: str, dataset_id: str = "", timeout: int = 180) -> dict:
    os.makedirs(LOGS_DIR, exist_ok=True)
    slug = re.sub(r"[^a-z0-9]+", "_", dataset_id.lower())[:40] or "unknown"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(LOGS_DIR, f"phase1_{slug}_{ts}.log")

    tokens = _dataset_id_tokens(dataset_id)
    domain = _url_domain(url)

    prompts = [
        _PROMPT_ATTEMPT1.format(url=url, dataset_id=dataset_id),
        _PROMPT_ATTEMPT2.format(url=url, dataset_id=dataset_id, dataset_id_tokens=tokens),
        _PROMPT_ATTEMPT3.format(url_domain=domain),
    ]

    api_key = os.environ.get("GEMINI_API_KEY", "")
    proc = subprocess.Popen(
        ["pi", "--mode", "rpc", "--no-session",
         "--provider", "google", "--model", "gemini-2.5-pro",
         "--api-key", api_key],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, text=True,
        env=os.environ, cwd=os.path.dirname(__file__),
    )

    counter = [0]
    result = {}

    with open(log_path, "w") as log:
        log.write(f"=== phase 1 log ===\ndataset: {dataset_id}\nurl: {url}\n")
        log.write(f"tokens: {tokens}\nstarted: {datetime.now().isoformat()}\n{'='*40}\n\n")

        timer = threading.Timer(timeout, proc.kill)
        try:
            timer.start()

            for attempt, prompt in enumerate(prompts, 1):
                log.write(f"\n{'─'*40}\n[attempt {attempt}/3]\n{'─'*40}\n")
                log.flush()

                proc.stdin.write(json.dumps({"type": "prompt", "message": prompt}) + "\n")
                proc.stdin.flush()

                final_text = _wait_for_agent_end(proc, log, counter)
                result = _parse_result(final_text)

                log.write(f"\n→ attempt {attempt}: matched={result.get('matched_folder')} conf={result.get('confidence')}\n")
                log.flush()

                # stop if we got any match (even low confidence)
                if result.get("matched_folder"):
                    break

                if attempt < len(prompts):
                    log.write("  no match — escalating to next strategy set\n")

        finally:
            timer.cancel()
            proc.kill()
            proc.wait()

        log.write(f"\n{'='*40}\nfinal: {json.dumps(result)}\nended: {datetime.now().isoformat()}\n")

    print(f"  [log] {log_path}")
    return result
