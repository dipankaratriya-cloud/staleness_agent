"""Phase 1: direct Gemini API matcher — replaces pi agent for URL→folder matching.

Fetches the datacommonsorg/data GitHub repo tree ONCE (shared across all workers),
then uses gemini-2.0-flash to classify each URL to its best matching folder.
Drop-in replacement for phase1_url_matcher.match_url().
"""

import json
import os
import re
import threading
import urllib.request

import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

GEMINI_MODEL  = "gemini-2.0-flash"
MAX_DEPTH     = 3    # only keep folders up to 3 levels deep
TOP_CANDIDATES = 40  # send only the best N pre-filtered folders to Gemini

_repo_tree: list[str] | None = None
_repo_lock = threading.Lock()


# ── repo tree (fetched once, reused by all workers) ───────────────────────────

def _get_repo_tree() -> list[str]:
    global _repo_tree
    with _repo_lock:
        if _repo_tree is not None:
            return _repo_tree
        token = os.environ.get("GITHUB_TOKEN", "")
        hdrs  = {"Authorization": f"token {token}"} if token else {}
        url   = "https://api.github.com/repos/datacommonsorg/data/git/trees/HEAD?recursive=1"
        req   = urllib.request.Request(url, headers=hdrs)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        folders = [
            x["path"] for x in data.get("tree", [])
            if x["type"] == "tree"
            and (x["path"].startswith("statvar_imports/") or x["path"].startswith("scripts/"))
            and x["path"].count("/") <= MAX_DEPTH
        ]
        _repo_tree = folders
        print(f"  [phase1-gemini] repo tree loaded: {len(folders)} folders")
        return _repo_tree


# ── candidate pre-filter (token matching before Gemini) ──────────────────────

def _candidate_folders(all_folders: list[str], dataset_id: str, url: str) -> list[str]:
    """Return top TOP_CANDIDATES folders ranked by token overlap with dataset_id/url.
    Falls back to full list if nothing matches."""
    # Extract searchable tokens from dataset_id and URL
    raw  = re.sub(r"^dc/base/", "", dataset_id)
    raw += " " + re.sub(r"https?://[^/]+", "", url).replace("/", " ")
    tokens = set(re.sub(r"([A-Z])", r" \1", raw).lower().split())
    tokens = {t for t in tokens if len(t) > 2}  # drop tiny tokens

    scored = []
    for folder in all_folders:
        folder_lower = folder.lower()
        score = sum(1 for t in tokens if t in folder_lower)
        scored.append((score, folder))

    scored.sort(key=lambda x: -x[0])
    top = [f for _, f in scored if _ > 0][:TOP_CANDIDATES]
    # Always include at least the full list if nothing scored
    return top if top else all_folders[:TOP_CANDIDATES]


# ── page title ────────────────────────────────────────────────────────────────

def _fetch_title(url: str) -> str:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read(8000).decode("utf-8", errors="ignore")
        for pat in [r"<title[^>]*>(.*?)</title>", r"<h1[^>]*>(.*?)</h1>"]:
            m = re.search(pat, html, re.I | re.S)
            if m:
                return re.sub(r"<[^>]+>", "", m.group(1)).strip()[:200]
    except Exception:
        pass
    return ""


# ── prompt ────────────────────────────────────────────────────────────────────

_PROMPT = """\
Match this dataset to its best folder in the datacommonsorg/data GitHub repo.

Dataset ID : {dataset_id}
Source URL : {url}
Page title : {title}

Available folders (scripts/ and statvar_imports/, max 3 levels deep):
{folders}

Pick the single best matching folder. Use dataset_id tokens, URL domain, and page title.
Examples: dc/base/CDC500 → scripts/us_cdc/500_places
          dc/base/CensusACS5YearSurvey → scripts/us_census/acs

Respond with ONLY this JSON on one line, no markdown:
{{"matched_folder": "scripts/...", "confidence": "high|medium|low", "dataset_name": "{title}", "reason": "one sentence"}}
If nothing matches: {{"matched_folder": null, "confidence": "none", "dataset_name": "{title}", "reason": "why"}}
"""


# ── JSON parser (shared pattern) ──────────────────────────────────────────────

def _parse(text: str) -> dict:
    text = re.sub(r"```[a-z]*\n?", "", text)
    for m in re.finditer(r'\{[^{}]*"matched_folder"[^{}]*\}', text, re.DOTALL):
        try:
            return json.loads(m.group())
        except Exception:
            pass
    for line in reversed(text.splitlines()):
        if '"matched_folder"' in line:
            try:
                return json.loads(line.strip())
            except Exception:
                pass
    return {"matched_folder": None, "confidence": "none",
            "dataset_name": "", "reason": "parse_error"}


# ── public API ────────────────────────────────────────────────────────────────

def match_url(url: str, dataset_id: str = "", timeout: int = 60) -> dict:
    """Match a URL to a repo folder using Gemini. Drop-in for phase1_url_matcher."""
    import time

    all_folders = _get_repo_tree()
    candidates  = _candidate_folders(all_folders, dataset_id, url)
    title       = _fetch_title(url)
    api_key     = os.environ.get("GEMINI_API_KEY", "")

    genai.configure(api_key=api_key)
    model  = genai.GenerativeModel(GEMINI_MODEL)
    prompt = _PROMPT.format(
        dataset_id=dataset_id,
        url=url,
        title=title or url,
        folders="\n".join(candidates),
    )

    # Retry up to 4 times with exponential backoff on 429 rate limit
    for attempt in range(4):
        try:
            resp = model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(temperature=0.1, max_output_tokens=300),
            )
            return _parse(resp.text.strip())
        except Exception as e:
            if "429" in str(e) and attempt < 3:
                wait = 10 * (2 ** attempt)   # 10s, 20s, 40s
                print(f"  [phase1-gemini] rate-limited, retrying in {wait}s ({dataset_id})")
                time.sleep(wait)
            else:
                print(f"  [phase1-gemini] error for {dataset_id}: {e}")
                return {"matched_folder": None, "confidence": "none",
                        "dataset_name": "", "reason": str(e)}

    return {"matched_folder": None, "confidence": "none",
            "dataset_name": "", "reason": "rate limit exhausted"}
