"""
Provenance URL → Last Refresh Date extractor.

5-tier pipeline per URL:
  Tier 1 : HTTP HEAD  → Last-Modified header
  Tier 2 : GET + HTML → JSON-LD / OpenGraph / Dublin-Core / regex
  Tier 3 : Gemini Flash on page text (NLP fallback)
  Tier 4 : Playwright full render (JS-heavy sites)
  Tier 5 : Groq compound-beta real browser (bot-blocked / JS-wall sites)

Run:
  python3 provenance_refresh_extractor.py
  python3 provenance_refresh_extractor.py --resume       # retry misses from last run
  python3 provenance_refresh_extractor.py --tier-max 2   # skip Gemini + Playwright + Groq
"""

import asyncio
import json
import os
import re
import sys
import csv
from collections import defaultdict
from datetime import datetime
from urllib.parse import urlparse

import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

import google.generativeai as genai
genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))

from groq import Groq as _Groq
_groq_client = _Groq(api_key=os.environ.get("GROQ_API_KEY", ""))

# ── config ────────────────────────────────────────────────────────────────────

INPUT_CSV    = os.path.join(os.path.dirname(__file__), "Provenance.csv")
OUTPUT_JSON  = os.path.join(os.path.dirname(__file__), "provenance_refresh_dates.json")
# overridden by --csv / --output CLI args
TIER_MAX     = int(os.environ.get("TIER_MAX", "5"))
GROQ_CONCURRENCY = 3   # compound-beta rate limit is ~30 RPM; 3 concurrent is safe
CONCURRENCY  = 20          # global async workers
DOMAIN_LIMIT = 2           # max concurrent requests per domain
REQUEST_TO   = 10          # HTTP timeout (seconds)
HEADERS      = {"User-Agent": "Mozilla/5.0 (compatible; StalenesBot/1.0)"}

_DATE_RE = re.compile(
    r'\b(?:last\s+(?:updated?|modified|refreshed?|revised?)|data\s+as\s+of|updated?|as\s+of)[:\s]+([A-Za-z0-9,\s/-]{4,30})',
    re.IGNORECASE,
)
_YEAR_RE = re.compile(r'\b(20[012]\d|19\d{2})\b')

# ── helpers ───────────────────────────────────────────────────────────────────

def _parse_date(val: str | None) -> str | None:
    if not val:
        return None
    val = val.strip().rstrip("Z").replace("T", " ")[:10]
    # keep if it looks like a date or year
    if re.match(r'^\d{4}(-\d{2}){0,2}', val):
        return val
    return None


def _first_year(text: str) -> str | None:
    m = _YEAR_RE.search(text)
    return m.group(1) if m else None


def _extract_from_html(html: str, url: str) -> tuple[str | None, str]:
    """Return (date_string, source_label) from raw HTML."""
    soup = BeautifulSoup(html, "html.parser")

    # JSON-LD
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
            for key in ("dateModified", "datePublished", "dateCreated"):
                val = _parse_date(data.get(key) or (data.get("@graph") or [{}])[0].get(key))
                if val:
                    return val, f"json-ld:{key}"
        except Exception:
            pass

    # OpenGraph / article meta
    for prop in ("article:modified_time", "og:updated_time", "article:published_time"):
        tag = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
        if tag:
            val = _parse_date(tag.get("content"))
            if val:
                return val, f"meta:{prop}"

    # Dublin Core / DCAT
    for name in ("dcterms.modified", "dc.date", "dcterms.date", "DC.date"):
        tag = soup.find("meta", attrs={"name": re.compile(name, re.I)})
        if tag:
            val = _parse_date(tag.get("content"))
            if val:
                return val, f"meta:{name}"

    # Visible "last updated" text
    body_text = soup.get_text(" ", strip=True)[:5000]
    m = _DATE_RE.search(body_text)
    if m:
        yr = _first_year(m.group(1))
        if yr:
            return yr, "body-text"

    return None, ""


# ── tier implementations ──────────────────────────────────────────────────────

async def _tier1(session: aiohttp.ClientSession, url: str) -> dict | None:
    try:
        async with session.head(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TO),
                                 allow_redirects=True, headers=HEADERS) as r:
            lm = r.headers.get("Last-Modified") or r.headers.get("X-Last-Modified")
            if lm:
                val = _parse_date(lm)
                if not val:
                    val = _first_year(lm)
                if val:
                    return {"date": val, "source": "Last-Modified header", "tier": 1}
    except Exception:
        pass
    return None


async def _tier2(session: aiohttp.ClientSession, url: str) -> dict | None:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TO),
                                allow_redirects=True, headers=HEADERS) as r:
            if r.content_type and "html" not in r.content_type and "text" not in r.content_type:
                return None
            html = await r.text(errors="replace")
            date, src = _extract_from_html(html, url)
            if date:
                return {"date": date, "source": src, "tier": 2, "_html": html}
            return {"date": None, "source": "", "tier": 2, "_html": html}  # pass html to tier3
    except Exception:
        pass
    return None


def _tier3_sync(page_text: str, url: str) -> dict | None:
    try:
        model = genai.GenerativeModel("gemini-2.0-flash-lite")
        prompt = (
            f"URL: {url}\n"
            f"Page text (first 3000 chars):\n{page_text[:3000]}\n\n"
            "What is the LAST REFRESH / LAST UPDATED date for the data on this page?\n"
            "Return ONLY valid JSON: "
            '{"date": "YYYY-MM-DD or YYYY or null", "source": "where you found it"}\n'
            "No markdown, no explanation."
        )
        resp = model.generate_content(prompt)
        raw = resp.text.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        data = json.loads(raw)
        val = _parse_date(str(data.get("date") or "")) or _first_year(str(data.get("date") or ""))
        if val:
            return {"date": val, "source": data.get("source", "gemini"), "tier": 3}
    except Exception:
        pass
    return None


async def _tier4(url: str) -> dict | None:
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(2000)
            html = await page.content()
            await browser.close()

        date, src = _extract_from_html(html, url)
        if date:
            return {"date": date, "source": src + " (playwright)", "tier": 4}

        # Playwright page text → Gemini
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
        result = _tier3_sync(text, url)
        if result:
            result["tier"] = 4
            result["source"] += " (playwright+gemini)"
            return result
    except Exception:
        pass
    return None


def _tier5_sync(url: str) -> dict | None:
    """Groq compound-beta: real browser visit — handles bot walls and JS-heavy sites."""
    try:
        resp = _groq_client.chat.completions.create(
            model="compound-beta",
            messages=[{"role": "user", "content": (
                f"Visit this URL: {url}\n"
                "Find the LAST REFRESH / LAST UPDATED / DATA AS OF date shown anywhere on the page "
                "(in the header, footer, sidebar, table caption, or metadata).\n"
                "Return ONLY valid JSON with no markdown: "
                '{"date": "YYYY-MM-DD or YYYY or null", "source": "exact text or element where you found it"}'
            )}],
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?|```$", "", raw, flags=re.MULTILINE).strip()
        data = json.loads(raw)
        val = _parse_date(str(data.get("date") or "")) or _first_year(str(data.get("date") or ""))
        if val:
            return {"date": val, "source": data.get("source", "groq"), "tier": 5}
    except Exception:
        pass
    return None


# ── per-URL orchestrator ──────────────────────────────────────────────────────

_domain_sems: dict[str, asyncio.Semaphore] = defaultdict(lambda: asyncio.Semaphore(DOMAIN_LIMIT))
_groq_sem: asyncio.Semaphore | None = None   # initialised in main()

async def process_url(session: aiohttp.ClientSession, url: str,
                       global_sem: asyncio.Semaphore, tier_max: int) -> dict:
    domain = urlparse(url).netloc
    async with global_sem, _domain_sems[domain]:
        result = {"url": url, "date": None, "source": None, "tier": None, "error": None}

        # Tier 1
        r = await _tier1(session, url)
        if r:
            result.update(r)
            return result

        if tier_max < 2:
            return result

        # Tier 2
        r = await _tier2(session, url)
        if r:
            html_cache = r.pop("_html", None)
            if r["date"]:
                result.update(r)
                return result
        else:
            html_cache = None

        if tier_max < 3:
            return result

        # Tier 3 — use cached HTML text if available
        if html_cache:
            soup = BeautifulSoup(html_cache, "html.parser")
            page_text = soup.get_text(" ", strip=True)
            r3 = await asyncio.get_event_loop().run_in_executor(
                None, _tier3_sync, page_text, url
            )
            if r3:
                result.update(r3)
                return result

        if tier_max < 4:
            return result

        # Tier 4 — Playwright
        r4 = await _tier4(url)
        if r4:
            result.update(r4)
            return result

        if tier_max < 5:
            return result

        # Tier 5 — Groq compound-beta (real browser, handles bot walls)
        async with _groq_sem:
            r5 = await asyncio.get_event_loop().run_in_executor(None, _tier5_sync, url)
        if r5:
            result.update(r5)

        return result


# ── main ─────────────────────────────────────────────────────────────────────

def load_urls(csv_path: str) -> dict[str, list[str]]:
    """Returns {url: [dataset_id, ...]} deduplicated."""
    url_map: dict[str, list[str]] = defaultdict(list)
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            url = row.get("provenance_url", "").strip().strip('"')
            did = row.get("id", "").strip()
            if url and url.startswith("http"):
                url_map[url].append(did)
    return url_map


async def main(tier_max: int, resume: bool, input_csv: str = None, output_json: str = None):
    global _groq_sem
    _groq_sem = asyncio.Semaphore(GROQ_CONCURRENCY)
    csv_path = input_csv or INPUT_CSV
    out_path = output_json or OUTPUT_JSON
    url_map = load_urls(csv_path)
    all_urls = list(url_map.keys())
    print(f"Loaded {len(all_urls)} unique URLs ({sum(len(v) for v in url_map.values())} total rows)")

    # Load existing results if resuming
    existing: dict = {}
    if resume and os.path.exists(out_path):
        with open(out_path) as f:
            existing = json.load(f)
        done_urls = {v["url"] for v in existing.values() if v.get("date")}
        all_urls = [u for u in all_urls if u not in done_urls]
        print(f"Resuming — {len(all_urls)} URLs remaining")

    global_sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(ssl=False, limit=CONCURRENCY)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [process_url(session, url, global_sem, tier_max) for url in all_urls]
        results_list = []
        done = 0
        for coro in asyncio.as_completed(tasks):
            r = await coro
            results_list.append(r)
            done += 1
            tier_label = f"T{r['tier']}" if r["tier"] else "miss"
            print(f"  [{done}/{len(all_urls)}] {tier_label}  {r['date'] or '—':<12}  {r['url'][:70]}")

    # Build output keyed by dataset_id
    output = dict(existing)
    url_results = {r["url"]: r for r in results_list}
    for url, dataset_ids in url_map.items():
        r = url_results.get(url)
        if r is None:
            continue  # was already in existing
        for did in dataset_ids:
            output[did] = {
                "url": url,
                "last_refresh_date": r["date"],
                "date_source": r["source"],
                "tier_used": r["tier"],
            }

    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    # Summary
    found    = sum(1 for v in output.values() if v.get("last_refresh_date"))
    by_tier  = defaultdict(int)
    for v in output.values():
        by_tier[v.get("tier_used")] += 1

    print(f"\n{'─'*50}")
    print(f"Results saved → {out_path}")
    print(f"Found date : {found}/{len(output)} datasets ({found*100//max(len(output),1)}%)")
    for t in sorted(k for k in by_tier if k):
        print(f"  Tier {t}    : {by_tier[t]}")
    print(f"  No date  : {by_tier[None]}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv",       default=INPUT_CSV,   help="Provenance CSV to read")
    ap.add_argument("--output",    default=OUTPUT_JSON, help="Output JSON path")
    ap.add_argument("--tier-max",  type=int, default=TIER_MAX)
    ap.add_argument("--resume",    action="store_true")
    args = ap.parse_args()
    asyncio.run(main(tier_max=args.tier_max, resume=args.resume,
                     input_csv=args.csv, output_json=args.output))
