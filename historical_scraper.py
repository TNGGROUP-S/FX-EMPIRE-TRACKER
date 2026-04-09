"""
FX Empire Historical Article Scraper — Playwright Edition v7
Phase 1: Scrape author pages (?page=1 to ?page=10)
Phase 2: Use Google search to find older articles beyond page 10
"""

print("🚀 PLAYWRIGHT VERSION v9 RUNNING")

import asyncio
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import json
import os
import time
import requests
from bs4 import BeautifulSoup
import re

# ── CONFIG ───────────────────────────────────────────────────────────────────
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "YOUR_SPREADSHEET_ID_HERE")
CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")

AUTHORS = {
    "Christopher Lewis":  ("fx-empire-analyst-christopher-lewis", "Christopher Lewis"),
    "James Hyerczyk":     ("jameshyerczyk", "James Hyerczyk"),
    "Arslan Ali":         ("arslanali", "Arslan Ali"),
    "Bruce Powers":       ("brucepowers", "Bruce Powers"),
    "Muhammad Umair":     ("muhammadumair", "Muhammad Umair"),
    "Vladimir Zernov":    ("vladimirzernov", "Vladimir Zernov"),
}

TARGET_KEYWORDS = {
    "gold", "xau", "xauusd", "xau/usd",
    "precious metal", "precious metals",
    "bullion", "yellow metal",
    "commodity", "commodities", "metals market",
    "usd", "dollar", "us dollar", "u.s. dollar",
    "greenback", "dxy",
    "yield", "yields",
    "treasury", "treasuries",
    "bond yield", "bond yields",
    "10-year", "10 year",
    "real yield", "real yields",
    "rates", "rate hike", "interest rate", "interest-rate",
    "safe-haven", "safe haven",
    "risk-off", "risk off",
    "inflation hedge",
}

SHEET_HEADERS = [
    "Title", "Author", "Date Published", "URL",
    "Word Count", "Date Scraped"
]

TRAINING_FILE = "historical_articles.json"

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

BASE_URL = "https://www.fxempire.com"

# ── Google search query templates per author ──────────────────────────────────
# We search Google for old articles since FX Empire caps at 10 pages
GOOGLE_SEARCH_QUERIES = {
    "Christopher Lewis": [
        '"Christopher Lewis" gold xau',
        '"Christopher Lewis" dollar dxy greenback',
        '"Christopher Lewis" treasury yield rates',
        '"Christopher Lewis" safe-haven risk-off inflation',
        '"Christopher Lewis" bullion precious metal',
    ],
    "James Hyerczyk": [
        '"James Hyerczyk" gold xau',
        '"James Hyerczyk" dollar dxy greenback',
        '"James Hyerczyk" treasury yield rates',
        '"James Hyerczyk" safe-haven risk-off inflation',
        '"James Hyerczyk" bullion precious metal',
    ],
    "Arslan Ali": [
        '"Arslan Ali" gold xau',
        '"Arslan Ali" dollar dxy greenback',
        '"Arslan Ali" treasury yield rates',
        '"Arslan Ali" safe-haven risk-off inflation',
        '"Arslan Ali" bullion precious metal',
    ],
    "Bruce Powers": [
        '"Bruce Powers" gold xau',
        '"Bruce Powers" dollar dxy greenback',
        '"Bruce Powers" treasury yield rates',
        '"Bruce Powers" safe-haven risk-off inflation',
        '"Bruce Powers" bullion precious metal',
    ],
    "Muhammad Umair": [
        '"Muhammad Umair" gold xau',
        '"Muhammad Umair" dollar dxy greenback',
        '"Muhammad Umair" treasury yield rates',
        '"Muhammad Umair" safe-haven risk-off inflation',
        '"Muhammad Umair" bullion precious metal',
    ],
    "Vladimir Zernov": [
        '"Vladimir Zernov" gold xau',
        '"Vladimir Zernov" dollar dxy greenback',
        '"Vladimir Zernov" treasury yield rates',
        '"Vladimir Zernov" safe-haven risk-off inflation',
        '"Vladimir Zernov" bullion precious metal',
    ],
}

# ─────────────────────────────────────────────────────────────────────────────

def get_google_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_dict = json.loads(CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        sheet = spreadsheet.worksheet("Historical")
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title="Historical", rows=20000, cols=10)
    return sheet


def ensure_headers(sheet):
    all_rows = sheet.get_all_values()
    data_rows = [row for row in all_rows if row != SHEET_HEADERS and any(row)]
    sheet.clear()
    sheet.insert_row(SHEET_HEADERS, 1)
    sheet.format("A1:F1", {
        "textFormat": {"bold": True},
        "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
    })
    if data_rows:
        sheet.append_rows(data_rows, value_input_option="USER_ENTERED")


def get_existing_urls(sheet):
    try:
        return set(sheet.col_values(4)[1:])
    except Exception:
        return set()


def keyword_match(text):
    text_lower = text.lower()
    return any(kw in text_lower for kw in TARGET_KEYWORDS)


def fetch_article_data(url):
    """Fetch title, real publish date, and clean plain text body."""
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Title
        title = ""
        h1 = soup.select_one("h1")
        if h1:
            title = h1.get_text(strip=True)

        # Real publish date
        date = ""
        for prop in ["article:published_time", "datePublished", "article:modified_time"]:
            meta = soup.find("meta", {"property": prop}) or soup.find("meta", {"name": prop})
            if meta and meta.get("content"):
                date = meta["content"][:10]
                break
        if not date:
            time_el = soup.select_one("time[datetime]")
            if time_el:
                date = time_el["datetime"][:10]
        if not date:
            next_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if next_tag and next_tag.string:
                try:
                    date = find_date_in_json(json.loads(next_tag.string))
                except Exception:
                    pass

        # Clean plain text body
        body_text = ""
        next_tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if next_tag and next_tag.string:
            try:
                raw = dig_for_body(json.loads(next_tag.string))
                if raw and len(raw) > 200:
                    inner_soup = BeautifulSoup(raw, "html.parser")
                    for tag in inner_soup.select("script,style,figure,[class*='ad'],ins"):
                        tag.decompose()
                    paras = inner_soup.find_all("p")
                    if paras:
                        body_text = "\n\n".join(
                            p.get_text(strip=True) for p in paras
                            if p.get_text(strip=True)
                        )
            except Exception:
                pass

        if not body_text or len(body_text) < 100:
            body_el = (
                soup.select_one("div.article-body")
                or soup.select_one("div[class*='articleBody']")
                or soup.select_one("div[class*='article-content']")
                or soup.select_one("div[class*='content-body']")
                or soup.select_one("article")
            )
            if body_el:
                for tag in body_el.select("script,style,ins,nav,aside,figure,[class*='ad']"):
                    tag.decompose()
                paras = body_el.find_all("p")
                body_text = "\n\n".join(
                    p.get_text(strip=True) for p in paras
                    if p.get_text(strip=True)
                )

        return title, date, body_text

    except Exception as e:
        print(f"      ❌ Fetch error: {e}")
        return "", "", ""


def find_date_in_json(data, depth=0):
    if depth > 8:
        return ""
    if isinstance(data, dict):
        for key in ("publishedAt", "published_at", "publishDate", "date",
                    "created_at", "post_date"):
            if key in data and isinstance(data[key], str) and len(data[key]) >= 10:
                val = data[key][:10]
                if re.match(r"\d{4}-\d{2}-\d{2}", val):
                    return val
        for v in data.values():
            result = find_date_in_json(v, depth + 1)
            if result:
                return result
    if isinstance(data, list):
        for item in data:
            result = find_date_in_json(item, depth + 1)
            if result:
                return result
    return ""


def dig_for_body(data, depth=0):
    if depth > 10:
        return ""
    if isinstance(data, str) and len(data) > 300:
        return data
    if isinstance(data, dict):
        for key in ("content", "body", "text", "article", "description",
                    "fullText", "articleBody", "post_content"):
            if key in data and isinstance(data[key], str) and len(data[key]) > 200:
                return data[key]
        best = ""
        for v in data.values():
            result = dig_for_body(v, depth + 1)
            if len(result) > len(best):
                best = result
        return best
    if isinstance(data, list):
        best = ""
        for item in data:
            result = dig_for_body(item, depth + 1)
            if len(result) > len(best):
                best = result
        return best
    return ""


LINK_FILTER_JS = """els => {
    return els
        .filter(el => {
            let parent = el.parentElement;
            while (parent) {
                const tag = parent.tagName.toLowerCase();
                const cls = (parent.className || '').toLowerCase();
                if (tag === 'nav' || tag === 'header' || tag === 'footer') return false;
                if (cls.includes('sidebar') || cls.includes('related') ||
                    cls.includes('recommended') || cls.includes('trending') ||
                    cls.includes('popular') || cls.includes('widget')) return false;
                parent = parent.parentElement;
            }
            return true;
        })
        .map(el => ({
            href: el.href,
            title: (el.querySelector('h2,h3,h4') || el).innerText.trim().split('\\n')[0]
        }))
        .filter(x =>
            x.href.includes('fxempire.com') &&
            (x.href.includes('/forecasts/article/') || x.href.includes('/analysis/article/')) &&
            /\\d{6,}/.test(x.href)
        )
}"""


async def phase1_author_pages(page, author_slug, seen_urls):
    """Phase 1: Scrape all 10 author pages."""
    base_author_url = f"{BASE_URL}/author/{author_slug}"
    articles = []

    for page_num in range(1, 201):
        url = base_author_url if page_num == 1 else f"{base_author_url}?page={page_num}"
        print(f"  📄 Page {page_num}: {url}")

        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            if resp and resp.status == 404:
                print(f"  ✅ Author pages exhausted after page {page_num - 1}")
                break
            await page.wait_for_timeout(2500)
        except Exception as e:
            print(f"  ❌ Error loading page {page_num}: {e}")
            break

        links = await page.eval_on_selector_all("a[href]", LINK_FILTER_JS)

        new_this_page = 0
        for link in links:
            href = link["href"].split("?")[0]
            title = link["title"].strip()
            if href and href not in seen_urls:
                seen_urls.add(href)
                articles.append({"title": title, "url": href, "date": ""})
                new_this_page += 1

        print(f"    ✅ {new_this_page} new articles (total: {len(articles)})")

        if new_this_page == 0:
            print(f"  ✅ No more new articles at page {page_num}")
            break

        await asyncio.sleep(1.5)

    return articles


def phase2_google_api_search(author_name, seen_urls):
    """
    Phase 2: Use Google Custom Search API to find older articles.
    No browser needed — direct API calls, no blocking.
    Free tier: 100 searches/day.
    """
    import urllib.parse

    api_key = os.environ.get("GOOGLE_API_KEY", "")
    cx = os.environ.get("GOOGLE_CX", "")

    if not api_key or not cx:
        print(f"  ⚠️  GOOGLE_API_KEY or GOOGLE_CX not set — skipping Phase 2")
        return []

    articles = []
    queries = GOOGLE_SEARCH_QUERIES.get(author_name, [])

    if not queries:
        return articles

    print(f"\n  🔍 Phase 2: Google API search for older {author_name} articles...")

    for query in queries:
        print(f"    🔎 Query: {query}")
        query_new = 0

        # Google Custom Search API returns max 10 results per call
        # We paginate using &start= (1, 11, 21... up to 91 = 100 results max)
        for start in range(1, 100, 10):
            try:
                api_url = (
                    f"https://www.googleapis.com/customsearch/v1"
                    f"?key={api_key}"
                    f"&cx={cx}"
                    f"&q={urllib.parse.quote(query)}"
                    f"&start={start}"
                    f"&num=10"
                )

                resp = requests.get(api_url, timeout=15)

                if resp.status_code == 429:
                    print(f"    ⚠️  API quota exceeded — stopping")
                    return articles

                if resp.status_code != 200:
                    print(f"    ❌ API error {resp.status_code} — skipping")
                    break

                data = resp.json()
                items = data.get("items", [])

                if not items:
                    print(f"    ⏹  No more results at start={start}")
                    break

                new_this_page = 0
                for item in items:
                    url = item.get("link", "").split("?")[0]
                    title = item.get("title", "")
                    if (url and url not in seen_urls and
                            "fxempire.com" in url and
                            ("/forecasts/article/" in url or "/analysis/article/" in url) and
                            re.search(r"\d{6,}", url)):
                        seen_urls.add(url)
                        articles.append({"title": title, "url": url, "date": ""})
                        new_this_page += 1
                        query_new += 1

                print(f"      start={start}: +{new_this_page} articles (total: {len(articles)})")

                if new_this_page == 0:
                    print(f"    ⏹  No new articles — moving to next query")
                    break

                # Be polite to API
                time.sleep(1)

            except Exception as e:
                print(f"    ❌ API error: {e}")
                break

        print(f"    ✅ Query found {query_new} new older articles")
        time.sleep(2)

    print(f"  📊 Phase 2 total: {len(articles)} older articles found via Google API")
    return articles


async def get_author_articles_playwright(author_name, author_slug, seen_urls):
    """Run Phase 1 (author pages) then Phase 2 (Google search) for each author."""
    print(f"\n  🌐 Launching browser for {author_name}...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=HTTP_HEADERS["User-Agent"],
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()

        await page.route(
            "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,mp4,mp3}",
            lambda r: r.abort()
        )

        # Phase 1: Author pages
        print(f"  📋 Phase 1: Scraping author pages...")
        phase1_articles = await phase1_author_pages(page, author_slug, seen_urls)
        print(f"  ✅ Phase 1 complete: {len(phase1_articles)} articles")

        # Phase 2: Google search for older articles
        phase2_articles = phase2_google_api_search(author_name, seen_urls)

        await browser.close()

    all_articles = phase1_articles + phase2_articles
    print(f"  📊 Total unique new articles found: {len(all_articles)}")
    return all_articles


def push_batch_to_sheet(sheet, rows):
    if rows:
        sheet.append_rows(rows, value_input_option="USER_ENTERED")
        time.sleep(1)


def load_training_file():
    if os.path.exists(TRAINING_FILE):
        return json.load(open(TRAINING_FILE, "r", encoding="utf-8"))
    return []


def save_training_file(data):
    json.dump(data, open(TRAINING_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=2)


async def main_async():
    print(f"\n{'='*60}")
    print(f"  FX Empire Historical Scraper (Playwright v7)")
    print(f"  {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    sheet = get_google_sheet()
    ensure_headers(sheet)

    existing_urls = get_existing_urls(sheet)
    print(f"  ℹ️  {len(existing_urls)} articles already in sheet — will skip these.\n")

    training_data = load_training_file()
    existing_training = {a["url"] for a in training_data}

    all_urls_this_run = set(existing_urls)
    total = 0

    for author_name, (slug, full_name) in AUTHORS.items():
        print(f"{'─'*50}")
        print(f"  👤 Author: {author_name}")

        all_articles = await get_author_articles_playwright(author_name, slug, all_urls_this_run)

        if not all_articles:
            print(f"  ⚠️  No new articles found for {author_name}\n")
            continue

        print(f"  ✅ {len(all_articles)} new articles to process for {author_name}")
        batch = []
        matched = 0

        for i, article in enumerate(all_articles, 1):
            print(f"    [{i}/{len(all_articles)}] ...{article['url'][-55:]}")

            title, date, body = fetch_article_data(article["url"])
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

            if not body:
                print(f"      ⚠️  No body — skipping")
                continue

            if not keyword_match(body):
                print(f"      ⏭  Skipped (no keyword match)")
                continue

            words = len(body.split())
            publish_date = date if date else now
            all_urls_this_run.add(article["url"])
            matched += 1

            print(f"      ✅ {words} words | {publish_date} | {title[:50]}")

            row = [title, author_name, publish_date, article["url"], words, now]
            batch.append(row)

            if article["url"] not in existing_training:
                training_data.append({
                    "author": author_name,
                    "title": title,
                    "date": publish_date,
                    "url": article["url"],
                    "word_count": words,
                    "body": body,
                })
                existing_training.add(article["url"])

            if len(batch) >= 20:
                push_batch_to_sheet(sheet, batch)
                total += len(batch)
                batch = []
                save_training_file(training_data)
                print(f"    💾 Saved batch — {total} total so far")

            time.sleep(0.5)

        if batch:
            push_batch_to_sheet(sheet, batch)
            total += len(batch)
            save_training_file(training_data)

        print(f"  ✅ Done with {author_name} — {matched} Gold articles matched\n")

    print(f"\n{'='*60}")
    print(f"  🎉 DONE! Total new Gold articles added: {total}")
    print(f"  ✅ JSON saved with {len(training_data)} total entries")
    print(f"{'='*60}\n")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
