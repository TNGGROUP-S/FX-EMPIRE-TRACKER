"""
FX Empire Historical Article Scraper
Scrapes ALL articles per author using __NEXT_DATA__ JSON extraction + API fallback.
Filters ONLY Gold/XAU/USD based on BODY content.
Outputs to Google Sheets + a local JSON training file.
"""

import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import json
import os
import time
import re

# ── CONFIG ───────────────────────────────────────────────────────────────────
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "YOUR_SPREADSHEET_ID_HERE")
CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")

AUTHORS = {
    "Christopher Lewis":  "fx-empire-analyst-christopher-lewis",
    "James Hyerczyk":     "jameshyerczyk",
    "Arslan Ali":         "arslanali",
    "Bruce Powers":       "brucepower",
    "Muhammad Umair":     "muhammadumair",
    "Vladimir Zernov":    "vladimirzernov",
}

# ✅ Keywords MUST appear inside the article BODY, not the title
TARGET_KEYWORDS = {
    # Gold / XAU
    "gold", "xau", "xauusd", "xau/usd",
    # Precious metals
    "precious metal", "precious metals",
    "bullion", "yellow metal",
    # Commodities
    "commodity", "commodities", "metals market",
    # USD / Dollar impact
    "usd", "dollar", "us dollar", "u.s. dollar",
    "greenback", "dxy",
    # Yields / Bonds / Rates
    "yield", "yields",
    "treasury", "treasuries",
    "bond yield", "bond yields",
    "10-year", "10 year",
    "real yield", "real yields",
    "rates", "rate hike", "interest rate", "interest-rate",
    # Safe haven / Risk sentiment
    "safe-haven", "safe haven",
    "risk-off", "risk off",
    "inflation hedge",
}

SHEET_HEADERS = [
    "Title", "Author", "Date Published", "URL",
    "Word Count", "Full Article Body", "Date Scraped"
]

TRAINING_FILE = "historical_articles.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

BASE_URL = "https://www.fxempire.com"
API_BASE = "https://api.fxempire.com"

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
    first_row = sheet.row_values(1)
    if first_row != SHEET_HEADERS:
        sheet.insert_row(SHEET_HEADERS, 1)
        sheet.format("A1:G1", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2},
        })


def get_existing_urls(sheet):
    try:
        return set(sheet.col_values(4)[1:])
    except Exception:
        return set()


def keyword_match(text):
    text_lower = text.lower()
    return any(kw in text_lower for kw in TARGET_KEYWORDS)


def extract_next_data(soup):
    """Pull the __NEXT_DATA__ JSON blob that Next.js embeds in every page."""
    tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if tag and tag.string:
        try:
            return json.loads(tag.string)
        except Exception:
            pass
    return None


def dig_for_body(data, depth=0):
    """Recursively search JSON for the longest text blob (the article body)."""
    if depth > 10:
        return ""
    if isinstance(data, str) and len(data) > 300:
        return data
    if isinstance(data, dict):
        for key in ("content", "body", "text", "article", "description", "fullText", "articleBody"):
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


def scrape_article_body(url):
    """Fetch the full article body from FX Empire article page."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # ── Try __NEXT_DATA__ first ───────────────────────────────────────────
        next_data = extract_next_data(soup)
        if next_data:
            body_text = dig_for_body(next_data)
            if body_text and len(body_text) > 100:
                return body_text

        # ── Fallback: standard HTML selectors ────────────────────────────────
        body_el = (
            soup.select_one("div.article-body")
            or soup.select_one("div[class*='articleBody']")
            or soup.select_one("div[class*='article-content']")
            or soup.select_one("div[class*='content-body']")
            or soup.select_one("article")
        )
        if not body_el:
            return ""

        for tag in body_el.select("script, style, ins, nav, aside, figure, [class*='ad']"):
            tag.decompose()

        paragraphs = body_el.find_all("p")
        return "\n\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))

    except Exception:
        return ""


def extract_articles_from_next_data(data):
    """
    Recursively search __NEXT_DATA__ for article lists.
    Returns list of dicts: {title, url, date}
    """
    articles = []
    seen = set()

    def search(obj, depth=0):
        if depth > 12 or not obj:
            return
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    url = item.get("url") or item.get("href") or item.get("slug") or ""
                    title = item.get("title") or item.get("name") or ""
                    date = (item.get("date") or item.get("publishedAt")
                            or item.get("created_at") or item.get("publish_date") or "")
                    if title and url and url not in seen:
                        full_url = url if url.startswith("http") else BASE_URL + url
                        articles.append({"title": title, "url": full_url, "date": date})
                        seen.add(url)
                search(item, depth + 1)
        elif isinstance(obj, dict):
            for v in obj.values():
                search(v, depth + 1)

    search(data)
    return articles


def try_api_endpoints(author_slug, page):
    """
    Try known FX Empire API patterns to get paginated article lists as JSON.
    Returns list of dicts: {title, url, date} or empty list.
    """
    endpoints = [
        f"{API_BASE}/v1/authors/{author_slug}/articles?page={page}&limit=20",
        f"{API_BASE}/v1/articles?author={author_slug}&page={page}&limit=20",
        f"{BASE_URL}/api/articles?author={author_slug}&page={page}",
        f"{BASE_URL}/api/v1/authors/{author_slug}?page={page}",
        f"{BASE_URL}/_next/data/articles/author/{author_slug}?page={page}",
    ]

    for endpoint in endpoints:
        try:
            resp = requests.get(endpoint, headers=HEADERS, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                items = (data if isinstance(data, list)
                         else data.get("data") or data.get("articles")
                         or data.get("items") or [])
                articles = []
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    url = item.get("url") or item.get("href") or item.get("slug") or ""
                    title = item.get("title") or item.get("name") or ""
                    date = (item.get("date") or item.get("publishedAt")
                            or item.get("created_at") or "")
                    if title and url:
                        full_url = url if url.startswith("http") else BASE_URL + url
                        articles.append({"title": title, "url": full_url, "date": date})
                if articles:
                    print(f"    ✅ API hit: {endpoint}")
                    return articles
        except Exception:
            continue

    return []


def get_author_article_urls(author_name, author_slug, existing_urls):
    """
    Scrape ALL article URLs for an author using 3 strategies:
    1. Extract from __NEXT_DATA__ JSON embedded in the author page
    2. Try known API endpoints with pagination
    3. Fall back to HTML card scraping
    """
    author_url = f"{BASE_URL}/author/{author_slug}"
    all_articles = []
    seen_urls = set()

    print(f"\n  📄 Scraping author page: {author_url}")

    # ── STRATEGY 1: __NEXT_DATA__ ─────────────────────────────────────────────
    try:
        resp = requests.get(author_url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        next_data = extract_next_data(soup)

        if next_data:
            print(f"    ✅ Found __NEXT_DATA__ — scanning for article list...")
            found = extract_articles_from_next_data(next_data)
            print(f"    📦 Extracted {len(found)} articles from page JSON")
            for a in found:
                if a["url"] not in seen_urls and a["url"] not in existing_urls:
                    all_articles.append(a)
                    seen_urls.add(a["url"])
        else:
            print(f"    ℹ️  No __NEXT_DATA__ found on author page")

    except Exception as e:
        print(f"    ❌ Error fetching author page: {e}")

    # ── STRATEGY 2: Direct API pagination ────────────────────────────────────
    print(f"    🔌 Trying direct API endpoints...")
    api_worked = False

    for page in range(1, 201):
        results = try_api_endpoints(author_slug, page)
        if not results:
            if page == 1:
                print(f"    ℹ️  No API endpoints responded")
            else:
                print(f"    ⏹  API exhausted after page {page - 1}")
            break

        api_worked = True
        new = 0
        for a in results:
            if a["url"] not in seen_urls and a["url"] not in existing_urls:
                all_articles.append(a)
                seen_urls.add(a["url"])
                new += 1

        print(f"    📄 API page {page}: {new} new articles")
        if new == 0:
            break
        time.sleep(0.8)

    # ── STRATEGY 3: HTML fallback ─────────────────────────────────────────────
    if not api_worked:
        print(f"    🔁 Falling back to HTML scraping...")
        for page in range(1, 201):
            paginated = f"{author_url}?page={page}" if page > 1 else author_url
            try:
                resp = requests.get(paginated, headers=HEADERS, timeout=20)
                if resp.status_code == 404:
                    break
                resp.raise_for_status()
            except Exception as e:
                print(f"    ❌ Page fetch error: {e}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            cards = soup.select(
                "article, div[class*='article-item'], div[class*='ArticleCard'], li[class*='article']"
            )
            if not cards:
                cards = soup.select("a[href*='/analysis/'], a[href*='/forecasts/article/']")
            if not cards:
                print(f"    ❌ No HTML cards on page {page} — stopping.")
                break

            found = 0
            for card in cards:
                a = card if card.name == "a" else card.select_one("a[href]")
                if not a:
                    continue
                href = a.get("href", "")
                if not href:
                    continue
                if not href.startswith("http"):
                    href = BASE_URL + href
                if href in seen_urls or href in existing_urls:
                    continue

                title_el = card.select_one("h2, h3, h4, [class*='title']")
                title = title_el.get_text(strip=True) if title_el else a.get_text(strip=True)
                date_el = card.select_one("time")
                date_pub = date_el.get("datetime", "") if date_el else ""

                all_articles.append({"title": title, "url": href, "date": date_pub})
                seen_urls.add(href)
                found += 1

            print(f"    ✅ HTML page {page}: {found} new articles")
            if found == 0:
                break
            time.sleep(1.2)

    print(f"    🏁 Total new articles found: {len(all_articles)}")
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


def main():
    print(f"\n{'='*60}")
    print(f"  FX Empire Historical Scraper — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    sheet = get_google_sheet()
    ensure_headers(sheet)
    existing_urls = get_existing_urls(sheet)
    print(f"  ℹ️  {len(existing_urls)} articles already in sheet — will skip these.\n")

    training_data = load_training_file()
    existing_training = {a["url"] for a in training_data}

    total = 0

    for author_name, slug in AUTHORS.items():
        print(f"{'─'*50}")
        print(f"  👤 Author: {author_name}")

        all_articles = get_author_article_urls(author_name, slug, existing_urls)

        if not all_articles:
            print(f"  ⚠️  No new articles found for {author_name}\n")
            continue

        print(f"  ✅ {len(all_articles)} new articles to process for {author_name}")
        batch = []

        for i, article in enumerate(all_articles, 1):
            print(f"    [{i}/{len(all_articles)}] {article['title'][:70]}...")

            body = scrape_article_body(article["url"])
            words = len(body.split()) if body else 0
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

            if not keyword_match(body):
                print(f"      ⏭  Skipped (no keyword match)")
                continue

            row = [
                article["title"], author_name, article["date"], article["url"],
                words, body[:49000], now,
            ]
            batch.append(row)

            if article["url"] not in existing_training:
                training_data.append({
                    "author": author_name,
                    "title": article["title"],
                    "date": article["date"],
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

            time.sleep(0.7)

        if batch:
            push_batch_to_sheet(sheet, batch)
            total += len(batch)
            save_training_file(training_data)

        print(f"  ✅ Done with {author_name}\n")

    print(f"\n{'='*60}")
    print(f"  🎉 DONE! Total new Gold articles added: {total}")
    print(f"  ✅ JSON saved with {len(training_data)} total entries")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
