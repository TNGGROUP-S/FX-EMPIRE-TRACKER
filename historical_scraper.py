"""
FX Empire Historical Article Scraper — Playwright Edition v3
Uses a real headless browser to scroll through ALL articles per author.
Filters only Gold/XAU/USD articles based on BODY content.
"""

print("🚀 PLAYWRIGHT VERSION v3 RUNNING")

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
    "Word Count", "Full Article Body", "Date Scraped"
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


def scrape_article_body(url):
    """Fetch article body using requests + BeautifulSoup."""
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Try __NEXT_DATA__ first
        tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if tag and tag.string:
            try:
                data = json.loads(tag.string)
                body = dig_for_body(data)
                if body and len(body) > 200:
                    return body
            except Exception:
                pass

        # Fallback to HTML selectors
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


def dig_for_body(data, depth=0):
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


def scrape_title_and_date(url):
    """Fetch title and date from article page."""
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        title = ""
        title_el = soup.select_one("h1")
        if title_el:
            title = title_el.get_text(strip=True)

        date = ""
        date_el = soup.select_one("time")
        if date_el:
            date = date_el.get("datetime", "") or date_el.get_text(strip=True)

        return title, date
    except Exception:
        return "", ""


async def get_all_articles_playwright(author_name, author_slug, existing_urls):
    """
    Use a real headless browser to scroll the author page and collect
    ONLY article links — filtering out navigation links.
    """
    author_url = f"{BASE_URL}/author/{author_slug}"
    articles = []
    seen_urls = set(existing_urls)

    print(f"\n  🌐 Launching browser for {author_name}...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=HTTP_HEADERS["User-Agent"],
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()

        # Block images/fonts/media to speed things up
        await page.route(
            "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,mp4,mp3}",
            lambda r: r.abort()
        )

        print(f"  📄 Loading: {author_url}")
        await page.goto(author_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(4000)

        scroll_attempts = 0
        max_scrolls = 500
        no_new_count = 0

        print(f"  🔄 Scrolling to load full article history...")

        while scroll_attempts < max_scrolls:
            # ── Grab ONLY article links, not nav links ────────────────────────
            # Article URLs on FX Empire follow the pattern:
            # /forecasts/article/slug-XXXXXXX or /analysis/slug-XXXXXXX
            # We filter to only links that have a numeric article ID at the end
            links = await page.eval_on_selector_all(
                "a[href]",
                """els => els
                    .map(el => ({
                        href: el.href,
                        title: (el.querySelector('h2,h3,h4') || el).innerText.trim()
                    }))
                    .filter(x => {
                        // Must be an fxempire article URL with numeric ID
                        return x.href.includes('fxempire.com') &&
                               (x.href.includes('/forecasts/') || x.href.includes('/analysis/')) &&
                               /\\d{6,}/.test(x.href) &&
                               !x.href.match(/\\/forecasts\\/(gold|silver|commodities|forex|indices|cryptocurrencies|natural-gas|wti-crude-oil|spx|stocks)\\/?$/)
                    })
                """
            )

            new_this_round = 0
            for link in links:
                href = link["href"].split("?")[0]  # Strip query params
                title = link["title"].strip()
                if href and href not in seen_urls:
                    seen_urls.add(href)
                    articles.append({"title": title, "url": href, "date": ""})
                    new_this_round += 1

            if new_this_round > 0:
                no_new_count = 0
                print(f"    scroll {scroll_attempts + 1}: +{new_this_round} articles (total: {len(articles)})")
            else:
                no_new_count += 1

            # Stop if 8 consecutive scrolls yield nothing new
            if no_new_count >= 8:
                print(f"  ✅ Reached end of article history after {scroll_attempts + 1} scrolls")
                break

            # Scroll down a full page height
            await page.evaluate("window.scrollBy(0, window.innerHeight)")
            await page.wait_for_timeout(2000)

            # Click "Load More" if it exists
            try:
                load_more = await page.query_selector(
                    "button:has-text('Load More'), button:has-text('Show More'), "
                    "a:has-text('Load More'), [class*='load-more'], [class*='loadMore']"
                )
                if load_more:
                    await load_more.click()
                    await page.wait_for_timeout(2500)
                    print(f"    🖱  Clicked 'Load More'")
            except Exception:
                pass

            scroll_attempts += 1

        await browser.close()

    print(f"  📊 Total unique articles found: {len(articles)}")
    return articles


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
    print(f"  FX Empire Historical Scraper (Playwright v3)")
    print(f"  {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
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

        all_articles = await get_all_articles_playwright(author_name, slug, existing_urls)

        if not all_articles:
            print(f"  ⚠️  No new articles found for {author_name}\n")
            continue

        print(f"  ✅ {len(all_articles)} new articles to process for {author_name}")
        batch = []
        matched = 0

        for i, article in enumerate(all_articles, 1):
            print(f"    [{i}/{len(all_articles)}] Fetching: ...{article['url'][-50:]}")

            body = scrape_article_body(article["url"])
            words = len(body.split()) if body else 0
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

            if not keyword_match(body):
                print(f"      ⏭  Skipped (no keyword match)")
                continue

            # Get title and date from the article page if missing
            title = article["title"]
            date = article["date"]
            if not title or not date:
                t, d = scrape_title_and_date(article["url"])
                title = title or t or article["url"].split("/")[-1].replace("-", " ").title()
                date = date or d or now

            matched += 1
            row = [title, author_name, date, article["url"], words, body[:49000], now]
            batch.append(row)

            if article["url"] not in existing_training:
                training_data.append({
                    "author": author_name,
                    "title": title,
                    "date": date,
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
