"""
FX Empire Historical Article Scraper — Playwright Edition v5
Fixes:
1. No duplicate header rows
2. No cross-author article contamination
3. Proper publish date extraction
4. Full history — aggressively clicks Load More + infinite scroll
"""

print("🚀 PLAYWRIGHT VERSION v5 RUNNING")

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
    """Ensure exactly one correct header row."""
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

        # ── Title ─────────────────────────────────────────────────────────────
        title = ""
        h1 = soup.select_one("h1")
        if h1:
            title = h1.get_text(strip=True)

        # ── Real Publish Date ─────────────────────────────────────────────────
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

        # ── Clean Plain Text Body ─────────────────────────────────────────────
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
                            p.get_text(strip=True) for p in paras if p.get_text(strip=True)
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
                    p.get_text(strip=True) for p in paras if p.get_text(strip=True)
                )

        return title, date, body_text

    except Exception as e:
        print(f"      ❌ Fetch error: {e}")
        return "", "", ""


def find_date_in_json(data, depth=0):
    if depth > 8:
        return ""
    if isinstance(data, dict):
        for key in ("publishedAt", "published_at", "publishDate", "date", "created_at", "post_date"):
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


def collect_links(page_links, seen_urls):
    """Filter and deduplicate article links."""
    new_articles = []
    for link in page_links:
        href = link["href"].split("?")[0]
        title = link["title"].strip()
        if href and href not in seen_urls:
            seen_urls.add(href)
            new_articles.append({"title": title, "url": href, "date": ""})
    return new_articles


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


async def get_author_articles_playwright(author_name, author_slug, seen_urls):
    """
    Use a real headless browser to get ALL of an author's articles by:
    1. Aggressively clicking Load More until exhausted
    2. Then scrolling to trigger any infinite scroll
    """
    author_url = f"{BASE_URL}/author/{author_slug}"
    articles = []

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

        print(f"  📄 Loading: {author_url}")
        await page.goto(author_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(4000)

        # ── PHASE 1: Click Load More repeatedly until it disappears ──────────
        print(f"  🖱  Phase 1: Clicking Load More until exhausted...")
        load_more_clicks = 0
        consecutive_not_found = 0

        while consecutive_not_found < 3:
            try:
                # Scroll to bottom first so Load More button is visible
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(2000)

                load_more = await page.query_selector(
                    "button:has-text('Load More'), button:has-text('Show More'), "
                    "button:has-text('load more'), button:has-text('show more'), "
                    "a:has-text('Load More'), a:has-text('Show More'), "
                    "[class*='load-more']:not([disabled]), [class*='loadMore']:not([disabled]), "
                    "[class*='show-more']:not([disabled])"
                )
                if load_more:
                    await load_more.scroll_into_view_if_needed()
                    await load_more.click()
                    load_more_clicks += 1
                    consecutive_not_found = 0
                    print(f"    🖱  Load More click #{load_more_clicks} — loading...")
                    await page.wait_for_timeout(3000)
                else:
                    consecutive_not_found += 1
                    await page.wait_for_timeout(1000)
            except Exception:
                consecutive_not_found += 1

        if load_more_clicks > 0:
            print(f"  ✅ Load More exhausted after {load_more_clicks} clicks")
        else:
            print(f"  ℹ️  No Load More button — using infinite scroll")

        # ── PHASE 2: Scroll to trigger any remaining infinite scroll ─────────
        print(f"  🔄 Phase 2: Scrolling for infinite scroll content...")
        no_new_count = 0
        scroll_attempt = 0
        max_scrolls = 200

        while scroll_attempt < max_scrolls:
            links = await page.eval_on_selector_all("a[href]", LINK_FILTER_JS)
            new = collect_links(links, seen_urls)
            articles.extend(new)

            if new:
                no_new_count = 0
                print(f"    scroll {scroll_attempt + 1}: +{len(new)} articles (total: {len(articles)})")
            else:
                no_new_count += 1

            if no_new_count >= 15:
                print(f"  ✅ No more new articles after {scroll_attempt + 1} scrolls")
                break

            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2500)
            scroll_attempt += 1

        # Final collection pass after all scrolling done
        links = await page.eval_on_selector_all("a[href]", LINK_FILTER_JS)
        final_new = collect_links(links, seen_urls)
        articles.extend(final_new)
        if final_new:
            print(f"  📦 Final pass: +{len(final_new)} more articles")

        await browser.close()

    print(f"  📊 Total unique new articles found: {len(articles)}")
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
    print(f"  FX Empire Historical Scraper (Playwright v5)")
    print(f"  {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    sheet = get_google_sheet()
    ensure_headers(sheet)

    existing_urls = get_existing_urls(sheet)
    print(f"  ℹ️  {len(existing_urls)} articles already in sheet — will skip these.\n")

    training_data = load_training_file()
    existing_training = {a["url"] for a in training_data}

    # Global URL tracker — prevents cross-author duplicates
    all_urls_this_run = set(existing_urls)

    total = 0

    for author_name, slug in AUTHORS.items():
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
