"""
FX Empire Historical Article Scraper
Scrapes ALL past Gold/XAU/USD articles per author, with full article body.
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

# Author slug mapping: display name → FX Empire URL slug
AUTHORS = {
    "Christopher Lewis":  "fx-empire-analyst-christopher-lewis",
    "James Hyerczyk":     "jameshyerczyk",
    "Arslan Ali":         "arslanali",
    "Bruce Powers":       "brucepower",
    "Muhammad Umair":     "muhammadumair",
    "Vladimir Zernov":    "vladimirzernov",
}

TARGET_KEYWORDS = {"gold", "xau", "xau/usd", "xauusd", "usd"}

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
# ─────────────────────────────────────────────────────────────────────────────


def get_google_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_dict = json.loads(CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)

    # Use a dedicated sheet tab called "Historical"
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        sheet = spreadsheet.worksheet("Historical")
    except gspread.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title="Historical", rows=10000, cols=10)

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
        url_col = sheet.col_values(4)   # Column D = URL
        return set(url_col[1:])
    except Exception:
        return set()


def keyword_match(text):
    text_lower = text.lower()
    return any(kw in text_lower for kw in TARGET_KEYWORDS)


def scrape_article_body(url):
    """Fetch the full article text from an individual article page."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # FX Empire article body is in these containers
        body_el = (
            soup.select_one("div.article-body")
            or soup.select_one("div[class*='articleBody']")
            or soup.select_one("div[class*='article-content']")
            or soup.select_one("div[class*='content-body']")
            or soup.select_one("article")
        )

        if not body_el:
            return ""

        # Remove ads, scripts, nav elements
        for tag in body_el.select("script, style, ins, nav, aside, figure, [class*='ad']"):
            tag.decompose()

        paragraphs = body_el.find_all("p")
        text = "\n\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        return text

    except Exception as e:
        print(f"    ⚠️  Could not fetch article body from {url}: {e}")
        return ""


def get_author_article_urls(author_name, author_slug, existing_urls):
    """
    Paginate through an author's full article list on FX Empire.
    Returns list of dicts: {title, url, date}.
    Stops when it hits already-scraped URLs or runs out of pages.
    """
    author_url = f"{BASE_URL}/author/{author_slug}"
    articles = []
    page = 1
    stop = False

    print(f"\n  📄 Scraping author page: {author_url}")

    while not stop:
        paginated_url = f"{author_url}?page={page}" if page > 1 else author_url
        try:
            resp = requests.get(paginated_url, headers=HEADERS, timeout=20)
            if resp.status_code == 404:
                break
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Collect article links from the author page
            cards = soup.select("article, div[class*='article-item'], div[class*='ArticleCard'], li[class*='article']")

            if not cards:
                # Fallback: grab all internal forecast/article links
                cards = soup.select("a[href*='/forecasts/article/'], a[href*='/analysis/']")

            if not cards:
                print(f"    No cards found on page {page} — stopping pagination.")
                break

            found_on_page = 0
            for card in cards:
                # Get link
                link_el = card if card.name == "a" else card.select_one("a[href]")
                if not link_el:
                    continue
                href = link_el.get("href", "")
                if not href:
                    continue
                if not href.startswith("http"):
                    href = BASE_URL + href

                # Skip if already scraped
                if href in existing_urls:
                    continue

                # Get title
                title_el = card.select_one("h2, h3, h4, [class*='title']")
                title = title_el.get_text(strip=True) if title_el else link_el.get_text(strip=True)

                # Get date
                date_el = card.select_one("time, [class*='date'], [class*='time']")
                date_pub = ""
                if date_el:
                    date_pub = date_el.get("datetime", "") or date_el.get_text(strip=True)

                # Only include XAU/Gold/USD articles
                if not keyword_match(title):
                    continue

                articles.append({"title": title, "url": href, "date": date_pub})
                found_on_page += 1

            print(f"    Page {page}: found {found_on_page} matching new articles")

            # Check if there's a next page
            next_btn = soup.select_one("a[aria-label='Next'], a.next, [class*='pagination'] a[href*='page=']")
            if not next_btn or found_on_page == 0:
                break

            page += 1
            time.sleep(2)

        except Exception as e:
            print(f"    ❌ Error on page {page}: {e}")
            break

    return articles


def push_batch_to_sheet(sheet, rows):
    """Append a batch of rows to the sheet efficiently."""
    if not rows:
        return
    sheet.append_rows(rows, value_input_option="USER_ENTERED")
    time.sleep(1)


def load_training_file():
    if os.path.exists(TRAINING_FILE):
        with open(TRAINING_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_training_file(data):
    with open(TRAINING_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    print(f"\n{'='*60}")
    print(f"  FX Empire Historical Scraper — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Keywords: Gold, XAU, XAU/USD, USD")
    print(f"  Authors:  {', '.join(AUTHORS.keys())}")
    print(f"{'='*60}\n")

    sheet = get_google_sheet()
    ensure_headers(sheet)
    existing_urls = get_existing_urls(sheet)
    print(f"  ℹ️  {len(existing_urls)} articles already in sheet — will skip these.\n")

    training_data = load_training_file()
    existing_training_urls = {a["url"] for a in training_data}

    total_added = 0

    for author_name, author_slug in AUTHORS.items():
        print(f"\n{'─'*50}")
        print(f"  👤 Author: {author_name}")

        article_list = get_author_article_urls(author_name, author_slug, existing_urls)
        print(f"  ✅ {len(article_list)} new matching articles to scrape.\n")

        batch_rows = []

        for i, article in enumerate(article_list, 1):
            print(f"    [{i}/{len(article_list)}] {article['title'][:70]}...")

            body = scrape_article_body(article["url"])
            word_count = len(body.split()) if body else 0
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

            # Sheet row
            row = [
                article["title"],
                author_name,
                article["date"],
                article["url"],
                word_count,
                body[:49000],   # Google Sheets cell limit ~50k chars
                now,
            ]
            batch_rows.append(row)

            # Training JSON entry
            if article["url"] not in existing_training_urls:
                training_data.append({
                    "author": author_name,
                    "title": article["title"],
                    "date": article["date"],
                    "url": article["url"],
                    "word_count": word_count,
                    "body": body,
                })
                existing_training_urls.add(article["url"])

            # Push to sheet in batches of 20 to avoid timeouts
            if len(batch_rows) >= 20:
                push_batch_to_sheet(sheet, batch_rows)
                total_added += len(batch_rows)
                print(f"    💾 Pushed {len(batch_rows)} rows to sheet.")
                batch_rows = []
                save_training_file(training_data)

            time.sleep(1.5)  # Polite delay between article fetches

        # Push remaining rows
        if batch_rows:
            push_batch_to_sheet(sheet, batch_rows)
            total_added += len(batch_rows)
            save_training_file(training_data)

        print(f"\n  ✅ Done with {author_name}.")

    print(f"\n{'='*60}")
    print(f"  🎉 Historical scrape complete!")
    print(f"  📊 Total new articles added: {total_added}")
    print(f"  📁 Training file saved: {TRAINING_FILE} ({len(training_data)} total articles)")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
