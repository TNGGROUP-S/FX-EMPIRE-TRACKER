import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import json
import os
import time

# ── CONFIG ──────────────────────────────────────────────────────────────────
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "YOUR_SPREADSHEET_ID_HERE")
CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")

TARGET_AUTHORS = {
    "christopher lewis",
    "arslan ali",
    "bruce powers",
    "muhammad umair",
    "james hyerczyk",
    "vladimir zernov",
}

TARGET_KEYWORDS = {"gold", "xau", "xau/usd", "usd"}

FXEMPIRE_SEARCH_URLS = [
    "https://www.fxempire.com/search/gold",
    "https://www.fxempire.com/search/xau",
    "https://www.fxempire.com/search/xauusd",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

SHEET_HEADERS = ["Title", "Author", "Date Published", "URL", "Date Added"]
# ────────────────────────────────────────────────────────────────────────────


def get_google_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_dict = json.loads(CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).sheet1
    return sheet


def ensure_headers(sheet):
    first_row = sheet.row_values(1)
    if first_row != SHEET_HEADERS:
        sheet.insert_row(SHEET_HEADERS, 1)
        # Style header row bold
        sheet.format("A1:E1", {"textFormat": {"bold": True}})


def get_existing_urls(sheet):
    try:
        url_col = sheet.col_values(4)  # Column D = URL
        return set(url_col[1:])        # Skip header
    except Exception:
        return set()


def keyword_match(text):
    text_lower = text.lower()
    return any(kw in text_lower for kw in TARGET_KEYWORDS)


def author_match(author):
    return author.strip().lower() in TARGET_AUTHORS


def scrape_fxempire_articles():
    articles = []

    for url in FXEMPIRE_SEARCH_URLS:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # FX Empire article cards — selectors tuned to their layout
            cards = soup.select("article, div.article-item, div[class*='article']")

            for card in cards:
                # Title
                title_el = card.select_one("h2, h3, h4, a[class*='title']")
                title = title_el.get_text(strip=True) if title_el else ""

                # URL
                link_el = card.select_one("a[href]")
                href = link_el["href"] if link_el else ""
                if href and not href.startswith("http"):
                    href = "https://www.fxempire.com" + href

                # Author
                author_el = card.select_one(
                    "[class*='author'], [class*='writer'], span.by"
                )
                author = author_el.get_text(strip=True).replace("By ", "").replace("by ", "") if author_el else ""

                # Date
                date_el = card.select_one("time, [class*='date'], [class*='time']")
                date_pub = ""
                if date_el:
                    date_pub = date_el.get("datetime", "") or date_el.get_text(strip=True)

                if not title or not href:
                    continue

                # Filter: author AND keyword must match
                if author_match(author) and keyword_match(title):
                    articles.append({
                        "title": title,
                        "author": author,
                        "date": date_pub,
                        "url": href,
                    })

            time.sleep(2)  # Be polite between requests

        except Exception as e:
            print(f"Error scraping {url}: {e}")

    # Deduplicate by URL
    seen = set()
    unique = []
    for a in articles:
        if a["url"] not in seen:
            seen.add(a["url"])
            unique.append(a)

    return unique


def push_to_sheet(sheet, articles, existing_urls):
    new_count = 0
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    for article in articles:
        if article["url"] in existing_urls:
            continue  # Already in sheet — skip

        row = [
            article["title"],
            article["author"],
            article["date"],
            article["url"],
            now,
        ]
        sheet.append_row(row, value_input_option="USER_ENTERED")
        existing_urls.add(article["url"])
        new_count += 1
        time.sleep(0.5)  # Avoid Google Sheets rate limits

    return new_count


def main():
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}] Starting FX Empire scraper...")

    sheet = get_google_sheet()
    ensure_headers(sheet)
    existing_urls = get_existing_urls(sheet)
    print(f"  Found {len(existing_urls)} existing articles in sheet.")

    articles = scrape_fxempire_articles()
    print(f"  Scraped {len(articles)} matching articles from FX Empire.")

    added = push_to_sheet(sheet, articles, existing_urls)
    print(f"  ✅ Added {added} new articles to Google Sheets.")


if __name__ == "__main__":
    main()
