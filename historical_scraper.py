"""
FX Empire Historical Article Scraper
Scrapes ALL articles per author, but filters ONLY Gold/XAU/USD based on title.
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
        return set(sheet.col_values(4)[1:])  # Skip header
    except Exception:
        return set()


def keyword_match(text):
    text_lower = text.lower()
    return any(kw in text_lower for kw in TARGET_KEYWORDS)


def scrape_article_body(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

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


def get_author_article_urls(author_name, author_slug, existing_urls):
    """
    Scrape ALL article URLs for an author (full history).
    No keyword filtering here — filtering happens later.
    """
    author_url = f"{BASE_URL}/author/{author_slug}"
    articles = []
    page = 1

    print(f"\n  📄 Scraping author: {author_name}")
    print(f"     URL: {author_url}")

    while True:
        paginated = f"{author_url}?page={page}" if page > 1 else author_url
        print(f"    🔎 Fetching page {page}")

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
            cards = soup.select("a[href*='/forecasts/article/'], a[href*='/analysis/']")
        if not cards:
            print("    ❌ No cards found — stopping.")
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

            if href in existing_urls:
                continue

            title_el = card.select_one("h2, h3, h4, [class*='title']")
            title = title_el.get_text(strip=True) if title_el else a.get_text(strip=True)

            date_el = card.select_one("time, [class*='date'], [class*='time']")
            date_pub = date_el.get("datetime", "") if date_el else ""

            articles.append({
                "title": title,
                "url": href,
                "date": date_pub
            })
            found += 1

        print(f"    ✅ Page {page}: {found} new articles")

        if found == 0:
            break

        page += 1
        time.sleep(1.5)

        if page > 200:
            print("    ⚠️ Page safety limit hit (200).")
            break

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


def main():
    print(f"\n{'='*60}")
    print(f"  FX Empire Historical Scraper — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    sheet = get_google_sheet()
    ensure_headers(sheet)
    existing_urls = get_existing_urls(sheet)

    training_data = load_training_file()
    existing_training = {a["url"] for a in training_data}

    total = 0

    for author_name, slug in AUTHORS.items():
        all_articles = get_author_article_urls(author_name, slug, existing_urls)

        # ✅ FILTER ONLY GOLD/XAU/USD ARTICLES HERE
        article_list = [a for a in all_articles if keyword_match(a["title"])]

        print(f"  ✅ {len(article_list)} Gold/XAU articles found for {author_name}")

        batch = []

        for i, article in enumerate(article_list, 1):
            print(f"    [{i}/{len(article_list)}] {article['title'][:70]}...")

            body = scrape_article_body(article["url"])
            words = len(body.split()) if body else 0
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

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

            time.sleep(1)

        if batch:
            push_batch_to_sheet(sheet, batch)
            total += len(batch)
            save_training_file(training_data)

        print(f"  ✅ Done with {author_name}\n")

    print(f"\n{'='*60}")
    print(f"  🎉 DONE! Total new Gold/XAU/USD articles added: {total}")
    print(f"  ✅ Training file saved ({len(training_data)} total entries)")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
