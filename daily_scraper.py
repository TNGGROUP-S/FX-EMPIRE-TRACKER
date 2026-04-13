"""
FX Empire Daily Article Scraper
Runs every 4 hours via GitHub Actions.
Checks page 1 of each author for new articles.
Appends new articles to Google Sheet (metadata) and historical_articles.json (full body).
"""

print("🚀 DAILY SCRAPER RUNNING")

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
    "Bruce Powers":       "brucepowers",
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

AUTHOR_FILES = {
    "Christopher Lewis": "articles_christopher_lewis.json",
    "James Hyerczyk":    "articles_james_hyerczyk.json",
    "Arslan Ali":        "articles_arslan_ali.json",
    "Bruce Powers":      "articles_bruce_powers.json",
    "Muhammad Umair":    "articles_muhammad_umair.json",
    "Vladimir Zernov":   "articles_vladimir_zernov.json",
}

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


def get_existing_urls(sheet):
    try:
        return set(sheet.col_values(4)[1:])
    except Exception:
        return set()


def keyword_match(text):
    text_lower = text.lower()
    return any(kw in text_lower for kw in TARGET_KEYWORDS)


def scrape_author_page(author_slug, existing_urls):
    """Scrape page 1 of an author page and return new article URLs."""
    url = f"{BASE_URL}/author/{author_slug}"
    articles = []
    seen_hrefs = set()  # Deduplicate within this page

    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove nav, header, footer, sidebar elements before scanning links
        for tag in soup.select("nav, header, footer, [class*='sidebar'], [class*='related'], [class*='recommended'], [class*='trending'], [class*='popular'], [class*='widget']"):
            tag.decompose()

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.startswith("http"):
                href = BASE_URL + href
            href = href.split("?")[0]

            if not re.search(r'\d{6,}', href):
                continue
            if "/forecasts/article/" not in href and "/analysis/article/" not in href:
                continue
            if href in existing_urls:
                continue
            if href in seen_hrefs:
                continue

            seen_hrefs.add(href)
            title_el = a.select_one("h2, h3, h4") or a
            title = title_el.get_text(strip=True).split("\n")[0]

            articles.append({"title": title, "url": href, "date": ""})

    except Exception as e:
        print(f"    ❌ Error scraping author page: {e}")

    return articles


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


def load_training_file():
    if os.path.exists(TRAINING_FILE):
        with open(TRAINING_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_training_file(data):
    # Save combined file
    with open(TRAINING_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    # Save individual author files
    from collections import defaultdict
    by_author = defaultdict(list)
    for article in data:
        by_author[article["author"]].append(article)
    
    for author, filename in AUTHOR_FILES.items():
        articles = by_author.get(author, [])
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)


def push_batch_to_sheet(sheet, rows):
    if rows:
        sheet.append_rows(rows, value_input_option="USER_ENTERED")
        time.sleep(1)


def sync_missing_from_sheet(sheet, training_data):
    """
    Automatically find any articles in the sheet but missing from JSON
    and fetch their full bodies. Runs at the start of every daily scrape.
    """
    existing_training_urls = {a["url"] for a in training_data}
    all_rows = sheet.get_all_values()[1:]  # Skip header

    missing = []
    for row in all_rows:
        if len(row) >= 4:
            url = row[3].strip()
            title = row[0].strip()
            author = row[1].strip()
            date = row[2].strip()
            if url and url not in existing_training_urls:
                missing.append({
                    "url": url,
                    "title": title,
                    "author": author,
                    "date": date
                })

    if not missing:
        print(f"  ✅ Sheet and JSON are in sync\n")
        return training_data

    print(f"  🔄 Found {len(missing)} articles in sheet but missing from JSON — syncing...")

    fixed = 0
    for article in missing:
        title, date, body = fetch_article_data(article["url"])
        if not body:
            continue

        words = len(body.split())
        publish_date = date or article["date"] or datetime.utcnow().strftime("%Y-%m-%d")
        final_title = title or article["title"]

        training_data.append({
            "author": article["author"],
            "title": final_title,
            "date": publish_date,
            "url": article["url"],
            "word_count": words,
            "body": body,
        })
        existing_training_urls.add(article["url"])
        fixed += 1
        time.sleep(0.3)

    print(f"  ✅ Synced {fixed} missing articles into JSON\n")
    return training_data


def main():
    print(f"\n{'='*60}")
    print(f"  FX Empire Daily Scraper")
    print(f"  {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    sheet = get_google_sheet()
    existing_urls = get_existing_urls(sheet)
    print(f"  ℹ️  {len(existing_urls)} articles already in sheet — will skip these.\n")

    training_data = load_training_file()
    print(f"  ℹ️  {len(training_data)} articles already in JSON.\n")

    # Auto-sync: fix any articles in sheet but missing from JSON
    print(f"  🔍 Checking sheet/JSON sync...")
    training_data = sync_missing_from_sheet(sheet, training_data)
    save_training_file(training_data)

    existing_training = {a["url"] for a in training_data}

    # Global tracker prevents cross-author duplicates
    all_urls_this_run = set(existing_urls)
    total = 0
    batch = []

    for author_name, slug in AUTHORS.items():
        print(f"{'─'*50}")
        print(f"  👤 {author_name}")

        new_articles = scrape_author_page(slug, all_urls_this_run)
        print(f"  🔎 Found {len(new_articles)} new article(s) on page 1")

        if not new_articles:
            print(f"  ✅ Nothing new\n")
            continue

        for article in new_articles:
            print(f"    → Fetching: ...{article['url'][-55:]}")

            title, date, body = fetch_article_data(article["url"])
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

            if not body:
                print(f"      ⚠️  No body — skipping")
                continue

            if not keyword_match(body):
                print(f"      ⏭  No keyword match — skipping")
                continue

            words = len(body.split())
            publish_date = date if date else now
            all_urls_this_run.add(article["url"])  # Prevent other authors grabbing same URL
            total += 1

            print(f"      ✅ {words} words | {publish_date} | {title[:50]}")

            batch.append([title, author_name, publish_date,
                          article["url"], words, now])

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

            time.sleep(0.5)

        print()

    if batch:
        push_batch_to_sheet(sheet, batch)

    save_training_file(training_data)

    print(f"\n{'='*60}")
    print(f"  🎉 DONE! New articles added: {total}")
    print(f"  ✅ JSON now has {len(training_data)} total entries")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
