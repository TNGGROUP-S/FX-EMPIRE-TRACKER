def get_author_article_urls(author_name, author_slug, existing_urls):
    """
    Paginate through an author's full article list on FX Empire.
    Returns list of dicts: {title, url, date}.
    Stops only when a page contains 0 NEW relevant articles.
    """
    author_url = f"{BASE_URL}/author/{author_slug}"
    articles = []
    page = 1

    print(f"\n  📄 Scraping author page: {author_url}")

    while True:
        paginated_url = f"{author_url}?page={page}" if page > 1 else author_url
        print(f"    🔎 Fetching page {page}: {paginated_url}")

        try:
            resp = requests.get(paginated_url, headers=HEADERS, timeout=20)
            if resp.status_code == 404:
                print(f"    ❌ 404 on page {page} — stopping.")
                break

            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Try multiple patterns for article cards
            cards = soup.select(
                "article, div[class*='article-item'], div[class*='ArticleCard'], li[class*='article']"
            )

            if not cards:
                # Secondary fallback
                cards = soup.select(
                    "a[href*='/forecasts/article/'], a[href*='/analysis/']"
                )

            if not cards:
                print(f"    ❌ No article cards found on page {page} — stopping.")
                break

            found_on_page = 0

            for card in cards:
                link_el = card if card.name == "a" else card.select_one("a[href]")

                if not link_el:
                    continue

                href = link_el.get("href", "")
                if not href:
                    continue

                if not href.startswith("http"):
                    href = BASE_URL + href

                if href in existing_urls:
                    continue

                title_el = card.select_one("h2, h3, h4, [class*='title']")
                title = (
                    title_el.get_text(strip=True)
                    if title_el
                    else link_el.get_text(strip=True)
                )

                date_el = card.select_one("time, [class*='date'], [class*='time']")
                date_pub = ""
                if date_el:
                    date_pub = date_el.get("datetime", "") or date_el.get_text(strip=True)

                # ✅ DO NOT FILTER HERE — return ALL articles
                articles.append({
                    "title": title,
                    "url": href,
                    "date": date_pub,
                })
                found_on_page += 1

            print(f"    ✅ Page {page}: found {found_on_page} new articles")

            if found_on_page == 0:
                print(f"    ✅ No new articles on page {page} — reached end.")
                break

            page += 1
            time.sleep(2)

            # Safety limit
            if page > 200:
                print("    ⚠️ Page limit (200) reached — stopping.")
                break

        except Exception as e:
            print(f"    ❌ Error on page {page}: {e}")
            break

    return articles
