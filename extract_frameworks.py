"""
Analyst Framework Extractor
============================
Reads all articles from historical_articles.json, processes each author
in batches, and produces one Analytical Framework document per author.

Usage:
    pip install anthropic
    python extract_frameworks.py

Output:
    One .txt file per author in ./frameworks/ folder
    e.g. frameworks/Christopher_Lewis_Framework.txt
"""

import json
import os
import time
import anthropic
from collections import defaultdict

# ── Config ─────────────────────────────────────────────────────────────────
JSON_FILE = "historical_articles.json"       # path to your JSON file
OUTPUT_DIR = "frameworks"                     # where framework docs are saved
BATCH_SIZE = 30                              # articles per batch (safe for context)
MODEL = "claude-opus-4-5"                  # best model for deep analysis
# ───────────────────────────────────────────────────────────────────────────

EXTRACTION_PROMPT = """You are analyzing a batch of articles written by {author} to extract their analytical framework for gold (XAU/USD) and commodity markets.

This is batch {batch_num} of {total_batches} for this author.

Here are the articles:

{articles_text}

---

From these articles, extract and document the following about {author}'s analytical approach. Be specific — cite actual indicators, price levels, and phrases they use. Do not generalize.

1. TECHNICAL INDICATORS
   - Which indicators do they use? (RSI, MACD, Fibonacci levels, Moving Averages, Bollinger Bands, ATR, etc.)
   - How central is each indicator to their analysis?
   - Do they combine indicators or use them standalone?
   - What specific Fibonacci levels do they reference most?
   - What Moving Average periods do they favour?

2. ECONOMIC & MACRO INDICATORS
   - Do they watch DXY (US Dollar Index)? How do they use it?
   - Fed policy / interest rate expectations — how much weight?
   - Real yields / Treasury yields — do they track these?
   - CPI / inflation data — how does this affect their outlook?
   - Geopolitical risk / safe haven flows — how do they factor this in?
   - Central bank gold buying — do they reference this?
   - Risk-on/risk-off sentiment — how do they use this?
   - Any other macro factors they consistently reference?

3. PRICE STRUCTURE APPROACH
   - Are they support/resistance focused?
   - Trend following or counter-trend?
   - Do they use chart patterns? Which ones?
   - How do they identify key price levels?
   - How do they define trend direction?

4. CONVICTION SIGNALS
   - What technical + macro combination makes them strongly BULLISH?
   - What makes them strongly BEARISH?
   - What creates uncertainty or neutrality in their analysis?
   - What conditions do they say they are "watching" before committing to a view?

5. RISK FRAMING
   - Do they give specific invalidation price levels?
   - How do they handle conflicting macro vs technical signals?
   - Do they have a natural bullish or bearish bias on gold overall?
   - How do they express uncertainty? (specific phrases or language)

6. TIMEFRAME FOCUS
   - Do they primarily use daily, weekly, or intraday charts?
   - Do they give short-term or longer-term structural outlooks?
   - Do they distinguish between the two?

7. MACRO vs TECHNICAL HIERARCHY
   - When macro and technicals conflict, which wins for them?
   - Do they lead with macro narrative then confirm with technicals, or vice versa?
   - Do they ever ignore one in favour of the other?

8. UNIQUE CHARACTERISTICS
   - Any analytical quirks or approaches unique to this author?
   - Specific assets or relationships they consistently track alongside gold? (e.g. silver ratio, DXY, yields)
   - How do they typically structure their analysis? (what comes first, what comes last)

Format your response as a clear, structured document with these 8 sections. Be specific and evidence-based from the articles provided."""


SYNTHESIS_PROMPT = """You are synthesizing {total_batches} batch analyses of articles by {author} into one definitive Analytical Framework document.

Here are the batch analyses:

{batch_analyses}

---

Now produce ONE final, comprehensive Analytical Framework document for {author}.

This document will be used by an AI system to replicate how {author} THINKS and ANALYSES gold markets — not how they write, but how they reason.

Structure it under these 8 sections, synthesizing and de-duplicating insights across all batches:

1. TECHNICAL INDICATORS
2. ECONOMIC & MACRO INDICATORS  
3. PRICE STRUCTURE APPROACH
4. CONVICTION SIGNALS
5. RISK FRAMING
6. TIMEFRAME FOCUS
7. MACRO vs TECHNICAL HIERARCHY
8. UNIQUE CHARACTERISTICS

At the end, add a section:

9. ANALYTICAL SUMMARY
   - A 200-word summary of how this analyst thinks, what drives their conclusions, and what makes their analytical approach distinctive compared to a generic gold analyst.

Be specific, evidence-based, and thorough. This is the foundation of an AI analyst system."""


def load_articles(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def group_by_author(articles):
    grouped = defaultdict(list)
    for article in articles:
        grouped[article["author"]].append(article)
    return grouped


def format_articles_for_prompt(articles):
    parts = []
    for i, article in enumerate(articles, 1):
        parts.append(
            f"--- ARTICLE {i} ---\n"
            f"Title: {article['title']}\n"
            f"Date: {article['date']}\n\n"
            f"{article['body']}\n"
        )
    return "\n".join(parts)


def extract_batch(client, author, articles, batch_num, total_batches):
    articles_text = format_articles_for_prompt(articles)
    prompt = EXTRACTION_PROMPT.format(
        author=author,
        batch_num=batch_num,
        total_batches=total_batches,
        articles_text=articles_text,
    )

    print(f"    Batch {batch_num}/{total_batches} — sending {len(articles)} articles...")

    response = client.messages.create(
        model=MODEL,
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}],
    )

    return response.content[0].text


def synthesize_frameworks(client, author, batch_analyses):
    combined = ""
    for i, analysis in enumerate(batch_analyses, 1):
        combined += f"\n\n=== BATCH {i} ANALYSIS ===\n{analysis}"

    prompt = SYNTHESIS_PROMPT.format(
        author=author,
        total_batches=len(batch_analyses),
        batch_analyses=combined,
    )

    print(f"    Synthesizing {len(batch_analyses)} batches into final framework...")

    response = client.messages.create(
        model=MODEL,
        max_tokens=6000,
        messages=[{"role": "user", "content": prompt}],
    )

    return response.content[0].text


def process_author(client, author, articles, output_dir):
    print(f"\n{'='*60}")
    print(f"Processing: {author} ({len(articles)} articles)")
    print(f"{'='*60}")

    # Split into batches
    batches = [articles[i:i+BATCH_SIZE] for i in range(0, len(articles), BATCH_SIZE)]
    total_batches = len(batches)
    print(f"  Split into {total_batches} batches of up to {BATCH_SIZE} articles each")

    # Extract from each batch
    batch_analyses = []
    for i, batch in enumerate(batches, 1):
        try:
            analysis = extract_batch(client, author, batch, i, total_batches)
            batch_analyses.append(analysis)
            time.sleep(1)  # small pause between batches
        except Exception as e:
            print(f"    ERROR on batch {i}: {e}")
            time.sleep(5)
            # retry once
            try:
                analysis = extract_batch(client, author, batch, i, total_batches)
                batch_analyses.append(analysis)
            except Exception as e2:
                print(f"    FAILED batch {i} after retry: {e2}")

    if not batch_analyses:
        print(f"  No analyses produced for {author}, skipping.")
        return

    # Synthesize into final framework
    if len(batch_analyses) == 1:
        final_framework = batch_analyses[0]
    else:
        final_framework = synthesize_frameworks(client, author, batch_analyses)

    # Save to file
    safe_name = author.replace(" ", "_")
    filepath = os.path.join(output_dir, f"{safe_name}_Framework.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"ANALYTICAL FRAMEWORK: {author.upper()}\n")
        f.write(f"Generated from {len(articles)} articles\n")
        f.write("=" * 60 + "\n\n")
        f.write(final_framework)

    print(f"  ✓ Saved: {filepath}")


def main():
    # Check for API key
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set.")
        print("Set it with: export ANTHROPIC_API_KEY='your-key-here'")
        return

    # Check for JSON file
    if not os.path.exists(JSON_FILE):
        print(f"ERROR: {JSON_FILE} not found in current directory.")
        print(f"Make sure your historical_articles.json is in the same folder as this script.")
        return

    # Setup
    client = anthropic.Anthropic(api_key=api_key)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load and group articles
    print(f"Loading articles from {JSON_FILE}...")
    articles = load_articles(JSON_FILE)
    grouped = group_by_author(articles)

    print(f"Found {len(articles)} articles across {len(grouped)} authors:")
    for author, arts in sorted(grouped.items()):
        print(f"  {author}: {len(arts)} articles")

    # Process each author
    for author, arts in sorted(grouped.items()):
        process_author(client, author, arts, OUTPUT_DIR)

    print(f"\n{'='*60}")
    print(f"DONE. Framework documents saved to ./{OUTPUT_DIR}/")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
