import os
import time
import sqlite3
import logging
import json
import re
from datetime import datetime

import feedparser
import anthropic
import requests
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
RELEVANCE_THRESHOLD = int(os.environ.get("RELEVANCE_THRESHOLD", "6"))
MAX_NEW_PER_FEED = int(os.environ.get("MAX_NEW_PER_FEED", "5"))
MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")

DB_PATH = "seen_articles.db"
CHECK_INTERVAL = 3 * 60 * 60  # 3 hours
ARTICLE_DELAY = 2              # seconds between articles

# ── RSS Feeds ─────────────────────────────────────────────────────────────────
RSS_FEEDS = [
    {"url": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",        "name": "WSJ Markets"},
    {"url": "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml",      "name": "WSJ Business"},
    {"url": "https://feeds.a.dj.com/rss/RSSOpinion.xml",            "name": "WSJ Opinion"},
    {"url": "https://finance.yahoo.com/news/rssindex",               "name": "Yahoo Finance"},
    {"url": "https://www.investopedia.com/feedbuilder/feed/getfeed/?feedName=rss_headline", "name": "Investopedia"},
    {"url": "https://www.reuters.com/rssFeed/2BBoPOQX",               "name": "Reuters Business"},
    {"url": "https://www.reuters.com/rssFeed/4OH8XCi4",              "name": "Reuters Finance"},
    {"url": "https://www.cnbc.com/id/10000664/device/rss/rss.html", "name": "CNBC Finance"},
    {"url": "https://www.economist.com/finance-and-economics/rss.xml", "name": "The Economist"},
    {"url": "https://www.reddit.com/r/investing/.rss",               "name": "r/investing",        "reddit": True},
    {"url": "https://www.reddit.com/r/stocks/.rss",                  "name": "r/stocks",            "reddit": True},
    {"url": "https://www.reddit.com/r/economics/.rss",               "name": "r/economics",         "reddit": True},
    {"url": "https://www.reddit.com/r/SecurityAnalysis/.rss",        "name": "r/SecurityAnalysis",  "reddit": True},
    {"url": "https://seekingalpha.com/market_currents.xml",          "name": "Seeking Alpha"},
    {"url": "https://feeds.marketwatch.com/marketwatch/topstories/", "name": "MarketWatch"},
]

# ── Category colors (Discord embed integer values) ────────────────────────────
CATEGORY_COLORS = {
    "macro":        0x1E90FF,  # blue
    "earnings":     0x00C853,  # green
    "geopolitical": 0xFF6D00,  # orange
    "sector":       0x7B1FA2,  # purple
}

MACRO_KEYWORDS = [
    "fed", "federal reserve", "interest rate", "inflation", "gdp", "recession",
    "monetary policy", "treasury", "yield", "cpi", "pce", "fomc", "central bank",
    "rate hike", "rate cut", "macro", "deficit", "debt ceiling", "jackson hole",
]
EARNINGS_KEYWORDS = [
    "earnings", "revenue", "profit", "quarterly", "eps", "beats", "misses",
    "guidance", "q1", "q2", "q3", "q4", "results", "sales", "forecast", "outlook",
]
GEOPOLITICAL_KEYWORDS = [
    "war", "sanctions", "geopolitical", "trade war", "tariff", "china", "russia",
    "ukraine", "middle east", "opec", "oil", "supply chain", "embargo", "nato",
    "conflict", "election", "policy",
]


# ── Database ──────────────────────────────────────────────────────────────────

def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_articles (
            url     TEXT PRIMARY KEY,
            seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
    conn.close()
    log.info("Database ready: %s", DB_PATH)


def is_seen(url: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute("SELECT 1 FROM seen_articles WHERE url = ?", (url,))
    result = cur.fetchone() is not None
    conn.close()
    return result


def mark_seen(url: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT OR IGNORE INTO seen_articles (url) VALUES (?)", (url,))
    conn.commit()
    conn.close()


# ── Feed helpers ──────────────────────────────────────────────────────────────

def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text).strip()


def fetch_feed(feed_info: dict) -> list:
    name = feed_info["name"]
    url = feed_info["url"]
    if feed_info.get("reddit"):
        user_agent = "script:MarketPulseBot:1.0 (by /u/marketpulsebot)"
    else:
        user_agent = "MarketPulseBot/1.0 (financial news aggregator)"
    headers = {"User-Agent": user_agent}
    try:
        parsed = feedparser.parse(url, request_headers=headers)
        if parsed.bozo and not parsed.entries:
            log.warning("Feed error [%s]: %s", name, parsed.bozo_exception)
            return []

        articles = []
        for entry in parsed.entries:
            article_url = entry.get("link", "")
            if not article_url:
                continue
            title = entry.get("title", "No Title")
            raw_summary = entry.get("summary", entry.get("description", ""))
            summary = _strip_html(raw_summary)[:2000]
            pub_date = entry.get("published", entry.get("updated", "Unknown date"))
            articles.append(
                {
                    "url": article_url,
                    "title": title,
                    "summary": summary,
                    "pub_date": pub_date,
                    "source_name": name,
                }
            )
        return articles

    except Exception as exc:
        log.error("Failed to fetch feed [%s]: %s", name, exc)
        return []


# ── Startup feed validation ───────────────────────────────────────────────────

def validate_feeds() -> None:
    log.info("=" * 60)
    log.info("Running startup feed validation...")
    log.info("=" * 60)
    ok = []
    failed = []
    for feed_info in RSS_FEEDS:
        articles = fetch_feed(feed_info)
        if articles:
            log.info("  OK  [%d articles] %s", len(articles), feed_info["name"])
            ok.append(feed_info["name"])
        else:
            log.warning("  FAIL [0 articles] %s", feed_info["name"])
            failed.append(feed_info["name"])
    log.info("=" * 60)
    log.info(
        "Feed validation complete: %d working, %d failed",
        len(ok),
        len(failed),
    )
    if failed:
        log.warning("Failed feeds: %s", ", ".join(failed))
    log.info("=" * 60)


# ── Article categorisation ────────────────────────────────────────────────────

def categorize_article(title: str, summary: str) -> str:
    text = (title + " " + summary).lower()
    scores = {
        "macro":        sum(1 for kw in MACRO_KEYWORDS        if kw in text),
        "earnings":     sum(1 for kw in EARNINGS_KEYWORDS     if kw in text),
        "geopolitical": sum(1 for kw in GEOPOLITICAL_KEYWORDS if kw in text),
    }
    top = max(scores, key=scores.get)
    return top if scores[top] > 0 else "sector"


# ── Claude analysis ───────────────────────────────────────────────────────────

ANALYSIS_PROMPT = """\
You are an assistant helping a retail investor focused on financial markets who is learning to become a better investor.

Analyse the following news article and rate it 1–10 for relevance to financial markets and investing.

Article Title: {title}
Article Summary: {summary}

Guidelines:
- Deprioritise: crypto-only news, celebrity business stories, and general tech product launches unless they have clear market implications.
- Prioritise: macroeconomic data, Fed/central bank decisions, earnings reports, geopolitical events with market impact, sector-wide trends, and regulatory changes affecting markets.

If the score is {threshold} or above, provide the following three sections:

**What's affected:**
Bullet list of specific financial instruments, asset classes, or sectors directly impacted (e.g. US Treasury bonds, S&P 500 financials sector, WTI crude, USD/JPY). Be specific — name the instruments, not just broad categories.

**What this tells us about current markets:**
2–3 bullets explaining what this event reveals about the current state of the market, economy, or investor sentiment right now.

**What to watch going forward:**
2–3 bullets on what future developments, data releases, or price movements this event sets up — what a retail investor should be paying attention to as a result of this news.

Return your response as valid JSON only — no markdown fences, no extra text:
{{"score": <integer 1–10>, "analysis": "<full formatted text or empty string if score < {threshold}>"}}
"""


def analyze_article(title: str, summary: str) -> dict | None:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt = ANALYSIS_PROMPT.format(
        title=title,
        summary=summary,
        threshold=RELEVANCE_THRESHOLD,
    )
    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text.strip()
        # Strip any accidental markdown fences
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        return json.loads(text)
    except json.JSONDecodeError as exc:
        log.error("JSON parse error from Claude: %s", exc)
        return None
    except Exception as exc:
        log.error("Anthropic API error: %s", exc)
        return None


# ── Discord posting ───────────────────────────────────────────────────────────

def post_to_discord(
    title: str,
    url: str,
    analysis: str,
    source_name: str,
    category: str,
    pub_date: str,
) -> None:
    color = CATEGORY_COLORS.get(category, CATEGORY_COLORS["sector"])
    embed = {
        "title":       title[:256],
        "url":         url,
        "description": analysis[:4096],
        "color":       color,
        "author":      {"name": f"{source_name} \u2022 {category.capitalize()}"},
        "footer":      {"text": f"Published: {pub_date}"},
    }
    payload = {
        "username": "Market Pulse \U0001f4c8",
        "embeds":   [embed],
    }
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        log.info("Posted to Discord: %.60s", title)
    except requests.RequestException as exc:
        log.error("Discord webhook error: %s", exc)


# ── Main cycle ────────────────────────────────────────────────────────────────

def seed_feed(articles: list) -> int:
    """Mark all but the 3 newest articles as seen without analyzing them.
    Called on the very first run to avoid a backlog of API calls."""
    seeded = 0
    for article in articles[3:]:
        if not is_seen(article["url"]):
            mark_seen(article["url"])
            seeded += 1
    return seeded


def is_first_run() -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute("SELECT COUNT(*) FROM seen_articles")
    count = cur.fetchone()[0]
    conn.close()
    return count == 0


def run_cycle() -> None:
    log.info("Starting feed check cycle...")
    first_run = is_first_run()
    if first_run:
        log.info("First run detected — seeding database to avoid backlog...")

    total_new = 0
    total_posted = 0
    total_seeded = 0

    for feed_info in RSS_FEEDS:
        articles = fetch_feed(feed_info)

        if first_run:
            total_seeded += seed_feed(articles)
            articles = articles[:3]  # only analyse the 3 newest on first run

        new_this_feed = 0
        for article in articles:
            url = article["url"]
            if is_seen(url):
                continue
            if new_this_feed >= MAX_NEW_PER_FEED:
                log.info("Cap reached for %s, skipping remaining new articles", feed_info["name"])
                break

            mark_seen(url)
            total_new += 1
            new_this_feed += 1
            log.info("New article [%s]: %.60s", article["source_name"], article["title"])

            result = analyze_article(article["title"], article["summary"])
            if result is None:
                time.sleep(ARTICLE_DELAY)
                continue

            score = result.get("score", 0)
            analysis = result.get("analysis", "")
            log.info("Score %d/10 — %.50s", score, article["title"])

            if score >= RELEVANCE_THRESHOLD and analysis:
                category = categorize_article(article["title"], article["summary"])
                post_to_discord(
                    title=article["title"],
                    url=article["url"],
                    analysis=analysis,
                    source_name=article["source_name"],
                    category=category,
                    pub_date=article["pub_date"],
                )
                total_posted += 1

            time.sleep(ARTICLE_DELAY)

    if first_run:
        log.info("Seeded %d backlog articles (skipped without analysis)", total_seeded)
    log.info("Cycle complete — new: %d, posted: %d", total_new, total_posted)


def main() -> None:
    # Validate required environment variables
    missing = [v for v in ("ANTHROPIC_API_KEY", "DISCORD_WEBHOOK_URL") if not os.environ.get(v)]
    if missing:
        raise SystemExit(f"Missing required environment variables: {', '.join(missing)}")

    log.info("Market Pulse bot starting up (threshold=%d/10)", RELEVANCE_THRESHOLD)
    init_db()
    validate_feeds()

    while True:
        try:
            run_cycle()
        except Exception as exc:
            log.error("Unexpected error in run cycle: %s", exc)

        log.info("Sleeping 3 hours until next cycle...")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
