#!/usr/bin/env python3
"""
Spotify artist crawler driven by Playwright only.

What it does
------------
- Starts from a seeded artist page (URL or artist name search).
- Visits each artist page with Playwright.
- Saves the artist name + monthly listeners to SQLite.
- Collects every other artist link on the page.
- Separately captures links from the "Fans also like" section when present.
- Queues newly discovered artists while preventing duplicates.

Notes
-----
- This intentionally removes the Spotify Web API dependency.
- It uses locator-based Playwright calls instead of query_selector/query_selector_all.
"""

from __future__ import annotations

import asyncio
import json
import random
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional
from urllib.parse import quote, urlsplit

from playwright.async_api import BrowserContext, Page, async_playwright
from playwright_stealth import Stealth

# ----------------- CONFIGURATION -----------------
SEED_ARTIST_NAME = "SOLIS4EVR"
SEED_ARTIST_URL: Optional[str] = (
    "https://open.spotify.com/artist/3ZbW5RPoVdTdsR7JmRtoms"
)
MAX_ARTISTS_TO_PROCESS = 250
SCRAPE_DELAY_MIN = 5
SCRAPE_DELAY_MAX = 15
DB_PATH = "spotify_playwright_crawler.db"
MAX_MONTHLY_LISTENERS = 10_000
SPOTIFY_BASE = "https://open.spotify.com"
# -------------------------------------------------

ARTIST_URL_RE = re.compile(r"https?://open\.spotify\.com/artist/([A-Za-z0-9]+)")
MONTHLY_LISTENERS_RE = re.compile(
    r"([\d][\d,\.\s]*)\s+monthly listeners", re.IGNORECASE
)


@dataclass(frozen=True)
class ArtistLink:
    id: str
    name: str
    url: str


# ----------------- DATABASE (SQLite) -----------------
def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS artists (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            source_url TEXT NOT NULL,
            monthly_listeners INTEGER,
            last_updated TIMESTAMP,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS artist_queue (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS artist_edges (
            from_artist_id TEXT NOT NULL,
            to_artist_id TEXT NOT NULL,
            edge_type TEXT NOT NULL,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (from_artist_id, to_artist_id, edge_type)
        )
        """
    )

    cur.execute(
        "UPDATE artist_queue SET status = 'pending' WHERE status = 'processing'"
    )

    conn.commit()
    conn.close()


def add_to_queue(artist_id: str, artist_name: str, artist_url: str) -> bool:
    """Add an artist if it is neither already stored nor already queued."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM artists WHERE id = ?", (artist_id,))
    already_saved = cur.fetchone() is not None

    cur.execute("SELECT 1 FROM artist_queue WHERE id = ?", (artist_id,))
    already_queued = cur.fetchone() is not None

    added = False
    if not already_saved and not already_queued:
        cur.execute(
            "INSERT INTO artist_queue (id, name, url) VALUES (?, ?, ?)",
            (artist_id, artist_name, artist_url),
        )
        added = True

    conn.commit()
    conn.close()
    return added


def get_next_from_queue() -> Optional[tuple[str, str, str]]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        "SELECT id, name, url FROM artist_queue "
        "WHERE status = 'pending' "
        "ORDER BY added_at ASC LIMIT 1"
    )
    row = cur.fetchone()

    if row:
        cur.execute(
            "UPDATE artist_queue SET status = 'processing' WHERE id = ?",
            (row[0],),
        )
        conn.commit()

    conn.close()
    return row


def mark_queue_done(artist_id: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE artist_queue SET status = 'done' WHERE id = ?", (artist_id,))
    conn.commit()
    conn.close()


def mark_queue_failed(artist_id: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE artist_queue SET status = 'failed' WHERE id = ?", (artist_id,))
    conn.commit()
    conn.close()


def save_artist_data(
    artist_id: str, name: str, url: str, monthly_listeners: Optional[int]
) -> None:
    if monthly_listeners is None or monthly_listeners > MAX_MONTHLY_LISTENERS:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("DELETE FROM artists WHERE id = ?", (artist_id,))
        purged = cur.rowcount
        conn.commit()
        conn.close()
        reason = (
            "unknown listener count"
            if monthly_listeners is None
            else f"{monthly_listeners} > {MAX_MONTHLY_LISTENERS}"
        )
        action = "purged existing row" if purged else "skipped"
        print(f"   🚫 {action} for {name} ({reason})")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO artists (id, name, source_url, monthly_listeners, last_updated)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name = excluded.name,
            source_url = excluded.source_url,
            monthly_listeners = excluded.monthly_listeners,
            last_updated = excluded.last_updated
        """,
        (
            artist_id,
            name,
            url,
            monthly_listeners,
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    conn.commit()
    conn.close()


def save_edge(from_artist_id: str, to_artist_id: str, edge_type: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO artist_edges (from_artist_id, to_artist_id, edge_type)
        VALUES (?, ?, ?)
        """,
        (from_artist_id, to_artist_id, edge_type),
    )
    conn.commit()
    conn.close()


def get_queue_size() -> int:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM artist_queue WHERE status IN ('pending', 'processing')"
    )
    count = cur.fetchone()[0]
    conn.close()
    return count


# ----------------- HELPERS -----------------
def extract_artist_id(url: str) -> Optional[str]:
    match = ARTIST_URL_RE.search(url)
    return match.group(1) if match else None


def canonical_artist_url(url: str) -> Optional[str]:
    artist_id = extract_artist_id(url)
    if not artist_id:
        return None
    return f"{SPOTIFY_BASE}/artist/{artist_id}"


def parse_monthly_listeners(text: str) -> Optional[int]:
    match = MONTHLY_LISTENERS_RE.search(text)
    if not match:
        return None
    raw = re.sub(r"[^\d]", "", match.group(1))
    return int(raw) if raw else None


def clean_name(text: str, fallback: str) -> str:
    value = re.sub(r"\s+", " ", (text or "")).strip()
    return value or fallback


async def extract_artist_name(page: Page, fallback: str) -> str:
    """Read the artist name from Spotify's main content H1, not the sidebar."""
    main_scope = page.locator("main, [role='main']").first
    candidates = [
        main_scope.locator('h1[data-encore-id="text"]'),
        main_scope.locator("h1"),
    ]

    for locator in candidates:
        try:
            await locator.first.wait_for(state="visible", timeout=10000)
            text = await locator.first.text_content()
            cleaned = clean_name(text or "", "")
            if cleaned and cleaned.casefold() != "your library":
                return cleaned
        except Exception:
            continue

    return fallback


def dedupe_artist_links(
    links: Iterable[ArtistLink], current_artist_id: str
) -> list[ArtistLink]:
    unique: dict[str, ArtistLink] = {}
    for link in links:
        if not link.id or link.id == current_artist_id:
            continue
        if link.id not in unique:
            unique[link.id] = link
    return list(unique.values())


# ----------------- PLAYWRIGHT SCRAPING -----------------
async def resolve_seed_artist(context: BrowserContext) -> ArtistLink:
    if SEED_ARTIST_URL:
        canonical = canonical_artist_url(SEED_ARTIST_URL)
        if not canonical:
            raise RuntimeError(f"Invalid SEED_ARTIST_URL: {SEED_ARTIST_URL}")
        artist_id = extract_artist_id(canonical)
        if not artist_id:
            raise RuntimeError(f"Could not extract artist ID from: {canonical}")
        page = await context.new_page()
        try:
            data = await scrape_artist_page(page, canonical)
            return ArtistLink(id=artist_id, name=data["name"], url=canonical)
        finally:
            await page.close()

    search_page = await context.new_page()
    try:
        search_url = f"{SPOTIFY_BASE}/search/{quote(SEED_ARTIST_NAME)}"
        await search_page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
        await search_page.locator("body").wait_for(state="visible", timeout=15000)
        await search_page.wait_for_timeout(2500)

        search_links = await collect_artist_links_from_locator(
            search_page.locator('a[href*="/artist/"]'),
            exclude_artist_id=None,
        )
        if not search_links:
            raise RuntimeError(f"Could not find seed artist named {SEED_ARTIST_NAME!r}")

        for candidate in search_links:
            if candidate.name.casefold() == SEED_ARTIST_NAME.casefold():
                return candidate
        return search_links[0]
    finally:
        await search_page.close()


async def collect_artist_links_from_locator(
    locator,
    exclude_artist_id: Optional[str],
) -> list[ArtistLink]:
    raw = await locator.evaluate_all(
        """
        (nodes) => nodes.map((a) => ({
            href: a.href || '',
            text: (a.textContent || '').trim()
        }))
        """
    )

    links: list[ArtistLink] = []
    for item in raw:
        href = item.get("href", "")
        artist_id = extract_artist_id(href)
        if not artist_id:
            continue
        if exclude_artist_id and artist_id == exclude_artist_id:
            continue

        canonical = canonical_artist_url(href)
        if not canonical:
            continue

        links.append(
            ArtistLink(
                id=artist_id,
                name=clean_name(item.get("text", ""), artist_id),
                url=canonical,
            )
        )

    if exclude_artist_id is None:
        return list({link.id: link for link in links}.values())
    return dedupe_artist_links(links, exclude_artist_id)


async def scrape_artist_page(page: Page, artist_url: str) -> dict[str, object]:
    await page.goto(artist_url, wait_until="domcontentloaded", timeout=45000)
    await page.locator("body").wait_for(state="visible", timeout=15000)
    await page.wait_for_timeout(2500)

    canonical = canonical_artist_url(page.url) or canonical_artist_url(artist_url)
    if not canonical:
        raise RuntimeError(f"Could not resolve artist URL for {artist_url}")

    artist_id = extract_artist_id(canonical)
    if not artist_id:
        raise RuntimeError(f"Could not extract artist ID for {canonical}")

    artist_name = await extract_artist_name(page, artist_id)

    listener_texts = await page.get_by_text(
        re.compile(r"monthly listeners", re.I)
    ).all_text_contents()
    monthly_listeners = None
    for text in listener_texts:
        monthly_listeners = parse_monthly_listeners(text)
        if monthly_listeners is not None:
            break

    if monthly_listeners is None:
        body_text = await page.locator("body").text_content() or ""
        monthly_listeners = parse_monthly_listeners(body_text)

    page_links = await collect_artist_links_from_locator(
        page.locator('a[href*="/artist/"]'),
        exclude_artist_id=artist_id,
    )

    related_links: list[ArtistLink] = []
    related_url = f"{canonical}/related"
    try:
        await page.goto(related_url, wait_until="domcontentloaded", timeout=45000)
        await page.locator("body").wait_for(state="visible", timeout=15000)
        await page.wait_for_timeout(2500)
        related_links = await collect_artist_links_from_locator(
            page.locator('a[href*="/artist/"]'),
            exclude_artist_id=artist_id,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"   ⚠️  Could not load related page ({related_url}): {exc}")

    return {
        "id": artist_id,
        "name": artist_name,
        "url": canonical,
        "monthly_listeners": monthly_listeners,
        "page_links": page_links,
        "related_links": related_links,
    }


# ----------------- MAIN CRAWL LOOP -----------------
async def crawl() -> None:
    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context()

        try:
            print("🔍 Resolving seed artist...")
            seed = await resolve_seed_artist(context)
            add_to_queue(seed.id, seed.name, seed.url)
            print(f"🌱 Seeded queue with: {seed.name} ({seed.id})")

            processed = 0
            page = await context.new_page()
            try:
                while processed < MAX_ARTISTS_TO_PROCESS:
                    if get_queue_size() == 0:
                        print("✅ Queue empty. Crawl finished.")
                        break

                    next_artist = get_next_from_queue()
                    if not next_artist:
                        print("⏳ No pending artists. Waiting 30s...")
                        await asyncio.sleep(30)
                        continue

                    artist_id, queued_name, artist_url = next_artist
                    print(
                        f"\n🎤 [{processed + 1}/{MAX_ARTISTS_TO_PROCESS}] "
                        f"{queued_name} ({artist_id})"
                    )

                    try:
                        data = await scrape_artist_page(page, artist_url)
                    except Exception as exc:  # noqa: BLE001
                        print(f"   ❌ Scrape failed: {exc}")
                        mark_queue_failed(artist_id)
                        continue

                    monthly = data["monthly_listeners"]
                    print(
                        f"   📊 Monthly listeners: {monthly if monthly is not None else 'N/A'}"
                    )

                    save_artist_data(
                        artist_id=data["id"],
                        name=data["name"],
                        url=data["url"],
                        monthly_listeners=monthly,
                    )
                    mark_queue_done(artist_id)
                    processed += 1

                    page_links: list[ArtistLink] = data["page_links"]
                    related_links: list[ArtistLink] = data["related_links"]

                    print(f"   🔗 Found {len(page_links)} other artist links on page")
                    for link in page_links:
                        save_edge(data["id"], link.id, "page_link")
                        if add_to_queue(link.id, link.name, link.url):
                            print(f"      ➕ Queued page link: {link.name}")

                    print(f"   🤝 Found {len(related_links)} related artists")
                    for link in related_links:
                        save_edge(data["id"], link.id, "related_artist")
                        if add_to_queue(link.id, link.name, link.url):
                            print(f"      ➕ Queued related artist: {link.name}")

                    wait = random.uniform(SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX)
                    print(f"   ⏳ Waiting {wait:.1f}s before next artist...")
                    await asyncio.sleep(wait)
            finally:
                await page.close()
        finally:
            await context.close()
            await browser.close()

    print("\n🏁 Crawl complete.")


def export_artist_summary_json(output_path: str = "artists_export.json") -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, source_url, monthly_listeners, last_updated, first_seen FROM artists"
    )
    rows = cur.fetchall()
    conn.close()

    payload = [
        {
            "id": artist_id,
            "name": name,
            "source_url": source_url,
            "monthly_listeners": monthly_listeners,
            "last_updated": last_updated,
            "first_seen": first_seen,
        }
        for artist_id, name, source_url, monthly_listeners, last_updated, first_seen in rows
    ]

    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


if __name__ == "__main__":
    init_db()
    asyncio.run(crawl())
