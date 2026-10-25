"""
News scraper: 10 sources x 1000 articles = 10,000 articles -> ONE csv file
==========================================================================

Why this version?  An RSS feed only holds the latest 10-50 items, so RSS alone
can never reach 1000 per source.  This script therefore works in layers:

  1. feedparser  : reads every RSS/Atom feed of the source (title, description,
                   author, date come straight from the feed)
  2. sitemaps    : if feeds give fewer than 1000 links, the site's sitemaps
                   (found automatically via robots.txt, newest first) supply
                   the rest of the article URLs
  3. article page: fetched for every URL that is missing data. This is where
                   the three source categories are handled:
       simple     -> plain requests, many threads
       captcha    -> a human solves ONE CAPTCHA in a visible Chrome window,
                     cookies are then reused for fast requests
       continuous -> every page opens in a real browser; the script pauses
                     whenever a challenge appears until a human solves it
  4. keep going  : candidates are processed in batches until the source really
                   has 1000 valid articles (failed pages are skipped, not counted)
  5. reserves    : if a source still ends short (blocked, no sitemap ...), spare
                   sources are scraped automatically to replace it
  6. final CSV   : the 10 best sources, max 1000 each, merged into one file

Output columns : title, description, date_of_publication, author_name
Date format    : YYYY-MM-DD (Asian/ISO).  Change DATE_FORMAT e.g. "%d-%m-%Y".

Install : pip install feedparser requests beautifulsoup4 lxml python-dateutil pandas selenium
Step 1  : python news_scraper_10k.py --check           (how many links each source can give)
Step 2  : python news_scraper_10k.py --only bbc --limit 30      (small test)
Step 3  : python news_scraper_10k.py                   (full run, safe to re-run: it resumes)

CAPTCHAs are never bypassed automatically - a human solves them. Respect each
site's Terms of Service; robots.txt is checked before page fetches.
"""

import argparse
import gzip
import json
import logging
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
from urllib import robotparser
from urllib.parse import urlparse

import feedparser
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------------------------------- #
# CONFIG
# --------------------------------------------------------------------------- #
ARTICLES_PER_SOURCE = 1000
TOP_N_SOURCES = 10              # final CSV = best 10 sources
OVERSAMPLE = 1.6                # collect 60% more candidate URLs than needed
BATCH = 50                      # articles processed per checkpoint
THREADS_PER_SOURCE = 10         # only for "simple" / cookie-based fetching
MAX_SITEMAPS = 60               # max sitemap files opened per source
DATE_FORMAT = "%Y-%m-%d"
UNKNOWN_AUTHOR = "Unknown"      # used when neither feed nor page names an author
OUTPUT_DIR = "data"
FINAL_CSV = "all_articles.csv"
HUMAN_WAIT_TIMEOUT = 600
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

FIELDS = ["title", "description", "date_of_publication", "author_name"]
COLS = FIELDS + ["url", "source"]

# category: "simple" | "captcha" | "continuous" -> what you see when opening ARTICLE pages
# optional keys: "sitemaps": [extra sitemap urls], "link_regex": regex for article urls
SOURCES = [
    {"name": "bbc", "category": "simple", "root": "https://www.bbc.com", "feeds": [
        "http://feeds.bbci.co.uk/news/rss.xml",
        "http://feeds.bbci.co.uk/news/world/rss.xml",
        "http://feeds.bbci.co.uk/news/world/asia/rss.xml",
        "http://feeds.bbci.co.uk/news/business/rss.xml",
        "http://feeds.bbci.co.uk/news/technology/rss.xml",
        "http://feeds.bbci.co.uk/news/health/rss.xml",
        "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml"]},
    {"name": "guardian", "category": "simple", "root": "https://www.theguardian.com", "feeds": [
        "https://www.theguardian.com/world/rss",
        "https://www.theguardian.com/business/rss",
        "https://www.theguardian.com/technology/rss",
        "https://www.theguardian.com/science/rss",
        "https://www.theguardian.com/environment/rss",
        "https://www.theguardian.com/politics/rss"]},
    {"name": "aljazeera", "category": "simple", "root": "https://www.aljazeera.com", "feeds": [
        "https://www.aljazeera.com/xml/rss/all.xml"]},
    {"name": "dawn", "category": "simple", "root": "https://www.dawn.com", "feeds": [
        "https://www.dawn.com/feeds/home",
        "https://www.dawn.com/feeds/latest-news",
        "https://www.dawn.com/feeds/pakistan",
        "https://www.dawn.com/feeds/world",
        "https://www.dawn.com/feeds/business"]},
    {"name": "thehindu", "category": "simple", "root": "https://www.thehindu.com", "feeds": [
        "https://www.thehindu.com/news/national/feeder/default.rss",
        "https://www.thehindu.com/news/international/feeder/default.rss",
        "https://www.thehindu.com/business/feeder/default.rss",
        "https://www.thehindu.com/sport/feeder/default.rss"]},
    {"name": "japantimes", "category": "captcha", "root": "https://www.japantimes.co.jp", "feeds": [
        "https://www.japantimes.co.jp/feed/",
        "https://www.japantimes.co.jp/news/feed/",
        "https://www.japantimes.co.jp/business/feed/"]},
    {"name": "scmp", "category": "captcha", "root": "https://www.scmp.com", "feeds": [
        "https://www.scmp.com/rss/91/feed",
        "https://www.scmp.com/rss/2/feed",
        "https://www.scmp.com/rss/3/feed",
        "https://www.scmp.com/rss/4/feed"]},
    {"name": "straitstimes", "category": "captcha", "root": "https://www.straitstimes.com", "feeds": [
        "https://www.straitstimes.com/news/world/rss.xml",
        "https://www.straitstimes.com/news/asia/rss.xml",
        "https://www.straitstimes.com/news/singapore/rss.xml",
        "https://www.straitstimes.com/news/business/rss.xml"]},
    {"name": "bloomberg", "category": "continuous", "root": "https://www.bloomberg.com", "feeds": [
        "https://feeds.bloomberg.com/markets/news.rss",
        "https://feeds.bloomberg.com/business/news.rss",
        "https://feeds.bloomberg.com/technology/news.rss",
        "https://feeds.bloomberg.com/politics/news.rss",
        "https://feeds.bloomberg.com/wealth/news.rss"]},
    {"name": "timesofindia", "category": "continuous", "root": "https://timesofindia.indiatimes.com", "feeds": [
        "https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
        "https://timesofindia.indiatimes.com/rssfeeds/296589292.cms",
        "https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms",
        "https://timesofindia.indiatimes.com/rssfeeds/1898055.cms"]},
]

# Spare sources: used automatically only if a primary source ends below the target.
RESERVE = [
    {"name": "tribune", "category": "simple", "root": "https://tribune.com.pk", "feeds": [
        "https://tribune.com.pk/feed/home", "https://tribune.com.pk/feed/pakistan",
        "https://tribune.com.pk/feed/world", "https://tribune.com.pk/feed/business"]},
    {"name": "hindustantimes", "category": "simple", "root": "https://www.hindustantimes.com", "feeds": [
        "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
        "https://www.hindustantimes.com/feeds/rss/world-news/rssfeed.xml",
        "https://www.hindustantimes.com/feeds/rss/business/rssfeed.xml"]},
    {"name": "dw", "category": "simple", "root": "https://www.dw.com", "feeds": [
        "https://rss.dw.com/rdf/rss-en-all", "https://rss.dw.com/rdf/rss-en-world",
        "https://rss.dw.com/rdf/rss-en-bus"]},
    {"name": "cna", "category": "simple", "root": "https://www.channelnewsasia.com", "feeds": [
        "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml"]},
    {"name": "npr", "category": "simple", "root": "https://www.npr.org", "feeds": [
        "https://feeds.npr.org/1001/rss.xml", "https://feeds.npr.org/1004/rss.xml"]},
]

CHALLENGE_MARKERS = [
    "captcha", "are you a robot", "verify you are human", "verifying you are human",
    "just a moment", "unusual traffic", "press & hold", "access denied",
    "checking your browser", "cf-challenge",
]
EXCLUDE_URL = re.compile(
    r"/(tag|tags|topic|topics|author|authors|video|videos|gallery|photos|podcast|"
    r"podcasts|live|search|category|profile|section|subscribe)/|\.(jpg|jpeg|png|gif|pdf|xml)$",
    re.I)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(processName)s] %(message)s")
log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# HELPERS
# --------------------------------------------------------------------------- #
def make_session():
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"})
    retry = Retry(total=3, backoff_factor=1.5,
                  status_forcelist=[500, 502, 503, 504], allowed_methods=["GET"])
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def is_challenge(html, status=200):
    if status in (403, 429, 503) or not html:
        return True
    soup = BeautifulSoup(html[:30000], "lxml")
    title = soup.title.string.lower() if soup.title and soup.title.string else ""
    visible = soup.get_text(" ", strip=True)[:1500].lower()
    return any(m in title or m in visible for m in CHALLENGE_MARKERS)


_robots = {}


def allowed_by_robots(url):
    """robots.txt check (fetched with requests + timeout; a 403/404 on robots.txt
    means 'no rules', unlike urllib's RobotFileParser.read() which treats 403 as
    'everything disallowed' and would silently block every article)."""
    p = urlparse(url)
    root = f"{p.scheme}://{p.netloc}"
    if root not in _robots:
        rp = None
        try:
            r = requests.get(root + "/robots.txt", headers={"User-Agent": USER_AGENT}, timeout=10)
            if r.status_code == 200:
                rp = robotparser.RobotFileParser()
                rp.parse(r.text.splitlines())
        except requests.RequestException:
            pass
        _robots[root] = rp
    rp = _robots[root]
    return True if rp is None else rp.can_fetch(USER_AGENT, url)


def clean_text(html):
    return BeautifulSoup(html or "", "lxml").get_text(" ", strip=True)


def fmt_date(raw):
    if not raw:
        return ""
    try:
        return dtparser.parse(raw).strftime(DATE_FORMAT)
    except Exception:
        return ""


def blank_row(url, lastmod="", source=""):
    return {"title": "", "description": "", "date_of_publication": "",
            "author_name": "", "url": url, "source": source, "_lastmod": lastmod}


# --------------------------------------------------------------------------- #
# LAYER 1: RSS FEEDS (feedparser)
# --------------------------------------------------------------------------- #
def entry_to_row(e, source):
    author = e.get("author", "")
    if not author and e.get("authors"):
        author = ", ".join(a.get("name", "") for a in e["authors"] if a.get("name"))
    if e.get("published_parsed"):
        date = time.strftime(DATE_FORMAT, e["published_parsed"])
    elif e.get("updated_parsed"):
        date = time.strftime(DATE_FORMAT, e["updated_parsed"])
    else:
        date = fmt_date(e.get("published") or e.get("updated"))
    row = blank_row(e.get("link", ""), source=source)
    row.update({
        "title": clean_text(e.get("title", "")),
        "description": clean_text(e.get("summary") or e.get("description", "")),
        "date_of_publication": date,
        "author_name": clean_text(author),
    })
    return row


def read_feed(session, url):
    try:
        r = session.get(url, timeout=20)
        if r.status_code != 200:
            log.warning("feed %s -> HTTP %s", url, r.status_code)
            return []
        d = feedparser.parse(r.content)
        if d.bozo and not d.entries:
            log.warning("feed %s could not be parsed", url)
        return d.entries
    except requests.RequestException as e:
        log.warning("feed %s failed: %s", url, e)
        return []


def collect_feed_rows(cfg, session):
    rows, seen = [], set()
    for url in cfg["feeds"]:
        entries = read_feed(session, url)
        log.info("[%s] feed %s -> %d entries", cfg["name"], url, len(entries))
        for e in entries:
            row = entry_to_row(e, cfg["name"])
            if row["url"] and row["title"] and row["url"] not in seen:
                seen.add(row["url"])
                rows.append(row)
        time.sleep(random.uniform(0.2, 0.6))
    return rows


# --------------------------------------------------------------------------- #
# LAYER 2: SITEMAPS
# --------------------------------------------------------------------------- #
def make_link_filter(cfg):
    if cfg.get("link_regex"):
        rx = re.compile(cfg["link_regex"])
        return lambda u: bool(rx.search(u))
    domain = urlparse(cfg["root"]).netloc.replace("www.", "")

    def looks_like_article(u):
        p = urlparse(u)
        if domain not in p.netloc:
            return False
        path = p.path
        return (len(path) >= 25 and ("-" in path or any(c.isdigit() for c in path))
                and not EXCLUDE_URL.search(u))
    return looks_like_article


def discover_sitemaps(cfg, raw_get):
    found = list(cfg.get("sitemaps", []))
    data = raw_get(cfg["root"] + "/robots.txt")
    if data:
        text = data.decode("utf-8", "ignore")
        found += re.findall(r"(?im)^\s*sitemap:\s*(\S+)", text)
    return list(dict.fromkeys(found)) or [cfg["root"] + "/sitemap.xml"]


def collect_sitemap_entries(cfg, raw_get, want, max_sitemaps=MAX_SITEMAPS):
    """Return [(url, lastmod)] newest first, from sitemap indexes (recursive)."""
    link_ok = make_link_filter(cfg)
    queue = [(u, "") for u in discover_sitemaps(cfg, raw_get)]
    entries, opened = {}, 0
    while queue and opened < max_sitemaps and len(entries) < want:
        queue.sort(key=lambda x: x[1], reverse=True)         # newest sitemap first
        url, _ = queue.pop(0)
        data = raw_get(url)
        opened += 1
        if not data:
            continue
        try:
            if data[:2] == b"\x1f\x8b":
                data = gzip.decompress(data)
            soup = BeautifulSoup(data, "xml")
        except Exception:
            continue
        if soup.find("sitemapindex"):
            for sm in soup.find_all("sitemap"):
                if sm.loc:
                    queue.append((sm.loc.text.strip(),
                                  sm.lastmod.text.strip() if sm.lastmod else ""))
        else:
            for u in soup.find_all("url"):
                if not u.loc:
                    continue
                loc = u.loc.text.strip()
                if loc not in entries and link_ok(loc):
                    entries[loc] = u.lastmod.text.strip() if u.lastmod else ""
    out = sorted(entries.items(), key=lambda x: x[1], reverse=True)
    return out


# --------------------------------------------------------------------------- #
# LAYER 3: ARTICLE-PAGE FETCHERS (the three categories)
# --------------------------------------------------------------------------- #
def make_driver():
    from selenium import webdriver
    opts = webdriver.ChromeOptions()
    opts.add_argument(f"--user-agent={USER_AGENT}")
    opts.add_argument("--window-size=1200,900")
    return webdriver.Chrome(options=opts)            # Selenium Manager gets the driver


def wait_for_human(driver, name):
    start = time.time()
    if is_challenge(driver.page_source):
        log.warning("[%s] Challenge detected - please solve it in the browser window...", name)
    while is_challenge(driver.page_source):
        if time.time() - start > HUMAN_WAIT_TIMEOUT:
            raise TimeoutError("CAPTCHA not solved in time")
        time.sleep(3)


def browser_fetch_text(driver, url):
    """Download a text file (e.g. sitemap) from inside the verified browser session."""
    js = ("const cb = arguments[arguments.length-1];"
          "fetch(arguments[0], {credentials:'include'})"
          ".then(r => r.ok ? r.text() : null).then(cb).catch(() => cb(null));")
    try:
        driver.set_script_timeout(60)
        txt = driver.execute_async_script(js, url)
        return txt.encode("utf-8") if txt else None
    except Exception:
        return None


class SimpleFetcher:
    """Category 1: plain requests."""
    def __init__(self, cfg):
        self.name = cfg["name"]
        self.session = make_session()

    def get(self, url):
        try:
            r = self.session.get(url, timeout=20)
            return None if is_challenge(r.text, r.status_code) else r.text
        except requests.RequestException:
            return None

    def get_raw(self, url):
        try:
            r = self.session.get(url, timeout=30)
            return r.content if r.status_code == 200 else None
        except requests.RequestException:
            return None

    def close(self):
        self.session.close()


class CaptchaFetcher(SimpleFetcher):
    """Category 2: human verifies once in Chrome, cookies reused with requests."""
    def __init__(self, cfg):
        super().__init__(cfg)
        self.driver = make_driver()
        self.driver.get(cfg["root"])
        wait_for_human(self.driver, self.name)
        self._sync()

    def _sync(self):
        for c in self.driver.get_cookies():
            self.session.cookies.set(c["name"], c["value"], domain=c.get("domain"))

    def get(self, url):
        html = super().get(url)
        if html:
            return html
        self.driver.get(url)                         # challenge is back -> human again
        wait_for_human(self.driver, self.name)
        self._sync()
        return self.driver.page_source

    def get_raw(self, url):
        return super().get_raw(url) or browser_fetch_text(self.driver, url)

    def close(self):
        super().close()
        self.driver.quit()


class ContinuousFetcher:
    """Category 3: every page in a real browser; pause on every challenge."""
    def __init__(self, cfg):
        self.name = cfg["name"]
        self.driver = make_driver()
        self.driver.get(cfg["root"])
        wait_for_human(self.driver, self.name)

    def get(self, url):
        try:
            self.driver.get(url)
            time.sleep(random.uniform(2, 4))
            wait_for_human(self.driver, self.name)
            return self.driver.page_source
        except Exception as e:
            log.warning("[%s] browser error on %s: %s", self.name, url, e)
            return None

    def get_raw(self, url):
        return browser_fetch_text(self.driver, url)

    def close(self):
        self.driver.quit()


FETCHERS = {"simple": SimpleFetcher, "captcha": CaptchaFetcher,
            "continuous": ContinuousFetcher}


def parse_page(html):
    """Extract fields from an article page (JSON-LD first, then meta tags)."""
    soup = BeautifulSoup(html, "lxml")
    out = {f: "" for f in FIELDS}

    def author_str(a):
        if isinstance(a, str):
            return a
        if isinstance(a, dict):
            return str(a.get("name", ""))
        if isinstance(a, list):
            return ", ".join(filter(None, (author_str(x) for x in a)))
        return ""

    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        items = data if isinstance(data, list) else (
            data.get("@graph", [data]) if isinstance(data, dict) else [])
        for it in items:
            if isinstance(it, dict) and "Article" in str(it.get("@type", "")):
                out["title"] = out["title"] or str(it.get("headline", ""))
                out["description"] = out["description"] or str(it.get("description", ""))
                out["date_of_publication"] = out["date_of_publication"] or fmt_date(
                    str(it.get("datePublished", "")))
                out["author_name"] = out["author_name"] or author_str(it.get("author"))

    def meta(*names):
        for n in names:
            el = soup.find("meta", attrs={"property": n}) or soup.find("meta", attrs={"name": n})
            if el and el.get("content"):
                return el["content"].strip()
        return ""

    out["title"] = out["title"] or meta("og:title", "twitter:title") or (
        soup.title.string.strip() if soup.title and soup.title.string else "")
    out["description"] = out["description"] or meta("og:description", "description")
    out["date_of_publication"] = out["date_of_publication"] or fmt_date(
        meta("article:published_time", "datePublished", "pubdate", "date"))
    if not out["date_of_publication"]:
        t = soup.find("time")
        if t:
            out["date_of_publication"] = fmt_date(t.get("datetime") or t.get_text(strip=True))
    out["author_name"] = out["author_name"] or meta("author", "article:author", "byl")
    return out


# --------------------------------------------------------------------------- #
# ONE SOURCE (runs in its own process)
# --------------------------------------------------------------------------- #
def needs_page(row, mode):
    if not row["title"]:
        return True                       # sitemap-only rows must be fetched
    if mode == "all":
        return True
    if mode == "missing":
        return not all(row[f] for f in FIELDS)
    return False


def scrape_source(cfg):
    name, target = cfg["name"], cfg["limit"]
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"{name}.csv")

    rows, done = [], set()                                   # resume support
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        try:
            old = pd.read_csv(out_path).fillna("")
            rows, done = old.to_dict("records"), set(old["url"])
            log.info("[%s] resuming with %d saved articles", name, len(rows))
        except Exception:
            pass

    session = make_session()
    holder = {"fetcher": None}

    def get_fetcher():
        if holder["fetcher"] is None:
            log.info("[%s] starting %s fetcher", name, cfg["category"])
            holder["fetcher"] = FETCHERS[cfg["category"]](cfg)
        return holder["fetcher"]

    def raw_get(url):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 200 and r.content:
                return r.content
        except requests.RequestException:
            pass
        if cfg["category"] != "simple":
            return get_fetcher().get_raw(url)                # use verified browser/cookies
        return None

    pool = ThreadPoolExecutor(THREADS_PER_SOURCE) if cfg["category"] in ("simple", "captcha") else None
    try:
        # ---- candidates: feeds first, then sitemaps ----
        cands = [r for r in collect_feed_rows(cfg, session) if r["url"] not in done]
        need = target - len(rows)
        if len(cands) < need * OVERSAMPLE:
            ents = collect_sitemap_entries(cfg, raw_get, int(need * OVERSAMPLE))
            seen = done | {c["url"] for c in cands}
            cands += [blank_row(u, lm, name) for u, lm in ents if u not in seen]
            log.info("[%s] sitemaps gave %d article urls", name, len(ents))
        log.info("[%s] %d candidates for %d missing articles", name, len(cands), need)

        # ---- process in batches until the target is really reached ----
        for i in range(0, len(cands), BATCH):
            if len(rows) >= target:
                break
            batch = cands[i:i + BATCH]
            todo = [r for r in batch if needs_page(r, cfg["enrich"])]
            if todo:
                fetcher = get_fetcher()

                def enrich(row):
                    if not allowed_by_robots(row["url"]):
                        return
                    time.sleep(random.uniform(0.2, 0.8))
                    html = fetcher.get(row["url"])
                    if html:
                        page = parse_page(html)
                        for f in FIELDS:
                            row[f] = row[f] or page[f]

                # threads are safe for plain requests; the browser is not thread-safe
                if pool and cfg["category"] == "simple":
                    list(pool.map(enrich, todo))
                else:
                    for r in todo:
                        enrich(r)

            for r in batch:
                if not r["title"] or r["url"] in done or len(rows) >= target:
                    continue
                r["date_of_publication"] = r["date_of_publication"] or fmt_date(r.get("_lastmod", ""))
                r["author_name"] = r["author_name"] or UNKNOWN_AUTHOR
                r["source"] = name
                rows.append(r)
                done.add(r["url"])
            pd.DataFrame(rows, columns=COLS).to_csv(out_path, index=False, encoding="utf-8-sig")
            log.info("[%s] %d/%d articles", name, len(rows), target)
    except Exception as e:
        log.error("[%s] stopped by error: %s", name, e)
    finally:
        if pool:
            pool.shutdown()
        session.close()
        if holder["fetcher"]:
            holder["fetcher"].close()
        if rows:
            pd.DataFrame(rows, columns=COLS).to_csv(out_path, index=False, encoding="utf-8-sig")
        flag = "OK" if len(rows) >= target else "SHORT"
        log.info("[%s] finished: %d/%d articles (%s)", name, len(rows), target, flag)
    return {"name": name, "path": out_path, "count": len(rows)}


# --------------------------------------------------------------------------- #
# CHECK, MERGE, MAIN
# --------------------------------------------------------------------------- #
def check_sources(sources, limit):
    """Dry run: how many article links can each source give (plain requests only)?"""
    s = make_session()

    def raw_get(url):
        try:
            r = s.get(url, timeout=30)
            return r.content if r.status_code == 200 else None
        except requests.RequestException:
            return None

    print(f"\n{'source':<15}{'category':<12}{'feeds':>7}{'sitemap':>9}{'total':>8}  verdict")
    for cfg in sources:
        feed_urls = {r["url"] for r in collect_feed_rows(cfg, s)}
        sm = collect_sitemap_entries(cfg, raw_get, limit * 2, max_sitemaps=8)
        total = len(feed_urls | {u for u, _ in sm})
        if total >= limit:
            verdict = "OK"
        elif total == 0 and cfg["category"] != "simple":
            verdict = "blocked for plain requests (browser will be used)"
        else:
            verdict = "SHORT - add feeds/sitemaps or use a reserve"
        print(f"{cfg['name']:<15}{cfg['category']:<12}{len(feed_urls):>7}{len(sm):>9}{total:>8}  {verdict}")


def merge_to_csv(results, limit, top_n):
    counts = sorted(results, key=lambda r: r["count"], reverse=True)[:top_n]
    frames = []
    for r in counts:
        if r["count"] == 0:
            continue
        df = pd.read_csv(r["path"]).drop_duplicates(subset=["url"])
        df["_d"] = pd.to_datetime(df["date_of_publication"], errors="coerce")
        frames.append(df.sort_values("_d", ascending=False).head(limit))
    if not frames:
        log.error("No data scraped.")
        return
    final = pd.concat(frames, ignore_index=True)
    final = final.sort_values("_d", ascending=False)             # latest first
    final[FIELDS].to_csv(FINAL_CSV, index=False, encoding="utf-8-sig")

    print("\n=========== REPORT ===========")
    for r in sorted(results, key=lambda r: r["count"], reverse=True):
        used = "used" if r in counts else "not used"
        print(f"{r['name']:<15}{r['count']:>6} articles   ({used})")
    print(f"\nFINAL CSV: {FINAL_CSV}  ->  {len(final)} articles "
          f"(target {limit * top_n})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="comma-separated source names (primary or reserve)")
    ap.add_argument("--limit", type=int, default=ARTICLES_PER_SOURCE, help="articles per source")
    ap.add_argument("--enrich", choices=["none", "missing", "all"], default="missing",
                    help="fetch article pages: never / when a field is missing / always")
    ap.add_argument("--no-reserve", action="store_true", help="do not use reserve sources")
    ap.add_argument("--check", action="store_true", help="dry run: count reachable links")
    args = ap.parse_args()

    primaries = SOURCES
    if args.only:
        wanted = {n.strip() for n in args.only.split(",")}
        primaries = [s for s in SOURCES + RESERVE if s["name"] in wanted]

    if args.check:
        check_sources(primaries + ([] if args.only else RESERVE), args.limit)
        raise SystemExit

    def prep(lst):
        return [{**s, "limit": args.limit, "enrich": args.enrich} for s in lst]

    with Pool(processes=len(primaries)) as p:                    # all sources at once
        results = p.map(scrape_source, prep(primaries))

    short = [r for r in results if r["count"] < args.limit]
    if short and not args.only and not args.no_reserve:
        spare = RESERVE[:len(short)]
        log.warning("%d source(s) ended short -> scraping reserves: %s",
                    len(short), ", ".join(s["name"] for s in spare))
        with Pool(processes=len(spare)) as p:
            results += p.map(scrape_source, prep(spare))

    merge_to_csv(results, args.limit, TOP_N_SOURCES if not args.only else len(primaries))
