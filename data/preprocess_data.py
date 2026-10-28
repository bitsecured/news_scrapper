"""Clean a scraped news CSV (title, description, date_of_publication, author_name, url, source).

Usage:
    python preprocess_news.py bbc.csv                 -> bbc_clean.csv
    python preprocess_news.py bbc.csv -o out.csv
    python preprocess_news.py bbc.csv --no-xlsx       (CSV only)
    python preprocess_news.py bbc.csv --date-format "%Y-%m-%d"   (keep ISO dates, e.g. for PostgreSQL)

What it does
  title / description : fixes spacing, capitalises the first letter (Latin script only)
  description         : same cleaning as title (no word limit)
  date_of_publication : day/month/year  (31/12/2026)
  author_name         : names only, tidy list "Name One, Name Two"
  url                 : valid http(s) URL, tracking parameters removed
  source              : bbc -> BBC
"""
import argparse
import os
import re
import unicodedata
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import pandas as pd

DATE_OUT = "%d/%m/%Y"
UNKNOWN_AUTHOR = "Unknown"
ACRONYMS = {"bbc": "BBC", "cnn": "CNN", "npr": "NPR", "dw": "DW", "cbsnews": "CBS News",
            "nbcnews": "NBC News", "foxnews": "Fox News", "abcnews_au": "ABC News Australia",
            "techcrunch": "TechCrunch", "theverge": "The Verge", "guardian": "The Guardian",
            "tribune": "Express Tribune", "independent": "The Independent", "wired": "Wired"}

TRACKING = re.compile(r"^(utm_|at_|fbclid$|gclid$|ocid$|ref$|ref_src$)", re.I)
LATIN = re.compile(r"[A-Za-zÀ-ɏ]")                  # Latin letters incl. accented
JUNK_CHARS = re.compile(r"[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F​﻿]")
# ‌ (zero-width non-joiner) is kept on purpose: Persian / Urdu need it to spell correctly.


# --------------------------------------------------------------------------- text
MOJIBAKE = re.compile(r"[\u00C2\u00C3\u00E2][\u0080-\u00BF\u20AC\u2122\u0153\u2018-\u201E\u2020-\u2022\u2026\u2030\u2039\u203A\u0160\u0161\u017D\u017E\u0178\u02C6\u02DC]")


def fix_mojibake(s):
    """'SgrÃ¹dadh' -> 'Sgrùdadh' (UTF-8 text that was once read as Windows-1252)."""
    if not MOJIBAKE.search(s):
        return s
    try:
        raw = b"".join(c.encode("cp1252") if c.encode("cp1252", "ignore") else c.encode("latin-1")
                       for c in s)
        fixed = raw.decode("utf-8")
        return fixed if len(fixed) < len(s) else s
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


def fix_spacing(s):
    s = unicodedata.normalize("NFC", fix_mojibake(str(s or "")))
    s = JUNK_CHARS.sub("", s)
    s = re.sub(r"[\s  -   　]+", " ", s)   # all whitespace/newlines -> 1 space
    s = re.sub(r"\s+([,.;:!?])", r"\1", s)                               # "word ," -> "word,"
    s = re.sub(r"(?<=[A-Za-zÀ-ɏ])([,;])(?=[A-Za-zÀ-ɏ])", r"\1 ", s)  # "a,b" -> "a, b"
    s = re.sub(r"([(\[{])\s+", r"\1", s)                                 # "( word" -> "(word"
    s = re.sub(r"\s+([)\]}])", r"\1", s)                                 # "word )" -> "word)"
    s = re.sub(r"([.!?])\1{3,}", r"\1\1\1", s)                           # "!!!!!" -> "!!!"
    return s.strip()


def capitalize_start(s):
    """Upper-case the first letter, only when that letter is Latin. Other scripts have no case."""
    for i, ch in enumerate(s):
        if ch.isalpha():
            return s[:i] + ch.upper() + s[i + 1:] if LATIN.match(ch) else s
        if ch.isdigit():
            return s
    return s


def capitalize_sentences(s):
    """Capital letter after . ! ? (Latin only) and for the lone pronoun 'i'."""
    s = capitalize_start(s)
    s = re.sub(r"([.!?]\s+)([a-zà-ÿ])", lambda m: m.group(1) + m.group(2).upper(), s)
    s = re.sub(r"\bi\b(?=['’ ]|$)", "I", s) if re.search(r"\b(the|and|is|was|that)\b", s) else s
    return s


def clean_title(s):
    return capitalize_start(fix_spacing(s))


def clean_description(s):
    return capitalize_sentences(fix_spacing(s))


# --------------------------------------------------------------------------- other columns
def clean_date(v, out_fmt):
    v = str(v or "").strip()
    if not v or v.lower() in ("nan", "nat", "none", "#name?"):
        return ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(v[:len(datetime.now().strftime(fmt))], fmt).strftime(out_fmt)
        except ValueError:
            continue
    try:
        return pd.to_datetime(v, errors="raise").strftime(out_fmt)
    except Exception:
        return ""


def clean_url(v):
    v = str(v or "").strip().replace(" ", "")
    if v and not re.match(r"^[a-z][a-z0-9+.\-]*://", v, re.I):
        v = "https://" + v.lstrip("/")
    try:
        p = urlsplit(v)
    except ValueError:
        return ""
    if p.scheme.lower() not in ("http", "https") or "." not in p.netloc:
        return ""
    query = urlencode([(k, val) for k, val in parse_qsl(p.query, keep_blank_values=True)
                       if not TRACKING.match(k)])
    path = p.path.rstrip("/") if p.path not in ("", "/") else p.path
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), path, query, ""))


def _name(n):
    n = re.sub(r"https?://\S+|\S+@\S+", " ", n)                    # drop urls / emails
    n = re.sub(r"(?i)^\s*(by|written by|reporting by|words by|von|por)\s+", "", n)
    # keep letters AND combining marks (Burmese, Hindi, Tamil... need them), space, ' . -
    n = "".join(c if (unicodedata.category(c)[0] in "LM" or c in " '’.-‌‍") else " " for c in n)
    n = re.sub(r"\s+", " ", n).strip(" .-'’")
    if re.match(r"(?i)^(the )?bbc", n) or n.startswith("බීබීසී"):                       # service names ("BBC News Mundo") stay as written
        return n
    letters = [c for c in n if LATIN.match(c)]
    if letters and (n.islower() or n.isupper()) and len(n) > 3:     # "john SMITH" untouched, "JOHN SMITH" fixed
        n = " ".join(w.capitalize() for w in n.split())
    elif letters:
        n = " ".join(capitalize_start(w) if w.islower() and w not in ("de", "da", "van", "von", "der", "bin", "al", "el", "la", "le", "di", "du") else w
                     for w in n.split())
    return n


def clean_author(v):
    v = fix_spacing(v)
    if re.match(r"(?i)^(the )?bbc", v) or v.startswith("බීබීසී"):      # a service name, not a person: keep whole
        return v
    parts = re.split(r"\s*(?:,|;|&|\+|/|\||\u3001|\uFF0C)\s*|\s+(?:and|e|y|na|et|und)\s+", v, flags=re.I)
    seen, out = set(), []
    for p in parts:
        nm = _name(p)
        if nm and nm.lower() not in seen and nm.lower() not in ("unknown", "staff", "n a", "na"):
            seen.add(nm.lower())
            out.append(nm)
    return ", ".join(out) if out else UNKNOWN_AUTHOR


def clean_source(v):
    v = fix_spacing(v)
    return ACRONYMS.get(v.lower(), v.upper() if len(v) <= 4 else v[:1].upper() + v[1:])


# --------------------------------------------------------------------------- excel
COL_WIDTHS = {"title": 60, "description": 90, "date_of_publication": 16,
              "author_name": 32, "url": 55, "source": 16}


def save_xlsx(df, path):
    """Real Excel file: fixed column widths, dates kept as text (no ##### or day/month flips),
    wrapped text, frozen + filtered header. Nothing is guessed when the file is opened."""
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "articles"
    ws.append(list(df.columns))
    for row in df.itertuples(index=False):
        ws.append([str(v) for v in row])
    for r in ws.iter_rows(min_row=2):
        for c in r:
            c.data_type = "s"                               # text, so "=..." or "-..." is never a formula
            c.number_format = "@"
            c.alignment = Alignment(wrap_text=True, vertical="top")
    for i, name in enumerate(df.columns, 1):
        ws.column_dimensions[get_column_letter(i)].width = COL_WIDTHS.get(name, 20)
        h = ws.cell(row=1, column=i)
        h.font = Font(bold=True, color="FFFFFF")
        h.fill = PatternFill("solid", fgColor="305496")
        h.alignment = Alignment(vertical="center")
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    wb.save(path)


# --------------------------------------------------------------------------- main
def preprocess(df, date_fmt=DATE_OUT):
    out = pd.DataFrame(index=df.index)
    out["title"] = df["title"].map(clean_title)
    out["description"] = df["description"].map(clean_description)
    out["date_of_publication"] = df["date_of_publication"].map(lambda s: clean_date(s, date_fmt))
    out["author_name"] = df["author_name"].map(clean_author)
    out["url"] = df["url"].map(clean_url)
    out["source"] = df["source"].map(clean_source)
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input")
    ap.add_argument("-o", "--output")
    ap.add_argument("--no-xlsx", action="store_true", help="do not also write the .xlsx file")
    ap.add_argument("--date-format", default=DATE_OUT, help='output date format (default "%%d/%%m/%%Y")')
    args = ap.parse_args()

    raw = pd.read_csv(args.input, dtype=str, encoding="utf-8-sig").fillna("")
    n0 = len(raw)
    clean = preprocess(raw, args.date_format)

    bad = (clean["title"].str.strip("# ").str.upper().isin(["NAME?", ""])      # Excel's "#NAME?" damage
           | (clean["url"] == ""))
    dropped_bad = int(bad.sum())
    clean = clean[~bad]
    dup = int(clean.duplicated(subset="url").sum())
    clean = clean.drop_duplicates(subset="url", keep="first")

    out_path = args.output or os.path.splitext(args.input)[0] + "_clean.csv"
    clean.to_csv(out_path, index=False, encoding="utf-8-sig")
    if not args.no_xlsx:
        xlsx_path = os.path.splitext(out_path)[0] + ".xlsx"
        save_xlsx(clean, xlsx_path)
        print(f"  also wrote {xlsx_path}  (open this one in Excel: column sizes and dates stay as set)")

    print(f"read {n0} rows -> wrote {len(clean)} rows to {out_path}")
    print(f"  removed {dropped_bad} unusable rows (broken title like #NAME? or invalid url), {dup} duplicate urls")
    print(f"  rows with no date: {int((clean['date_of_publication'] == '').sum())}, "
          f"author 'Unknown': {int((clean['author_name'] == UNKNOWN_AUTHOR).sum())}")