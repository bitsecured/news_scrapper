"""EDA (exploratory data analysis) for every news CSV in a folder.

Usage (run inside the folder that holds your csv files):
    python eda_news.py                    all *.csv in the current folder
    python eda_news.py data               all *.csv in the folder "data"
    python eda_news.py bbc.csv npr.csv    only these files
    python eda_news.py --no-report        print only, do not write eda_report.xlsx

Prints stats for each file, then a comparison table of all files, and saves everything
to eda_report.xlsx (one summary sheet + one sheet per file).
"""
import argparse
import glob
import os
import re

import pandas as pd

EXPECTED = ["title", "description", "date_of_publication", "author_name", "url", "source"]
MOJIBAKE = r"[ÂÃâ][\u0080-¿€™“”‘’…]"
LATIN = re.compile(r"[A-Za-z]")


def parse_dates(s):
    """Understands 2026-10-22 (ISO) and 22/10/2026 (day/month/year)."""
    s = s.astype(str).str.strip()
    iso = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
    dmy = pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")
    return iso.fillna(dmy)


def words(s):
    return s.astype(str).str.split().str.len().fillna(0)


def analyse(path):
    name = os.path.basename(path)
    try:
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception as e:
        return None, None, f"could not read {name}: {e}"
    missing_cols = [c for c in EXPECTED if c not in df.columns]
    n = len(df)
    st = {"file": name, "rows": n, "columns": len(df.columns)}
    detail = {}
    if n == 0:
        return st, detail, None

    # --- blanks per column
    blanks = {c: int((df[c].str.strip() == "").sum()) for c in df.columns}
    detail["blank values per column"] = pd.Series(blanks)
    st["blank title"] = blanks.get("title", 0)
    st["blank description"] = blanks.get("description", 0)

    # --- duplicates
    if "url" in df:
        st["duplicate urls"] = int(df["url"].duplicated().sum())
        st["unique urls"] = int(df["url"].nunique())
        st["invalid urls"] = int((~df["url"].str.match(r"^https?://\S+\.\S+$")).sum())
        detail["top websites (url domain)"] = (df["url"].str.extract(r"^https?://([^/]+)")[0]
                                               .value_counts().head(5))
    if "title" in df:
        st["duplicate titles"] = int(df["title"].duplicated().sum())

    # --- dates
    if "date_of_publication" in df:
        d = parse_dates(df["date_of_publication"])
        has = df["date_of_publication"].str.strip() != ""
        now = pd.Timestamp.now()
        st["blank/invalid dates"] = int(d.isna().sum())
        st["future dates"] = int((d > now + pd.Timedelta(days=1)).sum())
        ok = d[d.notna() & (d <= now + pd.Timedelta(days=1))]
        st["oldest date"] = ok.min().strftime("%d/%m/%Y") if len(ok) else "-"
        st["newest date"] = ok.max().strftime("%d/%m/%Y") if len(ok) else "-"
        st["days covered"] = int((ok.max() - ok.min()).days + 1) if len(ok) else 0
        detail["articles per month"] = ok.dt.to_period("M").value_counts().sort_index().tail(12)
        detail["busiest days"] = ok.dt.strftime("%d/%m/%Y").value_counts().head(5)

    # --- authors
    if "author_name" in df:
        a = df["author_name"].str.strip()
        st["author Unknown/blank"] = int(((a == "") | (a.str.lower() == "unknown")).sum())
        named = a[(a != "") & (a.str.lower() != "unknown")]
        st["unique authors"] = int(named.str.split(r",\s*").explode().nunique())
        detail["top authors"] = named.str.split(r",\s*").explode().value_counts().head(8)

    # --- text length
    for col in ("title", "description"):
        if col in df:
            w = words(df[col])
            st[f"{col} avg words"] = round(float(w.mean()), 1)
            st[f"{col} max words"] = int(w.max())
    if "description" in df:
        same = (df["description"].str.strip() == df["title"].str.strip()) & (df["description"].str.strip() != "")
        st["description same as title"] = int(same.sum())

    # --- language (share of titles written mostly in Latin letters) and encoding problems
    if "title" in df:
        latin_share = df["title"].map(lambda t: len(LATIN.findall(t)) / max(1, sum(c.isalpha() for c in t)))
        st["non-Latin titles"] = int((latin_share < 0.5).sum())
    text = df.get("title", pd.Series(dtype=str)) + " " + df.get("description", pd.Series(dtype=str))
    st["broken accents (mojibake)"] = int(text.str.contains(MOJIBAKE, regex=True).sum())
    st["#NAME? damage"] = int(text.str.contains("#NAME?", regex=False).sum())
    st["extra spaces / newlines"] = int(text.str.contains(r"\s{2,}|\n").sum())

    # --- sources
    if "source" in df:
        detail["sources"] = df["source"].value_counts().head(5)
    if missing_cols:
        st["missing columns"] = ", ".join(missing_cols)
    return st, detail, None


def print_file(st, detail):
    print("\n" + "=" * 70)
    print(f"{st['file']}")
    print("=" * 70)
    for k, v in st.items():
        if k != "file":
            print(f"  {k:<28}{v}")
    for title, s in detail.items():
        print(f"\n  {title}:")
        for k, v in s.items():
            print(f"    {str(k)[:60]:<62}{v}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="*", help="csv files or a folder (default: current folder)")
    ap.add_argument("--no-report", action="store_true", help="do not write eda_report.xlsx")
    args = ap.parse_args()

    files = []
    for p in (args.paths or ["."]):
        files += sorted(glob.glob(os.path.join(p, "*.csv"))) if os.path.isdir(p) else [p]
    files = [f for f in files if os.path.exists(f)]
    if not files:
        print("No csv files found. Run this inside the folder with your csv files, or pass a folder/file name.")
        return

    results, summaries = [], []
    for f in files:
        st, detail, err = analyse(f)
        if err:
            print("!!", err)
            continue
        print_file(st, detail)
        results.append((os.path.basename(f), detail))
        summaries.append(st)
    if not summaries:
        return

    summary = pd.DataFrame(summaries).set_index("file").T
    print("\n" + "#" * 70)
    print("COMPARISON OF ALL FILES")
    print("#" * 70)
    with pd.option_context("display.width", 250, "display.max_columns", 50, "display.max_colwidth", 22):
        print(summary.to_string())

    # urls repeated across different files
    urls = {}
    for f in files:
        try:
            u = pd.read_csv(f, dtype=str, usecols=["url"], encoding="utf-8-sig")["url"].dropna()
            urls[os.path.basename(f)] = set(u)
        except Exception:
            pass
    allu = pd.Series([x for s in urls.values() for x in s])
    print(f"\nTOTAL rows in all files: {int(summary.loc['rows'].sum())}"
          f" | urls appearing in more than one file: {int(allu.duplicated().sum())}")

    if not args.no_report:
        with pd.ExcelWriter("eda_report.xlsx", engine="openpyxl") as xw:
            summary.to_excel(xw, sheet_name="summary")
            xw.sheets["summary"].column_dimensions["A"].width = 30
            for col in "BCDEFGHIJKLMNOP":
                xw.sheets["summary"].column_dimensions[col].width = 20
            for name, detail in results:
                row = 0
                sheet = re.sub(r"[\\/*?:\[\]]", "", os.path.splitext(name)[0])[:31]
                for title, s in detail.items():
                    out = s.rename(title).to_frame()
                    out.index = out.index.astype(str)
                    out.to_excel(xw, sheet_name=sheet, startrow=row)
                    row += len(s) + 3
                xw.sheets[sheet].column_dimensions["A"].width = 60
                xw.sheets[sheet].column_dimensions["B"].width = 14
        print("report saved to eda_report.xlsx")


if __name__ == "__main__":
    main()