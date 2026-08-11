"""Test Hacker News API lokalno — simulira sta Lambda radi, bez AWS-a.

Usage:
    python scripts/test_hn_api_local.py
    python scripts/test_hn_api_local.py --date 2026-01-15
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

HN_SEARCH_URL = "https://hn.algolia.com/api/v1/search_by_date"
HN_CONTENT_TYPES = ("story", "ask_hn", "comment", "job", "poll")
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "local_test_output" / "hackernews"


def _day_bounds(date_str: str | None) -> tuple[datetime, datetime]:
    if date_str:
        day = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        day = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _fetch(content_type: str, page: int, start_epoch: int, end_epoch: int) -> dict:
    query = urlencode(
        {
            "tags": content_type,
            "numericFilters": f"created_at_i>={start_epoch},created_at_i<{end_epoch}",
            "hitsPerPage": 10,
            "page": page,
        }
    )
    request = Request(
        f"{HN_SEARCH_URL}?{query}",
        headers={"User-Agent": "social-media-data-platform-local-test/1.0"},
    )
    with urlopen(request, timeout=20) as response:
        return json.loads(response.read())


def main() -> int:
    parser = argparse.ArgumentParser(description="Test HN API fetch locally.")
    parser.add_argument("--date", help="UTC date YYYY-MM-DD (default: yesterday)")
    args = parser.parse_args()

    start, end = _day_bounds(args.date)
    start_epoch = int(start.timestamp())
    end_epoch = int(end.timestamp())

    print("=" * 60)
    print("HACKER NEWS API LOCAL TEST")
    print("=" * 60)
    print(f"Datum: {start.date()} (UTC)")
    print(f"API:   {HN_SEARCH_URL}")
    print()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    total_hits = 0
    sample_keys: list[str] = []

    for content_type in HN_CONTENT_TYPES:
        data = _fetch(content_type, page=0, start_epoch=start_epoch, end_epoch=end_epoch)
        hits = data.get("hits", [])
        nb_pages = data.get("nbPages", 0)
        total_hits += len(hits)

        out_file = OUTPUT_DIR / f"{content_type}_page0.json"
        out_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

        s3_key = (
            f"bronze/hackernews/date={start:%Y-%m-%d}/"
            f"type={content_type}/page=0000.json"
        )
        sample_keys.append(s3_key)

        print(f"[{content_type:10}] {len(hits):3} hitova na str.0 | ukupno stranica: {nb_pages}")
        if hits:
            first = hits[0]
            print(f"             primer: id={first.get('objectID')} author={first.get('author')}")

    print()
    print(f"Ukupno hitova (samo str.0 po tipu): {total_hits}")
    print(f"Lokalni JSON sacuvan u: {OUTPUT_DIR}")
    print()
    print("U S3 bi Lambda upisala fajlove poput:")
    for key in sample_keys:
        print(f"  s3://<bucket>/{key}")

    print()
    print("[OK] API radi. Lambda bi isto ovo radila, samo sa 1000 hitova po stranici")
    print("     i upisivala SVAKU stranicu u S3 (ne samo prvu).")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[FAIL] {exc}", file=sys.stderr)
        raise SystemExit(1)
