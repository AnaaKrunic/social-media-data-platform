"""Local Silver normalization test — no AWS account required.

Usage:
    python scripts/test_silver_local.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "lambda" / "normalization"))

from common import epoch_to_iso8601, strip_html  # noqa: E402
from hackernews import normalize_hn_hits  # noqa: E402
from x_bitcoin import normalize_bitcoin_chunk  # noqa: E402


def _test_html_and_time() -> None:
    assert strip_html("<p>Hello <i>world</i></p>") == "Hello world"
    assert epoch_to_iso8601(1736978058) == "2025-01-15T21:54:18Z"
    print("[OK]   HTML cleaning and timestamp conversion")


def _test_hackernews() -> None:
    sample = {
        "hits": [
            {
                "objectID": "123",
                "author": "alice",
                "title": "<p>My story</p>",
                "created_at_i": 1736978058,
                "points": 42,
                "author_karma": 1000,
                "kids": [456, 789],
            }
        ]
    }
    users, posts = normalize_hn_hits(sample["hits"], "story")
    assert len(users) == 1
    assert users.iloc[0]["platform"] == "Hacker News"
    assert users.iloc[0]["karma_score"] == 1000
    assert posts.iloc[0]["content_text"] == "My story"
    assert posts.iloc[0]["post_type"] == "story"
    assert posts.iloc[0]["kids"] == "456,789"
    print("[OK]   Hacker News normalization")


def _test_bitcoin() -> None:
    for name in ("Bitcoin_tweets_aws.csv", "Bitcoin_tweets_dataset_2.csv"):
        csv_path = PROJECT_ROOT / "data" / "bronze" / "x" / "bitcoin" / name
        if csv_path.is_file():
            break
    else:
        print("[SKIP] Bitcoin CSV not found locally")
        return

    chunk = pd.read_csv(csv_path, nrows=5)
    users, posts = normalize_bitcoin_chunk(chunk)
    assert len(users) >= 1
    assert len(posts) == 5
    assert set(posts["post_type"]).issubset({"tweet", "retweet"})
    assert users.iloc[0]["platform"] == "X"
    assert users.iloc[0]["is_verified"] in {True, False, None}
    print(f"[OK]   Bitcoin normalization ({len(posts)} sample tweets)")


def _test_hn_local_json() -> None:
    hn_dir = PROJECT_ROOT / "data" / "local_test_output" / "hackernews"
    sample_file = hn_dir / "story_page0.json"
    if not sample_file.is_file():
        print("[SKIP] Run scripts/test_hn_api_local.py first for HN JSON sample")
        return

    payload = json.loads(sample_file.read_text(encoding="utf-8"))
    users, posts = normalize_hn_hits(payload.get("hits", []), "story")
    print(f"[OK]   HN API sample -> {len(users)} users, {len(posts)} posts")


def main() -> int:
    print("=" * 60)
    print("SILVER NORMALIZATION LOCAL TEST")
    print("=" * 60)

    _test_html_and_time()
    _test_hackernews()
    _test_bitcoin()
    _test_hn_local_json()

    print()
    print("Silver logika radi lokalno. Na AWS-u ista logika ide u Lambda funkciji.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
