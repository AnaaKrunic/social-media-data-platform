import json
import sys
from pathlib import Path

import pandas as pd
import pytest

NORMALIZATION_DIR = Path(__file__).resolve().parents[2] / "lambda" / "normalization"
sys.path.insert(0, str(NORMALIZATION_DIR))

from common import epoch_to_iso8601, make_user_id, strip_html  # noqa: E402
from hackernews import content_type_from_key, normalize_hn_hits  # noqa: E402
from x_bitcoin import normalize_bitcoin_chunk  # noqa: E402


def test_strip_html():
    assert strip_html("<p>Hi</p>") == "Hi"


def test_epoch_to_iso8601():
    assert epoch_to_iso8601(1736978058) == "2025-01-15T21:54:18Z"


def test_make_user_id_is_deterministic():
    first = make_user_id("X", "Alice")
    second = make_user_id("X", "Alice")
    assert first == second


def test_content_type_from_key():
    key = "bronze/hackernews/date=2026-01-15/type=story/page=0000.json"
    assert content_type_from_key(key) == "story"


def test_normalize_hn_hits():
    hits = [
        {
            "objectID": "1",
            "author": "bob",
            "comment_text": "<i>note</i>",
            "created_at_i": 1736978058,
            "points": 10,
            "author_karma": 500,
        }
    ]
    users, posts = normalize_hn_hits(hits, "comment")
    assert users.iloc[0]["username"] == "bob"
    assert posts.iloc[0]["post_type"] == "comment"
    assert posts.iloc[0]["content_text"] == "note"


def test_normalize_bitcoin_chunk():
    chunk = pd.DataFrame(
        [
            {
                "user_name": "trader1",
                "user_followers": 100,
                "user_verified": False,
                "user_created": "2020-01-01 00:00:00",
                "date": "2021-02-10 23:59:04",
                "text": "BTC to the moon #bitcoin",
                "is_retweet": False,
            }
        ]
    )
    users, posts = normalize_bitcoin_chunk(chunk)
    assert users.iloc[0]["platform"] == "X"
    assert posts.iloc[0]["post_type"] == "tweet"
