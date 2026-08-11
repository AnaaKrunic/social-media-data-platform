import sys
from pathlib import Path

import pandas as pd

GOLD_DIR = Path(__file__).resolve().parents[2] / "lambda" / "gold"
sys.path.insert(0, str(GOLD_DIR))

from metrics import daily_users_metric, prepare_posts, top_hn_posts_by_score  # noqa: E402


def test_daily_users_metric():
    users = pd.DataFrame(
        [
            {"user_id": "1", "username": "a", "platform": "Hacker News", "created_at": "2026-08-10T00:00:00Z"},
            {"user_id": "2", "username": "b", "platform": "X", "created_at": "2026-08-09T00:00:00Z"},
        ]
    )
    result = daily_users_metric(users, "2026-08-10")
    assert len(result) == 2
    assert result[result["platform"] == "Hacker News"]["new_users"].iloc[0] == 1


def test_top_hn_posts():
    users = pd.DataFrame([{"user_id": "1", "username": "a", "platform": "Hacker News", "created_at": "2026-08-10T00:00:00Z"}])
    posts = pd.DataFrame(
        [
            {"post_id": "1", "author_username": "a", "content_text": "x", "created_at": "2026-08-10T01:00:00Z", "post_type": "story", "score": 10},
            {"post_id": "2", "author_username": "a", "content_text": "y", "created_at": "2026-08-10T02:00:00Z", "post_type": "story", "score": 50},
        ]
    )
    enriched = prepare_posts(posts, users)
    top = top_hn_posts_by_score(enriched, "2026-08-10")
    assert top.iloc[0]["score"] == 50


def test_prepare_posts_empty_dataframe():
    result = prepare_posts(pd.DataFrame(), pd.DataFrame())
    assert "created_at" in result.columns
    assert len(result) == 0
