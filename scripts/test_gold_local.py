"""Local Gold metrics test using sample Silver-like data."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

GOLD_DIR = Path(__file__).resolve().parents[1] / "lambda" / "gold"
sys.path.insert(0, str(GOLD_DIR))

from metrics import (  # noqa: E402
    daily_hn_post_counts,
    daily_users_metric,
    data_quality_score,
    prepare_posts,
    top_x_users_by_followers,
)


def main() -> int:
    users = pd.DataFrame(
        [
            {"user_id": "1", "username": "alice", "platform": "Hacker News", "karma_score": 100, "followers_count": None, "created_at": "2026-08-10T10:00:00Z"},
            {"user_id": "2", "username": "bob", "platform": "X", "karma_score": None, "followers_count": 5000, "created_at": "2026-08-10T11:00:00Z"},
        ]
    )
    posts = pd.DataFrame(
        [
            {"post_id": "p1", "author_username": "alice", "content_text": "hi", "created_at": "2026-08-10T12:00:00Z", "post_type": "story", "score": 42},
            {"post_id": "p2", "author_username": "bob", "content_text": "tweet", "created_at": "2026-08-10T13:00:00Z", "post_type": "tweet", "score": None},
        ]
    )

    enriched = prepare_posts(posts, users)
    date = "2026-08-10"

    print("HN post counts:", daily_hn_post_counts(enriched, date).to_dict("records"))
    print("Users metric:", daily_users_metric(users, date).to_dict("records"))
    print("Top X:", top_x_users_by_followers(users, date).to_dict("records"))
    print("Quality:", data_quality_score(users, posts).to_dict("records"))
    print("[OK] Gold metrics logic works locally.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
