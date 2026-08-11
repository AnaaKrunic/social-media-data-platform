"""Normalize Bitcoin/X Bronze CSV rows into users/posts DataFrames."""

from __future__ import annotations

import hashlib
from typing import Any

import pandas as pd

from common import make_user_id, parse_datetime_to_iso8601, partition_date_parts, strip_html


def _parse_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _make_post_id(username: str, created_at: str | None, text: str) -> str:
    raw = f"{username}|{created_at or ''}|{text}".encode("utf-8")
    return "x-" + hashlib.sha256(raw).hexdigest()[:24]


def normalize_bitcoin_chunk(chunk: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    user_rows: dict[str, dict[str, Any]] = {}
    post_rows: list[dict[str, Any]] = []

    for _, row in chunk.iterrows():
        username = str(row.get("user_name") or "").strip()
        if not username:
            continue

        tweet_time = parse_datetime_to_iso8601(str(row.get("date") or ""))
        user_created = parse_datetime_to_iso8601(str(row.get("user_created") or ""))
        year, month, day = partition_date_parts(tweet_time)
        platform = "X"
        is_retweet = _parse_bool(row.get("is_retweet"))
        content_text = strip_html(str(row.get("text") or ""))

        user_rows[username] = {
            "user_id": make_user_id(platform, username),
            "username": username,
            "platform": platform,
            "karma_score": None,
            "is_verified": _parse_bool(row.get("user_verified")),
            "followers_count": pd.to_numeric(row.get("user_followers"), errors="coerce"),
            "created_at": user_created,
        }

        post_rows.append(
            {
                "post_id": _make_post_id(username, tweet_time, content_text),
                "author_username": username,
                "content_text": content_text,
                "created_at": tweet_time,
                "post_type": "retweet" if is_retweet else "tweet",
                "score": None,
                "parent_id": None,
                "kids": None,
                "year": year,
                "month": month,
                "day": day,
            }
        )

    users_df = pd.DataFrame(list(user_rows.values()))
    posts_df = pd.DataFrame(post_rows)
    return users_df, posts_df
