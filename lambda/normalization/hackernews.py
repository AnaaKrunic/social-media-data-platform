"""Normalize Hacker News Bronze JSON into users/posts DataFrames."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from common import (
    epoch_to_iso8601,
    make_user_id,
    partition_date_parts,
    strip_html,
)

HN_TYPE_MAP = {
    "story": "story",
    "ask_hn": "ask",
    "comment": "comment",
    "job": "job",
    "poll": "poll",
}


def _content_text(hit: dict[str, Any]) -> str:
    if hit.get("comment_text"):
        return strip_html(hit["comment_text"])
    if hit.get("story_text"):
        return strip_html(hit["story_text"])
    if hit.get("title"):
        return strip_html(hit["title"])
    return ""


def _post_type(content_type: str) -> str:
    return HN_TYPE_MAP.get(content_type, "story")


def normalize_hn_hits(hits: list[dict[str, Any]], content_type: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    user_rows: dict[str, dict[str, Any]] = {}
    post_rows: list[dict[str, Any]] = []

    for hit in hits:
        username = (hit.get("author") or "").strip()
        if not username:
            continue

        created_at = epoch_to_iso8601(hit.get("created_at_i"))
        year, month, day = partition_date_parts(created_at)
        platform = "Hacker News"

        user_rows[username] = {
            "user_id": make_user_id(platform, username),
            "username": username,
            "platform": platform,
            "karma_score": hit.get("author_karma"),
            "is_verified": None,
            "followers_count": None,
            "created_at": created_at,
        }

        post_rows.append(
            {
                "post_id": str(hit.get("objectID") or hit.get("story_id") or ""),
                "author_username": username,
                "content_text": _content_text(hit),
                "created_at": created_at,
                "post_type": _post_type(content_type),
                "score": hit.get("points"),
                "parent_id": str(hit.get("parent_id")) if hit.get("parent_id") else None,
                "kids": ",".join(str(kid) for kid in hit.get("kids") or []) or None,
                "year": year,
                "month": month,
                "day": day,
            }
        )

    users_df = pd.DataFrame(list(user_rows.values()))
    posts_df = pd.DataFrame(post_rows)
    return users_df, posts_df


def parse_hn_bronze_object(raw_bytes: bytes, content_type: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    payload = json.loads(raw_bytes)
    hits = payload.get("hits") or []
    return normalize_hn_hits(hits, content_type)


def content_type_from_key(key: str) -> str | None:
    marker = "type="
    if marker not in key:
        return None
    segment = key.split(marker, 1)[1]
    return segment.split("/", 1)[0]
