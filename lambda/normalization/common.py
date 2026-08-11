"""Shared helpers for Silver-layer normalization."""

from __future__ import annotations

import html
import re
import uuid
from datetime import datetime, timezone

HTML_TAG_PATTERN = re.compile(r"<[^>]+>")


def strip_html(value: str | None) -> str:
    if not value:
        return ""
    without_tags = HTML_TAG_PATTERN.sub("", value)
    return html.unescape(without_tags).strip()


def epoch_to_iso8601(value: int | str | None) -> str | None:
    if value is None or value == "":
        return None
    timestamp = datetime.fromtimestamp(int(value), tz=timezone.utc)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_datetime_to_iso8601(value: str | None) -> str | None:
    if not value:
        return None

    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            parsed = datetime.strptime(normalized, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue

    return None


def make_user_id(platform: str, username: str) -> str:
    """Deterministic UUID so re-runs do not create duplicate users."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{platform}:{username.lower()}"))


def partition_date_parts(iso_timestamp: str | None) -> tuple[str | None, str | None, str | None]:
    if not iso_timestamp:
        return None, None, None
    date_part = iso_timestamp[:10]
    year, month, day = date_part.split("-")
    return year, month, day
