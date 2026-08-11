"""Daily Hacker News ingestion for the Bronze layer.

The objects written to S3 are the unmodified response bytes returned by the
Hacker News Search API.  Parsing is used only to discover whether another page
exists; normalization belongs to the Silver layer and is intentionally absent.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import boto3


HN_SEARCH_URL = "https://hn.algolia.com/api/v1/search_by_date"
HN_CONTENT_TYPES = ("story", "ask_hn", "comment", "job", "poll")
PAGE_SIZE = 1_000
HTTP_TIMEOUT_SECONDS = 20

s3 = boto3.client("s3")


def _previous_utc_day(now: datetime) -> tuple[datetime, datetime]:
    """Return [start, end) boundaries for the previous calendar day in UTC."""
    current_day = now.astimezone(timezone.utc).date()
    end = datetime.combine(current_day, datetime.min.time(), tzinfo=timezone.utc)
    return end - timedelta(days=1), end


def _fetch_page(content_type: str, page: int, start_epoch: int, end_epoch: int) -> bytes:
    """Fetch one raw HN Search response without transforming its content."""
    query = urlencode(
        {
            "tags": content_type,
            "numericFilters": f"created_at_i>={start_epoch},created_at_i<{end_epoch}",
            "hitsPerPage": PAGE_SIZE,
            "page": page,
        }
    )
    request = Request(
        f"{HN_SEARCH_URL}?{query}",
        headers={"User-Agent": "social-media-data-platform/1.0"},
    )
    with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        return response.read()


def handler(event, context):
    bucket_name = os.environ["RAW_BUCKET_NAME"]
    max_pages = int(os.environ.get("HN_MAX_PAGES", "9999"))
    start, end = _previous_utc_day(datetime.now(timezone.utc))
    start_epoch = int(start.timestamp())
    end_epoch = int(end.timestamp())
    objects_written = []

    for content_type in HN_CONTENT_TYPES:
        page = 0

        while True:
            raw_response = _fetch_page(content_type, page, start_epoch, end_epoch)
            response_metadata = json.loads(raw_response)

            key = (
                f"bronze/hackernews/date={start:%Y-%m-%d}/"
                f"type={content_type}/page={page:04d}.json"
            )
            s3.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=raw_response,
                ContentType="application/json",
            )
            objects_written.append(key)

            if page >= response_metadata["nbPages"] - 1:
                break
            if page >= max_pages - 1:
                break
            page += 1

    return {
        "source": "hackernews",
        "date": start.strftime("%Y-%m-%d"),
        "max_pages_per_type": max_pages,
        "objects_written": objects_written,
        "event_id": event.get("id"),
        "request_id": context.aws_request_id,
    }
